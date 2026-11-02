// Copyright Inorderly.com 2026
// SPDX-License-Identifier: Apache-2.0

// Package broker reads events from, and writes ordered events to, Redis
// Streams, NATS JetStream and Kafka, with small clients written against the
// standard library only.
//
// Inorderly is the only reader of an input: it keeps where it got to in its
// own checkpoint, as an opaque position string, and starts from there. A sink
// is written by one run at a time; Last reads back the last line written, so
// a run that was killed can skip what is already there.
package broker

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"strings"
	"time"
)

// Endpoint is a parsed broker URL.
type Endpoint struct {
	Kind  string // "redis", "nats" or "kafka"
	TLS   bool
	Host  string // host:port
	User  string
	Pass  string
	Name  string // stream or topic
	Query url.Values
}

// Defaults for each kind: the port, and the schemes that mean TLS.
var schemes = map[string]struct {
	kind string
	tls  bool
	port string
}{
	"redis":  {"redis", false, "6379"},
	"rediss": {"redis", true, "6379"},
	"nats":   {"nats", false, "4222"},
	"tls":    {"nats", true, "4222"},
	"kafka":  {"kafka", false, "9092"},
	"kafkas": {"kafka", true, "9092"},
}

// IsURL says whether s names a broker rather than a file.
func IsURL(s string) bool {
	i := strings.Index(s, "://")
	if i <= 0 {
		return false
	}
	_, ok := schemes[strings.ToLower(s[:i])]
	return ok
}

// Parse reads a broker URL.
func Parse(s string) (*Endpoint, error) {
	u, err := url.Parse(s)
	if err != nil {
		return nil, fmt.Errorf("%s: %v", redact(s), err)
	}
	sc, ok := schemes[strings.ToLower(u.Scheme)]
	if !ok {
		return nil, fmt.Errorf("%s: unknown scheme %q", redact(s), u.Scheme)
	}
	e := &Endpoint{Kind: sc.kind, TLS: sc.tls, Query: u.Query()}
	if u.Host == "" {
		return nil, fmt.Errorf("%s: no host", redact(s))
	}
	e.Host = u.Host
	if u.Port() == "" {
		e.Host = net.JoinHostPort(u.Hostname(), sc.port)
	}
	if u.User != nil {
		e.User = u.User.Username()
		e.Pass, _ = u.User.Password()
	}
	if e.User != "" && e.Pass == "" && e.Kind != "nats" {
		return nil, fmt.Errorf("%s: a user needs a password, as in %s://user:password@%s", redact(s), u.Scheme, u.Host)
	}
	e.Name = strings.TrimPrefix(u.Path, "/")
	if e.Name == "" || strings.Contains(e.Name, "/") {
		return nil, fmt.Errorf("%s: name one %s after the host, as in %s://%s/events", redact(s), map[string]string{"redis": "stream", "nats": "stream", "kafka": "topic"}[e.Kind], u.Scheme, u.Host)
	}
	for k := range e.Query {
		if !allowed[e.Kind][k] {
			return nil, fmt.Errorf("%s: unknown option %q", redact(s), k)
		}
	}
	return e, nil
}

var allowed = map[string]map[string]bool{
	"redis": {"field": true, "db": true, "insecure": true},
	"nats":  {"subject": true, "insecure": true},
	"kafka": {"sasl": true, "insecure": true},
}

// String is the URL without its password, for checkpoints and messages.
func (e *Endpoint) String() string {
	scheme := e.Kind
	if e.TLS {
		scheme = map[string]string{"redis": "rediss", "nats": "tls", "kafka": "kafkas"}[e.Kind]
	}
	u := url.URL{Scheme: scheme, Host: e.Host, Path: "/" + e.Name, RawQuery: e.Query.Encode()}
	if e.User != "" && !(e.Kind == "nats" && e.Pass == "") { // a NATS user alone is a token
		u.User = url.User(e.User)
	}
	return u.String()
}

func redact(s string) string {
	if u, err := url.Parse(s); err == nil && u.User != nil {
		if _, has := u.User.Password(); !has && schemes[strings.ToLower(u.Scheme)].kind == "nats" {
			u.User = nil
		} else {
			u.User = url.User(u.User.Username())
		}
		return u.String()
	}
	return s
}

// Message is one input line from a broker.
type Message struct {
	Value   []byte
	At      string // where it came from, for the rejects file
	Problem string // set when the message holds no line at all
}

// ErrIdle is what a following source returns when nothing came in time.
var ErrIdle = errors.New("no new messages")

// A Source reads one input in order.
type Source interface {
	// Next returns the next message. A bounded read returns io.EOF at its end;
	// a following one waits up to wait and then returns ErrIdle.
	Next(ctx context.Context, wait time.Duration) (Message, error)
	// Position is where the next read starts, for the checkpoint.
	Position() string
	Close() error
}

// A Sink takes the ordered output, one message per line.
type Sink interface {
	// Last returns the pos and line of the last message written, or 0.
	Last(ctx context.Context) (uint64, []byte, error)
	// Write queues a line.
	Write(ctx context.Context, pos uint64, line []byte) error
	// Flush returns once every queued line is stored.
	Flush(ctx context.Context) error
	// Mark is the output's own position now: what Each reads on from.
	Mark(ctx context.Context) (string, error)
	// Each calls f with every line stored after the mark, in order.
	Each(ctx context.Context, after string, f func(line []byte) error) error
	Close() error
}

// each reads a stream or topic from after a position to its end now.
func each(ctx context.Context, e *Endpoint, after string, f func(Message) error) error {
	until, err := End(ctx, e)
	if err != nil {
		return err
	}
	src, err := OpenSource(ctx, e, after, until)
	if err != nil {
		return err
	}
	defer src.Close()
	for {
		m, err := src.Next(ctx, 0)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := f(m); err != nil {
			return err
		}
	}
}

// End returns the position just after the last message an input holds now:
// a bounded read stops there.
func End(ctx context.Context, e *Endpoint) (string, error) {
	switch e.Kind {
	case "redis":
		return redisEnd(ctx, e)
	case "nats":
		return natsEnd(ctx, e)
	default:
		return kafkaEnd(ctx, e)
	}
}

// OpenSource starts reading after position from ("" is the beginning). With
// until set, the read ends there; without it, it follows the input.
func OpenSource(ctx context.Context, e *Endpoint, from, until string) (Source, error) {
	switch e.Kind {
	case "redis":
		return openRedisSource(ctx, e, from, until)
	case "nats":
		return openNATSSource(ctx, e, from, until)
	default:
		return openKafkaSource(ctx, e, from, until)
	}
}

// OpenSink connects to an output.
func OpenSink(ctx context.Context, e *Endpoint) (Sink, error) {
	switch e.Kind {
	case "redis":
		return openRedisSink(ctx, e)
	case "nats":
		return openNATSSink(ctx, e)
	default:
		return openKafkaSink(ctx, e)
	}
}

// dial opens a connection, with TLS when the URL asks for it.
func dial(ctx context.Context, e *Endpoint, host string) (net.Conn, error) {
	d := net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	c, err := d.DialContext(ctx, "tcp", host)
	if err != nil {
		return nil, err
	}
	if !e.TLS {
		return c, nil
	}
	return startTLS(ctx, e, c, host)
}

func startTLS(ctx context.Context, e *Endpoint, c net.Conn, host string) (net.Conn, error) {
	name, _, _ := net.SplitHostPort(host)
	cfg := &tls.Config{ServerName: name, MinVersion: tls.VersionTLS12, InsecureSkipVerify: e.Query.Get("insecure") == "true"}
	tc := tls.Client(c, cfg)
	if err := tc.HandshakeContext(ctx); err != nil {
		c.Close()
		return nil, err
	}
	return tc, nil
}

// PosOf reads the pos an output line starts with: {"pos":N,...}.
func PosOf(line []byte) (uint64, error) {
	const head = `{"pos":`
	s := string(line)
	if !strings.HasPrefix(s, head) {
		return 0, errors.New("it isn't a line Inorderly wrote")
	}
	var n uint64
	j := len(head)
	for ; j < len(s) && s[j] >= '0' && s[j] <= '9' && j < len(head)+20; j++ {
		n = n*10 + uint64(s[j]-'0')
	}
	if j == len(head) || n == 0 || j >= len(s) || (s[j] != ',' && s[j] != '}') {
		return 0, errors.New("it isn't a line Inorderly wrote")
	}
	return n, nil
}

// Produce appends plain messages to a stream or topic, the way a producer
// would. Tests and examples use it to fill inputs.
func Produce(ctx context.Context, e *Endpoint, values [][]byte) error {
	switch e.Kind {
	case "redis":
		return redisProduce(ctx, e, values)
	case "nats":
		return natsProduce(ctx, e, values)
	default:
		return kafkaProduce(ctx, e, values)
	}
}

// ReadAll returns every message value a stream or topic holds, in order
// (for Kafka, partition 0 then the others).
func ReadAll(ctx context.Context, e *Endpoint) ([][]byte, error) {
	var out [][]byte
	err := each(ctx, e, "", func(m Message) error {
		out = append(out, m.Value)
		return nil
	})
	return out, err
}

// Ensure creates a NATS stream (with one subject of the same name) or a
// Kafka topic (one partition) if it is missing; Redis streams need nothing.
// Inorderly itself never creates them: tests and examples do.
func Ensure(ctx context.Context, e *Endpoint) error {
	switch e.Kind {
	case "nats":
		return natsEnsure(ctx, e)
	case "kafka":
		return kafkaEnsure(ctx, e)
	}
	return nil
}

// Purge empties a Redis stream or a NATS stream; tests use it. Kafka topics
// are left alone (errors.ErrUnsupported).
func Purge(ctx context.Context, e *Endpoint) error {
	switch e.Kind {
	case "redis":
		return redisPurge(ctx, e)
	case "nats":
		return natsPurge(ctx, e)
	}
	return errors.ErrUnsupported
}
