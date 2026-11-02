// Copyright Inorderly.com 2026
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

// A Redis Streams input is read with XRANGE (a bounded read) or XREAD BLOCK
// (following), from after the last entry ID taken. An output gets one entry
// per line, with the ID <pos>-1, so Redis itself refuses a line written twice.
// Each entry keeps the line in one field, "event" unless ?field= says
// otherwise. Exclusive ranges need Redis 6.2 or newer, or Valkey.

type redisConn struct {
	c       net.Conn
	r       *bufio.Reader
	w       *bufio.Writer
	pending int
}

type redisError string

func (e redisError) Error() string { return string(e) }

func dialRedis(ctx context.Context, e *Endpoint) (*redisConn, error) {
	c, err := dial(ctx, e, e.Host)
	if err != nil {
		return nil, fmt.Errorf("redis %s: %v", e.Host, err)
	}
	rc := &redisConn{c: c, r: bufio.NewReaderSize(c, 1<<16), w: bufio.NewWriterSize(c, 1<<16)}
	if e.Pass != "" || e.User != "" {
		args := []string{"AUTH", e.Pass}
		if e.User != "" {
			args = []string{"AUTH", e.User, e.Pass}
		}
		if _, err := rc.do(30*time.Second, args...); err != nil {
			rc.Close()
			return nil, fmt.Errorf("redis %s: %v", e.Host, err)
		}
	}
	if db := e.Query.Get("db"); db != "" {
		if _, err := strconv.Atoi(db); err != nil {
			rc.Close()
			return nil, fmt.Errorf("redis %s: db must be a number", e.Host)
		}
		if _, err := rc.do(30*time.Second, "SELECT", db); err != nil {
			rc.Close()
			return nil, fmt.Errorf("redis %s: %v", e.Host, err)
		}
	}
	return rc, nil
}

func (rc *redisConn) Close() error { return rc.c.Close() }

func (rc *redisConn) send(args ...string) error {
	fmt.Fprintf(rc.w, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(rc.w, "$%d\r\n", len(a))
		rc.w.WriteString(a)
		rc.w.WriteString("\r\n")
	}
	rc.pending++
	return nil
}

// do sends one command and reads its reply.
func (rc *redisConn) do(timeout time.Duration, args ...string) (interface{}, error) {
	rc.send(args...)
	if err := rc.w.Flush(); err != nil {
		return nil, err
	}
	rc.c.SetDeadline(time.Now().Add(timeout))
	v, err := rc.read()
	rc.pending--
	if err != nil {
		return nil, err
	}
	if re, ok := v.(redisError); ok {
		return nil, re
	}
	return v, nil
}

// read reads one RESP2 reply: a string, an int64, an []interface{}, a
// redisError or nil.
func (rc *redisConn) read() (interface{}, error) {
	line, err := rc.r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if len(line) < 3 || !strings.HasSuffix(line, "\r\n") {
		return nil, errors.New("redis: a reply that isn't RESP")
	}
	body := line[1 : len(line)-2]
	switch line[0] {
	case '+':
		return body, nil
	case '-':
		return redisError(body), nil
	case ':':
		return strconv.ParseInt(body, 10, 64)
	case '$':
		n, err := strconv.Atoi(body)
		if err != nil || n < -1 || n > 512<<20 {
			return nil, errors.New("redis: a bad bulk length")
		}
		if n == -1 {
			return nil, nil
		}
		b := make([]byte, n+2)
		if _, err := io.ReadFull(rc.r, b); err != nil {
			return nil, err
		}
		return string(b[:n]), nil
	case '*':
		n, err := strconv.Atoi(body)
		if err != nil || n < -1 || n > 1<<24 {
			return nil, errors.New("redis: a bad array length")
		}
		if n == -1 {
			return nil, nil
		}
		a := make([]interface{}, n)
		for i := range a {
			if a[i], err = rc.read(); err != nil {
				return nil, err
			}
		}
		return a, nil
	}
	return nil, fmt.Errorf("redis: unexpected reply %q", line[0])
}

type redisEntry struct {
	id     string
	fields map[string]string
}

func redisEntries(v interface{}) ([]redisEntry, error) {
	a, ok := v.([]interface{})
	if v == nil {
		return nil, nil
	}
	if !ok {
		return nil, errors.New("redis: a stream reply that isn't a list")
	}
	out := make([]redisEntry, 0, len(a))
	for _, x := range a {
		pair, ok := x.([]interface{})
		if !ok || len(pair) != 2 {
			return nil, errors.New("redis: a stream entry that isn't an ID and fields")
		}
		id, ok := pair[0].(string)
		if !ok {
			return nil, errors.New("redis: a stream entry without an ID")
		}
		e := redisEntry{id: id, fields: map[string]string{}}
		fv, _ := pair[1].([]interface{})
		for i := 0; i+1 < len(fv); i += 2 {
			k, _ := fv[i].(string)
			val, _ := fv[i+1].(string)
			if _, dup := e.fields[k]; !dup {
				e.fields[k] = val
			}
		}
		out = append(out, e)
	}
	return out, nil
}

func redisField(e *Endpoint) string {
	if f := e.Query.Get("field"); f != "" {
		return f
	}
	return "event"
}

// lastEntry returns the stream's last entry, or nil when it has none.
func (rc *redisConn) lastEntry(stream string) (*redisEntry, error) {
	v, err := rc.do(30*time.Second, "XREVRANGE", stream, "+", "-", "COUNT", "1")
	if err != nil {
		return nil, err
	}
	es, err := redisEntries(v)
	if err != nil || len(es) == 0 {
		return nil, err
	}
	return &es[0], nil
}

func redisEnd(ctx context.Context, e *Endpoint) (string, error) {
	rc, err := dialRedis(ctx, e)
	if err != nil {
		return "", err
	}
	defer rc.Close()
	last, err := rc.lastEntry(e.Name)
	if err != nil {
		return "", fmt.Errorf("redis %s: %v", e.Name, err)
	}
	if last == nil {
		return "0-0", nil
	}
	return last.id, nil
}

type redisSource struct {
	e     *Endpoint
	rc    *redisConn
	field string
	last  string // the ID of the last entry taken
	until string // "" when following
	buf   []redisEntry
	done  bool
}

func openRedisSource(ctx context.Context, e *Endpoint, from, until string) (Source, error) {
	if from == "" {
		from = "0-0"
	}
	if !validRedisID(from) || (until != "" && !validRedisID(until)) {
		return nil, fmt.Errorf("redis %s: a damaged position %q", e.Name, from)
	}
	rc, err := dialRedis(ctx, e)
	if err != nil {
		return nil, err
	}
	return &redisSource{e: e, rc: rc, field: redisField(e), last: from, until: until}, nil
}

func validRedisID(id string) bool {
	ms, seq, ok := strings.Cut(id, "-")
	if !ok {
		return false
	}
	_, err1 := strconv.ParseUint(ms, 10, 64)
	_, err2 := strconv.ParseUint(seq, 10, 64)
	return err1 == nil && err2 == nil
}

// redisIDLess compares two stream IDs.
func redisIDLess(a, b string) bool {
	am, as, _ := strings.Cut(a, "-")
	bm, bs, _ := strings.Cut(b, "-")
	x, _ := strconv.ParseUint(am, 10, 64)
	y, _ := strconv.ParseUint(bm, 10, 64)
	if x != y {
		return x < y
	}
	x, _ = strconv.ParseUint(as, 10, 64)
	y, _ = strconv.ParseUint(bs, 10, 64)
	return x < y
}

func (s *redisSource) Next(ctx context.Context, wait time.Duration) (Message, error) {
	for len(s.buf) == 0 {
		if err := ctx.Err(); err != nil {
			return Message{}, err
		}
		if s.done {
			return Message{}, io.EOF
		}
		if s.until != "" {
			if !redisIDLess(s.last, s.until) {
				s.done = true
				return Message{}, io.EOF
			}
			v, err := s.rc.do(60*time.Second, "XRANGE", s.e.Name, "("+s.last, s.until, "COUNT", "512")
			if err != nil {
				return Message{}, fmt.Errorf("redis %s: %v", s.e.Name, err)
			}
			if s.buf, err = redisEntries(v); err != nil {
				return Message{}, err
			}
			if len(s.buf) == 0 {
				// Trimmed away since the run started: nothing more to read.
				s.done = true
			}
			continue
		}
		ms := wait.Milliseconds()
		if ms < 1 {
			ms = 1
		}
		v, err := s.rc.do(wait+30*time.Second, "XREAD", "COUNT", "512", "BLOCK", strconv.FormatInt(ms, 10), "STREAMS", s.e.Name, s.last)
		if err != nil {
			return Message{}, fmt.Errorf("redis %s: %v", s.e.Name, err)
		}
		if v == nil {
			return Message{}, ErrIdle
		}
		streams, ok := v.([]interface{})
		if !ok || len(streams) != 1 {
			return Message{}, errors.New("redis: an XREAD reply that isn't one stream")
		}
		pair, ok := streams[0].([]interface{})
		if !ok || len(pair) != 2 {
			return Message{}, errors.New("redis: an XREAD reply that isn't one stream")
		}
		if s.buf, err = redisEntries(pair[1]); err != nil {
			return Message{}, err
		}
		if len(s.buf) == 0 {
			return Message{}, ErrIdle
		}
	}
	en := s.buf[0]
	s.buf = s.buf[1:]
	s.last = en.id
	m := Message{At: "entry " + en.id}
	if v, ok := en.fields[s.field]; ok {
		m.Value = []byte(v)
	} else {
		m.Problem = fmt.Sprintf("the entry has no %q field", s.field)
	}
	return m, nil
}

func (s *redisSource) Position() string { return s.last }

func (s *redisSource) Close() error { return s.rc.Close() }

type redisSink struct {
	e     *Endpoint
	rc    *redisConn
	field string
}

func openRedisSink(ctx context.Context, e *Endpoint) (Sink, error) {
	rc, err := dialRedis(ctx, e)
	if err != nil {
		return nil, err
	}
	return &redisSink{e: e, rc: rc, field: redisField(e)}, nil
}

func (s *redisSink) Last(ctx context.Context) (uint64, []byte, error) {
	last, err := s.rc.lastEntry(s.e.Name)
	if err != nil {
		return 0, nil, fmt.Errorf("redis %s: %v", s.e.Name, err)
	}
	if last == nil {
		return 0, nil, nil
	}
	line, ok := last.fields[s.field]
	pos, perr := PosOf([]byte(line))
	if !ok || perr != nil || last.id != strconv.FormatUint(pos, 10)+"-1" {
		return 0, nil, fmt.Errorf("redis %s: the last entry, %s, wasn't written by Inorderly; write to an empty stream", s.e.Name, last.id)
	}
	return pos, []byte(line), nil
}

func (s *redisSink) Write(ctx context.Context, pos uint64, line []byte) error {
	s.rc.send("XADD", s.e.Name, strconv.FormatUint(pos, 10)+"-1", s.field, string(line))
	if s.rc.pending >= 1000 {
		return s.Flush(ctx)
	}
	return nil
}

func (s *redisSink) Flush(ctx context.Context) error {
	if s.rc.pending == 0 {
		return nil
	}
	if err := s.rc.w.Flush(); err != nil {
		return fmt.Errorf("redis %s: %v", s.e.Name, err)
	}
	s.rc.c.SetDeadline(time.Now().Add(60 * time.Second))
	var first error
	for s.rc.pending > 0 {
		v, err := s.rc.read()
		s.rc.pending--
		if err != nil {
			return fmt.Errorf("redis %s: %v", s.e.Name, err)
		}
		if re, ok := v.(redisError); ok && first == nil {
			first = fmt.Errorf("redis %s: XADD: %v", s.e.Name, re)
		}
	}
	return first
}

func (s *redisSink) Close() error { return s.rc.Close() }

func redisProduce(ctx context.Context, e *Endpoint, values [][]byte) error {
	rc, err := dialRedis(ctx, e)
	if err != nil {
		return err
	}
	defer rc.Close()
	s := &redisSink{e: e, rc: rc, field: redisField(e)}
	for _, v := range values {
		rc.send("XADD", e.Name, "*", s.field, string(v))
		if rc.pending >= 1000 {
			if err := s.Flush(ctx); err != nil {
				return err
			}
		}
	}
	return s.Flush(ctx)
}
