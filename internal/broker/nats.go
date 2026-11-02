// Copyright Inorderly.com 2026
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

// A NATS input is a JetStream stream, read through an ephemeral pull consumer
// from after the last stream sequence taken. Each batch is acked at its last
// message (AckAll), which keeps the consumer's acks pending below its limit;
// the stream sequence and the count still pending come from each message's
// ack subject. ?subject= reads
// only that subject. An output is published to the stream's subject (or
// ?subject=) with a JetStream ack for every message and a Nats-Msg-Id, and
// read back with the stream's last message on that subject.

type natsMsg struct {
	subject string
	reply   string
	status  string // "404", "408" and so on, from a header-only status message
	header  []byte
	data    []byte
}

type natsConn struct {
	e      *Endpoint
	c      net.Conn
	r      *bufio.Reader
	w      *bufio.Writer
	inbox  string // our reply subjects are inbox.N
	n      int
	maxPay int
}

type natsInfo struct {
	TLSRequired  bool `json:"tls_required"`
	TLSAvailable bool `json:"tls_available"`
	Headers      bool `json:"headers"`
	MaxPayload   int  `json:"max_payload"`
	JetStream    bool `json:"jetstream"`
}

func dialNATS(ctx context.Context, e *Endpoint) (*natsConn, error) {
	d := net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	c, err := d.DialContext(ctx, "tcp", e.Host)
	if err != nil {
		return nil, fmt.Errorf("nats %s: %v", e.Host, err)
	}
	nc := &natsConn{e: e, c: c, r: bufio.NewReaderSize(c, 1<<16)}
	c.SetDeadline(time.Now().Add(30 * time.Second))
	line, err := nc.r.ReadString('\n')
	if err != nil || !strings.HasPrefix(line, "INFO ") {
		c.Close()
		return nil, fmt.Errorf("nats %s: no INFO from the server", e.Host)
	}
	var info natsInfo
	if err := json.Unmarshal([]byte(strings.TrimSpace(line[5:])), &info); err != nil {
		c.Close()
		return nil, fmt.Errorf("nats %s: a bad INFO: %v", e.Host, err)
	}
	if info.TLSRequired && !e.TLS {
		c.Close()
		return nil, fmt.Errorf("nats %s: the server needs TLS; use tls://", e.Host)
	}
	if e.TLS {
		tc, err := startTLS(ctx, e, c, e.Host)
		if err != nil {
			return nil, fmt.Errorf("nats %s: %v", e.Host, err)
		}
		nc.c, nc.r = tc, bufio.NewReaderSize(tc, 1<<16)
	}
	nc.w = bufio.NewWriterSize(nc.c, 1<<16)
	if !info.Headers {
		nc.Close()
		return nil, fmt.Errorf("nats %s: the server doesn't support headers; it is too old", e.Host)
	}
	nc.maxPay = info.MaxPayload
	conn := map[string]interface{}{"verbose": false, "pedantic": false, "tls_required": e.TLS, "name": "inorderly",
		"lang": "go", "version": "inorderly", "protocol": 1, "headers": true, "no_responders": true}
	switch {
	case e.User != "" && e.Pass != "":
		conn["user"], conn["pass"] = e.User, e.Pass
	case e.User != "":
		conn["auth_token"] = e.User
	}
	b, _ := json.Marshal(conn)
	var rb [8]byte
	rand.Read(rb[:])
	nc.inbox = "_INBOX." + hex.EncodeToString(rb[:])
	fmt.Fprintf(nc.w, "CONNECT %s\r\nSUB %s.* 1\r\nPING\r\n", b, nc.inbox)
	if err := nc.w.Flush(); err != nil {
		nc.Close()
		return nil, fmt.Errorf("nats %s: %v", e.Host, err)
	}
	for {
		op, _, err := nc.read()
		if err != nil {
			nc.Close()
			return nil, fmt.Errorf("nats %s: %v", e.Host, err)
		}
		if op == "PONG" {
			break
		}
	}
	return nc, nil
}

func (nc *natsConn) Close() error { return nc.c.Close() }

func (nc *natsConn) nextReply() string {
	nc.n++
	return nc.inbox + "." + strconv.Itoa(nc.n)
}

// read reads one protocol operation, answering PINGs as it goes.
func (nc *natsConn) read() (string, *natsMsg, error) {
	for {
		line, err := nc.r.ReadString('\n')
		if err != nil {
			return "", nil, err
		}
		line = strings.TrimRight(line, "\r\n")
		op, rest, _ := strings.Cut(line, " ")
		switch strings.ToUpper(op) {
		case "PING":
			nc.w.WriteString("PONG\r\n")
			if err := nc.w.Flush(); err != nil {
				return "", nil, err
			}
		case "PONG", "+OK", "INFO":
			if strings.ToUpper(op) != "PONG" {
				continue
			}
			return "PONG", nil, nil
		case "-ERR":
			return "", nil, fmt.Errorf("server error %s", rest)
		case "MSG", "HMSG":
			f := strings.Fields(rest)
			hdr := 0
			m := &natsMsg{}
			var total int
			var perr error
			if strings.ToUpper(op) == "MSG" {
				if len(f) != 3 && len(f) != 4 {
					return "", nil, errors.New("a bad MSG line")
				}
				m.subject = f[0]
				if len(f) == 4 {
					m.reply = f[2]
				}
				total, perr = strconv.Atoi(f[len(f)-1])
			} else {
				if len(f) != 4 && len(f) != 5 {
					return "", nil, errors.New("a bad HMSG line")
				}
				m.subject = f[0]
				if len(f) == 5 {
					m.reply = f[2]
				}
				hdr, perr = strconv.Atoi(f[len(f)-2])
				if perr == nil {
					total, perr = strconv.Atoi(f[len(f)-1])
				}
			}
			if perr != nil || total < 0 || hdr < 0 || hdr > total || total > 64<<20 {
				return "", nil, errors.New("a bad message length")
			}
			buf := make([]byte, total+2)
			if _, err := io.ReadFull(nc.r, buf); err != nil {
				return "", nil, err
			}
			m.header, m.data = buf[:hdr], buf[hdr:total]
			if hdr > 0 {
				first, _, _ := bytes.Cut(m.header, []byte("\r\n"))
				if fs := strings.Fields(string(first)); len(fs) >= 2 {
					m.status = fs[1]
				}
			}
			return "MSG", m, nil
		default:
			return "", nil, fmt.Errorf("an unknown operation %q", op)
		}
	}
}

// request publishes payload and waits for the one reply.
func (nc *natsConn) request(subject string, payload []byte, timeout time.Duration) ([]byte, error) {
	reply := nc.nextReply()
	fmt.Fprintf(nc.w, "PUB %s %s %d\r\n", subject, reply, len(payload))
	nc.w.Write(payload)
	nc.w.WriteString("\r\n")
	if err := nc.w.Flush(); err != nil {
		return nil, err
	}
	nc.c.SetDeadline(time.Now().Add(timeout))
	for {
		op, m, err := nc.read()
		if err != nil {
			return nil, err
		}
		if op != "MSG" || m.subject != reply {
			continue
		}
		if m.status == "503" {
			return nil, errors.New("no responders: is JetStream on, and does the stream exist?")
		}
		return m.data, nil
	}
}

type jsError struct {
	Code        int    `json:"code"`
	ErrCode     int    `json:"err_code"`
	Description string `json:"description"`
}

// api makes a JetStream API request and decodes the reply into v.
func (nc *natsConn) api(subject string, req interface{}, v interface{}) error {
	var payload []byte
	if req != nil {
		payload, _ = json.Marshal(req)
	}
	b, err := nc.request("$JS.API."+subject, payload, 30*time.Second)
	if err != nil {
		return err
	}
	var e struct {
		Error *jsError `json:"error"`
	}
	if err := json.Unmarshal(b, &e); err != nil {
		return fmt.Errorf("a JetStream reply that isn't JSON: %v", err)
	}
	if e.Error != nil {
		return &jsAPIError{*e.Error}
	}
	return json.Unmarshal(b, v)
}

type jsAPIError struct{ e jsError }

func (e *jsAPIError) Error() string {
	return fmt.Sprintf("%s (%d)", e.e.Description, e.e.ErrCode)
}

type streamInfo struct {
	Config struct {
		Subjects []string `json:"subjects"`
	} `json:"config"`
	State struct {
		LastSeq uint64 `json:"last_seq"`
	} `json:"state"`
}

func (nc *natsConn) streamInfo(stream string) (*streamInfo, error) {
	var si streamInfo
	if err := nc.api("STREAM.INFO."+stream, nil, &si); err != nil {
		return nil, fmt.Errorf("nats stream %s: %v", stream, err)
	}
	return &si, nil
}

func natsEnd(ctx context.Context, e *Endpoint) (string, error) {
	nc, err := dialNATS(ctx, e)
	if err != nil {
		return "", err
	}
	defer nc.Close()
	si, err := nc.streamInfo(e.Name)
	if err != nil {
		return "", err
	}
	return strconv.FormatUint(si.State.LastSeq, 10), nil
}

type natsSource struct {
	e        *Endpoint
	nc       *natsConn
	consumer string
	last     uint64
	until    uint64
	bounded  bool
	drained  bool // a bounded read saw a message with nothing pending after it
	buf      []Message
	seqs     []uint64
	done     bool
}

func parseSeq(s string) (uint64, bool) {
	if s == "" {
		return 0, true
	}
	n, err := strconv.ParseUint(s, 10, 64)
	return n, err == nil
}

func openNATSSource(ctx context.Context, e *Endpoint, from, until string) (Source, error) {
	last, ok1 := parseSeq(from)
	end, ok2 := parseSeq(until)
	if !ok1 || !ok2 {
		return nil, fmt.Errorf("nats %s: a damaged position %q", e.Name, from)
	}
	nc, err := dialNATS(ctx, e)
	if err != nil {
		return nil, err
	}
	s := &natsSource{e: e, nc: nc, last: last, until: end, bounded: until != ""}
	if s.bounded && last >= end {
		s.done = true
		return s, nil
	}
	cfg := map[string]interface{}{"ack_policy": "all", "ack_wait": int64(time.Hour), "max_ack_pending": 10000,
		"mem_storage": true, "num_replicas": 1, "inactive_threshold": int64(5 * time.Minute), "replay_policy": "instant"}
	if last == 0 {
		cfg["deliver_policy"] = "all"
	} else {
		cfg["deliver_policy"] = "by_start_sequence"
		cfg["opt_start_seq"] = last + 1
	}
	if sub := e.Query.Get("subject"); sub != "" {
		cfg["filter_subject"] = sub
	}
	var ci struct {
		Name string `json:"name"`
	}
	if err := nc.api("CONSUMER.CREATE."+e.Name, map[string]interface{}{"stream_name": e.Name, "config": cfg}, &ci); err != nil {
		nc.Close()
		return nil, fmt.Errorf("nats stream %s: %v", e.Name, err)
	}
	s.consumer = ci.Name
	return s, nil
}

// pull asks for up to 256 messages and reads them, ending at a status
// message or when the batch is full. It returns whether the server had none.
func (s *natsSource) pull(wait time.Duration) (bool, error) {
	const batch = 256
	if s.bounded {
		wait = time.Second
	} else if wait < 10*time.Millisecond {
		wait = 10 * time.Millisecond
	}
	req := map[string]interface{}{"batch": batch, "expires": int64(wait)}
	b, _ := json.Marshal(req)
	nc := s.nc
	reply := nc.nextReply()
	fmt.Fprintf(nc.w, "PUB $JS.API.CONSUMER.MSG.NEXT.%s.%s %s %d\r\n", s.e.Name, s.consumer, reply, len(b))
	nc.w.Write(b)
	nc.w.WriteString("\r\n")
	if err := nc.w.Flush(); err != nil {
		return false, err
	}
	nc.c.SetDeadline(time.Now().Add(wait + 30*time.Second))
	got := 0
	lastReply := ""
	defer func() {
		// Ack everything up to the last message taken.
		if lastReply != "" {
			fmt.Fprintf(nc.w, "PUB %s 0\r\n\r\n", lastReply)
			nc.w.Flush()
		}
	}()
	for got < batch {
		op, m, err := nc.read()
		if err != nil {
			return false, err
		}
		if op != "MSG" {
			continue
		}
		// Deliveries keep the subject they were published to and come on our
		// inbox subscription with an ack subject naming this consumer; status
		// messages come on the pull's own reply subject.
		if m.status == "" && !ackFor(m.reply, s.e.Name, s.consumer) {
			continue
		}
		if m.status != "" {
			if m.subject != reply {
				continue
			}
			switch m.status {
			case "100": // a heartbeat
				continue
			case "404", "408", "409":
				return got == 0, nil
			}
			return false, fmt.Errorf("pull status %s", m.status)
		}
		seq, pending, ok := ackSeq(m.reply)
		if !ok {
			return false, fmt.Errorf("a message without a JetStream ack subject (%q)", m.reply)
		}
		got++
		lastReply = m.reply
		prev := s.last
		if len(s.seqs) > 0 {
			prev = s.seqs[len(s.seqs)-1]
		}
		if seq <= prev {
			return false, fmt.Errorf("stream sequence %d came after %d", seq, prev)
		}
		s.buf = append(s.buf, Message{Value: m.data, At: "seq " + strconv.FormatUint(seq, 10)})
		s.seqs = append(s.seqs, seq)
		if pending == 0 && s.bounded {
			s.drained = true // nothing more is waiting: the rest of the batch won't come
			return false, nil
		}
	}
	return false, nil
}

// ackFor says whether an ack subject belongs to this stream's consumer.
func ackFor(reply, stream, consumer string) bool {
	t := strings.Split(reply, ".")
	switch {
	case len(t) < 9 || t[0] != "$JS" || t[1] != "ACK":
		return false
	case len(t) >= 11:
		return t[4] == stream && t[5] == consumer
	default:
		return t[2] == stream && t[3] == consumer
	}
}

// ackSeq reads the stream sequence and the number still pending from a
// JetStream ack subject, in either form:
// $JS.ACK.<stream>.<consumer>.<delivered>.<sseq>.<cseq>.<ts>.<pending> or the
// newer one with a domain and account hash before the stream.
func ackSeq(reply string) (seq, pending uint64, ok bool) {
	t := strings.Split(reply, ".")
	if len(t) < 9 || t[0] != "$JS" || t[1] != "ACK" {
		return 0, 0, false
	}
	i := 5
	if len(t) >= 11 {
		i = 7
	}
	seq, err1 := strconv.ParseUint(t[i], 10, 64)
	pending, err2 := strconv.ParseUint(t[i+3], 10, 64)
	return seq, pending, err1 == nil && err2 == nil
}

func (s *natsSource) Next(ctx context.Context, wait time.Duration) (Message, error) {
	for len(s.buf) == 0 {
		if err := ctx.Err(); err != nil {
			return Message{}, err
		}
		if s.done || s.drained {
			return Message{}, io.EOF
		}
		empty, err := s.pull(wait)
		if err != nil {
			return Message{}, fmt.Errorf("nats stream %s: %v", s.e.Name, err)
		}
		if len(s.buf) == 0 && empty {
			if s.bounded {
				s.done = true
				continue
			}
			return Message{}, ErrIdle
		}
	}
	if s.bounded && s.seqs[0] > s.until {
		s.done, s.buf, s.seqs = true, nil, nil
		return Message{}, io.EOF
	}
	m := s.buf[0]
	s.last = s.seqs[0]
	s.buf, s.seqs = s.buf[1:], s.seqs[1:]
	return m, nil
}

func (s *natsSource) Position() string { return strconv.FormatUint(s.last, 10) }

func (s *natsSource) Close() error {
	if s.consumer != "" {
		var v struct{}
		s.nc.api("CONSUMER.DELETE."+s.e.Name+"."+s.consumer, nil, &v)
	}
	return s.nc.Close()
}

type natsSink struct {
	e       *Endpoint
	nc      *natsConn
	subject string
	pending map[string]uint64 // reply subject -> pos
}

func openNATSSink(ctx context.Context, e *Endpoint) (Sink, error) {
	nc, err := dialNATS(ctx, e)
	if err != nil {
		return nil, err
	}
	si, err := nc.streamInfo(e.Name)
	if err != nil {
		nc.Close()
		return nil, err
	}
	subj := e.Query.Get("subject")
	if subj == "" {
		if len(si.Config.Subjects) != 1 || strings.ContainsAny(si.Config.Subjects[0], "*>") {
			nc.Close()
			return nil, fmt.Errorf("nats stream %s: say which subject to write to with ?subject=", e.Name)
		}
		subj = si.Config.Subjects[0]
	}
	if strings.ContainsAny(subj, "*> ") {
		nc.Close()
		return nil, fmt.Errorf("nats stream %s: the output subject %q has a wildcard", e.Name, subj)
	}
	return &natsSink{e: e, nc: nc, subject: subj, pending: map[string]uint64{}}, nil
}

func (s *natsSink) Last(ctx context.Context) (uint64, []byte, error) {
	var r struct {
		Message struct {
			Data string `json:"data"`
			Seq  uint64 `json:"seq"`
		} `json:"message"`
	}
	err := s.nc.api("STREAM.MSG.GET."+s.e.Name, map[string]string{"last_by_subj": s.subject}, &r)
	var ae *jsAPIError
	if errors.As(err, &ae) && ae.e.Code == 404 {
		return 0, nil, nil
	}
	if err != nil {
		return 0, nil, fmt.Errorf("nats stream %s: %v", s.e.Name, err)
	}
	line, err := base64.StdEncoding.DecodeString(r.Message.Data)
	if err != nil {
		return 0, nil, fmt.Errorf("nats stream %s: %v", s.e.Name, err)
	}
	pos, err := PosOf(line)
	if err != nil {
		return 0, nil, fmt.Errorf("nats stream %s: the last message on %s, seq %d, wasn't written by Inorderly; write to an empty subject", s.e.Name, s.subject, r.Message.Seq)
	}
	return pos, line, nil
}

func (s *natsSink) Write(ctx context.Context, pos uint64, line []byte) error {
	if s.nc.maxPay > 0 && len(line) > s.nc.maxPay-200 {
		return fmt.Errorf("nats stream %s: line %d is longer than the server's %d byte message limit", s.e.Name, pos, s.nc.maxPay)
	}
	reply := s.nc.nextReply()
	hdr := "NATS/1.0\r\nNats-Msg-Id: " + s.subject + ":" + strconv.FormatUint(pos, 10) + "\r\n\r\n"
	fmt.Fprintf(s.nc.w, "HPUB %s %s %d %d\r\n%s", s.subject, reply, len(hdr), len(hdr)+len(line), hdr)
	s.nc.w.Write(line)
	s.nc.w.WriteString("\r\n")
	s.pending[reply] = pos
	if len(s.pending) >= 1000 {
		return s.Flush(ctx)
	}
	return nil
}

func (s *natsSink) Flush(ctx context.Context) error {
	if len(s.pending) == 0 {
		return nil
	}
	if err := s.nc.w.Flush(); err != nil {
		return fmt.Errorf("nats stream %s: %v", s.e.Name, err)
	}
	s.nc.c.SetDeadline(time.Now().Add(60 * time.Second))
	for len(s.pending) > 0 {
		op, m, err := s.nc.read()
		if err != nil {
			return fmt.Errorf("nats stream %s: %v", s.e.Name, err)
		}
		if op != "MSG" {
			continue
		}
		pos, ok := s.pending[m.subject]
		if !ok {
			continue
		}
		delete(s.pending, m.subject)
		if m.status == "503" {
			return fmt.Errorf("nats stream %s: no stream takes subject %s", s.e.Name, s.subject)
		}
		var ack struct {
			Stream string   `json:"stream"`
			Error  *jsError `json:"error"`
		}
		if err := json.Unmarshal(m.data, &ack); err != nil || ack.Error != nil || ack.Stream != s.e.Name {
			desc := "a bad ack"
			if ack.Error != nil {
				desc = ack.Error.Description
			} else if ack.Stream != "" {
				desc = "stored in stream " + ack.Stream
			}
			return fmt.Errorf("nats stream %s: line %d: %s", s.e.Name, pos, desc)
		}
	}
	return nil
}

func (s *natsSink) Close() error { return s.nc.Close() }

func natsProduce(ctx context.Context, e *Endpoint, values [][]byte) error {
	sk, err := openNATSSink(ctx, e)
	if err != nil {
		return err
	}
	s := sk.(*natsSink)
	defer s.Close()
	for _, v := range values {
		reply := s.nc.nextReply()
		fmt.Fprintf(s.nc.w, "PUB %s %s %d\r\n", s.subject, reply, len(v))
		s.nc.w.Write(v)
		s.nc.w.WriteString("\r\n")
		s.pending[reply] = 0
		if len(s.pending) >= 1000 {
			if err := s.Flush(ctx); err != nil {
				return err
			}
		}
	}
	return s.Flush(ctx)
}

// natsEnsure creates the stream with one subject of the same name, if it is
// missing.
func natsEnsure(ctx context.Context, e *Endpoint) error {
	nc, err := dialNATS(ctx, e)
	if err != nil {
		return err
	}
	defer nc.Close()
	if _, err := nc.streamInfo(e.Name); err == nil {
		return nil
	}
	var v struct{}
	cfg := map[string]interface{}{"name": e.Name, "subjects": []string{e.Name}, "storage": "file", "num_replicas": 1}
	if err := nc.api("STREAM.CREATE."+e.Name, cfg, &v); err != nil {
		return fmt.Errorf("nats stream %s: %v", e.Name, err)
	}
	return nil
}
