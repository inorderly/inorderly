// Copyright Inorderly.com 2026
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"
)

// A Kafka input is every partition of a topic, read without a consumer
// group: the next offset of each partition is the position, and the
// partitions are taken a message at a time in turn. An output is a topic of
// one partition, written with acks=all, one uncompressed record batch per
// flush. Requests use fixed versions that Kafka 2.1 and every later release,
// 4.x included, accept: Metadata v7, ListOffsets v4, Fetch v10, Produce v7,
// CreateTopics v3, SaslHandshake v1 and SaslAuthenticate v1. Records are read
// uncommitted, without transaction markers; batches compressed with gzip are
// read, other codecs are refused by name.

const (
	apiProduce     = 0
	apiFetch       = 1
	apiListOffsets = 2
	apiMetadata    = 3
	apiSaslHS      = 17
	apiCreate      = 19
	apiSaslAuth    = 36
)

var crc32c = crc32.MakeTable(crc32.Castagnoli)

// kenc builds a request body.
type kenc struct{ b []byte }

func (e *kenc) i8(v int8)   { e.b = append(e.b, byte(v)) }
func (e *kenc) i16(v int16) { e.b = binary.BigEndian.AppendUint16(e.b, uint16(v)) }
func (e *kenc) i32(v int32) { e.b = binary.BigEndian.AppendUint32(e.b, uint32(v)) }
func (e *kenc) i64(v int64) { e.b = binary.BigEndian.AppendUint64(e.b, uint64(v)) }
func (e *kenc) str(s string) {
	e.i16(int16(len(s)))
	e.b = append(e.b, s...)
}
func (e *kenc) nullStr() { e.i16(-1) }
func (e *kenc) bytes(b []byte) {
	e.i32(int32(len(b)))
	e.b = append(e.b, b...)
}
func (e *kenc) varint(v int64) { e.b = binary.AppendVarint(e.b, v) }

// kdec reads a response; the first error sticks.
type kdec struct {
	b   []byte
	err error
}

func (d *kdec) need(n int) bool {
	if d.err != nil {
		return false
	}
	if n < 0 || len(d.b) < n {
		d.err = errors.New("kafka: a response cut short")
		return false
	}
	return true
}
func (d *kdec) i8() int8 {
	if !d.need(1) {
		return 0
	}
	v := int8(d.b[0])
	d.b = d.b[1:]
	return v
}
func (d *kdec) i16() int16 {
	if !d.need(2) {
		return 0
	}
	v := int16(binary.BigEndian.Uint16(d.b))
	d.b = d.b[2:]
	return v
}
func (d *kdec) i32() int32 {
	if !d.need(4) {
		return 0
	}
	v := int32(binary.BigEndian.Uint32(d.b))
	d.b = d.b[4:]
	return v
}
func (d *kdec) i64() int64 {
	if !d.need(8) {
		return 0
	}
	v := int64(binary.BigEndian.Uint64(d.b))
	d.b = d.b[8:]
	return v
}
func (d *kdec) str() string {
	n := int(d.i16())
	if n < 0 || !d.need(n) {
		return ""
	}
	s := string(d.b[:n])
	d.b = d.b[n:]
	return s
}
func (d *kdec) bytes() []byte {
	n := int(d.i32())
	if n < 0 || !d.need(n) {
		return nil
	}
	v := d.b[:n]
	d.b = d.b[n:]
	return v
}
func (d *kdec) array() int {
	n := int(d.i32())
	if n < 0 {
		return 0
	}
	if n > len(d.b) {
		d.err = errors.New("kafka: a response cut short")
		return 0
	}
	return n
}
func (d *kdec) varint() int64 {
	if d.err != nil {
		return 0
	}
	v, n := binary.Varint(d.b)
	if n <= 0 {
		d.err = errors.New("kafka: a bad varint")
		return 0
	}
	d.b = d.b[n:]
	return v
}

type kafkaConn struct {
	c    net.Conn
	r    *bufio.Reader
	corr int32
}

func (kc *kafkaConn) Close() error { return kc.c.Close() }

func (kc *kafkaConn) call(api, version int16, body []byte, timeout time.Duration) (*kdec, error) {
	kc.corr++
	h := kenc{}
	h.i16(api)
	h.i16(version)
	h.i32(kc.corr)
	h.str("inorderly")
	msg := make([]byte, 4, 4+len(h.b)+len(body))
	binary.BigEndian.PutUint32(msg, uint32(len(h.b)+len(body)))
	msg = append(append(msg, h.b...), body...)
	kc.c.SetDeadline(time.Now().Add(timeout))
	if _, err := kc.c.Write(msg); err != nil {
		return nil, err
	}
	var sz [4]byte
	if _, err := io.ReadFull(kc.r, sz[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(sz[:])
	if n < 4 || n > 64<<20 {
		return nil, errors.New("kafka: a bad response size")
	}
	resp := make([]byte, n)
	if _, err := io.ReadFull(kc.r, resp); err != nil {
		return nil, err
	}
	if int32(binary.BigEndian.Uint32(resp)) != kc.corr {
		return nil, errors.New("kafka: a response to another request")
	}
	return &kdec{b: resp[4:]}, nil
}

// kafkaErr names the error codes Inorderly is likely to meet.
func kafkaErr(code int16) error {
	if code == 0 {
		return nil
	}
	names := map[int16]string{1: "offset out of range (the messages were deleted)", 3: "unknown topic or partition",
		5: "leader not available", 6: "not the leader", 7: "request timed out", 10: "message too large",
		29: "topic authorization failed", 31: "cluster authorization failed", 36: "topic already exists",
		58: "SASL authentication failed", 87: "invalid record"}
	if s, ok := names[code]; ok {
		return fmt.Errorf("kafka error %d: %s", code, s)
	}
	return fmt.Errorf("kafka error %d", code)
}

type kafkaClient struct {
	e       *Endpoint
	conns   map[int32]*kafkaConn
	boot    *kafkaConn
	brokers map[int32]string
	leaders map[int32]int32 // partition -> leader node
	parts   []int32
}

func (k *kafkaClient) dial(ctx context.Context, host string) (*kafkaConn, error) {
	c, err := dial(ctx, k.e, host)
	if err != nil {
		return nil, fmt.Errorf("kafka %s: %v", host, err)
	}
	kc := &kafkaConn{c: c, r: bufio.NewReaderSize(c, 1<<16)}
	if k.e.User != "" {
		if err := kafkaSASL(kc, k.e); err != nil {
			kc.Close()
			return nil, fmt.Errorf("kafka %s: %v", host, err)
		}
	}
	return kc, nil
}

func newKafka(ctx context.Context, e *Endpoint) (*kafkaClient, error) {
	k := &kafkaClient{e: e, conns: map[int32]*kafkaConn{}}
	var err error
	if k.boot, err = k.dial(ctx, e.Host); err != nil {
		return nil, err
	}
	if err := k.metadata(); err != nil {
		k.Close()
		return nil, err
	}
	return k, nil
}

func (k *kafkaClient) Close() error {
	for _, c := range k.conns {
		c.Close()
	}
	if k.boot != nil {
		k.boot.Close()
	}
	return nil
}

func (k *kafkaClient) metadata() error {
	req := kenc{}
	req.i32(1)
	req.str(k.e.Name)
	req.i8(0) // don't create the topic
	d, err := k.boot.call(apiMetadata, 7, req.b, 30*time.Second)
	if err != nil {
		return fmt.Errorf("kafka %s: %v", k.e.Host, err)
	}
	d.i32() // throttle
	k.brokers = map[int32]string{}
	for i, n := 0, d.array(); i < n; i++ {
		id := d.i32()
		host := d.str()
		port := d.i32()
		d.str() // rack
		k.brokers[id] = net.JoinHostPort(host, strconv.Itoa(int(port)))
	}
	d.str() // cluster id
	d.i32() // controller
	k.leaders = map[int32]int32{}
	k.parts = nil
	var topicErr int16
	for i, n := 0, d.array(); i < n; i++ {
		code := d.i16()
		name := d.str()
		d.i8()
		for j, m := 0, d.array(); j < m; j++ {
			d.i16()
			p := d.i32()
			leader := d.i32()
			d.i32()
			for _, x := range []int{0, 1, 2} {
				_ = x
				for q, r := 0, d.array(); q < r; q++ {
					d.i32()
				}
			}
			if name == k.e.Name {
				k.leaders[p] = leader
				k.parts = append(k.parts, p)
			}
		}
		if name == k.e.Name {
			topicErr = code
		}
	}
	if d.err != nil {
		return d.err
	}
	if topicErr != 0 {
		return fmt.Errorf("kafka topic %s: %v", k.e.Name, kafkaErr(topicErr))
	}
	if len(k.parts) == 0 {
		return fmt.Errorf("kafka topic %s: no such topic", k.e.Name)
	}
	sort.Slice(k.parts, func(i, j int) bool { return k.parts[i] < k.parts[j] })
	return nil
}

// leader returns the connection to a partition's leader.
func (k *kafkaClient) leader(ctx context.Context, p int32) (*kafkaConn, error) {
	id, ok := k.leaders[p]
	if !ok || id < 0 {
		return nil, errNoLeader
	}
	if c := k.conns[id]; c != nil {
		return c, nil
	}
	host, ok := k.brokers[id]
	if !ok {
		return nil, fmt.Errorf("kafka topic %s: no address for broker %d", k.e.Name, id)
	}
	c, err := k.dial(ctx, host)
	if err != nil {
		return nil, err
	}
	k.conns[id] = c
	return c, nil
}

// errNoLeader: the metadata names no leader for a partition, for a moment.
var errNoLeader = errors.New("no leader")

// retry runs f, refreshing the leaders and trying again when leadership moved.
// A timeout (7) is retried only when f is safe to repeat: a produce that timed
// out may have been written, and must not be sent twice.
func (k *kafkaClient) retry(f func() (int16, error)) error { return k.retryIf(true, f) }

func (k *kafkaClient) retryIf(repeatable bool, f func() (int16, error)) error {
	for attempt := 0; ; attempt++ {
		code, err := f()
		if errors.Is(err, errNoLeader) {
			code, err = 5, nil
		}
		if err == nil && code == 0 {
			return nil
		}
		if err == nil && (code == 5 || code == 6 || (code == 7 && repeatable)) && attempt < 8 {
			time.Sleep(time.Duration(200*(attempt+1)) * time.Millisecond)
			for id, c := range k.conns {
				c.Close()
				delete(k.conns, id)
			}
			if err := k.metadata(); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			return err
		}
		return fmt.Errorf("kafka topic %s: %v", k.e.Name, kafkaErr(code))
	}
}

// offsets returns the earliest (-2) or next-to-write (-1) offset of each
// partition.
func (k *kafkaClient) offsets(ctx context.Context, at int64) (map[int32]int64, error) {
	out := map[int32]int64{}
	for _, p := range k.parts {
		err := k.retry(func() (int16, error) {
			c, err := k.leader(ctx, p)
			if err != nil {
				return 0, err
			}
			req := kenc{}
			req.i32(-1)
			req.i8(1) // read committed: the end is the last stable offset
			req.i32(1)
			req.str(k.e.Name)
			req.i32(1)
			req.i32(p)
			req.i32(-1)
			req.i64(at)
			d, err := c.call(apiListOffsets, 4, req.b, 30*time.Second)
			if err != nil {
				return 0, err
			}
			d.i32()
			var code int16
			for i, n := 0, d.array(); i < n; i++ {
				d.str()
				for j, m := 0, d.array(); j < m; j++ {
					d.i32()
					code = d.i16()
					d.i64()
					out[p] = d.i64()
					d.i32()
				}
			}
			return code, d.err
		})
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

type abortedTxn struct{ pid, first int64 }

type kafkaRecord struct {
	offset int64
	value  []byte
}

// fetch reads records of one partition from offset on. It also returns the
// offset after the last batch it saw, transaction markers included.
func (k *kafkaClient) fetch(ctx context.Context, p int32, offset int64, wait time.Duration) ([]kafkaRecord, int64, error) {
	var recs []kafkaRecord
	after := offset
	err := k.retry(func() (int16, error) {
		c, err := k.leader(ctx, p)
		if err != nil {
			return 0, err
		}
		req := kenc{}
		req.i32(-1)
		req.i32(int32(wait.Milliseconds()))
		req.i32(1)
		req.i32(8 << 20)
		req.i8(1) // read committed
		req.i32(0)
		req.i32(-1) // no fetch session
		req.i32(1)
		req.str(k.e.Name)
		req.i32(1)
		req.i32(p)
		req.i32(-1)
		req.i64(offset)
		req.i64(-1)
		req.i32(4 << 20)
		req.i32(0) // nothing forgotten
		d, err := c.call(apiFetch, 10, req.b, wait+30*time.Second)
		if err != nil {
			return 0, err
		}
		d.i32()
		if code := d.i16(); code != 0 {
			return code, nil
		}
		d.i32()
		var code int16
		for i, n := 0, d.array(); i < n; i++ {
			d.str()
			for j, m := 0, d.array(); j < m; j++ {
				d.i32()
				code = d.i16()
				d.i64()
				d.i64()
				d.i64()
				var aborted []abortedTxn
				for q, r := 0, d.array(); q < r; q++ {
					aborted = append(aborted, abortedTxn{pid: d.i64(), first: d.i64()})
				}
				records := d.bytes()
				if d.err == nil && code == 0 {
					var err error
					if recs, after, err = decodeBatches(records, offset, aborted...); err != nil {
						return 0, fmt.Errorf("kafka topic %s partition %d: %v", k.e.Name, p, err)
					}
				}
			}
		}
		return code, d.err
	})
	return recs, after, err
}

// decodeBatches reads record batches (magic 2), skipping records before from,
// transaction markers, and a batch cut short at the end.
func decodeBatches(b []byte, from int64, aborted ...abortedTxn) ([]kafkaRecord, int64, error) {
	var out []kafkaRecord
	after := from
	sort.Slice(aborted, func(i, j int) bool { return aborted[i].first < aborted[j].first })
	open := map[int64]bool{} // producers whose current transaction was aborted
	for len(b) >= 17 {
		base := int64(binary.BigEndian.Uint64(b))
		length := int(int32(binary.BigEndian.Uint32(b[8:])))
		if length < 0 || 12+length > len(b) {
			break // the server cut it at its size limit
		}
		batch := b[12 : 12+length]
		b = b[12+length:]
		if len(batch) < 49 {
			return nil, 0, errors.New("a record batch cut short")
		}
		if last := base + int64(int32(binary.BigEndian.Uint32(batch[11:]))) + 1; last > after {
			after = last
		}
		if magic := batch[4]; magic != 2 {
			return nil, 0, fmt.Errorf("an old message format (magic %d); Inorderly reads record batches, Kafka 0.11 and newer", magic)
		}
		crc := binary.BigEndian.Uint32(batch[5:])
		if crc32.Checksum(batch[9:], crc32c) != crc {
			return nil, 0, fmt.Errorf("the record batch at offset %d has a bad CRC", base)
		}
		attrs := int16(binary.BigEndian.Uint16(batch[9:]))
		count := int(int32(binary.BigEndian.Uint32(batch[45:])))
		recs := batch[49:]
		pid := int64(binary.BigEndian.Uint64(batch[31:]))
		lastOff := base + int64(int32(binary.BigEndian.Uint32(batch[11:])))
		for len(aborted) > 0 && aborted[0].first <= lastOff {
			open[aborted[0].pid] = true
			aborted = aborted[1:]
		}
		if attrs&0x20 != 0 {
			delete(open, pid) // a transaction marker ends the producer's transaction
			continue
		}
		if attrs&0x10 != 0 && open[pid] {
			continue // part of an aborted transaction
		}
		switch codec := attrs & 7; codec {
		case 0:
		case 1:
			zr, err := gzip.NewReader(bytes.NewReader(recs))
			if err != nil {
				return nil, 0, fmt.Errorf("the gzip batch at offset %d: %v", base, err)
			}
			if recs, err = io.ReadAll(io.LimitReader(zr, 256<<20)); err != nil {
				return nil, 0, fmt.Errorf("the gzip batch at offset %d: %v", base, err)
			}
		default:
			name := map[int16]string{2: "snappy", 3: "lz4", 4: "zstd"}[codec]
			return nil, 0, fmt.Errorf("the batch at offset %d is compressed with %s; Inorderly reads uncompressed or gzip batches", base, name)
		}
		d := &kdec{b: recs}
		for i := 0; i < count && d.err == nil; i++ {
			n := d.varint()
			if !d.need(int(n)) {
				break
			}
			rd := &kdec{b: d.b[:n]}
			d.b = d.b[n:]
			rd.i8()
			rd.varint()
			delta := rd.varint()
			kl := rd.varint()
			if kl > 0 && rd.need(int(kl)) {
				rd.b = rd.b[kl:]
			}
			vl := rd.varint()
			var val []byte
			if vl >= 0 && rd.need(int(vl)) {
				val = rd.b[:vl]
			}
			if rd.err != nil {
				return nil, 0, fmt.Errorf("a bad record in the batch at offset %d", base)
			}
			if off := base + delta; off >= from {
				out = append(out, kafkaRecord{offset: off, value: val})
			}
		}
		if d.err != nil {
			return nil, 0, fmt.Errorf("the batch at offset %d: %v", base, d.err)
		}
	}
	return out, after, nil
}

// encodeBatch builds one uncompressed record batch v2 of values.
func encodeBatch(values [][]byte, now int64) []byte {
	var recs kenc
	for i, v := range values {
		var r kenc
		r.i8(0)
		r.varint(0)
		r.varint(int64(i))
		r.varint(-1) // no key
		r.varint(int64(len(v)))
		r.b = append(r.b, v...)
		r.varint(0) // no headers
		recs.varint(int64(len(r.b)))
		recs.b = append(recs.b, r.b...)
	}
	var tail kenc // from attributes on: what the CRC covers
	tail.i16(0)
	tail.i32(int32(len(values) - 1))
	tail.i64(now)
	tail.i64(now)
	tail.i64(-1) // producer id
	tail.i16(-1)
	tail.i32(-1)
	tail.i32(int32(len(values)))
	tail.b = append(tail.b, recs.b...)
	var b kenc
	b.i64(0)
	b.i32(int32(4 + 1 + 4 + len(tail.b)))
	b.i32(-1) // partition leader epoch
	b.i8(2)
	b.i32(int32(crc32.Checksum(tail.b, crc32c)))
	b.b = append(b.b, tail.b...)
	return b.b
}

// produce writes values to a partition with acks=all.
func (k *kafkaClient) produce(ctx context.Context, p int32, values [][]byte) error {
	batch := encodeBatch(values, time.Now().UnixMilli())
	return k.retryIf(false, func() (int16, error) {
		c, err := k.leader(ctx, p)
		if err != nil {
			return 0, err
		}
		req := kenc{}
		req.nullStr()
		req.i16(-1)
		req.i32(30000)
		req.i32(1)
		req.str(k.e.Name)
		req.i32(1)
		req.i32(p)
		req.bytes(batch)
		d, err := c.call(apiProduce, 7, req.b, 60*time.Second)
		if err != nil {
			return 0, err
		}
		var code int16
		for i, n := 0, d.array(); i < n; i++ {
			d.str()
			for j, m := 0, d.array(); j < m; j++ {
				d.i32()
				code = d.i16()
				d.i64()
				d.i64()
				d.i64()
			}
		}
		return code, d.err
	})
}

func kafkaEnd(ctx context.Context, e *Endpoint) (string, error) {
	k, err := newKafka(ctx, e)
	if err != nil {
		return "", err
	}
	defer k.Close()
	end, err := k.offsets(ctx, -1)
	if err != nil {
		return "", err
	}
	return formatOffsets(k.parts, end), nil
}

func formatOffsets(parts []int32, m map[int32]int64) string {
	var b strings.Builder
	for i, p := range parts {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "%d:%d", p, m[p])
	}
	return b.String()
}

func parseOffsets(s string) (map[int32]int64, error) {
	out := map[int32]int64{}
	if s == "" {
		return out, nil
	}
	for _, f := range strings.Split(s, ",") {
		ps, os, ok := strings.Cut(f, ":")
		p, err1 := strconv.ParseInt(ps, 10, 32)
		o, err2 := strconv.ParseInt(os, 10, 64)
		if !ok || err1 != nil || err2 != nil || o < 0 {
			return nil, fmt.Errorf("a damaged position %q", s)
		}
		out[int32(p)] = o
	}
	return out, nil
}

type kafkaSource struct {
	k       *kafkaClient
	next    map[int32]int64
	until   map[int32]int64
	bounded bool
	buf     map[int32][]kafkaRecord
	turn    int
}

func openKafkaSource(ctx context.Context, e *Endpoint, from, until string) (Source, error) {
	// A position is the next offset of each partition and, after "@", whose
	// turn it is, so a resumed read takes the partitions in the same turn.
	turn := 0
	if i := strings.LastIndex(from, "@"); i >= 0 {
		t, err := strconv.Atoi(from[i+1:])
		if err != nil || t < 0 {
			return nil, fmt.Errorf("kafka topic %s: a damaged position %q", e.Name, from)
		}
		turn, from = t, from[:i]
	}
	next, err := parseOffsets(from)
	if err != nil {
		return nil, fmt.Errorf("kafka topic %s: %v", e.Name, err)
	}
	end, err := parseOffsets(until)
	if err != nil {
		return nil, fmt.Errorf("kafka topic %s: %v", e.Name, err)
	}
	k, err := newKafka(ctx, e)
	if err != nil {
		return nil, err
	}
	s := &kafkaSource{k: k, next: next, until: end, bounded: until != "", buf: map[int32][]kafkaRecord{}, turn: turn}
	if from == "" {
		if s.next, err = k.offsets(ctx, -2); err != nil {
			k.Close()
			return nil, err
		}
	}
	for _, p := range k.parts {
		if _, ok := s.next[p]; !ok {
			if from != "" {
				k.Close()
				return nil, fmt.Errorf("kafka topic %s: partition %d appeared since the run started; a run reads the partitions it began with", e.Name, p)
			}
		}
	}
	return s, nil
}

func (s *kafkaSource) open(p int32) bool {
	if !s.bounded {
		return true
	}
	return s.next[p] < s.until[p]
}

func (s *kafkaSource) take(p int32) Message {
	r := s.buf[p][0]
	s.buf[p] = s.buf[p][1:]
	s.next[p] = r.offset + 1
	return Message{Value: r.value, At: fmt.Sprintf("partition %d offset %d", p, r.offset)}
}

func (s *kafkaSource) Next(ctx context.Context, wait time.Duration) (Message, error) {
	parts := s.k.parts
	if s.bounded {
		// A message from each partition in turn, fetching when one runs dry:
		// the same order every time over the same messages.
		for tries := 0; tries < len(parts); tries++ {
			p := parts[s.turn%len(parts)]
			s.turn++
			if !s.open(p) {
				continue
			}
			if err := ctx.Err(); err != nil {
				return Message{}, err
			}
			for empty := 0; len(s.buf[p]) == 0 && s.open(p); empty++ {
				if empty == 20 {
					return Message{}, fmt.Errorf("kafka topic %s partition %d: nothing comes back from offset %d, below the end %d", s.k.e.Name, p, s.next[p], s.until[p])
				}
				recs, after, err := s.k.fetch(ctx, p, s.next[p], 500*time.Millisecond)
				if err != nil {
					return Message{}, err
				}
				var keep []kafkaRecord
				for _, r := range recs {
					if r.offset < s.until[p] {
						keep = append(keep, r)
					}
				}
				s.buf[p] = keep
				if len(keep) == 0 && after > s.next[p] {
					s.next[p] = after // past transaction markers, or the end
					if s.next[p] > s.until[p] {
						s.next[p] = s.until[p]
					}
				}
			}
			if len(s.buf[p]) > 0 {
				return s.take(p), nil
			}
		}
		for _, p := range parts {
			if s.open(p) {
				return s.Next(ctx, wait)
			}
		}
		return Message{}, io.EOF
	}
	for round := 0; round < 2; round++ {
		for tries := 0; tries < len(parts); tries++ {
			p := parts[s.turn%len(parts)]
			s.turn++
			if len(s.buf[p]) > 0 {
				return s.take(p), nil
			}
		}
		if round == 1 {
			break
		}
		if err := ctx.Err(); err != nil {
			return Message{}, err
		}
		each := wait / time.Duration(len(parts))
		for _, p := range parts {
			recs, after, err := s.k.fetch(ctx, p, s.next[p], each)
			if err != nil {
				return Message{}, err
			}
			s.buf[p] = recs
			if len(recs) == 0 && after > s.next[p] {
				s.next[p] = after
			}
		}
	}
	return Message{}, ErrIdle
}

func (s *kafkaSource) Position() string {
	return formatOffsets(s.k.parts, s.next) + "@" + strconv.Itoa(s.turn%len(s.k.parts))
}

func (s *kafkaSource) Close() error { return s.k.Close() }

type kafkaSink struct {
	k       *kafkaClient
	pending [][]byte
	size    int
}

func openKafkaSink(ctx context.Context, e *Endpoint) (Sink, error) {
	k, err := newKafka(ctx, e)
	if err != nil {
		return nil, err
	}
	if len(k.parts) != 1 {
		k.Close()
		return nil, fmt.Errorf("kafka topic %s has %d partitions; an output topic needs one, so its order is the output's", e.Name, len(k.parts))
	}
	return &kafkaSink{k: k}, nil
}

func (s *kafkaSink) Last(ctx context.Context) (uint64, []byte, error) {
	p := s.k.parts[0]
	end, err := s.k.offsets(ctx, -1)
	if err != nil {
		return 0, nil, err
	}
	start, err := s.k.offsets(ctx, -2)
	if err != nil {
		return 0, nil, err
	}
	if end[p] == start[p] {
		return 0, nil, nil
	}
	// The last offset may be a transaction marker; look back a little.
	from := end[p] - 1
	for back := int64(0); back < 64 && from >= start[p]; back += 8 {
		recs, _, err := s.k.fetch(ctx, p, from, 0)
		if err != nil {
			return 0, nil, err
		}
		if len(recs) > 0 {
			last := recs[len(recs)-1]
			pos, err := PosOf(last.value)
			if err != nil {
				return 0, nil, fmt.Errorf("kafka topic %s: the last record, offset %d, wasn't written by Inorderly; write to an empty topic", s.k.e.Name, last.offset)
			}
			return pos, last.value, nil
		}
		from -= 8
	}
	return 0, nil, fmt.Errorf("kafka topic %s holds only transaction markers at its end; write to an empty topic", s.k.e.Name)
}

// kafkaBatchBytes keeps a produce request under the broker's default
// message.max.bytes of about 1 MB.
const kafkaBatchBytes = 900 << 10

func (s *kafkaSink) Write(ctx context.Context, pos uint64, line []byte) error {
	if len(line) > kafkaBatchBytes {
		return fmt.Errorf("kafka topic %s: line %d is %d bytes, more than one message may hold", s.k.e.Name, pos, len(line))
	}
	if s.size+len(line) > kafkaBatchBytes {
		if err := s.Flush(ctx); err != nil {
			return err
		}
	}
	s.pending = append(s.pending, line)
	s.size += len(line) + 16
	return nil
}

func (s *kafkaSink) Flush(ctx context.Context) error {
	if len(s.pending) == 0 {
		return nil
	}
	err := s.k.produce(ctx, s.k.parts[0], s.pending)
	s.pending, s.size = nil, 0
	return err
}

func (s *kafkaSink) Close() error { return s.k.Close() }

func (s *kafkaSink) Mark(ctx context.Context) (string, error) { return kafkaEnd(ctx, s.k.e) }

func (s *kafkaSink) Each(ctx context.Context, after string, f func([]byte) error) error {
	return each(ctx, s.k.e, after, func(m Message) error { return f(m.Value) })
}

// kafkaProduce spreads the values over the partitions in turn.
func kafkaProduce(ctx context.Context, e *Endpoint, values [][]byte) error {
	k, err := newKafka(ctx, e)
	if err != nil {
		return err
	}
	defer k.Close()
	n := len(k.parts)
	batches := make([][][]byte, n)
	sizes := make([]int, n)
	for i, v := range values {
		j := i % n
		if sizes[j]+len(v) > kafkaBatchBytes && len(batches[j]) > 0 {
			if err := k.produce(ctx, k.parts[j], batches[j]); err != nil {
				return err
			}
			batches[j], sizes[j] = nil, 0
		}
		batches[j] = append(batches[j], v)
		sizes[j] += len(v) + 16
	}
	for j, b := range batches {
		if len(b) > 0 {
			if err := k.produce(ctx, k.parts[j], b); err != nil {
				return err
			}
		}
	}
	return nil
}

// kafkaEnsure creates a topic of one partition, if it is missing.
func kafkaEnsure(ctx context.Context, e *Endpoint) error { return EnsureTopic(ctx, e, 1) }

// EnsureTopic creates a Kafka topic with this many partitions, if it is
// missing. Tests use it.
func EnsureTopic(ctx context.Context, e *Endpoint, partitions int) error {
	k := &kafkaClient{e: e, conns: map[int32]*kafkaConn{}}
	var err error
	if k.boot, err = k.dial(ctx, e.Host); err != nil {
		return err
	}
	defer k.Close()
	req := kenc{}
	req.i32(1)
	req.str(e.Name)
	req.i32(int32(partitions))
	req.i16(1)
	req.i32(0)
	req.i32(0)
	req.i32(30000)
	req.i8(0)
	d, err := k.boot.call(apiCreate, 3, req.b, 60*time.Second)
	if err != nil {
		return fmt.Errorf("kafka %s: %v", e.Host, err)
	}
	d.i32()
	var code int16
	for i, n := 0, d.array(); i < n; i++ {
		d.str()
		code = d.i16()
		d.str()
	}
	if d.err != nil {
		return d.err
	}
	if code != 0 && code != 36 {
		return fmt.Errorf("kafka topic %s: %v", e.Name, kafkaErr(code))
	}
	// Wait for the new partition to have a leader.
	for i := 0; i < 50; i++ {
		if err := k.metadata(); err == nil && len(k.parts) == partitions {
			led := true
			for _, p := range k.parts {
				led = led && k.leaders[p] >= 0
			}
			if led {
				return nil
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("kafka topic %s: no leader after creating it", e.Name)
}

// kafkaSASL authenticates with PLAIN or SCRAM-SHA-256/512.
func kafkaSASL(kc *kafkaConn, e *Endpoint) error {
	mech := strings.ToUpper(e.Query.Get("sasl"))
	if mech == "" {
		mech = "PLAIN"
	}
	if mech != "PLAIN" && mech != "SCRAM-SHA-256" && mech != "SCRAM-SHA-512" {
		return fmt.Errorf("unknown SASL mechanism %q; use plain, scram-sha-256 or scram-sha-512", mech)
	}
	hs := kenc{}
	hs.str(mech)
	d, err := kc.call(apiSaslHS, 1, hs.b, 30*time.Second)
	if err != nil {
		return err
	}
	if code := d.i16(); code != 0 {
		var mechs []string
		for i, n := 0, d.array(); i < n; i++ {
			mechs = append(mechs, d.str())
		}
		return fmt.Errorf("the broker doesn't take SASL %s (it takes %s)", mech, strings.Join(mechs, ", "))
	}
	auth := func(b []byte) ([]byte, error) {
		req := kenc{}
		req.bytes(b)
		d, err := kc.call(apiSaslAuth, 1, req.b, 30*time.Second)
		if err != nil {
			return nil, err
		}
		code := d.i16()
		msg := d.str()
		out := d.bytes()
		d.i64()
		if code != 0 {
			if msg == "" {
				msg = kafkaErr(code).Error()
			}
			return nil, errors.New(msg)
		}
		return out, d.err
	}
	if mech == "PLAIN" {
		_, err := auth([]byte("\x00" + e.User + "\x00" + e.Pass))
		return err
	}
	h := sha256.New
	if mech == "SCRAM-SHA-512" {
		h = sha512.New
	}
	return scram(h, e.User, e.Pass, auth)
}

func scram(h func() hash.Hash, user, pass string, auth func([]byte) ([]byte, error)) error {
	nonce := scramNonce()
	esc := strings.NewReplacer("=", "=3D", ",", "=2C").Replace(user)
	bare := "n=" + esc + ",r=" + nonce
	first, err := auth([]byte("n,," + bare))
	if err != nil {
		return err
	}
	var rnonce, salt64 string
	iter := 0
	for _, f := range strings.Split(string(first), ",") {
		k, v, _ := strings.Cut(f, "=")
		switch k {
		case "r":
			rnonce = v
		case "s":
			salt64 = v
		case "i":
			iter, _ = strconv.Atoi(v)
		}
	}
	salt, err := base64.StdEncoding.DecodeString(salt64)
	if !strings.HasPrefix(rnonce, nonce) || err != nil || iter < 1 || iter > 1<<20 {
		return errors.New("a bad SCRAM server message")
	}
	salted := pbkdf2(h, []byte(pass), salt, iter)
	mac := func(key []byte, msg string) []byte {
		m := hmac.New(h, key)
		m.Write([]byte(msg))
		return m.Sum(nil)
	}
	clientKey := mac(salted, "Client Key")
	hh := h()
	hh.Write(clientKey)
	stored := hh.Sum(nil)
	final := "c=biws,r=" + rnonce
	msg := bare + "," + string(first) + "," + final
	sig := mac(stored, msg)
	proof := make([]byte, len(clientKey))
	for i := range proof {
		proof[i] = clientKey[i] ^ sig[i]
	}
	last, err := auth([]byte(final + ",p=" + base64.StdEncoding.EncodeToString(proof)))
	if err != nil {
		return err
	}
	want := base64.StdEncoding.EncodeToString(mac(mac(salted, "Server Key"), msg))
	if string(last) != "v="+want {
		return errors.New("the broker's SCRAM signature doesn't match")
	}
	return nil
}

// scramNonce makes the client nonce; tests replace it.
var scramNonce = func() string {
	var nb [18]byte
	rand.Read(nb[:])
	return base64.RawStdEncoding.EncodeToString(nb[:])
}

func pbkdf2(h func() hash.Hash, pass, salt []byte, iter int) []byte {
	m := hmac.New(h, pass)
	m.Write(salt)
	m.Write([]byte{0, 0, 0, 1})
	u := m.Sum(nil)
	out := append([]byte(nil), u...)
	for i := 1; i < iter; i++ {
		m.Reset()
		m.Write(u)
		u = m.Sum(u[:0])
		for j := range out {
			out[j] ^= u[j]
		}
	}
	return out
}
