// Copyright Inorderly.com 2026
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/binary"
	"hash/crc32"
	"strconv"
	"strings"
	"testing"
)

func TestParse(t *testing.T) {
	e, err := Parse("redis://u:secret@localhost/events?field=line")
	if err != nil || e.Kind != "redis" || e.Host != "localhost:6379" || e.Name != "events" || e.Pass != "secret" || e.Query.Get("field") != "line" {
		t.Fatalf("%+v %v", e, err)
	}
	if s := e.String(); strings.Contains(s, "secret") || !strings.HasPrefix(s, "redis://u@localhost:6379/events") {
		t.Fatalf("String() = %s", s)
	}
	for in, kind := range map[string]string{"tls://h/s": "nats", "kafkas://h:1/t": "kafka", "rediss://h/s": "redis", "nats://h/s": "nats"} {
		e, err := Parse(in)
		if err != nil || e.Kind != kind {
			t.Fatalf("%s: %+v %v", in, e, err)
		}
	}
	for _, bad := range []string{"redis://h", "redis://h/a/b", "kafka:///t", "nats://h/s?bogus=1"} {
		if _, err := Parse(bad); err == nil {
			t.Fatalf("%s was accepted", bad)
		}
	}
	if _, err := Parse("redis://u:secret@h/a/b"); err == nil || strings.Contains(err.Error(), "secret") {
		t.Fatalf("an error shows the password: %v", err)
	}
	if IsURL("file.ndjson") || IsURL("C:\\x") || !IsURL("KAFKA://h/t") || IsURL("http://h/x") {
		t.Fatal("IsURL")
	}
}

func TestPosOf(t *testing.T) {
	for line, want := range map[string]uint64{`{"pos":12,"producer":"p"}`: 12, `{"pos":1}`: 1} {
		if n, err := PosOf([]byte(line)); err != nil || n != want {
			t.Fatalf("%s: %d %v", line, n, err)
		}
	}
	for _, bad := range []string{`{"hello":1}`, `{"pos":0,"x":1}`, `{"pos":x}`, `{"pos":12x}`, ``, `{"pos":123456789012345678901234,"x":1}`} {
		if _, err := PosOf([]byte(bad)); err == nil {
			t.Fatalf("%s was taken as an output line", bad)
		}
	}
}

func TestCRC32C(t *testing.T) {
	if got := crc32.Checksum([]byte("123456789"), crc32c); got != 0xE3069283 {
		t.Fatalf("crc32c = %x", got)
	}
}

func TestRecordBatchRoundTrip(t *testing.T) {
	vals := [][]byte{[]byte(`{"pos":1}`), {}, bytes.Repeat([]byte("x"), 300)}
	b := encodeBatch(vals, 1700000000000)
	// Put it at base offset 40, and add a cut-short batch after it.
	binary.BigEndian.PutUint64(b, 40)
	in := append(append([]byte{}, b...), b[:30]...)
	recs, after, err := decodeBatches(in, 41)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 2 || recs[0].offset != 41 || !bytes.Equal(recs[1].value, vals[2]) || after != 43 {
		t.Fatalf("%d records, after %d: %+v", len(recs), after, recs)
	}
	// A damaged byte breaks the CRC.
	bad := append([]byte{}, b...)
	bad[len(bad)-1] ^= 1
	if _, _, err := decodeBatches(bad, 0); err == nil || !strings.Contains(err.Error(), "CRC") {
		t.Fatalf("a damaged batch: %v", err)
	}
}

// withAttrs rewrites a batch's attributes and records, fixing the CRC.
func withAttrs(b []byte, attrs int16, recs []byte) []byte {
	head := append([]byte{}, b[:12+49]...)
	out := append(head, recs...)
	binary.BigEndian.PutUint32(out[8:], uint32(len(out)-12))
	binary.BigEndian.PutUint16(out[12+9:], uint16(attrs))
	binary.BigEndian.PutUint32(out[12+5:], crc32.Checksum(out[12+9:], crc32c))
	return out
}

func TestGzipControlAndOtherCodecs(t *testing.T) {
	b := encodeBatch([][]byte{[]byte("a"), []byte("bb")}, 0)
	recs := b[12+49:]
	var z bytes.Buffer
	zw := gzip.NewWriter(&z)
	zw.Write(recs)
	zw.Close()
	got, _, err := decodeBatches(withAttrs(b, 1, z.Bytes()), 0)
	if err != nil || len(got) != 2 || string(got[1].value) != "bb" {
		t.Fatalf("gzip: %v %+v", err, got)
	}
	got, after, err := decodeBatches(withAttrs(b, 0x20, recs), 0)
	if err != nil || len(got) != 0 || after != 2 {
		t.Fatalf("a transaction marker was read as records: %v %+v %d", err, got, after)
	}
	if _, _, err := decodeBatches(withAttrs(b, 4, recs), 0); err == nil || !strings.Contains(err.Error(), "zstd") {
		t.Fatalf("zstd: %v", err)
	}
	old := append([]byte{}, b...)
	old[12+4] = 1
	if _, _, err := decodeBatches(old, 0); err == nil || !strings.Contains(err.Error(), "magic 1") {
		t.Fatalf("magic 1: %v", err)
	}
}

// RFC 7677's example exchange.
func TestSCRAMSHA256(t *testing.T) {
	scramNonce = func() string { return "rOprNGfwEbeRWgbNEkqO" }
	step := 0
	err := scram(sha256.New, "user", "pencil", func(b []byte) ([]byte, error) {
		step++
		switch step {
		case 1:
			if string(b) != "n,,n=user,r=rOprNGfwEbeRWgbNEkqO" {
				t.Fatalf("client first %q", b)
			}
			return []byte("r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096"), nil
		default:
			want := "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="
			if string(b) != want {
				t.Fatalf("client final %q", b)
			}
			return []byte("v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4="), nil
		}
	})
	if err != nil || step != 2 {
		t.Fatal(err)
	}
}

func TestAckSeq(t *testing.T) {
	for reply, want := range map[string]uint64{
		"$JS.ACK.events.c1.1.57.3.1700000000000000000.0":                   57,
		"$JS.ACK.hub.acchash.events.c1.1.58.4.1700000000000000000.0.token": 58,
	} {
		if n, ok := ackSeq(reply); !ok || n != want {
			t.Fatalf("%s: %d %v", reply, n, ok)
		}
	}
	if _, ok := ackSeq("_INBOX.x.1"); ok {
		t.Fatal("an inbox subject read as an ack")
	}
}

func TestNATSRead(t *testing.T) {
	in := "PING\r\nMSG a.b 1 _INBOX.x.2 5\r\nhello\r\nHMSG _INBOX.x.3 1 32 32\r\nNATS/1.0 408 Request Timeout\r\n\r\n\r\n" +
		"HMSG s 1 r 18 21\r\nNATS/1.0\r\nK: v\r\n\r\nabc\r\n-ERR 'Authorization Violation'\r\n"
	var out bytes.Buffer
	nc := &natsConn{r: bufio.NewReader(strings.NewReader(in)), w: bufio.NewWriter(&out)}
	_, m, err := nc.read()
	if err != nil || m.subject != "a.b" || m.reply != "_INBOX.x.2" || string(m.data) != "hello" || out.String() != "PONG\r\n" {
		t.Fatalf("%+v %v %q", m, err, out.String())
	}
	if _, m, err = nc.read(); err != nil || m.status != "408" || len(m.data) != 0 {
		t.Fatalf("%+v %v", m, err)
	}
	if _, m, err = nc.read(); err != nil || m.status != "" || string(m.data) != "abc" {
		t.Fatalf("%+v %v", m, err)
	}
	if _, _, err = nc.read(); err == nil || !strings.Contains(err.Error(), "Authorization") {
		t.Fatalf("-ERR: %v", err)
	}
}

func TestRESPRead(t *testing.T) {
	in := "+OK\r\n:42\r\n$3\r\nabc\r\n$-1\r\n*2\r\n$1\r\na\r\n*1\r\n:1\r\n-ERR nope\r\n$99999999999\r\n"
	rc := &redisConn{r: bufio.NewReader(strings.NewReader(in))}
	want := []string{"OK", "42", "abc", "<nil>", "[a [1]]", "ERR nope"}
	for _, w := range want {
		v, err := rc.read()
		if err != nil {
			t.Fatal(err)
		}
		got := fmtV(v)
		if got != w {
			t.Fatalf("got %q, want %q", got, w)
		}
	}
	if _, err := rc.read(); err == nil {
		t.Fatal("an absurd bulk length was read")
	}
}

func fmtV(v interface{}) string {
	switch x := v.(type) {
	case nil:
		return "<nil>"
	case []interface{}:
		var parts []string
		for _, y := range x {
			parts = append(parts, fmtV(y))
		}
		return "[" + strings.Join(parts, " ") + "]"
	case redisError:
		return string(x)
	case int64:
		return strconv.FormatInt(x, 10)
	case string:
		return x
	}
	return "?"
}
