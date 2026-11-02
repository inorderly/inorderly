// Copyright Inorderly.com 2026
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/inorderly/inorderly"
	"github.com/inorderly/inorderly/internal/broker"
	"github.com/inorderly/inorderly/internal/scenario"
)

// The broker tests run against real servers named by environment variables:
// INORDERLY_TEST_REDIS (host:port), INORDERLY_TEST_NATS (host:port, with
// JetStream on) and INORDERLY_TEST_KAFKA (host:port of a broker that
// creates topics on first use). Each is skipped when its variable is unset.

type brokerKind struct {
	name   string
	env    string
	scheme string
}

var brokerKinds = []brokerKind{
	{"redis", "INORDERLY_TEST_REDIS", "redis"},
	{"nats", "INORDERLY_TEST_NATS", "nats"},
	{"kafka", "INORDERLY_TEST_KAFKA", "kafka"},
}

// eachBroker runs f once for each broker that is configured.
func eachBroker(t *testing.T, f func(t *testing.T, newURL func(string) string)) {
	for _, k := range brokerKinds {
		t.Run(k.name, func(t *testing.T) {
			host := os.Getenv(k.env)
			if host == "" {
				t.Skipf("%s is not set", k.env)
			}
			n := 0
			stamp := time.Now().UnixNano()
			f(t, func(base string) string {
				n++
				u := fmt.Sprintf("%s://%s/it-%s-%d-%d", k.scheme, host, base, stamp, n)
				e, err := broker.Parse(u)
				if err == nil {
					err = broker.Ensure(context.Background(), e)
				}
				if err != nil {
					t.Fatal(err)
				}
				return u
			})
		})
	}
}

func produce(t *testing.T, url string, lines [][]byte) {
	t.Helper()
	e, err := broker.Parse(url)
	if err == nil {
		err = broker.Produce(context.Background(), e, lines)
	}
	if err != nil {
		t.Fatal(err)
	}
}

func readAll(t *testing.T, url string) []byte {
	t.Helper()
	e, err := broker.Parse(url)
	var vals [][]byte
	if err == nil {
		vals, err = broker.ReadAll(context.Background(), e)
	}
	if err != nil {
		t.Fatal(err)
	}
	var b bytes.Buffer
	for _, v := range vals {
		b.Write(v)
		b.WriteByte('\n')
	}
	return b.Bytes()
}

func lines(b []byte) [][]byte {
	var out [][]byte
	for _, l := range bytes.Split(bytes.TrimSuffix(b, []byte("\n")), []byte("\n")) {
		if len(l) > 0 {
			out = append(out, l)
		}
	}
	return out
}

// brokerSet is a shuffled event set with late events, split over two inputs.
func brokerSet(events int) (a, b [][]byte) {
	set := scenario.Generate(scenario.Params{Seed: 11, Events: events, Producers: 5, Hold: 100})
	order := scenario.Arrive(set.Events, 100, 3, 21)
	order, _ = scenario.AddLate(order, 100, 300, 7)
	for i, e := range order {
		x, _ := json.Marshal(e)
		if i%2 == 0 {
			a = append(a, x)
		} else {
			b = append(b, x)
		}
	}
	a = append(a, []byte("not json"))
	return a, b
}

func joinLines(ls [][]byte) []byte {
	var b bytes.Buffer
	for _, l := range ls {
		b.Write(l)
		b.WriteByte('\n')
	}
	return b.Bytes()
}

// Reading from brokers gives what reading the same lines from files gives,
// and writing to a broker stores the same lines a file gets.
func TestBrokerInAndOut(t *testing.T) {
	eachBroker(t, func(t *testing.T, newURL func(string) string) {
		dir := t.TempDir()
		a, b := brokerSet(3000)
		fa := write(t, dir, "a.ndjson", joinLines(a))
		fb := write(t, dir, "b.ndjson", joinLines(b))
		ref := runCLI(t, nil, "-in", fa, "-in", fb, "-window", "100")
		if ref.code != 0 {
			t.Fatal(string(ref.stderr))
		}
		ua, ub, uo := newURL("a"), newURL("b"), newURL("out")
		produce(t, ua, a)
		produce(t, ub, b)
		got := runCLI(t, nil, "-in", ua, "-in", ub, "-window", "100", "-rejects", filepath.Join(dir, "rej"))
		if got.code != 0 {
			t.Fatal(string(got.stderr))
		}
		if !bytes.Equal(got.stdout, ref.stdout) || got.sum.Hash != ref.sum.Hash || got.sum.Invalid != 1 || got.sum.Late != ref.sum.Late {
			t.Fatalf("broker inputs gave %s, files %s", got.stderr, ref.stderr)
		}
		rej, _ := os.ReadFile(filepath.Join(dir, "rej"))
		if !bytes.Contains(rej, []byte(`"at":`)) || !bytes.Contains(rej, []byte(`"reason":"invalid"`)) {
			t.Fatalf("rejects: %s", rej)
		}
		out := runCLI(t, nil, "-in", fa, "-in", fb, "-window", "100", "-out", uo)
		if out.code != 0 {
			t.Fatal(string(out.stderr))
		}
		if stored := readAll(t, uo); !bytes.Equal(stored, ref.stdout) {
			t.Fatalf("the output stream holds %d bytes, the file %d", len(stored), len(ref.stdout))
		}
		// A second run without -state refuses to add to it.
		again := runCLI(t, nil, "-in", fa, "-out", uo)
		if again.code == 0 || !bytes.Contains(again.stderr, []byte("already holds lines")) {
			t.Fatalf("a full output was written again: %s", again.stderr)
		}
	})
}

// A run from two broker inputs to a broker output, killed with SIGKILL again
// and again, ends with the same output stream as a run never interrupted.
func TestBrokerKilledAndResumed(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("SIGKILL is a Unix test")
	}
	eachBroker(t, func(t *testing.T, newURL func(string) string) {
		dir := t.TempDir()
		a, b := brokerSet(20000)
		fa := write(t, dir, "a.ndjson", joinLines(a))
		fb := write(t, dir, "b.ndjson", joinLines(b))
		ref := runCLI(t, nil, "-in", fa, "-in", fb, "-window", "100", "-remember", "2000")
		if ref.code != 0 {
			t.Fatal(string(ref.stderr))
		}
		ua, ub, uo := newURL("a"), newURL("b"), newURL("out")
		produce(t, ua, a)
		produce(t, ub, b)
		state := filepath.Join(dir, "state")
		args := []string{"-in", ua, "-in", ub, "-out", uo, "-window", "100", "-remember", "2000",
			"-state", state, "-checkpoint-every", "499"}
		cpFile := filepath.Join(state, "checkpoint.json")
		stamp := func() time.Time {
			if st, err := os.Stat(cpFile); err == nil {
				return st.ModTime()
			}
			return time.Time{}
		}
		rng := rand.New(rand.NewPCG(3, 4))
		kills := 0
		var last result
		for attempt := 0; ; attempt++ {
			before := stamp()
			cmd := exec.Command(bin, args...)
			var errb bytes.Buffer
			cmd.Stderr = &errb
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- cmd.Wait() }()
			killed := false
			if kills < 10 {
			wait:
				for {
					select {
					case err := <-done:
						done <- err
						break wait
					case <-time.After(2 * time.Millisecond):
						if !stamp().Equal(before) {
							time.Sleep(time.Duration(rng.IntN(30)) * time.Millisecond)
							killed = cmd.Process.Signal(syscall.SIGKILL) == nil
							break wait
						}
					}
				}
			}
			err := <-done
			if killed && err != nil {
				kills++
				continue
			}
			if err != nil {
				t.Fatalf("run %d failed: %v %s", attempt, err, errb.String())
			}
			last = result{stderr: errb.Bytes()}
			json.Unmarshal(last.stderr, &last.sum)
			break
		}
		if stored := readAll(t, uo); !bytes.Equal(stored, ref.stdout) {
			t.Fatalf("after %d kills the output stream differs (%d vs %d bytes)", kills, len(stored), len(ref.stdout))
		}
		if last.sum.Hash != ref.sum.Hash || !last.sum.Resumed {
			t.Fatalf("summary %s, want hash %s", last.stderr, ref.sum.Hash)
		}
		if kills < 5 {
			t.Fatalf("only %d kills", kills)
		}
		t.Logf("%d kills, %d ordered", kills, ref.sum.Ordered)
	})
}

// Following: a run reads what comes as it comes, stops on SIGTERM with a
// checkpoint, and the next run carries on. The output is always a prefix of
// what one bounded run over everything writes.
func TestBrokerFollow(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("signals are a Unix test")
	}
	eachBroker(t, func(t *testing.T, newURL func(string) string) {
		dir := t.TempDir()
		a, b := brokerSet(4000)
		var all [][]byte
		for i := range a {
			all = append(all, a[i])
			if i < len(b) {
				all = append(all, b[i])
			}
		}
		fa := write(t, dir, "all.ndjson", joinLines(all))
		ref := runCLI(t, nil, "-in", fa, "-window", "100")
		if ref.code != 0 {
			t.Fatal(string(ref.stderr))
		}
		ui, uo := newURL("in"), newURL("out")
		state := filepath.Join(dir, "state")
		args := []string{"-in", ui, "-out", uo, "-window", "100", "-state", state, "-follow", "-checkpoint-interval", "100ms"}
		half := len(all) / 2
		produce(t, ui, all[:half])
		session := func(more [][]byte) result {
			cmd := exec.Command(bin, args...)
			var errb bytes.Buffer
			cmd.Stderr = &errb
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			if more != nil {
				time.Sleep(300 * time.Millisecond)
				produce(t, ui, more)
			}
			// Wait until the output stops growing, then stop the run.
			prev := -1
			for i := 0; i < 100; i++ {
				time.Sleep(200 * time.Millisecond)
				n := len(readAll(t, uo))
				if n == prev && n > 0 {
					break
				}
				prev = n
			}
			cmd.Process.Signal(syscall.SIGTERM)
			err := cmd.Wait()
			r := result{stderr: errb.Bytes()}
			if err != nil {
				t.Fatalf("the followed run failed: %v %s", err, r.stderr)
			}
			json.Unmarshal(r.stderr, &r.sum)
			if !r.sum.Stopped {
				t.Fatalf("summary %s", r.stderr)
			}
			return r
		}
		session(nil)
		first := readAll(t, uo)
		if len(first) == 0 || !bytes.HasPrefix(ref.stdout, first) {
			t.Fatalf("the first session wrote %d bytes, not a prefix of the reference", len(first))
		}
		r := session(all[half:])
		second := readAll(t, uo)
		if len(second) <= len(first) || !bytes.HasPrefix(ref.stdout, second) || !r.sum.Resumed {
			t.Fatalf("the second session wrote %d bytes (first %d): %s", len(second), len(first), strings.ReplaceAll(string(r.stderr), "\n", " "))
		}
		// Everything not yet released is still held: the reference has more,
		// within the window of the newest time.
		if n := len(lines(second)); uint64(n) >= ref.sum.Ordered {
			t.Fatalf("a followed run released everything (%d), as if it had flushed", n)
		}
	})
}

// The line a broker output holds at its last pos must be the one the run
// makes; another writer is refused.
func TestBrokerOutputWithAnotherWriter(t *testing.T) {
	eachBroker(t, func(t *testing.T, newURL func(string) string) {
		uo := newURL("foreign")
		produce(t, uo, [][]byte{[]byte(`{"hello":1}`)})
		dir := t.TempDir()
		in := write(t, dir, "in.ndjson", []byte(`{"producer":"p","seq":1,"time":1}`+"\n"))
		r := runCLI(t, nil, "-in", in, "-out", uo, "-state", filepath.Join(dir, "s"))
		if r.code == 0 || !strings.Contains(string(r.stderr), "Inorderly") {
			t.Fatalf("wrote after a foreign message: %s", r.stderr)
		}
	})
}

// A Kafka input of three partitions: two bounded runs read the same lines in
// the same order, and with a window wide enough that nothing is late, the
// output is the one the same lines give from a file.
func TestKafkaPartitions(t *testing.T) {
	host := os.Getenv("INORDERLY_TEST_KAFKA")
	if host == "" {
		t.Skip("INORDERLY_TEST_KAFKA is not set")
	}
	u := fmt.Sprintf("kafka://%s/it-parts-%d", host, time.Now().UnixNano())
	e, err := broker.Parse(u)
	if err == nil {
		err = broker.EnsureTopic(context.Background(), e, 3)
	}
	if err != nil {
		t.Fatal(err)
	}
	a, b := brokerSet(3000)
	all := append(a[:len(a)-1], b...)
	produce(t, u, all)
	dir := t.TempDir()
	file := write(t, dir, "all.ndjson", joinLines(all))
	ref := runCLI(t, nil, "-in", file, "-window", "1000000000")
	one := runCLI(t, nil, "-in", u, "-window", "1000000000")
	two := runCLI(t, nil, "-in", u, "-window", "1000000000")
	if ref.code != 0 || one.code != 0 || two.code != 0 {
		t.Fatalf("%s %s %s", ref.stderr, one.stderr, two.stderr)
	}
	if !bytes.Equal(one.stdout, two.stdout) || !bytes.Equal(one.stdout, ref.stdout) || one.sum.In != uint64(len(all)) {
		t.Fatalf("partitioned input: %s, file: %s", one.stderr, ref.stderr)
	}
	// An output topic needs one partition.
	r := runCLI(t, nil, "-in", file, "-out", u)
	if r.code == 0 || !bytes.Contains(r.stderr, []byte("needs one")) {
		t.Fatalf("wrote to a topic of 3 partitions: %s", r.stderr)
	}
}

var _ = inorderly.Version

// After a checkpoint, the output must go on from the checkpoint's pos with no
// gap, and each stored line must be the one the run makes. A run stopped with
// SIGTERM is restarted after a line is planted in its output: a gap, or a
// different line at the next pos. Both are refused.
func TestBrokerOutputCheckedOnRestart(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("signals are a Unix test")
	}
	eachBroker(t, func(t *testing.T, newURL func(string) string) {
		for _, tc := range []struct{ name, plant, want string }{
			{"gap", `{"pos":%d,"producer":"x","seq":1,"time":1}`, "missing or there twice"},
			{"different", `{"pos":%d,"producer":"x","seq":1,"time":1}`, "different line"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				dir := t.TempDir()
				a, _ := brokerSet(2000)
				a = a[:len(a)-1]
				ui, uo := newURL("in"), newURL("out")
				state := filepath.Join(dir, "state")
				args := []string{"-in", ui, "-out", uo, "-window", "100", "-state", state, "-follow"}
				produce(t, ui, a[:1000])
				cmd := exec.Command(bin, args...)
				var errb bytes.Buffer
				cmd.Stderr = &errb
				if err := cmd.Start(); err != nil {
					t.Fatal(err)
				}
				for i := 0; i < 100 && len(readAll(t, uo)) == 0; i++ {
					time.Sleep(100 * time.Millisecond)
				}
				cmd.Process.Signal(syscall.SIGTERM)
				if err := cmd.Wait(); err != nil {
					t.Fatalf("%v %s", err, errb.String())
				}
				var cp checkpoint
				b, _ := os.ReadFile(filepath.Join(state, "checkpoint.json"))
				if json.Unmarshal(b, &cp) != nil || cp.OutPos == 0 || cp.OutMark == "" {
					t.Fatalf("checkpoint: out_pos %d, out_mark %q", cp.OutPos, cp.OutMark)
				}
				next := cp.OutPos + 1
				if tc.name == "gap" {
					next++
				}
				produce(t, uo, [][]byte{[]byte(fmt.Sprintf(tc.plant, next))})
				produce(t, ui, a[1000:])
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()
				r := exec.CommandContext(ctx, bin, args...)
				out, _ := r.CombinedOutput()
				if r.ProcessState == nil || r.ProcessState.ExitCode() != 1 || !strings.Contains(string(out), tc.want) {
					t.Fatalf("restart after a planted line: %v %s", r.ProcessState, out)
				}
			})
		}
	})
}
