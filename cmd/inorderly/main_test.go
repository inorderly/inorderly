// Copyright Inorderly.com 2026
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
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
	"github.com/inorderly/inorderly/internal/scenario"
)

var bin string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "inorderly-test-")
	if err != nil {
		panic(err)
	}
	bin = filepath.Join(dir, "inorderly")
	if runtime.GOOS == "windows" {
		bin += ".exe"
	}
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		panic(string(out))
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

type result struct {
	stdout, stderr []byte
	code           int
	sum            summary
}

func runCLI(t *testing.T, stdin []byte, args ...string) result {
	t.Helper()
	cmd := exec.Command(bin, args...)
	cmd.Stdin = bytes.NewReader(stdin)
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	err := cmd.Run()
	r := result{stdout: out.Bytes(), stderr: errb.Bytes()}
	if ee, ok := err.(*exec.ExitError); ok {
		r.code = ee.ExitCode()
	} else if err != nil {
		t.Fatal(err)
	}
	if r.code == 0 || r.code == 3 {
		json.Unmarshal(r.stderr, &r.sum)
	}
	return r
}

func ndjson(events []inorderly.Event) []byte {
	var b bytes.Buffer
	for _, e := range events {
		x, _ := json.Marshal(e)
		b.Write(x)
		b.WriteByte('\n')
	}
	return b.Bytes()
}

func sha(b []byte) string {
	s := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(s[:])
}

func write(t *testing.T, dir, name string, b []byte) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, b, 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestShuffledInputsGiveTheSameOutput(t *testing.T) {
	set := scenario.Generate(scenario.Params{Seed: 3, Events: 5000, Producers: 4, Hold: 50})
	ref := runCLI(t, ndjson(scenario.Arrive(set.Events, 0, 1, 1)), "-window", "50")
	if ref.code != 0 || ref.sum.Ordered != 5000 {
		t.Fatalf("code %d, %s", ref.code, ref.stderr)
	}
	if ref.sum.Hash != sha(ref.stdout) {
		t.Fatal("the summary hash isn't the SHA-256 of the output")
	}
	for seed := uint64(10); seed < 15; seed++ {
		r := runCLI(t, ndjson(scenario.Arrive(set.Events, 50, 4, seed)), "-window", "50")
		if r.code != 0 || !bytes.Equal(r.stdout, ref.stdout) || r.sum.Duplicates == 0 {
			t.Fatalf("seed %d: code %d, %d duplicates, output differs: %v", seed, r.code, r.sum.Duplicates, !bytes.Equal(r.stdout, ref.stdout))
		}
	}
}

func TestLineEndingsBlankLinesAndNoFinalNewline(t *testing.T) {
	in := "{\"id\":\"b\",\"producer\":\"p\",\"time\":2}\r\n\r\n   \n{\"id\":\"a\",\"producer\":\"p\",\"time\":1}"
	r := runCLI(t, []byte(in), "-window", "5")
	want := "{\"pos\":1,\"id\":\"a\",\"producer\":\"p\",\"time\":1}\n{\"pos\":2,\"id\":\"b\",\"producer\":\"p\",\"time\":2}\n"
	if r.code != 0 || string(r.stdout) != want {
		t.Fatalf("code %d, got %q", r.code, r.stdout)
	}
}

func TestRejectsAndStrict(t *testing.T) {
	dir := t.TempDir()
	in := strings.Join([]string{
		`{"id":"a","producer":"p","time":100}`,
		`not json`,
		`{"id":"late","producer":"p","time":1}`,
		`{"id":"a","producer":"p","time":100,"data":1}`,
		`{"id":"a","producer":"p","time":100}`,
		`{"id":"x","producer":"p","time":100,"extra":true}`,
	}, "\n")
	rej := filepath.Join(dir, "rejects.ndjson")
	r := runCLI(t, []byte(in), "-window", "10", "-rejects", rej)
	if r.code != 0 || r.sum.Invalid != 2 || r.sum.Late != 1 || r.sum.Conflicts != 1 || r.sum.Duplicates != 1 || r.sum.Ordered != 1 || r.sum.In != 6 {
		t.Fatalf("code %d, summary %s", r.code, r.stderr)
	}
	b, _ := os.ReadFile(rej)
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	var reasons []string
	for _, l := range lines {
		var x reject
		if err := json.Unmarshal([]byte(l), &x); err != nil {
			t.Fatal(err)
		}
		reasons = append(reasons, fmt.Sprintf("%s@%d", x.Reason, x.Line))
	}
	if strings.Join(reasons, " ") != "invalid@2 late@3 conflict@4 invalid@6" {
		t.Fatalf("rejects %v", reasons)
	}
	if r := runCLI(t, []byte(in), "-window", "10", "-strict"); r.code != 3 {
		t.Fatalf("-strict exited %d", r.code)
	}
	if r := runCLI(t, []byte(in[:37]), "-strict"); r.code != 0 {
		t.Fatalf("-strict on clean input exited %d: %s", r.code, r.stderr)
	}
}

func TestOverLongLineIsRejected(t *testing.T) {
	long := `{"id":"big","producer":"p","time":1,"data":"` + strings.Repeat("x", inorderly.MaxLine) + `"}`
	in := []byte(long + "\n" + `{"id":"a","producer":"p","time":2}` + "\n")
	r := runCLI(t, in)
	if r.code != 0 || r.sum.Invalid != 1 || r.sum.Ordered != 1 {
		t.Fatalf("code %d %s", r.code, r.stderr)
	}
}

// Two copies of one tick feed, each missing ticks, read a line at a time in
// turn: every tick that reached either copy comes out once, in order, and
// the ticks lost from both are reported as gaps.
func TestABFeedMerge(t *testing.T) {
	dir := t.TempDir()
	feeds := scenario.MakeFeeds(20000, 5, 9)
	a := write(t, dir, "a.ndjson", ndjson(feeds.A))
	b := write(t, dir, "b.ndjson", ndjson(feeds.B))
	out := filepath.Join(dir, "out.ndjson")
	r := runCLI(t, nil, "-in", a, "-in", b, "-out", out, "-window", "1000")
	if r.code != 0 {
		t.Fatalf("code %d %s", r.code, r.stderr)
	}
	want := scenario.ExpectedGaps(20000, feeds.Lost)
	if r.sum.Ordered != uint64(20000-len(feeds.Lost)) || r.sum.Late != 0 || len(r.sum.Gaps) != len(want) || r.sum.SeqBackwards != 0 {
		t.Fatalf("summary %s, want %d gaps", r.stderr, len(want))
	}
	for i := range want {
		if r.sum.Gaps[i] != want[i] {
			t.Fatalf("gap %d: %+v, want %+v", i, r.sum.Gaps[i], want[i])
		}
	}
	// B first gives the same output.
	out2 := filepath.Join(dir, "out2.ndjson")
	if r := runCLI(t, nil, "-in", b, "-in", a, "-out", out2, "-window", "1000"); r.code != 0 {
		t.Fatal(string(r.stderr))
	}
	x, _ := os.ReadFile(out)
	y, _ := os.ReadFile(out2)
	if !bytes.Equal(x, y) {
		t.Fatal("reading B first changed the output")
	}
}

func TestStateFlags(t *testing.T) {
	dir := t.TempDir()
	in := write(t, dir, "in.ndjson", []byte(`{"id":"a","producer":"p","time":1}`+"\n"))
	if r := runCLI(t, nil, "-state", filepath.Join(dir, "s")); r.code == 0 {
		t.Fatal("-state with standard input ran")
	}
	if r := runCLI(t, nil, "-in", in, "-state", filepath.Join(dir, "s")); r.code == 0 {
		t.Fatal("-state with standard output ran")
	}
	out := filepath.Join(dir, "out.ndjson")
	state := filepath.Join(dir, "state")
	first := runCLI(t, nil, "-in", in, "-out", out, "-state", state)
	if first.code != 0 {
		t.Fatal(string(first.stderr))
	}
	// Starting a finished run again reports it and changes nothing.
	again := runCLI(t, nil, "-in", in, "-out", out, "-state", state)
	if again.code != 0 || again.sum.Hash != first.sum.Hash {
		t.Fatalf("rerun: %d %s", again.code, again.stderr)
	}
	if r := runCLI(t, nil, "-in", in, "-out", out, "-state", state, "-window", "9"); r.code == 0 || !strings.Contains(string(r.stderr), "other inputs, outputs or settings") {
		t.Fatalf("a changed window ran on the old checkpoint: %s", r.stderr)
	}
	if r := runCLI(t, nil, "-nope"); r.code != 2 {
		t.Fatalf("an unknown flag exited %d", r.code)
	}
	if r := runCLI(t, nil, "-in", "-", "-in", "-"); r.code == 0 {
		t.Fatal("standard input twice ran")
	}
}

func TestVersion(t *testing.T) {
	r := runCLI(t, nil, "version")
	var v map[string]string
	if r.code != 0 || json.Unmarshal(r.stdout, &v) != nil || v["version"] != inorderly.Version {
		t.Fatalf("%d %s", r.code, r.stdout)
	}
}

// The process is killed with SIGKILL again and again, at random moments,
// and started again each time with the same flags. The output must end up
// byte for byte the same as one run straight through.
func TestKilledAndResumed(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("SIGKILL is a Unix test")
	}
	dir := t.TempDir()
	set := scenario.Generate(scenario.Params{Seed: 5, Events: 120000, Producers: 8, Hold: 100})
	order := scenario.Arrive(set.Events, 100, 3, 77)
	order, _ = scenario.AddLate(order, 100, 300, 5)
	// Alternate lines go to two files, so reading them in turn gives the
	// arrival order back.
	var ea, eb []inorderly.Event
	for i, e := range order {
		if i%2 == 0 {
			ea = append(ea, e)
		} else {
			eb = append(eb, e)
		}
	}
	a := write(t, dir, "a.ndjson", append(ndjson(ea), []byte("garbage line\n")...))
	b := write(t, dir, "b.ndjson", ndjson(eb))
	flags := func(out, state string) []string {
		return []string{"-in", a, "-in", b, "-out", out, "-rejects", out + ".rejects", "-window", "100", "-state", state, "-checkpoint-every", "997"}
	}
	clean := filepath.Join(dir, "clean.ndjson")
	ref := runCLI(t, nil, flags(clean, filepath.Join(dir, "clean-state"))...)
	if ref.code != 0 {
		t.Fatal(string(ref.stderr))
	}
	out := filepath.Join(dir, "killed.ndjson")
	state := filepath.Join(dir, "killed-state")
	rng := rand.New(rand.NewPCG(1, 2))
	kills := 0
	var last result
	for attempt := 0; ; attempt++ {
		cmd := exec.Command(bin, flags(out, state)...)
		var errb bytes.Buffer
		cmd.Stderr = &errb
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- cmd.Wait() }()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("run %d failed: %v %s", attempt, err, errb.String())
			}
			last = result{stderr: errb.Bytes()}
			json.Unmarshal(last.stderr, &last.sum)
		case <-time.After(killAfter(rng, kills)):
			cmd.Process.Signal(syscall.SIGKILL)
			<-done
			kills++
			continue
		}
		break
	}
	x, _ := os.ReadFile(clean)
	y, _ := os.ReadFile(out)
	if !bytes.Equal(x, y) {
		t.Fatalf("after %d kills the output differs (%d vs %d bytes)", kills, len(x), len(y))
	}
	xr, _ := os.ReadFile(clean + ".rejects")
	yr, _ := os.ReadFile(out + ".rejects")
	if !bytes.Equal(bytes.ReplaceAll(xr, []byte("clean"), []byte("killed")), yr) {
		t.Fatal("the rejects differ")
	}
	if last.sum.Hash != ref.sum.Hash || last.sum.Hash != sha(y) || last.sum.Invalid != 1 || last.sum.Late != ref.sum.Late {
		t.Fatalf("summary %s, want hash %s", last.stderr, ref.sum.Hash)
	}
	if kills < 3 {
		t.Fatalf("only %d kills; the test proves nothing", kills)
	}
	t.Logf("%d kills, %d events, %d ordered, hash %s", kills, len(order), ref.sum.Ordered, ref.sum.Hash)
	if os.Getenv("INORDERLY_RECORD") != "" {
		fmt.Printf("RECORD kill_resume kills=%d lines=%d ordered=%d late=%d identical=true\n", kills, len(order)+1, ref.sum.Ordered, ref.sum.Late)
	}
}

// killAfter picks when to kill the next run: a random moment in its first
// 400 ms for the first 30 runs, then never, so the last run finishes.
func killAfter(rng *rand.Rand, kills int) time.Duration {
	if kills >= 30 {
		return time.Hour
	}
	return time.Duration(20+rng.IntN(380)) * time.Millisecond
}

// A producer written in Python, with its own JSON encoder, key order and
// escapes, gives the same output from every arrival order.
func TestPythonProducer(t *testing.T) {
	py, err := exec.LookPath("python3")
	if err != nil {
		if py, err = exec.LookPath("python"); err != nil {
			t.Skip("no Python")
		}
	}
	script := filepath.Join("..", "..", "examples", "python", "producer.py")
	gen := func(args ...string) []byte {
		out, err := exec.Command(py, append([]string{script, "--events", "20000", "--window", "500"}, args...)...).Output()
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	ref := runCLI(t, gen("--sorted"), "-window", "500")
	if ref.code != 0 || ref.sum.Ordered != 20000 {
		t.Fatalf("%d %s", ref.code, ref.stderr)
	}
	for _, seed := range []string{"1", "2", "3", "4"} {
		r := runCLI(t, gen("--seed", seed), "-window", "500")
		if r.code != 0 || !bytes.Equal(r.stdout, ref.stdout) || r.sum.Duplicates == 0 || r.sum.Conflicts != 0 {
			t.Fatalf("seed %s: %d %s", seed, r.code, r.stderr)
		}
	}
	if os.Getenv("INORDERLY_RECORD") != "" {
		fmt.Printf("RECORD python_producer orders=4 events=20000 identical=true hash=%s\n", ref.sum.Hash)
	}
}
