// Copyright Inorderly.com 2026
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
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
	"github.com/inorderly/inorderly/internal/usecase"
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
		return []string{"-in", a, "-in", b, "-out", out, "-rejects", out + ".rejects", "-window", "100",
			"-remember", "2000", "-state", state, "-checkpoint-every", "997"}
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
	cpFile := filepath.Join(state, "checkpoint.json")
	stamp := func() time.Time {
		if st, err := os.Stat(cpFile); err == nil {
			return st.ModTime()
		}
		return time.Time{}
	}
	for attempt := 0; ; attempt++ {
		before := stamp()
		cmd := exec.Command(bin, flags(out, state)...)
		var errb bytes.Buffer
		cmd.Stderr = &errb
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- cmd.Wait() }()
		// The first 30 runs are killed at a random moment after each has
		// written a new checkpoint, so every run makes some progress and the
		// kills land anywhere between checkpoints. The last run finishes.
		killed := false
		if kills < 30 {
			for !killed {
				select {
				case err := <-done:
					done <- err
				case <-time.After(2 * time.Millisecond):
					if !stamp().Equal(before) {
						time.Sleep(time.Duration(rng.IntN(40)) * time.Millisecond)
						killed = cmd.Process.Signal(syscall.SIGKILL) == nil
					}
					continue
				}
				break
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
	// With -remember, the record log was compacted along the way: one log
	// is left, much smaller than one record per event.
	logs, _ := filepath.Glob(filepath.Join(state, "released-*.ndjson"))
	if len(logs) != 1 {
		t.Fatalf("%d record logs left: %v", len(logs), logs)
	}
	if st, _ := os.Stat(logs[0]); st == nil || st.Size() > 3<<20 || logs[0] == filepath.Join(state, "released-1.ndjson") {
		t.Fatalf("the record log %v wasn't compacted", logs)
	}
	if kills != 30 {
		t.Fatalf("%d kills, want 30", kills)
	}
	t.Logf("%d kills, %d events, %d ordered, hash %s", kills, len(order), ref.sum.Ordered, ref.sum.Hash)
	if os.Getenv("INORDERLY_RECORD") != "" {
		fmt.Printf("RECORD kill_resume kills=%d lines=%d ordered=%d late=%d identical=true\n", kills, len(order)+1, ref.sum.Ordered, ref.sum.Late)
	}
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

func TestPathsAndFlagsChecked(t *testing.T) {
	dir := t.TempDir()
	in := write(t, dir, "in.ndjson", []byte(`{"id":"a","producer":"p","time":1}`+"\n"))
	for _, args := range [][]string{
		{"-in", in, "-out", in},
		{"-in", in, "-out", filepath.Join(dir, "o"), "-rejects", filepath.Join(dir, "o")},
		{"-in", in, "-rejects", in},
	} {
		if r := runCLI(t, nil, args...); r.code != 1 || !strings.Contains(string(r.stderr), "same file") && !strings.Contains(string(r.stderr), "both an input and an output") {
			t.Fatalf("%v: %d %s", args, r.code, r.stderr)
		}
	}
	for _, bad := range [][]string{{"-window", "-1"}, {"-remember", "-5"}, {"-hold", "-2"}} {
		if r := runCLI(t, nil, bad...); r.code != 2 {
			t.Fatalf("%v exited %d", bad, r.code)
		}
	}
}

func TestByteOrderMark(t *testing.T) {
	r := runCLI(t, []byte("\xef\xbb\xbf"+`{"id":"a","producer":"p","time":1}`+"\n"))
	if r.code != 0 || r.sum.Ordered != 1 || r.sum.Invalid != 0 {
		t.Fatalf("%d %s", r.code, r.stderr)
	}
}

func TestResumeRefusesChangedFiles(t *testing.T) {
	dir := t.TempDir()
	var lines []byte
	for i := 0; i < 50; i++ {
		lines = append(lines, fmt.Sprintf(`{"id":"e%d","producer":"p","time":%d}`+"\n", i, i)...)
	}
	in := write(t, dir, "in.ndjson", lines)
	out := filepath.Join(dir, "out.ndjson")
	state := filepath.Join(dir, "state")
	args := []string{"-in", in, "-out", out, "-state", state, "-checkpoint-every", "10"}
	if r := runCLI(t, nil, args...); r.code != 0 {
		t.Fatal(string(r.stderr))
	}
	// A finished run whose output was since changed is refused.
	os.WriteFile(out, []byte("tampered\n"), 0o644)
	if r := runCLI(t, nil, args...); r.code == 0 || !strings.Contains(string(r.stderr), "has changed") {
		t.Fatalf("%d %s", r.code, r.stderr)
	}
	// An unfinished checkpoint over an input that was since rewritten is
	// refused rather than carried on in the wrong place.
	os.RemoveAll(state)
	cp := checkpoint{}
	if r := runCLI(t, nil, args...); r.code != 0 {
		t.Fatal(string(r.stderr))
	}
	b, _ := os.ReadFile(filepath.Join(state, "checkpoint.json"))
	json.Unmarshal(b, &cp)
	cp.Finished, cp.Summary = false, nil
	cp.Engine = &inorderly.Snapshot{Format: 1, Partial: true}
	cp.Offsets, cp.Lines, cp.Done = []int64{0}, []uint64{0}, []bool{false}
	b, _ = json.Marshal(cp)
	os.WriteFile(filepath.Join(state, "checkpoint.json"), b, 0o644)
	time.Sleep(20 * time.Millisecond)
	os.WriteFile(in, append(lines, lines[:40]...), 0o644)
	if r := runCLI(t, nil, args...); r.code == 0 || !strings.Contains(string(r.stderr), "has changed since the checkpoint") {
		t.Fatalf("%d %s", r.code, r.stderr)
	}
}

// The use-case examples' files, ordered by the command exactly as the
// README's commands say, give the same output as the library, and every
// arrival order of one example gives the same output.
func TestUseCasesThroughTheCommand(t *testing.T) {
	if testing.Short() {
		t.Skip("the use cases take a few seconds")
	}
	dir := t.TempDir()
	for _, c := range usecase.All() {
		t.Run(c.Name, func(t *testing.T) {
			if err := c.Write(dir); err != nil {
				t.Fatal(err)
			}
			want, err := c.Order(c.Inputs[0])
			if err != nil {
				t.Fatal(err)
			}
			for _, line := range c.Commands() {
				args := append(strings.Fields(line)[1:], "-stats", "stats.json")
				cmd := exec.Command(bin, args...)
				cmd.Dir = filepath.Join(dir, c.Name)
				if out, err := cmd.CombinedOutput(); err != nil {
					t.Fatalf("%s: %v\n%s", line, err, out)
				}
				var sum summary
				b, err := os.ReadFile(filepath.Join(cmd.Dir, "stats.json"))
				if err == nil {
					err = json.Unmarshal(b, &sum)
				}
				if err != nil {
					t.Fatal(err)
				}
				if sum.Hash != want.Hash || sum.Ordered != want.Stats.Out || sum.Late != want.Stats.Late {
					t.Fatalf("%s: hash %s, %d ordered, %d late; the library gave %s, %d, %d",
						line, sum.Hash, sum.Ordered, sum.Late, want.Hash, want.Stats.Out, want.Stats.Late)
				}
			}
		})
	}
}

// A run killed after compaction wrote the next record log, but before a
// checkpoint named it, leaves that file behind. The restarted run lists it
// as left over and then compacts into the same name; the checkpoint that
// names it must not delete it.
func TestCompactionAfterAnInterruptedCompaction(t *testing.T) {
	saved := compactSlack
	defer func() { compactSlack = saved }()
	dir := t.TempDir()
	set := scenario.Generate(scenario.Params{Seed: 7, Events: 3000, Producers: 4, Hold: 20})
	in := write(t, dir, "in.ndjson", ndjson(set.Events))
	cfg := inorderly.Config{Window: 50, Hold: 50, Remember: 20}
	want := runCLI(t, nil, "-in", in, "-window", "50", "-hold", "50", "-remember", "20")
	if want.code != 0 {
		t.Fatalf("exit %d: %s", want.code, want.stderr)
	}
	out, state := filepath.Join(dir, "out.ndjson"), filepath.Join(dir, "state")
	start := func() *runner {
		r, err := newRunner(context.Background(), cfg, []string{in}, out, "", state, false)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	step := func(r *runner, lines int) {
		for i := 0; i < lines; i++ {
			lr, line, ok, err := r.nextLine(context.Background())
			if err != nil || !ok {
				t.Fatalf("line %d: %v %v", i, ok, err)
			}
			if err := r.handle(context.Background(), lr, line); err != nil {
				t.Fatal(err)
			}
		}
		if err := r.checkpoint(context.Background(), false); err != nil {
			t.Fatal(err)
		}
	}

	// The first run checkpoints without compacting, and is "killed" just
	// after writing its next log.
	compactSlack = 1 << 30
	r := start()
	step(r, 500)
	r.close()
	if err := os.WriteFile(filepath.Join(state, "released-2.ndjson"), []byte("{}\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	// The second run compacts at its first checkpoint, into released-2.
	compactSlack = 0
	r = start()
	step(r, 500)
	r.close()
	var cp checkpoint
	b, err := os.ReadFile(filepath.Join(state, "checkpoint.json"))
	if err == nil {
		err = json.Unmarshal(b, &cp)
	}
	if err != nil {
		t.Fatal(err)
	}
	if cp.Log != "released-2.ndjson" {
		t.Fatalf("the checkpoint names %s; the test expects a compaction into released-2", cp.Log)
	}
	st, err := os.Stat(filepath.Join(state, cp.Log))
	if err != nil || st.Size() < cp.LogSize {
		t.Fatalf("the log the checkpoint names is gone or short: %v", err)
	}

	// A third run carries on and finishes with the uninterrupted output.
	r = start()
	if _, err := r.process(context.Background(), 100); err != nil {
		t.Fatal(err)
	}
	r.close()
	got, _ := os.ReadFile(out)
	if !bytes.Equal(got, want.stdout) {
		t.Fatal("the resumed run's output differs from an uninterrupted run")
	}
}
