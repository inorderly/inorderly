// Copyright Inorderly.com 2026
// SPDX-License-Identifier: Apache-2.0

package inorderly

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

func ev(id, producer string, t int64, parents ...string) Event {
	return Event{ID: id, Producer: producer, Time: t, Parents: parents}
}

func mustNew(t *testing.T, cfg Config) *Orderer {
	t.Helper()
	o, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return o
}

// feed pushes events and flushes, returning the released ids in order.
func feed(t *testing.T, cfg Config, events ...Event) ([]string, *Orderer, []Released) {
	t.Helper()
	o := mustNew(t, cfg)
	var all []Released
	for _, e := range events {
		_, rel, err := o.Push(e)
		if err != nil {
			t.Fatal(err)
		}
		all = append(all, rel...)
	}
	all = append(all, o.Flush()...)
	var ids []string
	for _, r := range all {
		ids = append(ids, r.Event.Identity())
	}
	return ids, o, all
}

func same(t *testing.T, got []string, want ...string) {
	t.Helper()
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("got order %v, want %v", got, want)
	}
}

func TestOrderFields(t *testing.T) {
	// time, then logical, then producer, then seq, then id.
	events := []Event{
		{ID: "f", Producer: "b", Time: 2},
		{ID: "e", Producer: "a", Time: 2, Logical: 1},
		{ID: "d", Producer: "a", Time: 2},
		{Producer: "a", Seq: 2, Time: 1},
		{Producer: "a", Seq: 1, Time: 1},
		{ID: "a", Producer: "z", Time: 0},
	}
	ids, _, _ := feed(t, Config{Window: 10}, events...)
	same(t, ids, "a", "a#1", "a#2", "d", "f", "e")
}

func TestWatermarkReleasesAndRefusesLate(t *testing.T) {
	o := mustNew(t, Config{Window: 5})
	push := func(e Event) (Status, []Released) {
		t.Helper()
		s, rel, err := o.Push(e)
		if err != nil {
			t.Fatal(err)
		}
		return s, rel
	}
	if s, rel := push(ev("a", "p", 10)); s != Accepted || len(rel) != 0 {
		t.Fatalf("first event: %v, %d released", s, len(rel))
	}
	// 10 is not below the watermark 15-5=10, so nothing goes yet.
	if _, rel := push(ev("b", "p", 15)); len(rel) != 0 {
		t.Fatalf("released %d at watermark 10", len(rel))
	}
	// An event at the watermark itself is still on time.
	if s, _ := push(ev("c", "p", 10)); s != Accepted {
		t.Fatalf("an event at the watermark was %v", s)
	}
	_, rel := push(ev("d", "p", 16))
	if len(rel) != 2 || rel[0].Event.ID != "a" || rel[1].Event.ID != "c" || rel[0].Pos != 1 || rel[1].Pos != 2 {
		t.Fatalf("released %+v", rel)
	}
	if s, _ := push(ev("x", "p", 10)); s != Late {
		t.Fatalf("an event below the watermark was %v", s)
	}
	if wm, ok := o.Watermark(); !ok || wm != 11 {
		t.Fatalf("watermark %d %v", wm, ok)
	}
	if st := o.Stats(); st.Late != 1 || st.In != 5 || st.Out != 2 || st.Pending != 2 {
		t.Fatalf("stats %+v", st)
	}
}

func TestDuplicatesAndConflicts(t *testing.T) {
	o := mustNew(t, Config{Window: 100})
	a := ev("a", "p", 1)
	a.Data = json.RawMessage(`{ "x" : 1 }`)
	if s, _, _ := o.Push(a); s != Accepted {
		t.Fatal(s)
	}
	b := a
	b.Data = json.RawMessage(`{"x":1}`) // the same after compacting
	if s, _, _ := o.Push(b); s != Duplicate {
		t.Fatalf("a held duplicate was %v", s)
	}
	c := a
	c.Data = json.RawMessage(`{"x":2}`)
	if s, _, _ := o.Push(c); s != Conflict {
		t.Fatalf("a held conflict was %v", s)
	}
	o.Push(ev("z", "p", 500)) // releases a
	if s, _, _ := o.Push(b); s != Duplicate {
		t.Fatalf("a released duplicate was %v", s)
	}
	if s, _, _ := o.Push(c); s != Conflict {
		t.Fatalf("a released conflict was %v", s)
	}
	// An event without an id is the same event as its producer and seq.
	if s, _, _ := o.Push(Event{Producer: "q", Seq: 3, Time: 600}); s != Accepted {
		t.Fatal(s)
	}
	if s, _, _ := o.Push(Event{ID: "q#3", Producer: "q", Seq: 3, Time: 600}); s != Conflict {
		t.Fatalf("the same identity with an explicit id was %v", s)
	}
	st := o.Stats()
	if st.Duplicates != 2 || st.Conflicts != 3 {
		t.Fatalf("stats %+v", st)
	}
}

func TestInvalidEvents(t *testing.T) {
	o := mustNew(t, Config{})
	for _, e := range []Event{
		{ID: "a", Time: 1},
		{Producer: "p", Time: 1},
		{ID: "a", Producer: "p", Parents: []string{""}},
		{ID: "a", Producer: "p", Data: json.RawMessage(`{`)},
	} {
		if _, _, err := o.Push(e); err == nil {
			t.Fatalf("%+v was accepted", e)
		}
	}
	if o.Stats().In != 0 {
		t.Fatal("invalid events were counted in")
	}
	if _, err := New(Config{Window: -1}); err == nil {
		t.Fatal("a negative window was accepted")
	}
}

func TestParseLine(t *testing.T) {
	good := `{"id":"a","producer":"p","seq":4,"time":-7,"logical":2,"parents":["x","y"],"data":{"k": [1, 2]}}`
	e, err := ParseLine([]byte(good))
	if err != nil {
		t.Fatal(err)
	}
	want := `{"id":"a","producer":"p","seq":4,"time":-7,"logical":2,"parents":["x","y"],"data":{"k":[1,2]}}`
	if string(e.Canonical()) != want {
		t.Fatalf("canonical %s", e.Canonical())
	}
	for _, bad := range []string{
		``, `[]`, `{"producer":"p","seq":1}`, `{"time":1,"seq":1}`, `{"producer":"p","time":1}`,
		`{"producer":"p","seq":1,"time":1.5}`, `{"producer":"p","seq":1,"time":1,"extra":1}`,
		`{"producer":"p","seq":-1,"time":1}`, `{"producer":"p","seq":1,"time":1} {}`,
		`{"producer":"p","seq":1,"time":99999999999999999999}`, `{"producer":"p","seq":1,"time":1,"logical":4294967296}`,
	} {
		if _, err := ParseLine([]byte(bad)); err == nil {
			t.Fatalf("%q parsed", bad)
		}
	}
	if _, err := ParseLine([]byte(`{"producer":"p","seq":1,"time":1,"data":"` + strings.Repeat("x", MaxLine) + `"}`)); err == nil {
		t.Fatal("an over-long line parsed")
	}
}

func TestCanonicalEscapes(t *testing.T) {
	e := Event{ID: "a\"<b>\n", Producer: "pé", Time: 1}
	var back map[string]interface{}
	if err := json.Unmarshal(e.Canonical(), &back); err != nil || back["id"] != e.ID || back["producer"] != e.Producer {
		t.Fatalf("%s did not round-trip: %v", e.Canonical(), err)
	}
}

func TestParentsComeFirst(t *testing.T) {
	// The child's clock is behind its parent's: it waits for the parent and
	// goes straight after it, ahead of the events between.
	ids, _, rel := feed(t, Config{Window: 10, Hold: 10},
		ev("child", "p", 5, "parent"),
		ev("mid", "p", 6),
		ev("parent", "q", 8),
		ev("later", "p", 9),
	)
	same(t, ids, "mid", "parent", "child", "later")
	for _, r := range rel {
		if len(r.Missing) != 0 {
			t.Fatalf("%s reported missing %v", r.Event.ID, r.Missing)
		}
	}
	// A parent that already went out costs the child nothing.
	ids, _, _ = feed(t, Config{Window: 0, Hold: 10}, ev("parent", "q", 1), ev("x", "p", 5), ev("child", "p", 5, "parent"))
	same(t, ids, "parent", "child", "x")
}

func TestMissingParentWaitsForTheHold(t *testing.T) {
	ids, o, rel := feed(t, Config{Window: 2, Hold: 3},
		ev("orphan", "p", 1, "ghost"),
		ev("a", "p", 2), ev("b", "p", 4), ev("c", "p", 5), ev("d", "p", 20),
	)
	// The orphan waits until the end of time 1+3=4, so after b.
	same(t, ids, "a", "b", "orphan", "c", "d")
	if r := rel[2]; r.Event.ID != "orphan" || len(r.Missing) != 1 || r.Missing[0] != "ghost" {
		t.Fatalf("%+v", r)
	}
	if !strings.Contains(string(rel[2].Line), `"missing":["ghost"]`) {
		t.Fatalf("line %s", rel[2].Line)
	}
	if o.Stats().MissingParents != 1 {
		t.Fatal(o.Stats())
	}
}

func TestParentBeyondTheHold(t *testing.T) {
	// The parent comes after the child's deadline: the child goes at its
	// deadline and the parent is reported missing, though it comes later.
	ids, _, rel := feed(t, Config{Window: 20, Hold: 2},
		ev("child", "p", 1, "parent"), ev("x", "p", 3), ev("y", "p", 4), ev("parent", "q", 6),
	)
	same(t, ids, "x", "child", "y", "parent")
	if len(rel[1].Missing) != 1 {
		t.Fatalf("%+v", rel[1])
	}
}

func TestChainsCyclesAndSelf(t *testing.T) {
	cfg := Config{Window: 50, Hold: 5}
	// Grandchild waits on child waits on parent, all clocks inverted.
	ids, _, _ := feed(t, cfg, ev("g", "p", 1, "c"), ev("c", "p", 2, "a"), ev("a", "p", 4), ev("z", "p", 3))
	same(t, ids, "z", "a", "c", "g")
	// A cycle: each waits for the other until its deadline.
	ids, _, rel := feed(t, cfg, ev("x", "p", 1, "y"), ev("y", "p", 2, "x"), ev("w", "p", 100))
	same(t, ids, "x", "y", "w")
	if len(rel[0].Missing) != 1 || len(rel[1].Missing) != 0 {
		t.Fatalf("%+v", rel)
	}
	// An event that names itself waits its hold and reports itself missing.
	ids, _, rel = feed(t, cfg, ev("s", "p", 1, "s"), ev("t", "p", 3), ev("u", "p", 7))
	same(t, ids, "t", "s", "u")
	if len(rel[1].Missing) != 1 {
		t.Fatalf("%+v", rel[1])
	}
}

func TestRememberForgets(t *testing.T) {
	o := mustNew(t, Config{Window: 0, Hold: 0, Remember: 10})
	o.Push(ev("a", "p", 1))
	o.Push(ev("b", "p", 5))
	if s, _, _ := o.Push(ev("a", "p", 1)); s != Duplicate {
		t.Fatalf("remembered duplicate was %v", s)
	}
	o.Push(ev("c", "p", 30)) // watermark 30, forget below 20
	if s, _, _ := o.Push(ev("a", "p", 1)); s != Late {
		t.Fatalf("forgotten duplicate was %v", s)
	}
	// A parent more than Remember older than its child counts as missing,
	// whether or not it is still remembered.
	_, rel, _ := o.Push(ev("d", "p", 31, "c"))
	_, rel2, _ := o.Push(ev("e", "p", 45, "c"))
	rel = append(rel, rel2...)
	rel = append(rel, o.Flush()...)
	for _, r := range rel {
		if r.Event.ID == "d" && len(r.Missing) != 0 {
			t.Fatal("a recent parent was reported missing")
		}
		if r.Event.ID == "e" && len(r.Missing) != 1 {
			t.Fatal("a too-old parent wasn't reported missing")
		}
	}
}

func TestSequenceGaps(t *testing.T) {
	var events []Event
	for _, s := range []uint64{3, 4, 7, 5, 10} {
		events = append(events, Event{Producer: "f", Seq: s, Time: int64(s)})
	}
	events[3].Time = 8 // seq 5 arrives with a later time: it goes backwards
	_, o, _ := feed(t, Config{Window: 100}, events...)
	gaps := o.Gaps()
	if len(gaps) != 2 || gaps[0] != (Gap{"f", 5, 6}) || gaps[1] != (Gap{"f", 8, 9}) {
		t.Fatalf("gaps %+v", gaps)
	}
	if st := o.Stats(); st.SeqGaps != 2 || st.SeqBackwards != 1 {
		t.Fatalf("stats %+v", st)
	}
}

func TestHashIsOutputSHA256(t *testing.T) {
	_, o, rel := feed(t, Config{Window: 3}, ev("a", "p", 1), ev("b", "p", 9), ev("c", "p", 4))
	h := sha256.New()
	for _, r := range rel {
		h.Write(r.Line)
		h.Write([]byte("\n"))
	}
	if o.Hash() != "sha256:"+hex.EncodeToString(h.Sum(nil)) {
		t.Fatal("the hash isn't the SHA-256 of the output lines")
	}
	if _, _, err := o.Push(ev("d", "p", 100)); err != ErrClosed {
		t.Fatalf("push after flush: %v", err)
	}
	if _, err := o.Snapshot(); err != ErrClosed {
		t.Fatalf("snapshot after flush: %v", err)
	}
}

func TestExtremeTimes(t *testing.T) {
	const max, min = int64(math.MaxInt64), int64(math.MinInt64)
	// Window, hold and remember arithmetic saturates rather than wrapping.
	ids, o, _ := feed(t, Config{Window: 1 << 62, Hold: 1 << 62, Remember: 1 << 62},
		ev("lo", "p", min, "hi"), ev("mid", "p", 0), ev("hi", "p", max, "ghost"))
	same(t, ids, "lo", "mid", "hi")
	if st := o.Stats(); st.Late != 0 || st.MissingParents != 2 {
		t.Fatalf("stats %+v", st)
	}
}

// randomEvents makes a small, tangled set: shared times, parents that come
// before, after, never or in cycles, and each event's own copies.
func randomEvents(rng *rand.Rand, n int) []Event {
	var events []Event
	for i := 0; i < n; i++ {
		e := Event{Producer: fmt.Sprintf("p%d", rng.IntN(3)), Time: rng.Int64N(int64(n / 4))}
		if rng.IntN(2) == 0 {
			e.ID = fmt.Sprintf("e%d", i)
		} else {
			e.Seq = uint64(i + 1)
		}
		e.Logical = uint32(rng.IntN(2))
		for k := rng.IntN(4); k > 0; k-- {
			switch r := rng.IntN(10); {
			case r < 7:
				j := rng.IntN(n)
				if rng.IntN(2) == 0 {
					e.Parents = append(e.Parents, fmt.Sprintf("e%d", j))
				} else {
					e.Parents = append(e.Parents, fmt.Sprintf("p%d#%d", rng.IntN(3), j+1))
				}
			case r < 9:
				e.Parents = append(e.Parents, fmt.Sprintf("ghost%d", rng.IntN(5)))
			default:
				e.Parents = append(e.Parents, e.Identity())
			}
		}
		if rng.IntN(3) == 0 {
			e.Data = json.RawMessage(fmt.Sprintf(`{"i":%d}`, i))
		}
		events = append(events, e)
	}
	// Drop events whose identity repeats, so the set is distinct.
	seen := map[string]bool{}
	var out []Event
	for _, e := range events {
		if !seen[e.Identity()] {
			seen[e.Identity()] = true
			out = append(out, e)
		}
	}
	return out
}

// arrive shuffles copies of the events within the window.
func arrive(rng *rand.Rand, events []Event, window int64) []Event {
	type a struct {
		at  int64
		tie uint64
		e   Event
	}
	var list []a
	for _, e := range events {
		for c := 1 + rng.IntN(3); c > 0; c-- {
			list = append(list, a{e.Time + rng.Int64N(window+1), rng.Uint64(), e})
		}
	}
	for i := range list {
		for j := i + 1; j < len(list); j++ {
			if list[j].at < list[i].at || (list[j].at == list[i].at && list[j].tie < list[i].tie) {
				list[i], list[j] = list[j], list[i]
			}
		}
	}
	out := make([]Event, len(list))
	for i := range list {
		out[i] = list[i].e
	}
	return out
}

// run pushes an arrival order, taking and restoring snapshots at random
// points when snapshots is set, and returns the output hash.
func run(t *testing.T, cfg Config, order []Event, rng *rand.Rand, snapshots bool) (string, Stats) {
	t.Helper()
	o := mustNew(t, cfg)
	n := 0
	var records []Record
	for _, e := range order {
		if snapshots && rng.IntN(20) == 0 {
			// Half the time the whole state; half the time the held events,
			// with the released records kept beside them.
			held := rng.IntN(2) == 0
			var s *Snapshot
			var err error
			if held {
				s, err = o.SnapshotHeld()
			} else {
				s, err = o.Snapshot()
			}
			if err != nil {
				t.Fatal(err)
			}
			b, _ := json.Marshal(s)
			var back Snapshot
			if err := json.Unmarshal(b, &back); err != nil {
				t.Fatal(err)
			}
			var extra []Record
			if held {
				extra = records
			}
			if o, err = Restore(&back, extra); err != nil {
				t.Fatal(err)
			}
		}
		s, rel, err := o.Push(e)
		if err != nil {
			t.Fatal(err)
		}
		if s == Late || s == Conflict {
			t.Fatalf("%s was %v in an order within the window", e.Identity(), s)
		}
		n += len(rel)
		for _, r := range rel {
			records = append(records, r.Record)
		}
	}
	n += len(o.Flush())
	st := o.Stats()
	if uint64(n) != st.Out || st.Pending != 0 {
		t.Fatalf("released %d, stats %+v", n, st)
	}
	return o.Hash(), st
}

// TestSameEventsSameOutput is the main promise: any arrival order within
// the window, with any duplicates and any snapshot and restore along the
// way, gives the same output.
func TestSameEventsSameOutput(t *testing.T) {
	rounds := 300
	if testing.Short() {
		rounds = 30
	}
	if n, err := strconv.Atoi(os.Getenv("INORDERLY_ROUNDS")); err == nil {
		rounds = n
	}
	for round := 0; round < rounds; round++ {
		rng := rand.New(rand.NewPCG(uint64(round), 7))
		events := randomEvents(rng, 10+rng.IntN(120))
		cfg := Config{Window: rng.Int64N(8), Hold: rng.Int64N(8), Remember: rng.Int64N(3) * rng.Int64N(20)}
		if round%3 == 2 { // wide windows and holds over few distinct times
			cfg = Config{Window: rng.Int64N(40), Hold: rng.Int64N(40), Remember: rng.Int64N(2) * rng.Int64N(10)}
		}
		want, wantStats := run(t, cfg, arrive(rng, events, 0), rng, false)
		if wantStats.Out != uint64(len(events)) {
			t.Fatalf("round %d: %d of %d events out", round, wantStats.Out, len(events))
		}
		for k := 0; k < 20; k++ {
			got, st := run(t, cfg, arrive(rng, events, cfg.Window), rng, k%2 == 1)
			if got != want || st.MissingParents != wantStats.MissingParents || st.SeqGaps != wantStats.SeqGaps {
				t.Fatalf("round %d order %d (config %+v): %s %+v, want %s %+v", round, k, cfg, got, st, want, wantStats)
			}
		}
	}
}

// TestKeysOnlyGoUp checks every output position counts from 1, and that each
// event's missing list names exactly the parents that didn't come out before
// it.
func TestKeysOnlyGoUp(t *testing.T) {
	for round := 0; round < 100; round++ {
		rng := rand.New(rand.NewPCG(uint64(round), 99))
		events := randomEvents(rng, 80)
		cfg := Config{Window: 3, Hold: 4}
		o := mustNew(t, cfg)
		var rel []Released
		for _, e := range arrive(rng, events, cfg.Window) {
			_, r, _ := o.Push(e)
			rel = append(rel, r...)
		}
		rel = append(rel, o.Flush()...)
		pos := map[string]int{}
		for i, r := range rel {
			if r.Pos != uint64(i+1) {
				t.Fatalf("position %d at index %d", r.Pos, i)
			}
			pos[r.Event.Identity()] = i
		}
		for i, r := range rel {
			missing := map[string]bool{}
			for _, m := range r.Missing {
				missing[m] = true
			}
			for _, p := range r.Event.Parents {
				before, ok := pos[p]
				if ok && before < i && missing[p] {
					t.Fatalf("round %d: %s reports %s missing, but it came first", round, r.Event.Identity(), p)
				}
				if (!ok || before > i) && !missing[p] {
					t.Fatalf("round %d: %s doesn't report %s missing", round, r.Event.Identity(), p)
				}
			}
		}
	}
}

func TestRestoreRefusesDamage(t *testing.T) {
	o := mustNew(t, Config{Window: 5})
	o.Push(ev("a", "p", 1))
	o.Push(ev("b", "p", 20))
	s, _ := o.Snapshot()
	bad := *s
	bad.Format = 99
	if _, err := Restore(&bad, nil); err == nil {
		t.Fatal("a wrong format restored")
	}
	bad = *s
	bad.Pending = append([]Event{}, s.Pending...)
	bad.Pending = append(bad.Pending, s.Pending[0])
	if _, err := Restore(&bad, nil); err == nil {
		t.Fatal("a doubled event restored")
	}
	bad = *s
	bad.Released = append([]Record{}, s.Released...)
	bad.Released[0].Sum = "zz"
	if _, err := Restore(&bad, nil); err == nil {
		t.Fatal("a damaged checksum restored")
	}
	held, _ := o.SnapshotHeld()
	if _, err := Restore(held, nil); err == nil {
		t.Fatal("a partial snapshot restored without its records")
	}
	if _, err := Restore(held, append(s.Released, s.Released...)); err == nil {
		t.Fatal("a record given twice restored")
	}
	if _, err := Restore(held, s.Released); err != nil {
		t.Fatal(err)
	}
	bad = *s
	bad.Pending = []Event{ev("old", "p", 0)} // below the watermark: should have gone out
	if _, err := Restore(&bad, nil); err == nil {
		t.Fatal("a held event below the watermark restored")
	}
}

func TestInvalidUTF8Refused(t *testing.T) {
	o := mustNew(t, Config{})
	for _, e := range []Event{
		{ID: "\xff", Producer: "p", Time: 1},
		{ID: "a", Producer: "p\xfe", Time: 1},
		{ID: "a", Producer: "p", Time: 1, Parents: []string{"ok", "\xc3"}},
	} {
		if _, _, err := o.Push(e); err == nil {
			t.Fatalf("%q was accepted", e.Identity())
		}
	}
}

func TestParseLineKeysExactly(t *testing.T) {
	for _, bad := range []string{
		`{"ID":"a","producer":"p","time":1}`,
		`{"id":"a","PRODUCER":"p","time":1}`,
		`{"id":"a","producer":"p","Time":1}`,
		`{"id":"a","id":"b","producer":"p","time":1}`,
		`{"id":"a","producer":"p","time":1,"data":{"x":1,"x":2}}`,
		`{"id":1,"producer":"p","time":1}`,
		`{"id":"a","producer":"p","time":"1"}`,
	} {
		if _, err := ParseLine([]byte(bad)); err == nil {
			t.Fatalf("%s parsed", bad)
		}
	}
	e, err := ParseLine([]byte(` {"time":1, "data":null, "producer":"p", "id":"a"} `))
	if err != nil || e.Data != nil || e.ID != "a" {
		t.Fatalf("%+v %v", e, err)
	}
}

// Many children waiting on one parent that never comes must not cost time
// that grows with the square of their number.
func TestManyChildrenOfOneMissingParent(t *testing.T) {
	timed := func(n int) float64 {
		o := mustNew(t, Config{Window: 10, Hold: 1 << 40})
		start := time.Now()
		for i := 0; i < n; i++ {
			o.Push(ev(fmt.Sprintf("c%d", i), "p", int64(i), "ghost"))
		}
		if len(o.Flush()) != n {
			t.Fatal("not all released")
		}
		return time.Since(start).Seconds()
	}
	small, large := timed(20000), timed(80000)
	if large > 12*small+0.5 {
		t.Fatalf("20,000 children took %.2fs but 80,000 took %.2fs", small, large)
	}
}

// A long run of events, each naming the one before as its parent with a
// clock behind it, is placed straight after its parent only MaxChain deep;
// after that each goes at its own deadline. Keys stay short, so the run
// costs time in proportion to its length.
func TestLongInvertedChain(t *testing.T) {
	chain := func(n int) ([]Released, float64) {
		o := mustNew(t, Config{Window: 1 << 40, Hold: 1 << 40})
		start := time.Now()
		var rel []Released
		for i := 0; i < n; i++ {
			e := ev(fmt.Sprintf("x%07d", n-i), "p", 0)
			if i > 0 {
				e.Parents = []string{fmt.Sprintf("x%07d", n-i+1)}
			}
			_, r, _ := o.Push(e)
			rel = append(rel, r...)
		}
		rel = append(rel, o.Flush()...)
		return rel, time.Since(start).Seconds()
	}
	rel, small := chain(2000)
	// The first event's key is one point long, so links 1 to MaxChain-1 fit.
	for i := 1; i < MaxChain; i++ {
		if rel[i].Event.Parents[0] != rel[i-1].Event.ID || len(rel[i].Missing) != 0 {
			t.Fatalf("link %d isn't straight after its parent", i)
		}
	}
	// The rest go at their deadline, in their own order, so every link past
	// MaxChain comes before its parent and reports it missing.
	missing := 0
	for _, r := range rel {
		missing += len(r.Missing)
	}
	if missing != len(rel)-MaxChain-1 {
		t.Fatalf("%d links reported a missing parent, want %d", missing, len(rel)-MaxChain-1)
	}
	_, large := chain(8000)
	if large > 12*small+0.5 {
		t.Fatalf("a 2,000-link chain took %.2fs but 8,000 took %.2fs", small, large)
	}
}
