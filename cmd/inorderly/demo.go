// Copyright Inorderly.com 2026
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/inorderly/inorderly"
	"github.com/inorderly/inorderly/internal/scenario"
)

type demoReport struct {
	Version   string `json:"version"`
	Go        string `json:"go"`
	OS        string `json:"os"`
	Arch      string `json:"arch"`
	CPUs      int    `json:"cpus"`
	Seed      uint64 `json:"seed"`
	Events    int    `json:"events"`
	Producers int    `json:"producers"`
	Window    int64  `json:"window"`
	Hold      int64  `json:"hold"`

	Shuffles struct {
		Orders            int    `json:"orders"`
		Arrivals          int    `json:"arrivals_total"`
		DuplicatesDropped uint64 `json:"duplicates_dropped_total"`
		Late              uint64 `json:"late_total"`
		Ordered           uint64 `json:"ordered_each"`
		Identical         bool   `json:"identical"`
		Hash              string `json:"hash"`
		HashIsFileSHA256  bool   `json:"hash_is_output_sha256"`
		SortedArrival     bool   `json:"same_as_sorted_arrival"`
	} `json:"shuffles"`

	Restore struct {
		Snapshots int  `json:"snapshots_taken"`
		Identical bool `json:"identical"`
	} `json:"snapshot_restore"`

	Causal struct {
		Children        int    `json:"children"`
		Inverted        int    `json:"inverted_clock_children"`
		AfterParent     int    `json:"after_their_parent"`
		Orphans         int    `json:"orphans"`
		OrphansReported int    `json:"orphans_reported_missing"`
		MissingParents  uint64 `json:"events_with_missing_parents"`
	} `json:"causal"`

	Late struct {
		Injected      int    `json:"injected"`
		Refused       uint64 `json:"refused"`
		RestIdentical bool   `json:"rest_identical"`
	} `json:"late"`

	ABMerge struct {
		Ticks         int    `json:"ticks"`
		FeedA         int    `json:"feed_a_lines"`
		FeedB         int    `json:"feed_b_lines"`
		DroppedA      int    `json:"dropped_from_a"`
		DroppedB      int    `json:"dropped_from_b"`
		LostInBoth    int    `json:"lost_in_both"`
		Ordered       uint64 `json:"ordered"`
		Duplicates    uint64 `json:"duplicates_dropped"`
		GapsReported  uint64 `json:"gaps_reported"`
		GapsMatchLost bool   `json:"gaps_match_lost"`
		InSeqOrder    bool   `json:"in_seq_order"`
	} `json:"ab_merge"`

	Throughput struct {
		Pushes        int     `json:"pushes"`
		Ordered       uint64  `json:"ordered"`
		Seconds       float64 `json:"seconds"`
		PushesPerSec  float64 `json:"pushes_per_second"`
		OrderedPerSec float64 `json:"ordered_per_second"`
		NanosPerPush  float64 `json:"nanoseconds_per_push"`
	} `json:"throughput"`

	Passed   bool     `json:"passed"`
	Failures []string `json:"failures"`
}

// runOrder pushes events through a new Orderer and returns it flushed with
// its output lines.
func runOrder(cfg inorderly.Config, events []inorderly.Event, snapshotEvery int) (*inorderly.Orderer, [][]byte, int, error) {
	o, err := inorderly.New(cfg)
	if err != nil {
		return nil, nil, 0, err
	}
	var lines [][]byte
	snaps := 0
	var records []inorderly.Record
	for i, e := range events {
		if snapshotEvery > 0 && i > 0 && i%snapshotEvery == 0 {
			// Alternately the whole state, and the held events with the
			// released records kept beside them.
			held := snaps%2 == 1
			var s *inorderly.Snapshot
			var err error
			if held {
				s, err = o.SnapshotHeld()
			} else {
				s, err = o.Snapshot()
			}
			if err != nil {
				return nil, nil, 0, err
			}
			b, err := json.Marshal(s)
			if err != nil {
				return nil, nil, 0, err
			}
			var back inorderly.Snapshot
			if err := json.Unmarshal(b, &back); err != nil {
				return nil, nil, 0, err
			}
			var extra []inorderly.Record
			if held {
				extra = records
			}
			if o, err = inorderly.Restore(&back, extra); err != nil {
				return nil, nil, 0, err
			}
			snaps++
		}
		_, rel, err := o.Push(e)
		if err != nil {
			return nil, nil, 0, err
		}
		for _, r := range rel {
			lines = append(lines, r.Line)
			records = append(records, r.Record)
		}
	}
	for _, r := range o.Flush() {
		lines = append(lines, r.Line)
	}
	return o, lines, snaps, nil
}

func fileHash(lines [][]byte) string {
	h := sha256.New()
	for _, l := range lines {
		h.Write(l)
		h.Write([]byte{'\n'})
	}
	return "sha256:" + hex.EncodeToString(h.Sum(nil))
}

func demo(args []string) (int, error) {
	fs := flag.NewFlagSet("inorderly demo", flag.ContinueOnError)
	events := fs.Int("events", 100000, "distinct events in the main set")
	producers := fs.Int("producers", 16, "producers")
	window := fs.Int64("window", 200, "the window, in event-time units (two events per unit)")
	orders := fs.Int("orders", 20, "random arrival orders to compare")
	copies := fs.Int("copies", 5, "each event arrives 1 to this many times")
	ticks := fs.Int("ticks", 100000, "ticks in the A/B feed merge")
	seed := fs.Uint64("seed", 1, "the seed everything is made from")
	if err := fs.Parse(args); err != nil {
		return 2, err
	}
	if *orders < 2 || *events < 2000 || *producers < 1 || *window < 1 || *copies < 1 || *ticks < 100 {
		return 2, errors.New("need -orders 2 or more, -events 2000 or more, -ticks 100 or more, and -producers, -window and -copies at least 1")
	}
	rep := &demoReport{Version: inorderly.Version, Go: runtime.Version(), OS: runtime.GOOS, Arch: runtime.GOARCH,
		CPUs: runtime.NumCPU(), Seed: *seed, Events: *events, Producers: *producers, Window: *window, Hold: *window}
	fail := func(format string, a ...interface{}) { rep.Failures = append(rep.Failures, fmt.Sprintf(format, a...)) }
	cfg := inorderly.Config{Window: *window, Hold: *window}
	progress := func(format string, a ...interface{}) { fmt.Fprintf(os.Stderr, format+"\n", a...) }

	set := scenario.Generate(scenario.Params{Seed: *seed, Events: *events, Producers: *producers, Hold: *window})

	// The same events in many arrival orders, with duplicates.
	progress("shuffles: %d events, %d arrival orders", *events, *orders)
	rep.Shuffles.Orders = *orders
	rep.Shuffles.Identical = true
	var refLines [][]byte
	var reference string
	for i := 0; i < *orders; i++ {
		order := scenario.Arrive(set.Events, *window, *copies, *seed*1000+uint64(i))
		start := time.Now()
		o, lines, _, err := runOrder(cfg, order, 0)
		elapsed := time.Since(start)
		if err != nil {
			return 1, err
		}
		st := o.Stats()
		rep.Shuffles.Arrivals += len(order)
		rep.Shuffles.DuplicatesDropped += st.Duplicates
		rep.Shuffles.Late += st.Late
		if i == 0 {
			reference, refLines = o.Hash(), lines
			rep.Shuffles.Hash, rep.Shuffles.Ordered = reference, st.Out
			rep.Shuffles.HashIsFileSHA256 = fileHash(lines) == reference
			rep.Throughput.Pushes, rep.Throughput.Ordered = len(order), st.Out
			rep.Throughput.Seconds = elapsed.Seconds()
			rep.Throughput.PushesPerSec = float64(len(order)) / elapsed.Seconds()
			rep.Throughput.OrderedPerSec = float64(st.Out) / elapsed.Seconds()
			rep.Throughput.NanosPerPush = float64(elapsed.Nanoseconds()) / float64(len(order))
		} else if o.Hash() != reference || st.Out != rep.Shuffles.Ordered {
			rep.Shuffles.Identical = false
			fail("arrival order %d gave hash %s, not %s", i, o.Hash(), reference)
		}
	}
	if rep.Shuffles.Late != 0 {
		fail("%d events came out late in the shuffles, which keep within the window", rep.Shuffles.Late)
	}
	if rep.Shuffles.Ordered != uint64(*events) {
		fail("ordered %d events, not %d", rep.Shuffles.Ordered, *events)
	}
	if !rep.Shuffles.HashIsFileSHA256 {
		fail("the replay hash isn't the SHA-256 of the output")
	}
	sorted := scenario.Arrive(set.Events, 0, 1, *seed)
	if o, _, _, err := runOrder(cfg, sorted, 0); err != nil {
		return 1, err
	} else {
		rep.Shuffles.SortedArrival = o.Hash() == reference
		if !rep.Shuffles.SortedArrival {
			fail("events arriving in time order gave hash %s, not %s", o.Hash(), reference)
		}
	}

	// A run that saves and restores its whole state as it goes.
	progress("snapshot and restore")
	every := *events / 4
	if o, _, snaps, err := runOrder(cfg, scenario.Arrive(set.Events, *window, *copies, *seed*1000+999), every); err != nil {
		return 1, err
	} else {
		rep.Restore.Snapshots = snaps
		rep.Restore.Identical = o.Hash() == reference
		if !rep.Restore.Identical {
			fail("the run restored from snapshots gave hash %s, not %s", o.Hash(), reference)
		}
	}

	// Parents before children.
	progress("causal order")
	pos := map[string]int{}
	missing := map[string][]string{}
	for i, l := range refLines {
		var out struct {
			ID      string   `json:"id"`
			Missing []string `json:"missing"`
		}
		if err := json.Unmarshal(l, &out); err != nil {
			return 1, err
		}
		pos[out.ID] = i
		missing[out.ID] = out.Missing
	}
	for i, k := range set.Kinds {
		id := set.Events[i].ID
		switch k {
		case scenario.Child, scenario.Inverted:
			rep.Causal.Children++
			if k == scenario.Inverted {
				rep.Causal.Inverted++
			}
			if pos[set.Events[set.Parent[i]].ID] < pos[id] {
				rep.Causal.AfterParent++
			} else {
				fail("%s came before its parent %s", id, set.Events[set.Parent[i]].ID)
			}
		case scenario.Orphan:
			rep.Causal.Orphans++
			if len(missing[id]) == 1 && missing[id][0] == set.Events[i].Parents[0] {
				rep.Causal.OrphansReported++
			} else {
				fail("orphan %s wasn't reported with its missing parent", id)
			}
		}
	}
	{
		o, _, _, _ := runOrder(cfg, sorted, 0)
		rep.Causal.MissingParents = o.Stats().MissingParents
	}
	if rep.Causal.MissingParents != uint64(rep.Causal.Orphans) {
		fail("%d events reported missing parents, but there are %d orphans", rep.Causal.MissingParents, rep.Causal.Orphans)
	}

	// Late events are refused and change nothing else.
	progress("late events")
	lateOrder, added := scenario.AddLate(scenario.Arrive(set.Events, *window, *copies, *seed*1000+500), *window, *events/100, *seed)
	if o, _, _, err := runOrder(cfg, lateOrder, 0); err != nil {
		return 1, err
	} else {
		rep.Late.Injected, rep.Late.Refused = added, o.Stats().Late
		rep.Late.RestIdentical = o.Hash() == reference
		if rep.Late.Refused != uint64(added) {
			fail("%d late events were refused, not %d", rep.Late.Refused, added)
		}
		if !rep.Late.RestIdentical {
			fail("with late events added the hash was %s, not %s", o.Hash(), reference)
		}
	}

	// Two copies of one feed, each missing some ticks, merged.
	progress("A/B feed merge: %d ticks", *ticks)
	feeds := scenario.MakeFeeds(*ticks, 5, *seed)
	both := append(append([]inorderly.Event{}, feeds.A...), feeds.B...)
	merged := scenario.Arrive(both, *window, 1, *seed+7)
	rep.ABMerge.Ticks, rep.ABMerge.FeedA, rep.ABMerge.FeedB = *ticks, len(feeds.A), len(feeds.B)
	rep.ABMerge.DroppedA, rep.ABMerge.DroppedB, rep.ABMerge.LostInBoth = feeds.DroppedA, feeds.DroppedB, len(feeds.Lost)
	if o, lines, _, err := runOrder(cfg, merged, 0); err != nil {
		return 1, err
	} else {
		st := o.Stats()
		rep.ABMerge.Ordered, rep.ABMerge.Duplicates, rep.ABMerge.GapsReported = st.Out, st.Duplicates, st.SeqGaps
		want := scenario.ExpectedGaps(*ticks, feeds.Lost)
		got := o.Gaps()
		rep.ABMerge.GapsMatchLost = len(got) == len(want)
		for i := 0; rep.ABMerge.GapsMatchLost && i < len(got); i++ {
			rep.ABMerge.GapsMatchLost = got[i] == want[i]
		}
		rep.ABMerge.InSeqOrder = st.SeqBackwards == 0
		var last uint64
		for _, l := range lines {
			var out struct {
				Seq uint64 `json:"seq"`
			}
			json.Unmarshal(l, &out)
			if out.Seq <= last {
				rep.ABMerge.InSeqOrder = false
			}
			last = out.Seq
		}
		if st.Out != uint64(*ticks-len(feeds.Lost)) {
			fail("the merge ordered %d ticks, not %d", st.Out, *ticks-len(feeds.Lost))
		}
		if !rep.ABMerge.GapsMatchLost {
			fail("the merge reported gaps %v, not %v", got, want)
		}
		if !rep.ABMerge.InSeqOrder {
			fail("the merged ticks aren't in sequence order")
		}
	}

	rep.Passed = len(rep.Failures) == 0
	if rep.Failures == nil {
		rep.Failures = []string{}
	}
	b, _ := json.MarshalIndent(rep, "", "  ")
	fmt.Println(string(b))
	if !rep.Passed {
		return 1, errors.New("the demo found a problem")
	}
	progress("all checks passed")
	return 0, nil
}
