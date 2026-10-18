// Copyright Inorderly.com 2026
// SPDX-License-Identifier: Apache-2.0

package usecase

import (
	"fmt"

	"github.com/inorderly/inorderly"
)

// Feeds is a price feed for three symbols sent over two lines, A and B, as
// exchanges do. Each line loses about 2% of its ticks at random, and each
// has one outage of a few seconds. Line B runs a few milliseconds behind A.
// The ticks are merged live twice, with different line delays, and once
// more from the two capture files read side by side.
func Feeds() *Case {
	const ticks = 150000
	symbols := []string{"ACME", "BOLT", "CRUX"}
	price := []int{10000, 4000, 2500}
	seq := make([]uint64, len(symbols))
	rng := newRand(2, 0xfeed)
	outA := [2]int64{t0 + 600000, t0 + 604000}
	outB := [2]int64{t0 + 1200000, t0 + 1203000}
	var lineA, lineB []inorderly.Event
	var all []inorderly.Event
	inBoth, lost := 0, 0
	for k := 0; k < ticks; k++ {
		s := rng.IntN(len(symbols))
		seq[s]++
		price[s] += rng.IntN(5) - 2
		t := t0 + int64(k)*12
		e := inorderly.Event{Producer: symbols[s], Seq: seq[s], Time: t,
			Data: fmt.Appendf(nil, `{"price":%d,"size":%d}`, price[s], 100*(1+rng.IntN(10)))}
		all = append(all, e)
		a := rng.IntN(100) >= 2 && (t < outA[0] || t >= outA[1])
		b := rng.IntN(100) >= 2 && (t < outB[0] || t >= outB[1])
		if a {
			lineA = append(lineA, e)
		}
		if b {
			lineB = append(lineB, e)
		}
		switch {
		case a && b:
			inBoth++
		case !a && !b:
			lost++
		}
	}

	c := &Case{Name: "feeds", Config: inorderly.Config{Window: 10000, Hold: 10000}}
	for v := 1; v <= 2; v++ {
		d := newRand(uint64(200+v), 0x11e)
		var ds deliveries
		for _, e := range lineA {
			ds.add(e.Time+1+d.Int64N(3), e)
		}
		for _, e := range lineB {
			ds.add(e.Time+5+d.Int64N(15), e)
		}
		name := fmt.Sprintf("live-%d", v)
		c.Inputs = append(c.Inputs, Input{Name: name, Files: []File{{Name: name + ".ndjson", Events: ds.arrived()}}})
	}
	c.Inputs = append(c.Inputs, Input{Name: "capture", Files: []File{
		{Name: "line-a.ndjson", Events: lineA}, {Name: "line-b.ndjson", Events: lineB}}})

	c.check = func(c *Case, runs []Run) (map[string]any, error) {
		// A typical hand-written merge: pass on each tick the first time
		// its number is seen.
		outOfSequence := map[string]int{}
		for _, in := range c.Inputs[:2] {
			last := map[string]uint64{}
			seen := map[string]bool{}
			for _, e := range in.Arrivals() {
				id := e.Identity()
				if seen[id] {
					continue
				}
				seen[id] = true
				if e.Seq < last[e.Producer] {
					outOfSequence[in.Name]++
				}
				last[e.Producer] = max(last[e.Producer], e.Seq)
			}
		}
		var gaps []inorderly.Gap
		for _, r := range runs {
			if r.Stats.Out != uint64(ticks-lost) || r.Stats.Late+r.Stats.Conflicts != 0 {
				return nil, fmt.Errorf("%s: %d ticks out, %d expected, %d late", r.Input, r.Stats.Out, ticks-lost, r.Stats.Late)
			}
			if r.Stats.Duplicates != uint64(inBoth) {
				return nil, fmt.Errorf("%s: %d repeats dropped, %d ticks came on both lines", r.Input, r.Stats.Duplicates, inBoth)
			}
			if r.Stats.SeqBackwards != 0 {
				return nil, fmt.Errorf("%s: a tick went out before an earlier one", r.Input)
			}
			g, err := expectGaps(r)
			if err != nil {
				return nil, err
			}
			if missingCount(g) != uint64(lost) {
				return nil, fmt.Errorf("%s: gaps cover %d ticks, %d were lost on both lines", r.Input, missingCount(g), lost)
			}
			gaps = g
		}
		return map[string]any{
			"ticks":                 ticks,
			"line_a":                len(lineA),
			"line_b":                len(lineB),
			"on_both_lines":         inBoth,
			"lost_on_both":          lost,
			"gaps":                  len(gaps),
			"ordered":               runs[0].Stats.Out,
			"duplicates_dropped":    runs[0].Stats.Duplicates,
			"naive_out_of_sequence": outOfSequence,
			"outage_a_ms":           outA[1] - outA[0],
			"outage_b_ms":           outB[1] - outB[0],
		}, nil
	}
	return c
}
