// Copyright Inorderly.com 2026
// SPDX-License-Identifier: Apache-2.0

// Package scenario makes the event sets and arrival orders that the tests
// and the demo feed to Inorderly. Everything comes from a seed, so a run
// can be repeated exactly.
package scenario

import (
	"fmt"
	"math/rand/v2"
	"sort"
	"strconv"

	"github.com/inorderly/inorderly"
)

// Kind says how an event in a Set relates to the others.
type Kind int

const (
	// Plain events have no parents.
	Plain Kind = iota
	// Child events name a plain parent with an earlier or equal time.
	Child
	// Inverted events name a plain parent with a later time, within the
	// hold: the clocks disagree, and the child must wait for its parent.
	Inverted
	// Orphan events name a parent that never comes.
	Orphan
)

// Params sets the size and shape of a Set.
type Params struct {
	Seed      uint64
	Events    int
	Producers int
	Hold      int64
}

// Set is a set of distinct events, with how each relates to the others.
type Set struct {
	Events []inorderly.Event
	Kinds  []Kind
	Parent []int // index of the parent in Events, or -1
}

// Generate makes a Set: mostly plain events, with about one in ten a child
// of a recent plain event, one in twenty-five a child with an inverted
// clock, and one in a hundred an orphan whose parent never comes. Times run
// at two events per unit, so many events share a time.
func Generate(p Params) Set {
	rng := rand.New(rand.NewPCG(p.Seed, 0x5eed))
	s := Set{}
	var plains []int
	for k := 0; k < p.Events; k++ {
		e := inorderly.Event{
			ID:       "e" + strconv.Itoa(k),
			Producer: "p" + strconv.Itoa(rng.IntN(p.Producers)),
			Time:     int64(k / 2),
			Data:     []byte(fmt.Sprintf(`{"k":%d,"v":%d}`, k, rng.IntN(1000000))),
		}
		if rng.IntN(4) == 0 {
			e.Logical = uint32(rng.IntN(3))
		}
		kind, parent := Plain, -1
		r := rng.Float64()
		recent := plains
		if len(recent) > 200 {
			recent = recent[len(recent)-200:]
		}
		switch {
		case r < 0.10 && len(recent) > 0:
			kind, parent = Child, recent[rng.IntN(len(recent))]
			if t := s.Events[parent].Time; e.Time < t {
				e.Time = t
			}
		case r < 0.14 && len(recent) > 0 && p.Hold > 0:
			kind, parent = Inverted, recent[len(recent)-1-rng.IntN(min(len(recent), 50))]
			e.Time = s.Events[parent].Time - 1 - rng.Int64N(p.Hold)
		case r < 0.15:
			kind = Orphan
			e.Parents = []string{"ghost" + strconv.Itoa(k)}
		}
		if parent >= 0 {
			e.Parents = []string{s.Events[parent].ID}
		}
		if kind == Plain {
			plains = append(plains, k)
		}
		s.Events = append(s.Events, e)
		s.Kinds = append(s.Kinds, kind)
		s.Parent = append(s.Parent, parent)
	}
	return s
}

// Arrive is one arrival order for events: each event comes 1 to maxCopies
// times, each copy at a random point up to window behind its time, so no
// copy is late for an Orderer with that window.
func Arrive(events []inorderly.Event, window int64, maxCopies int, seed uint64) []inorderly.Event {
	rng := rand.New(rand.NewPCG(seed, 0xa441))
	type arrival struct {
		at   int64
		tie  uint64
		from int
	}
	var list []arrival
	for i, e := range events {
		copies := 1 + rng.IntN(maxCopies)
		for c := 0; c < copies; c++ {
			list = append(list, arrival{at: e.Time + rng.Int64N(window+1), tie: rng.Uint64(), from: i})
		}
	}
	sort.Slice(list, func(i, j int) bool {
		if list[i].at != list[j].at {
			return list[i].at < list[j].at
		}
		return list[i].tie < list[j].tie
	})
	out := make([]inorderly.Event, len(list))
	for i, a := range list {
		out[i] = events[a.from]
	}
	return out
}

// AddLate puts count late events into an arrival order: each has a time
// more than window below the newest time before it, so an Orderer with that
// window must refuse it.
func AddLate(order []inorderly.Event, window int64, count int, seed uint64) ([]inorderly.Event, int) {
	rng := rand.New(rand.NewPCG(seed, 0x1a7e))
	at := map[int]bool{}
	for len(at) < count && len(order) > 1000 {
		at[1000+rng.IntN(len(order)-1000)] = true
	}
	var out []inorderly.Event
	var maxSeen int64
	added := 0
	for i, e := range order {
		if i == 0 || e.Time > maxSeen {
			maxSeen = e.Time
		}
		out = append(out, e)
		if at[i] {
			out = append(out, inorderly.Event{
				ID: "late" + strconv.Itoa(i), Producer: "late",
				Time: maxSeen - window - 1 - rng.Int64N(1000), Data: []byte(`{"late":true}`),
			})
			added++
		}
	}
	return out, added
}

// Feeds is one tick stream sent twice, with each copy missing some ticks.
type Feeds struct {
	A, B []inorderly.Event
	// Lost lists the ticks missing from both.
	Lost []uint64
	// DroppedA and DroppedB count the ticks missing from each feed.
	DroppedA, DroppedB int
}

// MakeFeeds makes ticks 1..n of producer "feed" with time equal to seq, and
// drops about dropPercent of them from each copy at random.
func MakeFeeds(n int, dropPercent int, seed uint64) Feeds {
	rng := rand.New(rand.NewPCG(seed, 0xfeed))
	var f Feeds
	for s := 1; s <= n; s++ {
		e := inorderly.Event{Producer: "feed", Seq: uint64(s), Time: int64(s),
			Data: []byte(fmt.Sprintf(`{"price":%d}`, 10000+rng.IntN(500)))}
		inA := rng.IntN(100) >= dropPercent
		inB := rng.IntN(100) >= dropPercent
		if inA {
			f.A = append(f.A, e)
		} else {
			f.DroppedA++
		}
		if inB {
			f.B = append(f.B, e)
		} else {
			f.DroppedB++
		}
		if !inA && !inB {
			f.Lost = append(f.Lost, uint64(s))
		}
	}
	return f
}

// ExpectedGaps is what an Orderer should report for ticks 1..n with lost
// missing: every run of lost ticks between the first and last tick that
// arrived.
func ExpectedGaps(n int, lost []uint64) []inorderly.Gap {
	gone := map[uint64]bool{}
	for _, s := range lost {
		gone[s] = true
	}
	var gaps []inorderly.Gap
	first := true
	var start uint64
	for s := uint64(1); s <= uint64(n); s++ {
		if !gone[s] {
			if !first && start != 0 {
				gaps = append(gaps, inorderly.Gap{Producer: "feed", From: start, To: s - 1})
			}
			first, start = false, 0
			continue
		}
		if start == 0 {
			start = s
		}
	}
	return gaps
}
