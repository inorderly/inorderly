// Copyright Inorderly.com 2026
// SPDX-License-Identifier: Apache-2.0

// Package usecase builds the worked examples behind the use-case posts on
// inorderly.com: retried webhooks, two redundant market data feeds, sensor
// telemetry from devices with bad clocks, and a ledger that has to agree with
// a billing system and with an auditor's replay.
//
// Each Case is one set of events delivered in several arrival orders. Every
// arrival order is a list of NDJSON files, read a line at a time in turn,
// exactly as the inorderly command reads its -in files. Check runs each one
// through an Orderer, runs a typical hand-written consumer on the same
// arrivals for comparison, and returns the numbers the posts quote. It fails
// if any promise doesn't hold.
//
// Everything comes from fixed seeds, so the numbers are the same on every run.
package usecase

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/inorderly/inorderly"
)

// t0 is when every example starts: 09:00 UTC on 5 October 2026, in
// milliseconds since 1970. Every time in the examples is in milliseconds.
var t0 = time.Date(2026, 10, 5, 9, 0, 0, 0, time.UTC).UnixMilli()

// File is one NDJSON input file.
type File struct {
	Name   string
	Events []inorderly.Event
}

// Input is one arrival order: the files of one run of the command.
type Input struct {
	Name  string
	Files []File
}

// Case is one worked example.
type Case struct {
	Name   string
	Config inorderly.Config
	Inputs []Input
	check  func(c *Case, runs []Run) (map[string]any, error)
}

// Run is what an Orderer made of one Input.
type Run struct {
	Input    string
	Hash     string
	Stats    inorderly.Stats
	Gaps     []inorderly.Gap
	Released []inorderly.Released
	Late     []inorderly.Event
}

// All returns the four examples.
func All() []*Case {
	return []*Case{Webhooks(), Feeds(), Telemetry(), Audit()}
}

// Arrivals returns an Input's events in the order the command reads them:
// one line from each file in turn, skipping files that have run out.
func (in Input) Arrivals() []inorderly.Event {
	var out []inorderly.Event
	pos := make([]int, len(in.Files))
	for left := true; left; {
		left = false
		for i, f := range in.Files {
			if pos[i] < len(f.Events) {
				out = append(out, f.Events[pos[i]])
				pos[i]++
				left = true
			}
		}
	}
	return out
}

// Order runs one Input through a new Orderer.
func (c *Case) Order(in Input) (Run, error) {
	o, err := inorderly.New(c.Config)
	if err != nil {
		return Run{}, err
	}
	r := Run{Input: in.Name}
	for _, e := range in.Arrivals() {
		st, rel, err := o.Push(e)
		if err != nil {
			return Run{}, fmt.Errorf("%s: %v", in.Name, err)
		}
		if st == inorderly.Late {
			r.Late = append(r.Late, e)
		}
		r.Released = append(r.Released, rel...)
	}
	r.Released = append(r.Released, o.Flush()...)
	r.Hash, r.Stats, r.Gaps = o.Hash(), o.Stats(), o.Gaps()
	return r, nil
}

// Check orders every Input, checks the promises the example makes and
// returns its report.
func (c *Case) Check() (map[string]any, error) {
	var runs []Run
	hashes := map[string]string{}
	for _, in := range c.Inputs {
		r, err := c.Order(in)
		if err != nil {
			return nil, err
		}
		runs = append(runs, r)
		hashes[in.Name] = r.Hash
	}
	for _, r := range runs[1:] {
		if r.Hash != runs[0].Hash {
			return nil, fmt.Errorf("%s: %s and %s gave different output", c.Name, runs[0].Input, r.Input)
		}
	}
	rep, err := c.check(c, runs)
	if err != nil {
		return nil, fmt.Errorf("%s: %v", c.Name, err)
	}
	rep["config"] = c.Config
	rep["hashes"] = hashes
	rep["identical"] = true
	rep["commands"] = c.Commands()
	return rep, nil
}

// Commands are the inorderly commands that order each Input from the files
// Write makes, run from the example's directory.
func (c *Case) Commands() []string {
	var cmds []string
	for _, in := range c.Inputs {
		args := []string{"inorderly", "-window", strconv.FormatInt(c.Config.Window, 10)}
		if c.Config.Hold != c.Config.Window {
			args = append(args, "-hold", strconv.FormatInt(c.Config.Hold, 10))
		}
		for _, f := range in.Files {
			args = append(args, "-in", f.Name)
		}
		args = append(args, "-out", in.Name+".out.ndjson", "-rejects", in.Name+".rejects.ndjson")
		cmds = append(cmds, strings.Join(args, " "))
	}
	return cmds
}

// Write puts every Input's files in dir/<case name>.
func (c *Case) Write(dir string) error {
	d := filepath.Join(dir, c.Name)
	if err := os.MkdirAll(d, 0o755); err != nil {
		return err
	}
	done := map[string]bool{}
	for _, in := range c.Inputs {
		for _, f := range in.Files {
			if done[f.Name] {
				continue
			}
			done[f.Name] = true
			var b []byte
			for i := range f.Events {
				line, err := json.Marshal(&f.Events[i])
				if err != nil {
					return err
				}
				b = append(append(b, line...), '\n')
			}
			if err := os.WriteFile(filepath.Join(d, f.Name), b, 0o644); err != nil {
				return err
			}
		}
	}
	return nil
}

// delivery is one event arriving at time at. n breaks ties, so arrival order
// never depends on how a sort treats equal times.
type delivery struct {
	at int64
	n  int
	ev inorderly.Event
}

type deliveries []delivery

func (d *deliveries) add(at int64, ev inorderly.Event) {
	*d = append(*d, delivery{at: at, n: len(*d), ev: ev})
}

// arrived returns the events in the order they arrived.
func (d deliveries) arrived() []inorderly.Event {
	s := append(deliveries(nil), d...)
	sort.Slice(s, func(i, j int) bool {
		if s[i].at != s[j].at {
			return s[i].at < s[j].at
		}
		return s[i].n < s[j].n
	})
	out := make([]inorderly.Event, len(s))
	for i := range s {
		out[i] = s[i].ev
	}
	return out
}

func newRand(seed, stream uint64) *rand.Rand {
	return rand.New(rand.NewPCG(seed, stream))
}

// field reads one top-level member of an event's data.
func field[T any](e inorderly.Event, name string) T {
	var m map[string]json.RawMessage
	var v T
	if json.Unmarshal(e.Data, &m) == nil {
		json.Unmarshal(m[name], &v)
	}
	return v
}

// parentsFirst checks that every released event's parents were released
// before it, and that none was reported missing.
func parentsFirst(r Run) error {
	seen := map[string]bool{}
	for _, rel := range r.Released {
		if len(rel.Missing) > 0 {
			return fmt.Errorf("%s: %s went out with missing parents %v", r.Input, rel.Event.Identity(), rel.Missing)
		}
		for _, p := range rel.Event.Parents {
			if !seen[p] {
				return fmt.Errorf("%s: %s went out before its parent %s", r.Input, rel.Event.Identity(), p)
			}
		}
		seen[rel.Event.Identity()] = true
	}
	return nil
}

// expectGaps checks an Orderer's gaps against the sequence numbers that
// should be missing: for each producer, every run of numbers not released
// between the first and the last that were.
func expectGaps(r Run) ([]inorderly.Gap, error) {
	got := map[string]map[uint64]bool{}
	for _, rel := range r.Released {
		e := rel.Event
		if e.Seq == 0 {
			continue
		}
		if got[e.Producer] == nil {
			got[e.Producer] = map[uint64]bool{}
		}
		got[e.Producer][e.Seq] = true
	}
	var producers []string
	for p := range got {
		producers = append(producers, p)
	}
	sort.Strings(producers)
	var want []inorderly.Gap
	for _, p := range producers {
		lo, hi := ^uint64(0), uint64(0)
		for s := range got[p] {
			lo, hi = min(lo, s), max(hi, s)
		}
		for s := lo; s <= hi; s++ {
			if got[p][s] {
				continue
			}
			if n := len(want); n > 0 && want[n-1].Producer == p && want[n-1].To == s-1 {
				want[n-1].To = s
			} else {
				want = append(want, inorderly.Gap{Producer: p, From: s, To: s})
			}
		}
	}
	if r.Stats.SeqGaps != uint64(len(want)) {
		return nil, fmt.Errorf("%s: %d gaps reported, %d expected", r.Input, r.Stats.SeqGaps, len(want))
	}
	if len(want) <= len(r.Gaps) || len(r.Gaps) < 1000 {
		key := func(g inorderly.Gap) string { return fmt.Sprintf("%s#%d-%d", g.Producer, g.From, g.To) }
		have := map[string]bool{}
		for _, g := range r.Gaps {
			have[key(g)] = true
		}
		for _, g := range want {
			if !have[key(g)] {
				return nil, fmt.Errorf("%s: the gap %s isn't reported", r.Input, key(g))
			}
		}
	}
	return want, nil
}

// childBeforeParent counts the times an event comes before a parent it
// names, in a list taken in order.
func childBeforeParent(list []inorderly.Event) int {
	seen := map[string]bool{}
	n := 0
	for _, e := range list {
		for _, p := range e.Parents {
			if !seen[p] {
				n++
			}
		}
		seen[e.Identity()] = true
	}
	return n
}

// sortByTime sorts by timestamp, ties by identity: what a sort buffer does.
func sortByTime(list []inorderly.Event) {
	sort.SliceStable(list, func(i, j int) bool {
		if list[i].Time != list[j].Time {
			return list[i].Time < list[j].Time
		}
		return list[i].Identity() < list[j].Identity()
	})
}

func missingCount(gaps []inorderly.Gap) uint64 {
	var n uint64
	for _, g := range gaps {
		n += g.To - g.From + 1
	}
	return n
}
