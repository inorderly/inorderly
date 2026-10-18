// Copyright Inorderly.com 2026
// SPDX-License-Identifier: Apache-2.0

package inorderly

import (
	"container/heap"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"hash"
)

// Config sets the bounds of an Orderer, in the units of Event.Time.
type Config struct {
	// Window is how far behind the newest event time an event may arrive and
	// still be ordered. The watermark is the newest time seen minus Window:
	// events with a time below it are released, and an event that arrives
	// with a time below it is late and refused.
	Window int64 `json:"window"`
	// Hold is how long, past its own time, an event waits for a parent that
	// hasn't been released. After that it is released anyway and the parent
	// is reported missing.
	Hold int64 `json:"hold"`
	// Remember is how long, behind the watermark, released events are kept
	// for spotting duplicates and finding parents. Zero keeps them all. A
	// parent more than Remember older than its child counts as missing.
	Remember int64 `json:"remember"`
}

func (c Config) check() error {
	if c.Window < 0 || c.Hold < 0 || c.Remember < 0 {
		return errors.New("window, hold and remember can't be negative")
	}
	return nil
}

// Status says what Push did with an event.
type Status int

const (
	// Accepted: the event is held for ordering.
	Accepted Status = iota
	// Duplicate: the same event, with the same content, came before. Dropped.
	Duplicate
	// Late: the event's time is below the watermark. Refused.
	Late
	// Conflict: another event with the same identity but different content
	// came first and is kept. Refused.
	Conflict
)

func (s Status) String() string {
	if s < Accepted || s > Conflict {
		return "unknown"
	}
	return [...]string{"accepted", "duplicate", "late", "conflict"}[s]
}

// ErrClosed is returned by Push after Flush.
var ErrClosed = errors.New("inorderly: the orderer is flushed")

// Released is one event as it leaves the Orderer.
type Released struct {
	// Pos is the event's position in the output, from 1.
	Pos uint64
	// Event is the event.
	Event Event
	// Missing lists the parents that weren't released before this event.
	Missing []string
	// Line is the output line: the canonical event with pos and missing,
	// without a newline. The replay hash covers Line plus a newline.
	Line []byte
	// Record is what the Orderer remembers of the event, for keeping beside
	// a snapshot taken with SnapshotHeld.
	Record Record
}

// Record is what an Orderer remembers of a released event: its identity,
// its time, and the SHA-256 of its canonical form for telling a repeat from
// a conflict.
type Record struct {
	Ident string `json:"ident"`
	Time  int64  `json:"time"`
	Sum   string `json:"sum"`
}

// Gap is a run of sequence numbers missing from a producer's output.
type Gap struct {
	Producer string `json:"producer"`
	From     uint64 `json:"from"`
	To       uint64 `json:"to"`
}

// Stats counts what an Orderer has done.
type Stats struct {
	In             uint64 `json:"in"`
	Out            uint64 `json:"out"`
	Duplicates     uint64 `json:"duplicates"`
	Late           uint64 `json:"late"`
	Conflicts      uint64 `json:"conflicts"`
	MissingParents uint64 `json:"missing_parents"`
	SeqGaps        uint64 `json:"seq_gaps"`
	SeqBackwards   uint64 `json:"seq_backwards"`
	Pending        int    `json:"pending"`
}

// MaxChain is the longest chain of parents a child is placed straight
// after. A child whose parent's key already holds MaxChain points goes at its
// own deadline instead, so that a long run of events whose clocks each fall
// behind their parent's can't make keys grow without limit.
const MaxChain = 64

// maxGaps is how many gaps Gaps keeps; SeqGaps counts them all.
const maxGaps = 1000

type node struct {
	ev       Event
	ident    string
	own      point
	canon    []byte
	parents  []string // unique
	deadline int64
	resolved bool
	key      key
	waiting  []string // parents this node is listed under in waiters
}

// releasedInfo is what is remembered of a released event. Its key isn't
// needed: an event on time always sorts after everything already released,
// so a released parent never moves its child.
type releasedInfo struct {
	time int64
	sum  [32]byte
}

// Orderer orders events. It is not safe for concurrent use.
type Orderer struct {
	cfg     Config
	seen    bool
	maxSeen int64
	final   bool

	pending   map[string]*node
	ready     readyHeap
	deadlines deadlineHeap
	waiters   map[string][]*node
	waitCount map[string]int // unresolved nodes listed under each parent
	released  map[string]releasedInfo
	forget    forgetHeap
	held      heldHeap // held events by time, to know how far back to remember

	pos     uint64
	hash    hash.Hash
	stats   Stats
	lastSeq map[string]uint64
	gaps    []Gap
	out     []Released
}

// New makes an Orderer.
func New(cfg Config) (*Orderer, error) {
	if err := cfg.check(); err != nil {
		return nil, err
	}
	return &Orderer{
		cfg:       cfg,
		pending:   map[string]*node{},
		waiters:   map[string][]*node{},
		waitCount: map[string]int{},
		released:  map[string]releasedInfo{},
		hash:      sha256.New(),
		lastSeq:   map[string]uint64{},
	}, nil
}

// Watermark is the newest time seen minus Window, and ok is false before
// the first event.
func (o *Orderer) Watermark() (wm int64, ok bool) {
	return satSub(o.maxSeen, o.cfg.Window), o.seen
}

func (o *Orderer) passed(t int64) bool {
	if o.final {
		return true
	}
	wm, ok := o.Watermark()
	return ok && t < wm
}

// Push takes one event and returns what it did with it, and the events
// that are now released, in order. The event is validated first.
func (o *Orderer) Push(e Event) (Status, []Released, error) {
	if o.final {
		return 0, nil, ErrClosed
	}
	if err := e.Validate(); err != nil {
		return 0, nil, err
	}
	o.stats.In++
	n := &node{ev: e, ident: e.Identity(), own: ownPoint(&e), canon: e.Canonical()}
	if p, ok := o.pending[n.ident]; ok {
		return o.same(string(p.canon) == string(n.canon)), nil, nil
	}
	if r, ok := o.released[n.ident]; ok {
		return o.same(r.sum == sha256.Sum256(n.canon)), nil, nil
	}
	if o.passed(e.Time) {
		o.stats.Late++
		return Late, nil, nil
	}
	o.insert(n)
	o.resolveOrWait(n)
	if !o.seen || e.Time > o.maxSeen {
		o.seen = true
		o.maxSeen = e.Time
		o.advance()
	}
	return Accepted, o.take(), nil
}

func (o *Orderer) same(equal bool) Status {
	if equal {
		o.stats.Duplicates++
		return Duplicate
	}
	o.stats.Conflicts++
	return Conflict
}

// Flush releases every event still held, as if the stream ended. Push
// can't be called after it.
func (o *Orderer) Flush() []Released {
	o.final = true
	o.advance()
	return o.take()
}

func (o *Orderer) take() []Released {
	out := o.out
	o.out = nil
	return out
}

func (o *Orderer) insert(n *node) {
	seen := map[string]bool{}
	for _, p := range n.ev.Parents {
		if !seen[p] {
			seen[p] = true
			n.parents = append(n.parents, p)
		}
	}
	n.deadline = satAdd(n.ev.Time, o.cfg.Hold)
	o.pending[n.ident] = n
	heap.Push(&o.deadlines, n)
	if o.cfg.Remember > 0 {
		heap.Push(&o.held, n)
	}
}

// resolveOrWait gives n its key if it can be known now; if not, n waits on
// the parents it needs. Either way, children waiting on n try again.
func (o *Orderer) resolveOrWait(n *node) {
	if o.tryResolve(n, false) {
		o.propagate(n)
		return
	}
	for _, p := range n.parents {
		if !o.known(n, p) {
			o.waiters[p] = append(o.waiters[p], n)
			o.waitCount[p]++
			n.waiting = append(n.waiting, p)
		}
	}
}

// known reports whether parent p's contribution to n's key is settled.
func (o *Orderer) known(n *node, p string) bool {
	_, ok := o.candidate(n, p, false)
	return ok
}

// candidate is what parent p puts into child n's key, and whether that is
// known yet. A parent released or resolved before n's deadline puts n just
// after it. A missing parent, one too old to count, or one that comes at or
// after n's deadline puts n at its deadline. force settles a parent that is
// held but not yet resolved as missing.
func (o *Orderer) candidate(n *node, p string, force bool) (key, bool) {
	dk := key{deadlinePoint(n.deadline)}
	after := func(pk key) key {
		if pk.cmp(dk) < 0 && len(pk) < MaxChain {
			return pk.extend(n.own)
		}
		return dk.extend(n.own)
	}
	if r, ok := o.released[p]; ok {
		if o.tooOld(r.time, n) {
			return dk.extend(n.own), true
		}
		return key{n.own}, true
	}
	if pn, ok := o.pending[p]; ok && pn != n {
		if o.tooOld(pn.ev.Time, n) {
			return dk.extend(n.own), true
		}
		if pn.resolved {
			return after(pn.key), true
		}
		if force {
			return dk.extend(n.own), true
		}
		return nil, false
	}
	if force || o.passed(n.deadline) {
		return dk.extend(n.own), true
	}
	return nil, false
}

func (o *Orderer) tooOld(parentTime int64, n *node) bool {
	return o.cfg.Remember > 0 && parentTime < satSub(n.ev.Time, o.cfg.Remember)
}

func (o *Orderer) tryResolve(n *node, force bool) bool {
	k := key{n.own}
	for _, p := range n.parents {
		c, ok := o.candidate(n, p, force)
		if !ok {
			return false
		}
		if c.cmp(k) > 0 {
			k = c
		}
	}
	n.key = k
	n.resolved = true
	heap.Push(&o.ready, n)
	// n no longer waits: take it off its parents' lists, and drop a list
	// once nobody on it is waiting.
	for _, p := range n.waiting {
		if c, ok := o.waitCount[p]; ok {
			if c <= 1 {
				delete(o.waitCount, p)
				delete(o.waiters, p)
			} else {
				o.waitCount[p] = c - 1
			}
		}
	}
	n.waiting = nil
	return true
}

// propagate lets the children waiting on a node that was just resolved
// try again, and theirs in turn.
func (o *Orderer) propagate(n *node) {
	queue := []*node{n}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		children := o.waiters[cur.ident]
		delete(o.waiters, cur.ident)
		delete(o.waitCount, cur.ident)
		for _, c := range children {
			if !c.resolved && o.pending[c.ident] == c && o.tryResolve(c, false) {
				queue = append(queue, c)
			}
		}
	}
}

// advance settles every event whose deadline has passed, earliest deadline
// first, then releases every resolved event whose key is below the
// watermark, in key order.
func (o *Orderer) advance() {
	for o.deadlines.Len() > 0 && o.passed(o.deadlines[0].deadline) {
		n := heap.Pop(&o.deadlines).(*node)
		if n.resolved || o.pending[n.ident] != n {
			continue
		}
		if !o.tryResolve(n, false) {
			o.tryResolve(n, true)
		}
		o.propagate(n)
	}
	for o.ready.Len() > 0 && o.passed(o.ready[0].key[0].Time) {
		o.emit(heap.Pop(&o.ready).(*node))
	}
	if wm, ok := o.Watermark(); ok && o.cfg.Remember > 0 && !o.final {
		// Keep what a held event could still name as a recent parent: forget
		// only below the oldest held event's time, less Remember.
		for o.held.Len() > 0 && o.pending[o.held[0].ident] != o.held[0] {
			heap.Pop(&o.held)
		}
		if o.held.Len() > 0 && o.held[0].ev.Time < wm {
			wm = o.held[0].ev.Time
		}
		limit := satSub(wm, o.cfg.Remember)
		for o.forget.Len() > 0 && o.forget[0].time < limit {
			f := heap.Pop(&o.forget).(forgetEntry)
			delete(o.released, f.ident)
		}
	}
}

func (o *Orderer) emit(n *node) {
	var missing []string
	for _, p := range n.parents {
		r, ok := o.released[p]
		if !ok || o.tooOld(r.time, n) {
			missing = append(missing, p)
		}
	}
	o.pos++
	line := n.ev.appendJSON(nil, o.pos, missing)
	o.hash.Write(line)
	o.hash.Write([]byte{'\n'})
	sum := sha256.Sum256(n.canon)
	o.released[n.ident] = releasedInfo{time: n.ev.Time, sum: sum}
	if o.cfg.Remember > 0 {
		heap.Push(&o.forget, forgetEntry{time: n.ev.Time, ident: n.ident})
	}
	delete(o.pending, n.ident)
	o.stats.Out++
	if len(missing) > 0 {
		o.stats.MissingParents++
	}
	if s := n.ev.Seq; s > 0 {
		last, ok := o.lastSeq[n.ev.Producer]
		switch {
		case !ok || s > last:
			if ok && s > last+1 {
				o.stats.SeqGaps++
				if len(o.gaps) < maxGaps {
					o.gaps = append(o.gaps, Gap{Producer: n.ev.Producer, From: last + 1, To: s - 1})
				}
			}
			o.lastSeq[n.ev.Producer] = s
		default:
			o.stats.SeqBackwards++
		}
	}
	o.out = append(o.out, Released{Pos: o.pos, Event: n.ev, Missing: missing, Line: line,
		Record: Record{Ident: n.ident, Time: n.ev.Time, Sum: hex.EncodeToString(sum[:])}})
	// The node may stay in the deadline heap until its deadline passes; keep
	// only what the heaps compare.
	n.ev.Data, n.ev.Parents, n.parents, n.canon, n.key = nil, nil, nil, nil, nil
}

// Hash is the replay hash so far: "sha256:" and the hex SHA-256 of every
// output line with its newline, which is the SHA-256 of the output file.
func (o *Orderer) Hash() string {
	return "sha256:" + hex.EncodeToString(o.hash.Sum(nil))
}

// Stats returns the counts so far.
func (o *Orderer) Stats() Stats {
	s := o.stats
	s.Pending = len(o.pending)
	return s
}

// Gaps returns the first 1000 sequence gaps found in the output so far.
func (o *Orderer) Gaps() []Gap {
	return append(make([]Gap, 0, len(o.gaps)), o.gaps...)
}

// Config returns the configuration.
func (o *Orderer) Config() Config { return o.cfg }

type readyHeap []*node

func (h readyHeap) Len() int            { return len(h) }
func (h readyHeap) Less(i, j int) bool  { return h[i].key.cmp(h[j].key) < 0 }
func (h readyHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *readyHeap) Push(x interface{}) { *h = append(*h, x.(*node)) }
func (h *readyHeap) Pop() interface{} {
	old := *h
	n := old[len(old)-1]
	*h = old[:len(old)-1]
	return n
}

type deadlineHeap []*node

func (h deadlineHeap) Len() int { return len(h) }
func (h deadlineHeap) Less(i, j int) bool {
	if h[i].deadline != h[j].deadline {
		return h[i].deadline < h[j].deadline
	}
	return h[i].own.cmp(h[j].own) < 0
}
func (h deadlineHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *deadlineHeap) Push(x interface{}) { *h = append(*h, x.(*node)) }
func (h *deadlineHeap) Pop() interface{} {
	old := *h
	n := old[len(old)-1]
	*h = old[:len(old)-1]
	return n
}

type heldHeap []*node

func (h heldHeap) Len() int            { return len(h) }
func (h heldHeap) Less(i, j int) bool  { return h[i].ev.Time < h[j].ev.Time }
func (h heldHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *heldHeap) Push(x interface{}) { *h = append(*h, x.(*node)) }
func (h *heldHeap) Pop() interface{} {
	old := *h
	n := old[len(old)-1]
	*h = old[:len(old)-1]
	return n
}

type forgetEntry struct {
	time  int64
	ident string
}

type forgetHeap []forgetEntry

func (h forgetHeap) Len() int            { return len(h) }
func (h forgetHeap) Less(i, j int) bool  { return h[i].time < h[j].time }
func (h forgetHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *forgetHeap) Push(x interface{}) { *h = append(*h, x.(forgetEntry)) }
func (h *forgetHeap) Pop() interface{} {
	old := *h
	n := old[len(old)-1]
	*h = old[:len(old)-1]
	return n
}
