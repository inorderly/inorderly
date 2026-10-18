// Copyright Inorderly.com 2026
// SPDX-License-Identifier: Apache-2.0

package inorderly

import (
	"container/heap"
	"crypto/sha256"
	"encoding"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
)

// Snapshot is an Orderer's state, for saving to disk and carrying on later
// with Restore: the events still held, the released events it remembers,
// the counters and the replay hash so far.
type Snapshot struct {
	Format  int     `json:"format"`
	Config  Config  `json:"config"`
	Seen    bool    `json:"seen"`
	MaxSeen int64   `json:"max_seen"`
	Pending []Event `json:"pending"`
	// Released is left out by SnapshotHeld; the caller keeps the records
	// itself and hands them back to Restore.
	Released []Record          `json:"released"`
	Partial  bool              `json:"partial,omitempty"`
	Pos      uint64            `json:"pos"`
	Hash     []byte            `json:"hash"`
	Stats    Stats             `json:"stats"`
	LastSeq  map[string]uint64 `json:"last_seq"`
	Gaps     []Gap             `json:"gaps"`
}

const snapshotFormat = 1

// Snapshot captures the whole state between two calls to Push. It can't be
// taken after Flush.
func (o *Orderer) Snapshot() (*Snapshot, error) {
	return o.snapshot(true)
}

// SnapshotHeld captures the state without the released events it
// remembers, which can be many. Keep the Record of every Released event
// instead, as it comes out, and give them back to Restore. Records the
// Orderer has forgotten since can be given too; they are dropped again.
func (o *Orderer) SnapshotHeld() (*Snapshot, error) {
	return o.snapshot(false)
}

// Remembered returns the records of the released events the Orderer still
// remembers, sorted by identity. With Remember above 0 it can be far fewer
// than every record kept since the start, so a caller using SnapshotHeld
// can replace its own list with this one from time to time.
func (o *Orderer) Remembered() []Record {
	out := make([]Record, 0, len(o.released))
	for id, r := range o.released {
		out = append(out, Record{Ident: id, Time: r.time, Sum: hex.EncodeToString(r.sum[:])})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Ident < out[j].Ident })
	return out
}

func (o *Orderer) snapshot(withReleased bool) (*Snapshot, error) {
	if o.final {
		return nil, ErrClosed
	}
	h, err := o.hash.(encoding.BinaryMarshaler).MarshalBinary()
	if err != nil {
		return nil, err
	}
	s := &Snapshot{
		Format: snapshotFormat, Config: o.cfg, Seen: o.seen, MaxSeen: o.maxSeen,
		Pos: o.pos, Hash: h, Stats: o.stats, LastSeq: map[string]uint64{}, Gaps: append([]Gap{}, o.gaps...),
		Partial: !withReleased,
	}
	s.Stats.Pending = 0
	for _, n := range o.pending {
		s.Pending = append(s.Pending, n.ev)
	}
	sort.Slice(s.Pending, func(i, j int) bool { return s.Pending[i].Identity() < s.Pending[j].Identity() })
	if withReleased {
		s.Released = o.Remembered()
	}
	for p, v := range o.lastSeq {
		s.LastSeq[p] = v
	}
	return s, nil
}

// Restore makes an Orderer from a Snapshot, and for a snapshot from
// SnapshotHeld, the records of the events released before it. It carries
// on exactly where the snapshot was taken: the same events come out next.
func Restore(s *Snapshot, records []Record) (*Orderer, error) {
	if s.Format != snapshotFormat {
		return nil, fmt.Errorf("inorderly: snapshot format %d, expected %d", s.Format, snapshotFormat)
	}
	o, err := New(s.Config)
	if err != nil {
		return nil, err
	}
	o.seen, o.maxSeen, o.pos, o.stats = s.Seen, s.MaxSeen, s.Pos, s.Stats
	if err := o.hash.(encoding.BinaryUnmarshaler).UnmarshalBinary(s.Hash); err != nil {
		return nil, err
	}
	for p, v := range s.LastSeq {
		o.lastSeq[p] = v
	}
	o.gaps = append(o.gaps, s.Gaps...)
	if s.Partial && len(s.Released) > 0 {
		return nil, errors.New("inorderly: a partial snapshot holds released events")
	}
	for _, r := range append(append([]Record{}, s.Released...), records...) {
		sum, err := hex.DecodeString(r.Sum)
		if err != nil || len(sum) != sha256.Size || r.Ident == "" {
			return nil, errors.New("inorderly: a released event's record is damaged")
		}
		if _, dup := o.released[r.Ident]; dup {
			return nil, fmt.Errorf("inorderly: the event %q is recorded as released twice", r.Ident)
		}
		info := releasedInfo{time: r.Time}
		copy(info.sum[:], sum)
		o.released[r.Ident] = info
		if o.cfg.Remember > 0 {
			heap.Push(&o.forget, forgetEntry{time: r.Time, ident: r.Ident})
		}
	}
	var nodes []*node
	for _, e := range s.Pending {
		if err := e.Validate(); err != nil {
			return nil, fmt.Errorf("inorderly: a held event in the snapshot is damaged: %v", err)
		}
		n := &node{ev: e, ident: e.Identity(), own: ownPoint(&e), canon: e.Canonical()}
		if _, dup := o.pending[n.ident]; dup {
			return nil, errors.New("inorderly: the snapshot holds an event twice")
		}
		o.insert(n)
		nodes = append(nodes, n)
	}
	for _, n := range nodes {
		if !n.resolved {
			o.resolveOrWait(n)
		}
	}
	if o.pos != uint64(len(o.released)) && o.cfg.Remember == 0 {
		return nil, fmt.Errorf("inorderly: %d events were released but %d are recorded", o.pos, len(o.released))
	}
	// Deadlines that had passed were settled before the snapshot; settling
	// them again in the same order gives the same keys. Nothing can be due
	// for release: everything below the watermark went out before.
	o.advance()
	if len(o.out) > 0 {
		return nil, errors.New("inorderly: the snapshot doesn't match its own watermark")
	}
	return o, nil
}
