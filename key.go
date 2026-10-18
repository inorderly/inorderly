// Copyright Inorderly.com 2026
// SPDX-License-Identifier: Apache-2.0

package inorderly

import (
	"math"
	"strings"
)

// A point is one place in the order. An event's own point is its time,
// logical counter, producer, seq and id. A deadline point stands for the end
// of a time: it comes after every event with that time.
type point struct {
	Time     int64
	Deadline bool
	Logical  uint32
	Producer string
	Seq      uint64
	ID       string
}

func ownPoint(e *Event) point {
	return point{Time: e.Time, Logical: e.Logical, Producer: e.Producer, Seq: e.Seq, ID: e.ID}
}

func deadlinePoint(t int64) point {
	return point{Time: t, Deadline: true}
}

func (a point) cmp(b point) int {
	switch {
	case a.Time != b.Time:
		return cmpInt(a.Time < b.Time)
	case a.Deadline != b.Deadline:
		return cmpInt(!a.Deadline)
	case a.Logical != b.Logical:
		return cmpInt(a.Logical < b.Logical)
	case a.Producer != b.Producer:
		return strings.Compare(a.Producer, b.Producer)
	case a.Seq != b.Seq:
		return cmpInt(a.Seq < b.Seq)
	default:
		return strings.Compare(a.ID, b.ID)
	}
}

func cmpInt(less bool) int {
	if less {
		return -1
	}
	return 1
}

// A key is an event's place in the output: a list of points compared in
// turn, where a key that is a prefix of another comes first. An event with
// no parents to wait for has its own point as its key. A child that must
// follow a parent has the parent's key with its own point added, which puts
// it straight after the parent; a child that gave up waiting has a deadline
// point and its own point.
type key []point

func (a key) cmp(b key) int {
	for i := 0; i < len(a) && i < len(b); i++ {
		if c := a[i].cmp(b[i]); c != 0 {
			return c
		}
	}
	switch {
	case len(a) < len(b):
		return -1
	case len(a) > len(b):
		return 1
	}
	return 0
}

func (a key) extend(p point) key {
	k := make(key, len(a)+1)
	copy(k, a)
	k[len(a)] = p
	return k
}

func satAdd(a, b int64) int64 {
	if b > 0 && a > math.MaxInt64-b {
		return math.MaxInt64
	}
	return a + b
}

func satSub(a, b int64) int64 {
	if b > 0 && a < math.MinInt64+b {
		return math.MinInt64
	}
	return a - b
}
