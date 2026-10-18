// Copyright Inorderly.com 2026
// SPDX-License-Identifier: Apache-2.0

package hlc

import (
	"math"
	"sync"
	"testing"
)

type stamp struct {
	wall    int64
	logical uint32
}

func (a stamp) less(b stamp) bool {
	return a.wall < b.wall || (a.wall == b.wall && a.logical < b.logical)
}

func TestNowNeverGoesBack(t *testing.T) {
	wall := int64(100)
	c := New(func() int64 { return wall })
	var last stamp
	for i, w := range []int64{100, 100, 101, 90, 90, 200, 150} {
		wall = w
		s := stamp{}
		s.wall, s.logical = c.Now()
		if i > 0 && !last.less(s) {
			t.Fatalf("step %d: %v after %v", i, s, last)
		}
		last = s
	}
	if last != (stamp{200, 1}) {
		t.Fatalf("ended at %v", last)
	}
}

func TestObserveSortsAfterTheMessage(t *testing.T) {
	wall := int64(10)
	c := New(func() int64 { return wall })
	c.Now()
	// A sender whose clock is far ahead.
	got := stamp{}
	got.wall, got.logical = c.Observe(500, 7)
	if !(stamp{500, 7}).less(got) {
		t.Fatalf("receipt %v isn't after the message", got)
	}
	next := stamp{}
	next.wall, next.logical = c.Now()
	if !got.less(next) {
		t.Fatalf("%v isn't after %v", next, got)
	}
	// A message from the same time with a higher counter.
	m := next
	m.logical += 5
	r := stamp{}
	r.wall, r.logical = c.Observe(m.wall, m.logical)
	if !m.less(r) {
		t.Fatalf("receipt %v isn't after %v", r, m)
	}
	// A message from the past changes nothing but the counter.
	wall = 20
	r2 := stamp{}
	r2.wall, r2.logical = c.Observe(1, 0)
	if !r.less(r2) {
		t.Fatalf("receipt %v isn't after %v", r2, r)
	}
	// When the wall clock catches up, the counter starts again.
	wall = 1000
	r3 := stamp{}
	r3.wall, r3.logical = c.Observe(2, 0)
	if r3 != (stamp{1000, 0}) {
		t.Fatalf("got %v", r3)
	}
}

func TestCounterOverflowMovesTheWall(t *testing.T) {
	c := New(func() int64 { return 5 })
	w, l := c.Observe(5, math.MaxUint32-1)
	if w != 5 || l != math.MaxUint32 {
		t.Fatalf("%d %d", w, l)
	}
	w, l = c.Now()
	if w != 6 || l != 0 {
		t.Fatalf("after overflow: %d %d", w, l)
	}
}

func TestConcurrentStampsAreUnique(t *testing.T) {
	c := New(nil)
	var mu sync.Mutex
	seen := map[stamp]bool{}
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				s := stamp{}
				s.wall, s.logical = c.Now()
				mu.Lock()
				if seen[s] {
					t.Errorf("stamp %v given twice", s)
				}
				seen[s] = true
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
}
