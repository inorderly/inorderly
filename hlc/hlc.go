// Copyright Inorderly.com 2026
// SPDX-License-Identifier: Apache-2.0

// Package hlc is a hybrid logical clock for producers that feed Inorderly.
//
// A hybrid logical clock gives each event a wall time and a logical counter.
// It stays close to the real clock but never runs backwards, and an event
// stamped after a message was received always sorts after that message,
// even when the sender's clock was ahead. Stamp events with Now, and call
// Observe with the timestamp of every event you receive before stamping
// what you make from it; then a parent's timestamp is always below its
// child's, and Inorderly never has to wait for a parent.
package hlc

import (
	"math"
	"sync"
	"time"
)

// Clock is a hybrid logical clock. It is safe for concurrent use.
type Clock struct {
	mu      sync.Mutex
	now     func() int64
	wall    int64
	logical uint32
}

// New makes a Clock that reads the wall time from now, in the units your
// events use. A nil now reads milliseconds since 1970.
func New(now func() int64) *Clock {
	if now == nil {
		now = func() int64 { return time.Now().UnixMilli() }
	}
	return &Clock{now: now}
}

// Now stamps a local event.
func (c *Clock) Now() (wall int64, logical uint32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if pt := c.now(); pt > c.wall {
		c.wall, c.logical = pt, 0
	} else {
		c.tick()
	}
	return c.wall, c.logical
}

// Observe takes the timestamp of a received event and stamps the receipt,
// which sorts after both the received event and everything this clock
// stamped before.
func (c *Clock) Observe(wall int64, logical uint32) (int64, uint32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	pt := c.now()
	switch {
	case pt > c.wall && pt > wall:
		c.wall, c.logical = pt, 0
	case wall > c.wall:
		c.wall, c.logical = wall, logical
		c.tick()
	case wall == c.wall && logical > c.logical:
		c.logical = logical
		c.tick()
	default:
		c.tick()
	}
	return c.wall, c.logical
}

// tick moves one logical step on; when the counter is full, the wall time
// moves on one unit instead.
func (c *Clock) tick() {
	if c.logical == math.MaxUint32 {
		c.wall++
		c.logical = 0
		return
	}
	c.logical++
}
