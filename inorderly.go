// Copyright Inorderly.com 2026
// SPDX-License-Identifier: Apache-2.0

// Package inorderly turns events that arrive late, twice or out of order
// into one sequence that comes out the same every time.
//
// An Orderer holds incoming events for a bounded window of event time,
// drops duplicates, and releases the events in a fixed, published order:
// event time, then the logical counter of a hybrid logical clock, then
// producer, sequence number and id. An event that names causal parents is
// held until its parents are out, for a bounded time. Events that arrive too
// late are refused rather than slipped in somewhere new, so the same events
// give the same sequence however they arrived. docs/CONTRACT.md in the
// repository states the rules exactly.
//
// The replay hash is the SHA-256 of the ordered output, line by line, so two
// runs can be compared with one line each.
package inorderly

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
)

// Version is the release version of this code.
const Version = "0.1.0"

// MaxLine is the longest input line ParseLine accepts, in bytes.
const MaxLine = 16 << 20

// Event is one event. Producer and Time are required, and an event needs an
// identity: its ID, or else its Seq (above zero) within its Producer.
type Event struct {
	// ID identifies the event. Two events with the same identity are the
	// same event: one is kept and the other dropped as a duplicate.
	ID string `json:"id,omitempty"`
	// Producer names the source that made the event.
	Producer string `json:"producer"`
	// Seq is the event's position in its producer's own numbering. It
	// identifies the event when ID is empty, and gaps in it are reported.
	Seq uint64 `json:"seq,omitempty"`
	// Time is the event time, as an integer in units of your choice
	// (milliseconds since 1970 are common). Window, Hold and Remember are
	// in the same units.
	Time int64 `json:"time"`
	// Logical is the logical counter of a hybrid logical clock, which orders
	// events with the same Time. Package hlc makes these.
	Logical uint32 `json:"logical,omitempty"`
	// Parents lists the identities of events that must come before this one.
	Parents []string `json:"parents,omitempty"`
	// Data is the payload, any JSON value. It is kept in a canonical form:
	// no whitespace, object keys sorted, strings escaped one way, numbers
	// exactly as written.
	Data json.RawMessage `json:"data,omitempty"`
}

// Identity is the name the event is deduplicated under and that Parents
// refer to: the ID, or "producer#seq" when the ID is empty.
func (e *Event) Identity() string {
	if e.ID != "" {
		return e.ID
	}
	return e.Producer + "#" + strconv.FormatUint(e.Seq, 10)
}

// Validate reports whether the event can be ordered, and compacts Data.
func (e *Event) Validate() error {
	if e.Producer == "" {
		return errors.New("producer is empty")
	}
	if e.ID == "" && e.Seq == 0 {
		return errors.New("the event has neither an id nor a seq above zero")
	}
	for _, p := range e.Parents {
		if p == "" {
			return errors.New("a parent is empty")
		}
	}
	if len(e.Data) > 0 {
		c, err := canonicalJSON(e.Data)
		if err != nil {
			return fmt.Errorf("data is not valid JSON: %v", err)
		}
		e.Data = c
		if string(c) == "null" {
			e.Data = nil // the same as no data
		}
	}
	return nil
}

// ParseLine reads one event from one line of NDJSON. Unknown fields, a
// missing producer or time, fractional numbers and anything after the
// object are errors.
func ParseLine(line []byte) (Event, error) {
	if len(line) > MaxLine {
		return Event{}, fmt.Errorf("the line is longer than %d bytes", MaxLine)
	}
	var raw struct {
		ID       string          `json:"id"`
		Producer *string         `json:"producer"`
		Seq      uint64          `json:"seq"`
		Time     *int64          `json:"time"`
		Logical  uint32          `json:"logical"`
		Parents  []string        `json:"parents"`
		Data     json.RawMessage `json:"data"`
	}
	dec := json.NewDecoder(bytes.NewReader(line))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&raw); err != nil {
		return Event{}, err
	}
	if _, err := dec.Token(); err != io.EOF {
		return Event{}, errors.New("there is more after the event object")
	}
	if raw.Producer == nil {
		return Event{}, errors.New("producer is missing")
	}
	if raw.Time == nil {
		return Event{}, errors.New("time is missing")
	}
	e := Event{ID: raw.ID, Producer: *raw.Producer, Seq: raw.Seq, Time: *raw.Time,
		Logical: raw.Logical, Parents: raw.Parents, Data: raw.Data}
	if err := e.Validate(); err != nil {
		return Event{}, err
	}
	return e, nil
}

// Canonical is the event's fixed encoding: the fields in a set order, empty
// ones left out, Data without whitespace. Two arrivals of the same event
// have the same canonical bytes.
func (e *Event) Canonical() []byte {
	return e.appendJSON(nil, 0, nil)
}

// appendJSON writes the event as one JSON object. pos and missing are added
// for output lines; pos 0 leaves both out.
func (e *Event) appendJSON(b []byte, pos uint64, missing []string) []byte {
	b = append(b, '{')
	if pos > 0 {
		b = append(b, `"pos":`...)
		b = strconv.AppendUint(b, pos, 10)
		b = append(b, ',')
	}
	if e.ID != "" {
		b = append(b, `"id":`...)
		b = appendString(b, e.ID)
		b = append(b, ',')
	}
	b = append(b, `"producer":`...)
	b = appendString(b, e.Producer)
	if e.Seq != 0 {
		b = append(b, `,"seq":`...)
		b = strconv.AppendUint(b, e.Seq, 10)
	}
	b = append(b, `,"time":`...)
	b = strconv.AppendInt(b, e.Time, 10)
	if e.Logical != 0 {
		b = append(b, `,"logical":`...)
		b = strconv.AppendUint(b, uint64(e.Logical), 10)
	}
	if len(e.Parents) > 0 {
		b = append(b, `,"parents":`...)
		b = appendStrings(b, e.Parents)
	}
	if len(missing) > 0 {
		b = append(b, `,"missing":`...)
		b = appendStrings(b, missing)
	}
	if len(e.Data) > 0 {
		b = append(b, `,"data":`...)
		b = append(b, e.Data...)
	}
	return append(b, '}')
}

func appendStrings(b []byte, list []string) []byte {
	b = append(b, '[')
	for i, s := range list {
		if i > 0 {
			b = append(b, ',')
		}
		b = appendString(b, s)
	}
	return append(b, ']')
}
