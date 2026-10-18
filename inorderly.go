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
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"unicode/utf8"
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

// Validate reports whether the event can be ordered, and puts Data in its
// canonical form.
func (e *Event) Validate() error {
	return e.validate(true)
}

func (e *Event) validate(data bool) error {
	if e.Producer == "" {
		return errors.New("producer is empty")
	}
	if e.ID == "" && e.Seq == 0 {
		return errors.New("the event has neither an id nor a seq above zero")
	}
	// Identities must be valid UTF-8: invalid bytes would print as U+FFFD,
	// so two different identities could look the same, and a snapshot
	// couldn't hold them.
	if !utf8.ValidString(e.ID) || !utf8.ValidString(e.Producer) {
		return errors.New("the id or producer isn't valid UTF-8")
	}
	for _, p := range e.Parents {
		if p == "" {
			return errors.New("a parent is empty")
		}
		if !utf8.ValidString(p) {
			return errors.New("a parent isn't valid UTF-8")
		}
	}
	if data && len(e.Data) > 0 {
		c, err := canonicalJSON(e.Data)
		if err != nil {
			return fmt.Errorf("data is not valid JSON: %v", err)
		}
		e.Data = c
	}
	if string(e.Data) == "null" {
		e.Data = nil // the same as no data
	}
	return nil
}

// eventFields are the only keys an input line may have, spelled exactly so.
var eventFields = map[string]bool{"id": true, "producer": true, "seq": true, "time": true,
	"logical": true, "parents": true, "data": true}

// ParseLine reads one event from one line of NDJSON. Keys are matched
// exactly: unknown or repeated keys, a missing producer or time, fractional
// numbers and anything after the object are errors.
func ParseLine(line []byte) (Event, error) {
	if len(line) > MaxLine {
		return Event{}, fmt.Errorf("the line is longer than %d bytes", MaxLine)
	}
	members, err := topLevel(line)
	if err != nil {
		return Event{}, err
	}
	var e Event
	for k, v := range members {
		if !eventFields[k] {
			return Event{}, fmt.Errorf("unknown field %q", k)
		}
		var err error
		switch k {
		case "id":
			err = json.Unmarshal(v, &e.ID)
		case "producer":
			err = json.Unmarshal(v, &e.Producer)
		case "seq":
			err = json.Unmarshal(v, &e.Seq)
		case "time":
			err = json.Unmarshal(v, &e.Time)
		case "logical":
			err = json.Unmarshal(v, &e.Logical)
		case "parents":
			err = json.Unmarshal(v, &e.Parents)
		case "data":
			e.Data = v // already canonical
		}
		if err != nil {
			return Event{}, fmt.Errorf("%s: %v", k, err)
		}
	}
	if _, ok := members["producer"]; !ok {
		return Event{}, errors.New("producer is missing")
	}
	if _, ok := members["time"]; !ok {
		return Event{}, errors.New("time is missing")
	}
	if err := e.validate(false); err != nil {
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
