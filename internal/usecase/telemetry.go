// Copyright Inorderly.com 2026
// SPDX-License-Identifier: Apache-2.0

package usecase

import (
	"fmt"
	"sort"

	"github.com/inorderly/inorderly"
)

// Telemetry is three hours of temperature readings, one every 10 seconds,
// from 200 devices whose clocks are up to 3 seconds out. Most devices upload
// each reading at once. Fifty sit behind five gateways that send what they
// hold every 45 to 90 seconds. Twenty lose their connection for 2 to 12
// minutes and ten for 25 to 40, and upload what they kept when they come
// back. About 3 readings in 1,000 never arrive. The readings are uploaded
// three times, with different network delays and gateway timing each time.
//
// The window is 15 minutes, so the short outages' readings are ordered and
// the long outages' oldest readings are refused as late.
func Telemetry() *Case {
	const (
		devices  = 200
		readings = 1080
		every    = 10000
		window   = 15 * 60000
		bucket   = 60000
	)
	rng := newRand(3, 0x7e1e)
	type reading struct {
		ev   inorderly.Event
		at   int64 // when it was really taken
		dev  int
		lost bool
	}
	type outage struct{ from, to int64 }
	var all []reading
	outages := make([]outage, devices)
	lost := 0
	for d := 0; d < devices; d++ {
		name := fmt.Sprintf("dev-%03d", d)
		off := rng.Int64N(6001) - 3000
		phase := rng.Int64N(every)
		base := 180 + rng.IntN(80)
		switch {
		case d < 20:
			from := t0 + 1200000 + rng.Int64N(8400000)
			outages[d] = outage{from, from + 120000 + rng.Int64N(600000)}
		case d < 30:
			from := t0 + 1200000 + rng.Int64N(6600000)
			to := from + 1500000 + rng.Int64N(900000)
			// Come back half way between two readings' turns to be refused,
			// so the readings at the edge of the window are the same ones
			// however the network jitters. The newest time seen when the
			// device returns is about 2.5 seconds ahead of the real time.
			edge := to + 2500 - window - (t0 + phase + off)
			to += 5000 - ((edge%every)+every)%every
			outages[d] = outage{from, to}
		}
		for k := 0; k < readings; k++ {
			at := t0 + phase + int64(k)*every
			temp := base + (k/60)%7 - 3 + rng.IntN(3) - 1
			r := reading{dev: d, at: at, ev: inorderly.Event{Producer: name, Seq: uint64(k + 1), Time: at + off,
				Data: fmt.Appendf(nil, `{"temp_dc":%d}`, temp)}}
			if rng.IntN(1000) < 3 {
				r.lost = true
				lost++
			}
			all = append(all, r)
		}
	}

	c := &Case{Name: "telemetry", Config: inorderly.Config{Window: window, Hold: window}}
	for v := 1; v <= 3; v++ {
		n := newRand(uint64(300+v), 0x9a7)
		flushes := make([][]int64, 5)
		for g := range flushes {
			for t := t0 + n.Int64N(90000); t < t0+readings*every+200000; t += 45000 + n.Int64N(45000) {
				flushes[g] = append(flushes[g], t)
			}
		}
		var ds deliveries
		for _, r := range all {
			if r.lost {
				continue
			}
			at := r.at
			o := outages[r.dev]
			switch {
			case o.to > 0 && at >= o.from && at < o.to:
				at = o.to + 200 + n.Int64N(800) + (at-o.from)/every*20
			case r.dev >= 30 && r.dev < 80:
				f := flushes[r.dev%5]
				at = f[sort.Search(len(f), func(i int) bool { return f[i] > at })] + 100 + n.Int64N(400)
			default:
				at += 100 + n.Int64N(800)
			}
			ds.add(at, r.ev)
		}
		name := fmt.Sprintf("uploads-%d", v)
		c.Inputs = append(c.Inputs, Input{Name: name, Files: []File{{Name: name + ".ndjson", Events: ds.arrived()}}})
	}

	c.check = func(c *Case, runs []Run) (map[string]any, error) {
		// A typical hand-written aggregator: average each minute, by the
		// devices' own time, and close the minute once readings a minute
		// past its end have come in. Readings for a closed minute are lost.
		type avg struct{ sum, n int }
		naive := func(in Input) (map[int64]avg, int) {
			buckets := map[int64]avg{}
			var newest int64
			dropped := 0
			for _, e := range in.Arrivals() {
				b := e.Time / bucket
				if (b+1)*bucket+bucket <= newest {
					dropped++
				} else {
					a := buckets[b]
					buckets[b] = avg{a.sum + field[int](e, "temp_dc"), a.n + 1}
				}
				newest = max(newest, e.Time)
			}
			return buckets, dropped
		}
		var naiveDropped []int
		var naiveMinutes []map[int64]avg
		for _, in := range c.Inputs {
			b, d := naive(in)
			naiveMinutes = append(naiveMinutes, b)
			naiveDropped = append(naiveDropped, d)
		}
		differ := 0
		for b, a := range naiveMinutes[0] {
			if naiveMinutes[1][b] != a {
				differ++
			}
		}
		minutes := map[int64]avg{}
		for _, rel := range runs[0].Released {
			b := rel.Event.Time / bucket
			a := minutes[b]
			minutes[b] = avg{a.sum + field[int](rel.Event, "temp_dc"), a.n + 1}
		}
		wrong := 0
		for b, a := range naiveMinutes[0] {
			if minutes[b] != a {
				wrong++
			}
		}

		late := len(runs[0].Late)
		var gaps []inorderly.Gap
		for _, r := range runs {
			if len(r.Late) != late || r.Stats.Late != uint64(late) {
				return nil, fmt.Errorf("%s: %d late, %d in the first run", r.Input, len(r.Late), late)
			}
			if r.Stats.Out+uint64(late+lost) != devices*readings {
				return nil, fmt.Errorf("%s: %d ordered, %d late and %d lost don't add up to %d", r.Input, r.Stats.Out, late, lost, devices*readings)
			}
			for _, e := range r.Late {
				var d int
				fmt.Sscanf(e.Producer, "dev-%d", &d)
				if d < 20 || d >= 30 {
					return nil, fmt.Errorf("%s: a reading from %s, which had no long outage, was refused", r.Input, e.Producer)
				}
			}
			g, err := expectGaps(r)
			if err != nil {
				return nil, err
			}
			gaps = g
		}
		return map[string]any{
			"devices":                           devices,
			"readings":                          devices * readings,
			"lost_in_transit":                   lost,
			"ordered":                           runs[0].Stats.Out,
			"refused_late":                      late,
			"gaps":                              len(gaps),
			"missing_in_gaps":                   missingCount(gaps),
			"minutes":                           len(minutes),
			"naive_dropped":                     naiveDropped,
			"naive_minutes_differ_between_runs": differ,
			"naive_minutes_wrong":               wrong,
		}, nil
	}
	return c
}
