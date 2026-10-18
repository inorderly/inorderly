// Copyright Inorderly.com 2026
// SPDX-License-Identifier: Apache-2.0

package usecase

import (
	"fmt"
	"strings"

	"github.com/inorderly/inorderly"
)

// Webhooks is a shop's order events, delivered as webhooks. Checkout sends
// each order, payments sends its payment and sometimes a refund, and the
// warehouse sends the shipment. The payments service's clock runs up to 1.5
// seconds behind, so a payment can be stamped before the order it pays for.
// The webhook provider retries about one delivery in seven, sometimes twice,
// up to a minute and a half later. The same events are delivered three
// times, with different network delays and retries each time.
func Webhooks() *Case {
	const orders = 20000
	rng := newRand(1, 0x0eb1)
	type sent struct {
		ev inorderly.Event
		at int64 // when it was really sent
	}
	var events []sent
	var paid, refunded int64
	inverted := 0
	for i := 1; i <= orders; i++ {
		t := t0 + int64(i)*360 + rng.Int64N(200)
		total := 500 + rng.Int64N(20000)
		oid := fmt.Sprintf("ord_%05d", i)
		events = append(events, sent{inorderly.Event{ID: oid, Producer: "checkout", Time: t,
			Data: fmt.Appendf(nil, `{"order":%q,"total_cents":%d}`, oid, total)}, t})

		skew := rng.Int64N(1500) // how far the payments clock is behind
		paidAt := t + 300 + rng.Int64N(2700)
		pid := fmt.Sprintf("pay_%05d", i)
		events = append(events, sent{inorderly.Event{ID: pid, Producer: "payments", Time: paidAt - skew,
			Parents: []string{oid}, Data: fmt.Appendf(nil, `{"order":%q,"amount_cents":%d}`, oid, total)}, paidAt})
		paid += total
		if paidAt-skew < t {
			inverted++
		}
		if rng.IntN(100) < 8 {
			amount := total
			if rng.IntN(2) == 0 {
				amount = total / 2
			}
			at := paidAt + 60000 + rng.Int64N(1740000)
			events = append(events, sent{inorderly.Event{ID: fmt.Sprintf("ref_%05d", i), Producer: "payments",
				Time: at - skew, Parents: []string{pid},
				Data: fmt.Appendf(nil, `{"order":%q,"amount_cents":%d}`, oid, amount)}, at})
			refunded += amount
		} else {
			at := paidAt + 120000 + rng.Int64N(1080000)
			events = append(events, sent{inorderly.Event{ID: fmt.Sprintf("shp_%05d", i), Producer: "warehouse",
				Time: at, Parents: []string{pid}, Data: fmt.Appendf(nil, `{"order":%q}`, oid)}, at})
		}
	}

	c := &Case{Name: "webhooks", Config: inorderly.Config{Window: 60000, Hold: 60000}}
	retries := map[string]int{}
	for v := 1; v <= 3; v++ {
		d := newRand(uint64(100+v), 0xde1)
		var ds deliveries
		name := fmt.Sprintf("deliveries-%d", v)
		for _, s := range events {
			at := s.at + 50 + d.Int64N(750)
			ds.add(at, s.ev)
			if d.IntN(100) < 15 {
				at += 5000 + d.Int64N(35000)
				ds.add(at, s.ev)
				retries[name]++
				if d.IntN(5) == 0 {
					ds.add(at+40000+d.Int64N(15000), s.ev)
					retries[name]++
				}
			}
		}
		c.Inputs = append(c.Inputs, Input{Name: name, Files: []File{{Name: name + ".ndjson", Events: ds.arrived()}}})
	}

	c.check = func(c *Case, runs []Run) (map[string]any, error) {
		// A typical hand-written consumer: take each webhook as it comes.
		first := c.Inputs[0]
		var naiveRevenue int64
		for _, e := range first.Arrivals() {
			naiveRevenue += revenueOf(e)
		}
		early := childBeforeParent(first.Arrivals())
		// The usual fix: drop repeats by id, then sort by timestamp.
		var distinct []inorderly.Event
		for _, s := range events {
			distinct = append(distinct, s.ev)
		}
		sortByTime(distinct)
		byTime := childBeforeParent(distinct)
		if byTime != inverted {
			return nil, fmt.Errorf("sorting by time put %d children first, %d payments are stamped early", byTime, inverted)
		}

		for i, r := range runs {
			if err := parentsFirst(r); err != nil {
				return nil, err
			}
			if r.Stats.Out != uint64(len(events)) || r.Stats.Late+r.Stats.Conflicts != 0 {
				return nil, fmt.Errorf("%s: %d out of %d, %d late, %d conflicts", r.Input, r.Stats.Out, len(events), r.Stats.Late, r.Stats.Conflicts)
			}
			if r.Stats.Duplicates != uint64(retries[c.Inputs[i].Name]) {
				return nil, fmt.Errorf("%s: %d duplicates dropped, %d retries sent", r.Input, r.Stats.Duplicates, retries[c.Inputs[i].Name])
			}
			var revenue int64
			for _, rel := range r.Released {
				revenue += revenueOf(rel.Event)
			}
			if revenue != paid-refunded {
				return nil, fmt.Errorf("%s: revenue %d, want %d", r.Input, revenue, paid-refunded)
			}
		}
		return map[string]any{
			"orders":                        orders,
			"events":                        len(events),
			"deliveries":                    len(first.Files[0].Events),
			"retries":                       retries,
			"revenue_cents":                 paid - refunded,
			"naive_revenue_cents":           naiveRevenue,
			"naive_overcount_cents":         naiveRevenue - (paid - refunded),
			"naive_child_before_parent":     early,
			"payments_stamped_before_order": byTime,
			"duplicates_dropped":            runs[0].Stats.Duplicates,
			"ordered":                       runs[0].Stats.Out,
		}, nil
	}
	return c
}

// revenueOf is what an event adds to the takings: a payment's amount, less
// a refund's.
func revenueOf(e inorderly.Event) int64 {
	switch {
	case strings.HasPrefix(e.ID, "pay_"):
		return field[int64](e, "amount_cents")
	case strings.HasPrefix(e.ID, "ref_"):
		return -field[int64](e, "amount_cents")
	}
	return 0
}
