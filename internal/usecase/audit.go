// Copyright Inorderly.com 2026
// SPDX-License-Identifier: Apache-2.0

package usecase

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"

	"github.com/inorderly/inorderly"
)

// Audit is four hours of transactions on 300 accounts at four branches,
// whose clocks are up to 0.8 seconds apart: deposits, withdrawals that are
// declined when the balance is short, and reversals that undo an earlier
// transaction and name it as their parent. Two systems consume them, the
// ledger and billing, each through its own queue with its own delays. An
// auditor later replays the four branch logs, which were exported at least
// once, so about 2% of their lines are there twice.
func Audit() *Case {
	const (
		accounts = 300
		txns     = 100000
	)
	rng := newRand(4, 0xa0d1)
	skew := []int64{}
	for b := 0; b < 4; b++ {
		skew = append(skew, rng.Int64N(1601)-800)
	}
	type txn struct {
		ev     inorderly.Event
		at     int64
		branch int
	}
	var all []txn
	inverted := 0
	for k := 1; k <= txns; k++ {
		at := t0 + int64(k)*144 + rng.Int64N(144)
		b := rng.IntN(4)
		acct := fmt.Sprintf("acct-%03d", rng.IntN(accounts))
		kind, amount := "deposit", 1000+rng.IntN(49000)
		if rng.IntN(100) < 55 {
			kind, amount = "withdrawal", 1000+rng.IntN(39000)
		}
		all = append(all, txn{inorderly.Event{ID: fmt.Sprintf("txn_%06d", k), Producer: fmt.Sprintf("branch-%d", b+1),
			Time: at + skew[b], Data: fmt.Appendf(nil, `{"account":%q,"amount":%d,"kind":%q}`, acct, amount, kind)}, at, b})
		if rng.IntN(100) == 0 && len(all) > 1 {
			orig := all[len(all)-1-rng.IntN(min(len(all)-1, 20))]
			if len(orig.ev.Parents) > 0 {
				continue // a reversal isn't reversed
			}
			rb := rng.IntN(4)
			rat := orig.at + 300 + rng.Int64N(3000)
			rev := inorderly.Event{ID: "rev_" + orig.ev.ID[4:], Producer: fmt.Sprintf("branch-%d", rb+1), Time: rat + skew[rb],
				Parents: []string{orig.ev.ID},
				Data:    fmt.Appendf(nil, `{"account":%q,"kind":"reversal","of":%q}`, field[string](orig.ev, "account"), orig.ev.ID)}
			if rev.Time < orig.ev.Time {
				inverted++
			}
			all = append(all, txn{rev, rat, rb})
		}
	}
	// A transaction is reversed at most once.
	reversed := map[string]bool{}
	kept := all[:0]
	for _, t := range all {
		if len(t.ev.Parents) > 0 {
			if reversed[t.ev.Parents[0]] {
				continue
			}
			reversed[t.ev.Parents[0]] = true
		}
		kept = append(kept, t)
	}
	all = kept

	c := &Case{Name: "audit", Config: inorderly.Config{Window: 600000, Hold: 600000}}
	queue := func(name string, seed uint64, base, spread int64) Input {
		d := newRand(seed, 0x9e9e)
		var ds deliveries
		for _, t := range all {
			ds.add(t.at+base+d.Int64N(spread), t.ev)
		}
		return Input{Name: name, Files: []File{{Name: name + ".ndjson", Events: ds.arrived()}}}
	}
	c.Inputs = append(c.Inputs, queue("ledger", 401, 20, 380), queue("billing", 402, 50, 2950))
	exportRng := newRand(403, 0xe4e4)
	repeats := 0
	var logs []File
	for b := 0; b < 4; b++ {
		var mine []inorderly.Event
		for _, t := range all {
			if t.branch == b {
				mine = append(mine, t.ev)
			}
		}
		sortByTime(mine)
		var out []inorderly.Event
		for i, e := range mine {
			out = append(out, e)
			if exportRng.IntN(1000) < 2 {
				// The exporter resends a few lines when it restarts.
				back := min(i, exportRng.IntN(20))
				out = append(out, mine[i-back:i+1]...)
				repeats += back + 1
			}
		}
		logs = append(logs, File{Name: fmt.Sprintf("branch-%d.ndjson", b+1), Events: out})
	}
	c.Inputs = append(c.Inputs, Input{Name: "audit", Files: logs})

	c.check = func(c *Case, runs []Run) (map[string]any, error) {
		// Each system applies what it's given in the order it gets it.
		ledger := applyTxns(c.Inputs[0].Arrivals())
		billing := applyTxns(c.Inputs[1].Arrivals())
		differ := 0
		for a, bal := range ledger.balance {
			if billing.balance[a] != bal {
				differ++
			}
		}
		var states []string
		for _, r := range runs {
			if err := parentsFirst(r); err != nil {
				return nil, err
			}
			if r.Stats.Out != uint64(len(all)) || r.Stats.Late+r.Stats.Conflicts != 0 {
				return nil, fmt.Errorf("%s: %d out of %d, %d late", r.Input, r.Stats.Out, len(all), r.Stats.Late)
			}
			var list []inorderly.Event
			for _, rel := range r.Released {
				list = append(list, rel.Event)
			}
			states = append(states, applyTxns(list).digest())
		}
		var distinct []inorderly.Event
		for _, t := range all {
			distinct = append(distinct, t.ev)
		}
		sortByTime(distinct)
		ordered := applyTxns(runs[0].releasedEvents())
		return map[string]any{
			"accounts":                          accounts,
			"transactions":                      len(all),
			"reversals":                         len(reversed),
			"declined":                          ordered.declined,
			"export_repeats":                    repeats,
			"audit_duplicates_dropped":          runs[2].Stats.Duplicates,
			"naive_balances_differ":             differ,
			"naive_declined":                    map[string]int{"ledger": ledger.declined, "billing": billing.declined},
			"naive_reversals_before_original":   map[string]int{"ledger": ledger.early, "billing": billing.early},
			"reversals_stamped_before_original": childBeforeParent(distinct),
			"state":                             states[0],
			"states":                            states,
		}, checkSame(states, repeats, runs[2].Stats.Duplicates)
	}
	return c
}

func checkSame(states []string, repeats int, dropped uint64) error {
	for _, s := range states[1:] {
		if s != states[0] {
			return fmt.Errorf("the systems ended in different states")
		}
	}
	if uint64(repeats) != dropped {
		return fmt.Errorf("%d repeated lines in the logs, %d dropped", repeats, dropped)
	}
	return nil
}

func (r Run) releasedEvents() []inorderly.Event {
	list := make([]inorderly.Event, len(r.Released))
	for i, rel := range r.Released {
		list[i] = rel.Event
	}
	return list
}

type books struct {
	balance  map[string]int
	seen     map[string]bool
	applied  map[string]int // the change each applied transaction made
	declined int
	early    int // reversals that came before what they reverse, and so did nothing
}

// applyTxns is the account logic both systems share. A withdrawal the
// balance can't cover is declined. A reversal undoes what its transaction
// did; if that transaction hasn't been seen yet, there's nothing to undo.
func applyTxns(list []inorderly.Event) books {
	bk := books{balance: map[string]int{}, seen: map[string]bool{}, applied: map[string]int{}}
	for _, e := range list {
		acct := field[string](e, "account")
		switch field[string](e, "kind") {
		case "deposit":
			bk.balance[acct] += field[int](e, "amount")
			bk.applied[e.ID] = field[int](e, "amount")
		case "withdrawal":
			if amt := field[int](e, "amount"); bk.balance[acct] >= amt {
				bk.balance[acct] -= amt
				bk.applied[e.ID] = -amt
			} else {
				bk.declined++
			}
		case "reversal":
			of := field[string](e, "of")
			if !bk.seen[of] {
				bk.early++
			} else if change, ok := bk.applied[of]; ok {
				bk.balance[acct] -= change
				delete(bk.applied, of)
			}
		}
		bk.seen[e.ID] = true
	}
	return bk
}

// digest is a SHA-256 of every balance, in account order.
func (bk books) digest() string {
	var names []string
	for a := range bk.balance {
		names = append(names, a)
	}
	sort.Strings(names)
	h := sha256.New()
	for _, a := range names {
		fmt.Fprintf(h, "%s %d\n", a, bk.balance[a])
	}
	return "sha256:" + hex.EncodeToString(h.Sum(nil))
}
