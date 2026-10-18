// Copyright Inorderly.com 2026
// SPDX-License-Identifier: Apache-2.0

package inorderly_test

import (
	"testing"

	"github.com/inorderly/inorderly"
	"github.com/inorderly/inorderly/internal/scenario"
)

// BenchmarkDemoOrder pushes the demo's main set, 100,000 events each sent
// one to five times, through one Orderer and flushes it.
func BenchmarkDemoOrder(b *testing.B) {
	set := scenario.Generate(scenario.Params{Seed: 1, Events: 100000, Producers: 16, Hold: 200})
	order := scenario.Arrive(set.Events, 200, 5, 1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		o, _ := inorderly.New(inorderly.Config{Window: 200, Hold: 200})
		for _, e := range order {
			o.Push(e)
		}
		o.Flush()
	}
	b.ReportMetric(float64(len(order)), "pushes/op")
}
