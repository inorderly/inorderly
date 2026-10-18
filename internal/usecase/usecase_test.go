// Copyright Inorderly.com 2026
// SPDX-License-Identifier: Apache-2.0

package usecase

import (
	"encoding/json"
	"testing"
)

func TestUseCases(t *testing.T) {
	if testing.Short() {
		t.Skip("the use cases take a few seconds")
	}
	for _, c := range All() {
		t.Run(c.Name, func(t *testing.T) {
			rep, err := c.Check()
			if err != nil {
				t.Fatal(err)
			}
			delete(rep, "commands")
			b, _ := json.Marshal(rep)
			t.Logf("%s", b)
		})
	}
}
