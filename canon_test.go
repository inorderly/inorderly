// Copyright Inorderly.com 2026
// SPDX-License-Identifier: Apache-2.0

package inorderly

import (
	"encoding/json"
	"testing"
)

func TestCanonicalJSON(t *testing.T) {
	for in, want := range map[string]string{
		` { "b" : 1, "a" : [ true, false, null, 1.50, -2e3 ], "c": {"y":"é","x":"<&>"} } `: `{"a":[true,false,null,1.50,-2e3],"b":1,"c":{"x":"<&>","y":"é"}}`,
		`"tab\there\u0001"`:   `"tab\there\u0001"`,
		`[]`:                  `[]`,
		`{}`:                  `{}`,
		`"\ud800"`:            `"` + "�" + `"`,
		`{"é":1,"e":2,"z":3}`: `{"e":2,"z":3,"é":1}`,
	} {
		got, err := canonicalJSON([]byte(in))
		if err != nil || string(got) != want {
			t.Fatalf("%s: got %s %v, want %s", in, got, err, want)
		}
		again, _ := canonicalJSON(got)
		if string(again) != string(got) {
			t.Fatalf("%s isn't stable: %s", got, again)
		}
	}
	for _, bad := range []string{`{"a":1,"a":2}`, `{`, `[1,]`, `1 2`, `nul`} {
		if _, err := canonicalJSON([]byte(bad)); err == nil {
			t.Fatalf("%s was accepted", bad)
		}
	}
}

func TestSameEventFromTwoEncoders(t *testing.T) {
	a, err := ParseLine([]byte(`{"id":"x","producer":"p","time":1,"data":{"battery":7,"celsius":21.5,"note":"café"}}`))
	if err != nil {
		t.Fatal(err)
	}
	b, err := ParseLine([]byte(`{"data": {"note": "café", "celsius": 21.5, "battery": 7}, "time": 1, "producer": "p", "id": "x"}`))
	if err != nil {
		t.Fatal(err)
	}
	if string(a.Canonical()) != string(b.Canonical()) {
		t.Fatalf("%s != %s", a.Canonical(), b.Canonical())
	}
	o := mustNew(t, Config{})
	o.Push(a)
	if s, _, _ := o.Push(b); s != Duplicate {
		t.Fatalf("the same event from another encoder was %v", s)
	}
	n, _ := ParseLine([]byte(`{"id":"y","producer":"p","time":1,"data":null}`))
	if n.Data != nil {
		t.Fatal("null data was kept")
	}
	var back map[string]interface{}
	if err := json.Unmarshal(a.Canonical(), &back); err != nil {
		t.Fatal(err)
	}
}
