// Copyright Inorderly.com 2026
// SPDX-License-Identifier: Apache-2.0

// Command usecases builds the four worked examples on inorderly.com, checks
// them and prints what they found as JSON:
//
//	go run ./examples/usecases                 # check all four
//	go run ./examples/usecases -dir uc audit   # also write audit's files to uc/audit
//
// With -dir it writes each example's input files, so you can order them
// with the inorderly command; the report lists the exact commands. Every
// command for one example prints the same replay hash.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"runtime"

	"github.com/inorderly/inorderly"
	"github.com/inorderly/inorderly/internal/usecase"
)

func main() {
	dir := flag.String("dir", "", "write each example's input files to a folder of its name in this directory")
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: usecases [-dir DIR] [webhooks|feeds|telemetry|audit ...]")
		flag.PrintDefaults()
	}
	flag.Parse()
	want, left := map[string]bool{}, map[string]bool{}
	for _, a := range flag.Args() {
		want[a], left[a] = true, true
	}
	report := map[string]any{"version": inorderly.Version, "go": runtime.Version(), "os": runtime.GOOS, "arch": runtime.GOARCH}
	failed := false
	for _, c := range usecase.All() {
		if len(want) > 0 && !want[c.Name] {
			continue
		}
		delete(left, c.Name)
		fmt.Fprintf(os.Stderr, "%s...\n", c.Name)
		if *dir != "" {
			if err := c.Write(*dir); err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
		}
		rep, err := c.Check()
		if err != nil {
			fmt.Fprintln(os.Stderr, "FAIL", err)
			failed = true
			continue
		}
		report[c.Name] = rep
	}
	for name := range left {
		fmt.Fprintf(os.Stderr, "no example called %q\n", name)
		os.Exit(2)
	}
	b, _ := json.MarshalIndent(report, "", "  ")
	fmt.Println(string(b))
	if failed {
		os.Exit(1)
	}
}
