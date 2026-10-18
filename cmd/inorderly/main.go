// Copyright Inorderly.com 2026
// SPDX-License-Identifier: Apache-2.0

// Command inorderly reads events as NDJSON and writes them in a
// deterministic order: the same events give the same output, byte for byte,
// however they arrived.
//
//	inorderly [flags] < events.ndjson > ordered.ndjson
//	inorderly -in a.ndjson -in b.ndjson -out ordered.ndjson -state state/
//	inorderly version
//	inorderly demo
//
// Run inorderly -h for the flags. README.md is the manual.
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"

	"github.com/inorderly/inorderly"
)

func main() {
	args := os.Args[1:]
	var err error
	code := 0
	switch {
	case len(args) > 0 && args[0] == "version":
		err = printVersion()
	case len(args) > 0 && args[0] == "demo":
		code, err = demo(args[1:])
	case len(args) > 0 && args[0] == "help":
		usage(os.Stdout)
	default:
		code, err = run(args)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "inorderly:", err)
		if code == 0 {
			code = 1
		}
	}
	os.Exit(code)
}

func usage(w io.Writer) {
	fmt.Fprint(w, `inorderly orders events: the same events give the same output, byte for byte,
however late, twice or out of order they arrived.

  inorderly [flags]              read NDJSON events, write them in order
  inorderly version              print the version as JSON
  inorderly demo [flags]         run the self-checking demo and print its report

Each input line is one event:
  {"id":"e1","producer":"p1","seq":1,"time":1700000000000,"logical":0,"parents":["e0"],"data":{}}
producer and time are required, and id or seq. Each output line is the event
with its position ("pos") and any parents that didn't come before it
("missing"). The replay hash printed at the end is the SHA-256 of the output.

Flags:
`)
}

type multi []string

func (m *multi) String() string     { return strings.Join(*m, ",") }
func (m *multi) Set(s string) error { *m = append(*m, s); return nil }

type summary struct {
	Version        string           `json:"version"`
	Hash           string           `json:"hash"`
	In             uint64           `json:"in"`
	Ordered        uint64           `json:"ordered"`
	Duplicates     uint64           `json:"duplicates"`
	Late           uint64           `json:"late"`
	Conflicts      uint64           `json:"conflicts"`
	Invalid        uint64           `json:"invalid"`
	MissingParents uint64           `json:"missing_parents"`
	SeqGaps        uint64           `json:"seq_gaps"`
	SeqBackwards   uint64           `json:"seq_backwards"`
	Gaps           []inorderly.Gap  `json:"gaps"`
	Resumed        bool             `json:"resumed"`
	Config         inorderly.Config `json:"config"`
}

func run(args []string) (int, error) {
	fs := flag.NewFlagSet("inorderly", flag.ContinueOnError)
	fs.Usage = func() { usage(os.Stderr); fs.PrintDefaults() }
	var ins multi
	fs.Var(&ins, "in", "an input file of NDJSON events; repeat it to read several files a line at a time in turn (default: standard input)")
	out := fs.String("out", "-", "the output file; - is standard output")
	rejects := fs.String("rejects", "", "a file for late, conflicting and invalid events, one JSON line each with the reason")
	window := fs.Int64("window", 0, "how far, in event-time units, an event may arrive behind the newest time seen")
	hold := fs.Int64("hold", -1, "how long past its own time an event waits for a parent; -1 is the same as -window")
	remember := fs.Int64("remember", 0, "how long behind the watermark released events are remembered for duplicates and parents (0: always)")
	stateDir := fs.String("state", "", "a directory for checkpoints; a run killed part way carries on from the last one when started again with the same flags")
	every := fs.Int("checkpoint-every", 10000, "input lines between checkpoints")
	statsPath := fs.String("stats", "", "write the summary here instead of standard error")
	strict := fs.Bool("strict", false, "exit with status 3 if any event was late, conflicting or invalid")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0, nil
		}
		return 2, err
	}
	if fs.NArg() > 0 {
		return 2, fmt.Errorf("unexpected argument %q; inputs go after -in", fs.Arg(0))
	}
	if *hold < 0 {
		*hold = *window
	}
	if *every < 1 {
		return 2, errors.New("-checkpoint-every must be at least 1")
	}
	if len(ins) == 0 {
		ins = multi{"-"}
	}
	cfg := inorderly.Config{Window: *window, Hold: *hold, Remember: *remember}
	r, err := newRunner(cfg, ins, *out, *rejects, *stateDir)
	if err != nil {
		return 1, err
	}
	sum, err := r.process(*every)
	r.close()
	if err != nil {
		return 1, err
	}
	b, _ := json.MarshalIndent(sum, "", "  ")
	b = append(b, '\n')
	if *statsPath != "" {
		if err := os.WriteFile(*statsPath, b, 0o644); err != nil {
			return 1, err
		}
	} else {
		os.Stderr.Write(b)
	}
	if *strict && sum.Late+sum.Conflicts+sum.Invalid > 0 {
		return 3, nil
	}
	return 0, nil
}

func printVersion() error {
	info := map[string]interface{}{
		"version": inorderly.Version, "go": runtime.Version(), "os": runtime.GOOS, "arch": runtime.GOARCH, "revision": "",
	}
	if bi, ok := debug.ReadBuildInfo(); ok {
		for _, s := range bi.Settings {
			if s.Key == "vcs.revision" {
				info["revision"] = s.Value
			}
		}
	}
	b, err := json.Marshal(info)
	if err != nil {
		return err
	}
	fmt.Println(string(b))
	return nil
}

func absPath(p string) string {
	if p == "-" || p == "" {
		return p
	}
	if a, err := filepath.Abs(p); err == nil {
		return a
	}
	return p
}

// lineReader reads lines and counts the bytes it has used, so a checkpoint
// knows where to carry on. A line longer than inorderly.MaxLine comes back
// cut short with tooLong set.
type lineReader struct {
	path    string
	file    *os.File
	r       *bufio.Reader
	off     int64
	line    uint64
	done    bool
	tooLong bool
}

func (l *lineReader) next() ([]byte, error) {
	var buf []byte
	l.tooLong = false
	for {
		chunk, err := l.r.ReadSlice('\n')
		l.off += int64(len(chunk))
		if len(buf)+len(chunk) <= inorderly.MaxLine+2 {
			buf = append(buf, chunk...)
		} else {
			l.tooLong = true
		}
		if err == bufio.ErrBufferFull {
			continue
		}
		if err == io.EOF {
			if len(buf) == 0 && !l.tooLong {
				return nil, io.EOF
			}
			l.line++
			return trimEOL(buf), nil
		}
		if err != nil {
			return nil, err
		}
		l.line++
		return trimEOL(buf), nil
	}
}

func trimEOL(b []byte) []byte {
	b = bytes.TrimSuffix(b, []byte{'\n'})
	return bytes.TrimSuffix(b, []byte{'\r'})
}
