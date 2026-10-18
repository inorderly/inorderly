// Copyright Inorderly.com 2026
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/inorderly/inorderly"
)

// checkpoint is what -state keeps: where each input had got to, how much
// output was safely written, and the orderer's state at that moment.
type checkpoint struct {
	Format      int                 `json:"format"`
	Version     string              `json:"version"`
	Config      inorderly.Config    `json:"config"`
	Inputs      []string            `json:"inputs"`
	Sizes       []int64             `json:"input_sizes"`
	ModTimes    []int64             `json:"input_mod_times"`
	Offsets     []int64             `json:"offsets"`
	Lines       []uint64            `json:"lines"`
	Done        []bool              `json:"done"`
	Next        int                 `json:"next"`
	Out         string              `json:"out"`
	OutSize     int64               `json:"out_size"`
	Rejects     string              `json:"rejects"`
	RejectsSize int64               `json:"rejects_size"`
	Invalid     uint64              `json:"invalid"`
	Log         string              `json:"released_log"`
	LogSize     int64               `json:"released_log_size"`
	Finished    bool                `json:"finished"`
	Summary     *summary            `json:"summary,omitempty"`
	Engine      *inorderly.Snapshot `json:"engine,omitempty"`
}

const checkpointFormat = 1

type countingWriter struct {
	w *bufio.Writer
	n int64
}

func (c *countingWriter) write(b []byte) error {
	c.n += int64(len(b)) + 1
	if _, err := c.w.Write(b); err != nil {
		return err
	}
	return c.w.WriteByte('\n')
}

type runner struct {
	eng      *inorderly.Orderer
	inputs   []*lineReader
	next     int
	outFile  *os.File
	out      *countingWriter
	rejFile  *os.File
	rej      *countingWriter
	logFile  *os.File // with -state: the record of every released event
	log      *countingWriter
	logName  string
	logCount int
	oldLogs  []string // logs to remove once a checkpoint names a newer one
	stateDir string
	cp       checkpoint
	invalid  uint64
	resumed  bool
	finished *summary
}

func newRunner(cfg inorderly.Config, ins []string, out, rejects, stateDir string) (*runner, error) {
	r := &runner{stateDir: stateDir}
	abs := make([]string, len(ins))
	stdin := 0
	for i, p := range ins {
		abs[i] = absPath(p)
		if p == "-" {
			stdin++
		}
	}
	if stdin > 1 {
		return nil, errors.New("standard input can be read only once")
	}
	if stateDir != "" && (stdin > 0 || out == "-") {
		return nil, errors.New("-state needs -in files and an -out file: standard input and output can't be read again or cut back")
	}
	// No file may be both read and written, or written twice.
	if out != "-" && rejects != "" && absPath(out) == absPath(rejects) {
		return nil, errors.New("-out and -rejects are the same file")
	}
	for _, a := range abs {
		if (out != "-" && a == absPath(out)) || (rejects != "" && a == absPath(rejects)) {
			return nil, fmt.Errorf("%s is both an input and an output", a)
		}
	}
	r.cp = checkpoint{Format: checkpointFormat, Version: inorderly.Version, Config: cfg, Inputs: abs,
		Out: absPath(out), Rejects: absPath(rejects)}

	var old *checkpoint
	if stateDir != "" {
		if err := os.MkdirAll(stateDir, 0o755); err != nil {
			return nil, err
		}
		b, err := os.ReadFile(r.statePath())
		switch {
		case err == nil:
			old = &checkpoint{}
			if err := json.Unmarshal(b, old); err != nil {
				return nil, fmt.Errorf("the checkpoint in %s is damaged: %v", stateDir, err)
			}
			if err := r.matches(old); err != nil {
				return nil, err
			}
		case !errors.Is(err, os.ErrNotExist):
			return nil, err
		}
	}
	if old != nil && old.Finished {
		if st, err := os.Stat(out); err != nil || st.Size() != old.OutSize {
			return nil, fmt.Errorf("this run finished before, but %s has changed since; use another -state to run it again", out)
		}
		r.finished = old.Summary
		return r, nil
	}

	for i, p := range ins {
		lr := &lineReader{path: p}
		if p == "-" {
			lr.r = bufio.NewReaderSize(os.Stdin, 1<<16)
		} else {
			f, err := os.Open(p)
			if err != nil {
				r.close()
				return nil, err
			}
			lr.file = f
			st, err := f.Stat()
			if err != nil {
				r.close()
				return nil, err
			}
			r.cp.Sizes = append(r.cp.Sizes, st.Size())
			r.cp.ModTimes = append(r.cp.ModTimes, st.ModTime().UnixNano())
			if old != nil {
				if len(old.Sizes) != len(ins) || old.Sizes[i] != st.Size() || old.ModTimes[i] != st.ModTime().UnixNano() {
					r.close()
					return nil, fmt.Errorf("%s has changed since the checkpoint in %s; an interrupted run carries on only over the same input files", p, stateDir)
				}
				if _, err := f.Seek(old.Offsets[i], io.SeekStart); err != nil {
					r.close()
					return nil, err
				}
				lr.off, lr.line, lr.done = old.Offsets[i], old.Lines[i], old.Done[i]
			}
			lr.r = bufio.NewReaderSize(f, 1<<16)
		}
		r.inputs = append(r.inputs, lr)
	}

	var err error
	if old != nil {
		r.next = old.Next
		r.invalid = old.Invalid
		r.resumed = true
		r.outFile, r.out, err = reopen(out, old.OutSize)
		if err == nil && rejects != "" {
			r.rejFile, r.rej, err = reopen(rejects, old.RejectsSize)
		}
		r.logName = old.Log
		if err == nil {
			r.logFile, r.log, err = reopen(r.logPath(), old.LogSize)
		}
		var records []inorderly.Record
		if err == nil {
			records, err = readRecords(r.logPath(), old.LogSize)
			r.logCount = len(records)
		}
		if err == nil {
			r.oldLogs = r.otherLogs()
		}
		if err == nil {
			r.eng, err = inorderly.Restore(old.Engine, records)
		}
	} else {
		if out == "-" {
			r.out = &countingWriter{w: bufio.NewWriterSize(os.Stdout, 1<<16)}
		} else {
			r.outFile, r.out, err = reopen(out, 0)
		}
		if err == nil && rejects != "" {
			r.rejFile, r.rej, err = reopen(rejects, 0)
		}
		if err == nil && stateDir != "" {
			r.logName = "released-1.ndjson"
			r.oldLogs = r.otherLogs()
			r.logFile, r.log, err = reopen(r.logPath(), 0)
		}
		if err == nil {
			r.eng, err = inorderly.New(cfg)
		}
	}
	if err != nil {
		r.close()
		return nil, err
	}
	return r, nil
}

func (r *runner) statePath() string { return filepath.Join(r.stateDir, "checkpoint.json") }

func (r *runner) logPath() string { return filepath.Join(r.stateDir, r.logName) }

// otherLogs lists record logs in the state directory that the current
// checkpoint doesn't use: left over from compaction or an earlier run.
func (r *runner) otherLogs() []string {
	names, _ := filepath.Glob(filepath.Join(r.stateDir, "released-*.ndjson"))
	var out []string
	for _, n := range names {
		if filepath.Base(n) != r.logName {
			out = append(out, n)
		}
	}
	return out
}

// compact replaces the record log with the records the orderer still
// remembers, in a new file with a new name. The old file goes once a
// checkpoint names the new one. With -remember, that keeps the log, and the
// time a restart takes, in proportion to what is remembered.
func (r *runner) compact() error {
	if r.eng.Config().Remember == 0 {
		return nil
	}
	keep := r.eng.Remembered()
	if r.logCount < 2*len(keep)+compactSlack {
		return nil
	}
	var num int
	fmt.Sscanf(r.logName, "released-%d.ndjson", &num)
	name := fmt.Sprintf("released-%d.ndjson", num+1)
	f, w, err := reopen(filepath.Join(r.stateDir, name), 0)
	if err != nil {
		return err
	}
	for _, rec := range keep {
		b, _ := json.Marshal(rec)
		if err := w.write(b); err != nil {
			f.Close()
			return err
		}
	}
	if err := w.w.Flush(); err == nil {
		err = f.Sync()
	}
	if err != nil {
		f.Close()
		return err
	}
	r.logFile.Close()
	r.oldLogs = append(r.oldLogs, r.logPath())
	r.logFile, r.log, r.logName, r.logCount = f, w, name, len(keep)
	return nil
}

// compactSlack is how many records beyond twice what is remembered the log
// may hold before it is compacted.
var compactSlack = 10000

// readRecords reads the first size bytes of the released log.
func readRecords(path string, size int64) ([]inorderly.Record, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var records []inorderly.Record
	sc := bufio.NewScanner(io.LimitReader(f, size))
	sc.Buffer(make([]byte, 1<<16), inorderly.MaxLine+1024)
	for sc.Scan() {
		var rec inorderly.Record
		if err := json.Unmarshal(sc.Bytes(), &rec); err != nil {
			return nil, fmt.Errorf("%s is damaged: %v", path, err)
		}
		records = append(records, rec)
	}
	return records, sc.Err()
}

// matches checks that a checkpoint belongs to this run's flags.
func (r *runner) matches(old *checkpoint) error {
	if old.Format != checkpointFormat {
		return fmt.Errorf("the checkpoint in %s has format %d; this inorderly reads format %d", r.stateDir, old.Format, checkpointFormat)
	}
	same := old.Config == r.cp.Config && old.Out == r.cp.Out && old.Rejects == r.cp.Rejects && len(old.Inputs) == len(r.cp.Inputs)
	for i := 0; same && i < len(old.Inputs); i++ {
		same = old.Inputs[i] == r.cp.Inputs[i]
	}
	if !same {
		return fmt.Errorf("the checkpoint in %s is for other inputs, outputs or settings; start it with the same flags or use another -state", r.stateDir)
	}
	if !old.Finished && (old.Engine == nil || old.Log == "" || len(old.Offsets) != len(old.Inputs) || len(old.Lines) != len(old.Inputs) || len(old.Done) != len(old.Inputs)) {
		return fmt.Errorf("the checkpoint in %s is damaged", r.stateDir)
	}
	return nil
}

// reopen opens an output file, cut back to size bytes, for appending.
func reopen(path string, size int64) (*os.File, *countingWriter, error) {
	flags := os.O_RDWR | os.O_CREATE
	if size == 0 {
		flags |= os.O_TRUNC
	}
	f, err := os.OpenFile(path, flags, 0o644)
	if err != nil {
		return nil, nil, err
	}
	if size > 0 {
		st, err := f.Stat()
		if err == nil && st.Size() < size {
			err = fmt.Errorf("%s is shorter than the checkpoint says it was written to", path)
		}
		if err == nil {
			err = f.Truncate(size)
		}
		if err == nil {
			_, err = f.Seek(size, io.SeekStart)
		}
		if err != nil {
			f.Close()
			return nil, nil, err
		}
	}
	return f, &countingWriter{w: bufio.NewWriterSize(f, 1<<16), n: size}, nil
}

func (r *runner) close() {
	for _, in := range r.inputs {
		if in.file != nil {
			in.file.Close()
		}
	}
	if r.outFile != nil {
		r.outFile.Close()
	}
	if r.rejFile != nil {
		r.rejFile.Close()
	}
	if r.logFile != nil {
		r.logFile.Close()
	}
}

// nextLine reads the next line, taking the inputs a line at a time in turn.
func (r *runner) nextLine() (*lineReader, []byte, bool, error) {
	for range r.inputs {
		in := r.inputs[r.next]
		r.next = (r.next + 1) % len(r.inputs)
		if in.done {
			continue
		}
		line, err := in.next()
		if err == io.EOF {
			in.done = true
			continue
		}
		if err != nil {
			return nil, nil, false, fmt.Errorf("reading %s: %v", in.path, err)
		}
		return in, line, true, nil
	}
	for _, in := range r.inputs {
		if !in.done {
			return r.nextLine()
		}
	}
	return nil, nil, false, nil
}

type reject struct {
	Reason string          `json:"reason"`
	Input  string          `json:"input"`
	Line   uint64          `json:"line"`
	Error  string          `json:"error,omitempty"`
	Raw    string          `json:"raw,omitempty"`
	Event  json.RawMessage `json:"event,omitempty"`
}

func (r *runner) reject(rej reject) error {
	if r.rej == nil {
		return nil
	}
	b, err := json.Marshal(rej)
	if err != nil {
		return err
	}
	return r.rej.write(b)
}

func (r *runner) write(rel []inorderly.Released) error {
	for _, x := range rel {
		if err := r.out.write(x.Line); err != nil {
			return err
		}
		if r.log != nil {
			b, err := json.Marshal(x.Record)
			if err != nil {
				return err
			}
			if err := r.log.write(b); err != nil {
				return err
			}
			r.logCount++
		}
	}
	return nil
}

func (r *runner) process(every int) (*summary, error) {
	if r.finished != nil {
		return r.finished, nil
	}
	count := 0
	for {
		in, line, ok, err := r.nextLine()
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		if len(trimSpace(line)) > 0 {
			if err := r.handle(in, line); err != nil {
				return nil, err
			}
		}
		count++
		if r.stateDir != "" && count%every == 0 {
			if err := r.checkpoint(false); err != nil {
				return nil, err
			}
		}
	}
	if err := r.write(r.eng.Flush()); err != nil {
		return nil, err
	}
	sum := r.summary()
	if r.stateDir != "" {
		r.finished = sum
		if err := r.checkpoint(true); err != nil {
			return nil, err
		}
	} else if err := r.flush(); err != nil {
		return nil, err
	}
	return sum, nil
}

func (r *runner) handle(in *lineReader, line []byte) error {
	var ev inorderly.Event
	var err error
	if in.tooLong {
		err = fmt.Errorf("the line is longer than %d bytes", inorderly.MaxLine)
	} else {
		ev, err = inorderly.ParseLine(line)
	}
	if err != nil {
		r.invalid++
		raw := string(line)
		if len(raw) > 1000 {
			raw = raw[:1000]
		}
		return r.reject(reject{Reason: "invalid", Input: in.path, Line: in.line, Error: err.Error(), Raw: raw})
	}
	status, rel, err := r.eng.Push(ev)
	if err != nil {
		return err
	}
	if err := r.write(rel); err != nil {
		return err
	}
	if status == inorderly.Late || status == inorderly.Conflict {
		return r.reject(reject{Reason: status.String(), Input: in.path, Line: in.line, Event: ev.Canonical()})
	}
	return nil
}

func trimSpace(b []byte) []byte {
	for len(b) > 0 && (b[0] == ' ' || b[0] == '\t' || b[0] == '\r') {
		b = b[1:]
	}
	for len(b) > 0 && (b[len(b)-1] == ' ' || b[len(b)-1] == '\t' || b[len(b)-1] == '\r') {
		b = b[:len(b)-1]
	}
	return b
}

func (r *runner) flush() error {
	if err := r.out.w.Flush(); err != nil {
		return err
	}
	if r.rej != nil {
		if err := r.rej.w.Flush(); err != nil {
			return err
		}
	}
	if r.log != nil {
		if err := r.log.w.Flush(); err != nil {
			return err
		}
	}
	return nil
}

// checkpoint makes the output written so far durable, then records where
// every input had got to with the orderer's state, by writing a new file
// and renaming it over the old one.
func (r *runner) checkpoint(finished bool) error {
	if err := r.flush(); err != nil {
		return err
	}
	if err := r.outFile.Sync(); err != nil {
		return err
	}
	for _, f := range []*os.File{r.rejFile, r.logFile} {
		if f != nil {
			if err := f.Sync(); err != nil {
				return err
			}
		}
	}
	if !finished {
		if err := r.compact(); err != nil {
			return err
		}
	}
	cp := r.cp
	cp.Log = r.logName
	cp.OutSize = r.out.n
	if r.rej != nil {
		cp.RejectsSize = r.rej.n
	}
	cp.Invalid = r.invalid
	cp.LogSize = r.log.n
	cp.Next = r.next
	cp.Finished = finished
	if finished {
		cp.Summary = r.finished
	} else {
		snap, err := r.eng.SnapshotHeld()
		if err != nil {
			return err
		}
		cp.Engine = snap
		for _, in := range r.inputs {
			cp.Offsets = append(cp.Offsets, in.off)
			cp.Lines = append(cp.Lines, in.line)
			cp.Done = append(cp.Done, in.done)
		}
	}
	b, err := json.Marshal(cp)
	if err != nil {
		return err
	}
	tmp := r.statePath() + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, r.statePath()); err != nil {
		return err
	}
	if d, err := os.Open(r.stateDir); err == nil {
		d.Sync() // not every system can sync a directory
		d.Close()
	}
	for _, old := range r.oldLogs {
		os.Remove(old)
	}
	r.oldLogs = nil
	return nil
}

func (r *runner) summary() *summary {
	st := r.eng.Stats()
	return &summary{
		Version: inorderly.Version, Hash: r.eng.Hash(), In: st.In + r.invalid, Ordered: st.Out,
		Duplicates: st.Duplicates, Late: st.Late, Conflicts: st.Conflicts, Invalid: r.invalid,
		MissingParents: st.MissingParents, SeqGaps: st.SeqGaps, SeqBackwards: st.SeqBackwards,
		Gaps: r.eng.Gaps(), Resumed: r.resumed, Config: r.eng.Config(),
	}
}
