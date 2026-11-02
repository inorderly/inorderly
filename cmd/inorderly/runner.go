// Copyright Inorderly.com 2026
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/inorderly/inorderly"
	"github.com/inorderly/inorderly/internal/broker"
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
	Positions   []string            `json:"positions,omitempty"` // broker inputs: where to read on from
	Until       []string            `json:"until,omitempty"`     // broker inputs: where a bounded read ends
	Follow      bool                `json:"follow,omitempty"`
	Lines       []uint64            `json:"lines"`
	Done        []bool              `json:"done"`
	Next        int                 `json:"next"`
	Out         string              `json:"out"`
	OutSize     int64               `json:"out_size"`
	OutPos      uint64              `json:"out_pos,omitempty"` // a broker output: the last pos stored
	Rejects     string              `json:"rejects"`
	RejectsSize int64               `json:"rejects_size"`
	Late        string              `json:"late,omitempty"`
	LateSize    int64               `json:"late_size,omitempty"`
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

// output is where ordered lines go: a file, standard output or a broker.
type output interface {
	write(ctx context.Context, pos uint64, line []byte) error
	flush(ctx context.Context) error // buffers out
	durable(ctx context.Context) error
	size() int64
	lastPos() uint64
	finish() error // checks at the end of a run
	close()
}

type fileOutput struct {
	f   *os.File // nil for standard output
	w   *countingWriter
	pos uint64
}

func (o *fileOutput) write(_ context.Context, pos uint64, line []byte) error {
	o.pos = pos
	return o.w.write(line)
}
func (o *fileOutput) flush(context.Context) error { return o.w.w.Flush() }
func (o *fileOutput) durable(ctx context.Context) error {
	if err := o.flush(ctx); err != nil {
		return err
	}
	if o.f == nil {
		return nil
	}
	return o.f.Sync()
}
func (o *fileOutput) size() int64     { return o.w.n }
func (o *fileOutput) lastPos() uint64 { return o.pos }
func (o *fileOutput) finish() error   { return nil }
func (o *fileOutput) close() {
	if o.f != nil {
		o.f.Close()
	}
}

// brokerOutput writes one message per line. After a restart the broker may
// already hold lines past the checkpoint; those are not written again, and
// the last one must be the line this run makes at that pos.
type brokerOutput struct {
	name   string
	sink   broker.Sink
	skip   uint64 // lines up to here are already stored
	last   []byte // the stored line at skip
	pos    uint64
	stored uint64
}

func (o *brokerOutput) write(ctx context.Context, pos uint64, line []byte) error {
	o.pos = pos
	if pos <= o.skip {
		if pos == o.skip && !bytes.Equal(line, o.last) {
			return fmt.Errorf("%s holds a different line at pos %d than this run makes; something else writes there", o.name, pos)
		}
		return nil
	}
	return o.sink.Write(ctx, pos, line)
}
func (o *brokerOutput) flush(ctx context.Context) error { return o.sink.Flush(ctx) }
func (o *brokerOutput) durable(ctx context.Context) error {
	if err := o.sink.Flush(ctx); err != nil {
		return err
	}
	if o.pos > o.stored {
		o.stored = o.pos
	}
	return nil
}
func (o *brokerOutput) size() int64     { return 0 }
func (o *brokerOutput) lastPos() uint64 { return o.pos }
func (o *brokerOutput) finish() error {
	if o.pos < o.skip {
		return fmt.Errorf("%s holds lines up to pos %d, but this run made only %d", o.name, o.skip, o.pos)
	}
	return nil
}
func (o *brokerOutput) close() { o.sink.Close() }

type runner struct {
	eng      *inorderly.Orderer
	inputs   []*lineReader
	next     int
	out      output
	rejFile  *os.File
	rej      *countingWriter
	lateFile *os.File
	late     *countingWriter
	logFile  *os.File // with -state: the record of every released event
	log      *countingWriter
	logName  string
	logCount int
	oldLogs  []string // logs to remove once a checkpoint names a newer one
	stateDir string
	follow   bool
	interval time.Duration
	lastCP   time.Time
	cp       checkpoint
	invalid  uint64
	resumed  bool
	stopped  bool
	finished *summary
}

// The wait for each followed input in turn when there is nothing new.
const followWait = 200 * time.Millisecond

func newRunner(ctx context.Context, cfg inorderly.Config, ins []string, out, rejects, late, stateDir string, follow bool) (*runner, error) {
	r := &runner{stateDir: stateDir, follow: follow}
	names := make([]string, len(ins))
	eps := make([]*broker.Endpoint, len(ins))
	stdin := 0
	for i, p := range ins {
		if broker.IsURL(p) {
			e, err := broker.Parse(p)
			if err != nil {
				return nil, err
			}
			eps[i], names[i] = e, e.String()
			continue
		}
		names[i] = absPath(p)
		if p == "-" {
			stdin++
		}
	}
	var outEP *broker.Endpoint
	outName := absPath(out)
	if broker.IsURL(out) {
		e, err := broker.Parse(out)
		if err != nil {
			return nil, err
		}
		outEP, outName = e, e.String()
	}
	if stdin > 1 {
		return nil, errors.New("standard input can be read only once")
	}
	if stateDir != "" && (stdin > 0 || out == "-") {
		return nil, errors.New("-state needs -in files or brokers and an -out file or broker: standard input and output can't be read again or cut back")
	}
	// Nothing may be both read and written, or written twice.
	if out != "-" && rejects != "" && outName == absPath(rejects) {
		return nil, errors.New("-out and -rejects are the same file")
	}
	if (rejects != "" && broker.IsURL(rejects)) || (late != "" && broker.IsURL(late)) {
		return nil, errors.New("-rejects and -late are files")
	}
	if late != "" && ((out != "-" && absPath(late) == outName) || absPath(late) == absPath(rejects)) {
		return nil, errors.New("-late can't be the same file as -out or -rejects")
	}
	for _, a := range names {
		if (out != "-" && a == outName) || (rejects != "" && a == absPath(rejects)) || (late != "" && a == absPath(late)) {
			return nil, fmt.Errorf("%s is both an input and an output", a)
		}
	}
	r.cp = checkpoint{Format: checkpointFormat, Version: inorderly.Version, Config: cfg, Inputs: names,
		Out: outName, Rejects: absPath(rejects), Late: absPath(late), Follow: follow}

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

	// The output first: a broker output that already holds lines is only
	// for a run carrying on from a checkpoint.
	var bout *brokerOutput
	if outEP != nil {
		sink, err := broker.OpenSink(ctx, outEP)
		if err != nil {
			return nil, err
		}
		bout = &brokerOutput{name: outName, sink: sink}
		pos, line, err := sink.Last(ctx)
		if err != nil {
			sink.Close()
			return nil, err
		}
		switch {
		case old == nil && pos > 0:
			sink.Close()
			return nil, fmt.Errorf("%s already holds lines; write to an empty one, or carry on with the -state that wrote them", outName)
		case old != nil && pos < old.OutPos:
			sink.Close()
			return nil, fmt.Errorf("%s holds lines up to pos %d, but the checkpoint in %s says %d were stored; it has been changed", outName, pos, stateDir, old.OutPos)
		}
		bout.skip, bout.last, bout.stored = pos, line, pos
		r.out = bout
	}

	if old != nil && old.Finished {
		if bout != nil {
			if bout.skip != old.OutPos {
				r.close()
				return nil, fmt.Errorf("this run finished before, but %s has changed since; use another -state to run it again", out)
			}
		} else if st, err := os.Stat(out); err != nil || st.Size() != old.OutSize {
			r.close()
			return nil, fmt.Errorf("this run finished before, but %s has changed since; use another -state to run it again", out)
		}
		r.finished = old.Summary
		return r, nil
	}

	for i, p := range ins {
		lr := &lineReader{path: names[i]}
		switch {
		case eps[i] != nil:
			from, until := "", ""
			if old != nil {
				from, until = old.Positions[i], old.Until[i]
				lr.line, lr.done = old.Lines[i], old.Done[i]
			} else if !follow {
				var err error
				if until, err = broker.End(ctx, eps[i]); err != nil {
					r.close()
					return nil, err
				}
			}
			r.cp.Sizes = append(r.cp.Sizes, 0)
			r.cp.ModTimes = append(r.cp.ModTimes, 0)
			r.cp.Until = append(r.cp.Until, until)
			if !lr.done {
				src, err := broker.OpenSource(ctx, eps[i], from, until)
				if err != nil {
					r.close()
					return nil, err
				}
				lr.src = src
			}
			lr.until = until
		case p == "-":
			lr.r = bufio.NewReaderSize(os.Stdin, 1<<16)
		default:
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
			r.cp.Until = append(r.cp.Until, "")
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
		if bout == nil {
			var f *os.File
			var w *countingWriter
			f, w, err = reopen(out, old.OutSize)
			if err == nil {
				r.out = &fileOutput{f: f, w: w}
			}
		}
		if err == nil && rejects != "" {
			r.rejFile, r.rej, err = reopen(rejects, old.RejectsSize)
		}
		if err == nil && late != "" {
			r.lateFile, r.late, err = reopen(late, old.LateSize)
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
		switch {
		case bout != nil:
		case out == "-":
			r.out = &fileOutput{w: &countingWriter{w: bufio.NewWriterSize(os.Stdout, 1<<16)}}
		default:
			var f *os.File
			var w *countingWriter
			f, w, err = reopen(out, 0)
			if err == nil {
				r.out = &fileOutput{f: f, w: w}
			}
		}
		if err == nil && rejects != "" {
			r.rejFile, r.rej, err = reopen(rejects, 0)
		}
		if err == nil && late != "" {
			r.lateFile, r.late, err = reopen(late, 0)
		}
		if err == nil && stateDir != "" {
			r.logName = "released-1.ndjson"
			r.oldLogs = r.otherLogs()
			r.logFile, r.log, err = reopen(r.logPath(), 0)
		}
		if err == nil {
			r.eng, err = inorderly.New(cfg)
		}
		// A first checkpoint before anything is written, so a run killed
		// early carries on over the same broker positions and output.
		if err == nil && stateDir != "" {
			err = r.checkpoint(ctx, false)
		}
	}
	if err != nil {
		r.close()
		return nil, err
	}
	r.lastCP = time.Now()
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
	path := filepath.Join(r.stateDir, name)
	// A run killed after compacting but before its checkpoint named the new
	// log leaves a file of this name, which a restart lists as left over.
	// It is about to be the log, so it mustn't be removed with the rest.
	left := r.oldLogs[:0]
	for _, old := range r.oldLogs {
		if old != path {
			left = append(left, old)
		}
	}
	r.oldLogs = left
	f, w, err := reopen(path, 0)
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
	same := old.Config.Equal(r.cp.Config) && old.Out == r.cp.Out && old.Rejects == r.cp.Rejects && old.Late == r.cp.Late && len(old.Inputs) == len(r.cp.Inputs) && old.Follow == r.cp.Follow
	for i := 0; same && i < len(old.Inputs); i++ {
		same = old.Inputs[i] == r.cp.Inputs[i]
	}
	if !same {
		return fmt.Errorf("the checkpoint in %s is for other inputs, outputs or settings; start it with the same flags or use another -state", r.stateDir)
	}
	n := len(old.Inputs)
	if !old.Finished && (old.Engine == nil || old.Log == "" || len(old.Offsets) != n || len(old.Lines) != n || len(old.Done) != n) {
		return fmt.Errorf("the checkpoint in %s is damaged", r.stateDir)
	}
	for i, in := range old.Inputs {
		if broker.IsURL(in) && !old.Finished && (len(old.Positions) != n || len(old.Until) != n || (old.Positions[i] == "" && !old.Done[i] && old.Lines[i] > 0)) {
			return fmt.Errorf("the checkpoint in %s is damaged", r.stateDir)
		}
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
		if in.src != nil {
			in.src.Close()
		}
	}
	if r.out != nil {
		r.out.close()
	}
	if r.rejFile != nil {
		r.rejFile.Close()
	}
	if r.lateFile != nil {
		r.lateFile.Close()
	}
	if r.logFile != nil {
		r.logFile.Close()
	}
}

// errIdle: every followed input had nothing new in a full round.
var errIdle = errors.New("idle")

// nextLine reads the next line, taking the inputs a line at a time in turn.
func (r *runner) nextLine(ctx context.Context) (*lineReader, []byte, bool, error) {
	idle := 0
	for range r.inputs {
		in := r.inputs[r.next]
		r.next = (r.next + 1) % len(r.inputs)
		if in.done {
			continue
		}
		line, err := in.next(ctx, followWait/time.Duration(len(r.inputs)))
		if err == io.EOF {
			in.done = true
			continue
		}
		if err == broker.ErrIdle {
			idle++
			continue
		}
		if err != nil {
			if ctx.Err() != nil {
				return nil, nil, false, ctx.Err()
			}
			return nil, nil, false, fmt.Errorf("reading %s: %v", in.path, err)
		}
		return in, line, true, nil
	}
	if idle > 0 {
		return nil, nil, false, errIdle
	}
	for _, in := range r.inputs {
		if !in.done {
			return r.nextLine(ctx)
		}
	}
	return nil, nil, false, nil
}

type reject struct {
	Reason    string          `json:"reason"`
	Input     string          `json:"input"`
	Line      uint64          `json:"line"`
	At        string          `json:"at,omitempty"`
	Watermark *int64          `json:"watermark,omitempty"` // late: the time it had to reach
	After     *uint64         `json:"after,omitempty"`     // late: the lines written when it came
	Error     string          `json:"error,omitempty"`
	Raw       string          `json:"raw,omitempty"`
	Event     json.RawMessage `json:"event,omitempty"`
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

func (r *runner) write(ctx context.Context, rel []inorderly.Released) error {
	for _, x := range rel {
		if err := r.out.write(ctx, x.Pos, x.Line); err != nil {
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

func (r *runner) process(ctx context.Context, every int) (*summary, error) {
	if r.finished != nil {
		return r.finished, nil
	}
	count := 0
	for {
		in, line, ok, err := r.nextLine(ctx)
		if err == errIdle || errors.Is(err, context.Canceled) {
			if ctx.Err() != nil {
				r.stopped = true
				break
			}
			if err := r.timedCheckpoint(ctx); err != nil {
				return nil, err
			}
			continue
		}
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		if in.src != nil || len(trimSpace(line)) > 0 {
			if err := r.handle(ctx, in, line); err != nil {
				return nil, err
			}
		}
		count++
		if r.stateDir != "" && count%every == 0 {
			if err := r.checkpoint(ctx, false); err != nil {
				return nil, err
			}
		} else if err := r.timedCheckpoint(ctx); err != nil {
			return nil, err
		}
	}
	if r.stopped {
		// Following, stopped by a signal: keep what is held for the next run.
		if err := r.checkpoint(context.Background(), false); err != nil {
			return nil, err
		}
		sum := r.summary()
		sum.Stopped = true
		return sum, nil
	}
	if err := r.write(ctx, r.eng.Flush()); err != nil {
		return nil, err
	}
	if err := r.out.finish(); err != nil {
		return nil, err
	}
	sum := r.summary()
	if r.stateDir != "" {
		r.finished = sum
		if err := r.checkpoint(ctx, true); err != nil {
			return nil, err
		}
	} else if err := r.flush(ctx); err != nil {
		return nil, err
	}
	return sum, nil
}

func (r *runner) timedCheckpoint(ctx context.Context) error {
	if r.stateDir == "" || r.interval <= 0 || time.Since(r.lastCP) < r.interval {
		return nil
	}
	return r.checkpoint(ctx, false)
}

func (r *runner) handle(ctx context.Context, in *lineReader, line []byte) error {
	var ev inorderly.Event
	var err error
	switch {
	case in.problem != "":
		err = errors.New(in.problem)
	case in.tooLong:
		err = fmt.Errorf("the line is longer than %d bytes", inorderly.MaxLine)
	default:
		ev, err = inorderly.ParseLine(line)
	}
	if err != nil {
		r.invalid++
		raw := string(line)
		if len(raw) > 1000 {
			raw = raw[:1000]
		}
		return r.reject(reject{Reason: "invalid", Input: in.path, Line: in.line, At: in.at, Error: err.Error(), Raw: raw})
	}
	status, rel, err := r.eng.Push(ev)
	if err != nil {
		return err
	}
	if err := r.write(ctx, rel); err != nil {
		return err
	}
	switch status {
	case inorderly.Late:
		wm, _ := r.eng.WatermarkFor(ev.Producer)
		after := r.eng.Stats().Out
		if r.late != nil {
			if err := r.late.write(ev.Canonical()); err != nil {
				return err
			}
		}
		return r.reject(reject{Reason: "late", Input: in.path, Line: in.line, At: in.at, Watermark: &wm, After: &after, Event: ev.Canonical()})
	case inorderly.Conflict:
		return r.reject(reject{Reason: "conflict", Input: in.path, Line: in.line, At: in.at, Event: ev.Canonical()})
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

func (r *runner) flush(ctx context.Context) error {
	if err := r.out.flush(ctx); err != nil {
		return err
	}
	if r.rej != nil {
		if err := r.rej.w.Flush(); err != nil {
			return err
		}
	}
	if r.late != nil {
		if err := r.late.w.Flush(); err != nil {
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
func (r *runner) checkpoint(ctx context.Context, finished bool) error {
	if err := r.flush(ctx); err != nil {
		return err
	}
	if err := r.out.durable(ctx); err != nil {
		return err
	}
	for _, f := range []*os.File{r.rejFile, r.lateFile, r.logFile} {
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
	cp.OutSize = r.out.size()
	if bo, ok := r.out.(*brokerOutput); ok {
		cp.OutPos = bo.stored
	}
	if r.rej != nil {
		cp.RejectsSize = r.rej.n
	}
	if r.late != nil {
		cp.LateSize = r.late.n
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
		cp.Positions = nil
		for _, in := range r.inputs {
			cp.Offsets = append(cp.Offsets, in.off)
			cp.Lines = append(cp.Lines, in.line)
			cp.Done = append(cp.Done, in.done)
			pos := ""
			if in.src != nil {
				pos = in.src.Position()
			}
			cp.Positions = append(cp.Positions, pos)
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
		if old != r.logPath() {
			os.Remove(old)
		}
	}
	r.oldLogs = nil
	r.lastCP = time.Now()
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
