# Reference

The command, its files and the Go packages. [CONTRACT.md](CONTRACT.md) has
the ordering rules themselves.

## The command

```
inorderly [flags]           read NDJSON events, write them in order
inorderly version           print the version as JSON
inorderly demo [flags]      run the self-checking demo and print its report
```

### Flags

| Flag | Default | Meaning |
|---|---|---|
| `-in FILE` | standard input | An input file. Repeat it to read several files a line at a time in turn: one line from the first, one from the second, and so on. `-` is standard input. |
| `-out FILE` | standard output | The ordered output. |
| `-window N` | 0 | How far, in event-time units, an event may arrive behind the newest time seen. |
| `-hold N` | the window | How long past its own time an event waits for a parent. |
| `-remember N` | 0 (always) | How long behind the watermark released events are remembered, for duplicates and parents. |
| `-rejects FILE` | none | Late, conflicting and invalid lines, one JSON object each. |
| `-state DIR` | none | Checkpoints. A run killed part way carries on from the last checkpoint when started again with the same flags. Needs `-in` files and an `-out` file. |
| `-checkpoint-every N` | 10000 | Input lines between checkpoints. |
| `-stats FILE` | standard error | Where the summary goes. |
| `-strict` | off | Exit with status 3 if any line was late, conflicting or invalid. |

### Input

One event per line, as JSON. Blank lines are skipped. Lines may end in `\n`
or `\r\n`, and the last line needs no line ending. A line longer than 16 MiB
is invalid. Unknown fields, a missing `producer` or `time`, a fraction where
an integer belongs, a key repeated in one `data` object, or anything after the
event on the same line make the line invalid.

```json
{"id":"o-1001","producer":"checkout","time":1760000000123,"data":{"total":42.5}}
{"producer":"feed-a","seq":88,"time":1760000000124,"logical":1}
{"id":"pay-77","producer":"payments","time":1760000000120,"parents":["o-1001"]}
```

### Output

Each line is the event's canonical form with `pos` (from 1) and, when a
parent didn't come first, `missing`:

```json
{"pos":1,"id":"o-1001","producer":"checkout","time":1760000000123,"data":{"total":42.5}}
{"pos":2,"id":"pay-77","producer":"payments","time":1760000000120,"parents":["o-1001"]}
{"pos":3,"producer":"feed-a","seq":88,"time":1760000000124,"logical":1}
```

Here `pay-77` has a clock 3 ms behind its parent's, so it waited for
`o-1001` and went straight after it.

### Rejects

```json
{"reason":"late","input":"a.ndjson","line":4021,"event":{"id":"x","producer":"p","time":12}}
{"reason":"conflict","input":"b.ndjson","line":77,"event":{"id":"o-1001","producer":"checkout","time":1760000000123}}
{"reason":"invalid","input":"-","line":9,"error":"time is missing","raw":"{\"id\":\"y\",\"producer\":\"p\"}"}
```

`raw` holds at most the first 1,000 bytes of the line.

### Summary

Printed as JSON when the run ends:

| Field | Meaning |
|---|---|
| `hash` | The replay hash: `sha256:` and the SHA-256 of the output. `sha256sum` on the output file gives the same. |
| `in` | Lines read, not counting blank ones. |
| `ordered` | Events written. |
| `duplicates` | Repeats dropped. |
| `late`, `conflicts`, `invalid` | Lines refused, by reason. |
| `missing_parents` | Events written with at least one parent missing. |
| `seq_gaps`, `gaps` | Jumps in each producer's `seq` numbers, and the first 1,000 of them. |
| `seq_backwards` | Events whose `seq` was below one already written for their producer. |
| `resumed` | Whether this run carried on from a checkpoint. |
| `config` | The window, hold and remember used. |

### Exit status

0 when the run finished; 1 for an error such as an unreadable file or a
checkpoint that doesn't match the flags; 2 for bad flags; 3 with `-strict`
when any line was refused.

### The state directory

`-state DIR` holds `checkpoint.json` (where each input had got to, how much
output was written, and the held events) and `released.ndjson` (one record per
event written, for spotting duplicates and parents after a restart). Every
checkpoint first makes the output, the rejects and the record log durable,
then replaces `checkpoint.json` by writing a new file and renaming it over
the old one. Started again, `inorderly` cuts each file back to the checkpoint
and carries on; anything written after the checkpoint is written again, the
same. A finished run started again prints its summary and does nothing else.
Starting with different inputs, output or settings is refused.

## The Go packages

```go
import "github.com/inorderly/inorderly"

o, err := inorderly.New(inorderly.Config{Window: 500, Hold: 500})
status, released, err := o.Push(inorderly.Event{ID: "a", Producer: "p", Time: 1760000000123})
for _, r := range released {
    fmt.Println(string(r.Line)) // pos, event, missing
}
released = o.Flush() // at the end of the stream
fmt.Println(o.Hash())
```

| Name | What it does |
|---|---|
| `New(Config)` | A new Orderer. |
| `(*Orderer).Push(Event)` | Takes an event: `Accepted`, `Duplicate`, `Late` or `Conflict`, and the events released by it, in order. |
| `(*Orderer).Flush()` | Releases everything held. Push can't follow it. |
| `(*Orderer).Hash()` | The replay hash so far. |
| `(*Orderer).Stats()`, `Gaps()`, `Watermark()` | Counts, sequence gaps and the watermark. |
| `(*Orderer).Snapshot()` | The whole state, to save. |
| `(*Orderer).SnapshotHeld()` | The state without the remembered released events; keep each `Released.Record` yourself. |
| `Restore(*Snapshot, []Record)` | An Orderer that carries on where the snapshot was taken. |
| `ParseLine([]byte)` | One event from one NDJSON line, strictly. |
| `(*Event).Canonical()` | The event's canonical bytes. |
| `(*Event).Identity()` | `id`, or `producer#seq`. |

An Orderer is not safe for concurrent use; give each stream its own.

```go
import "github.com/inorderly/inorderly/hlc"

clock := hlc.New(nil) // milliseconds since 1970
wall, logical := clock.Now()                      // stamp a local event
wall, logical = clock.Observe(msg.Time, msg.Logical) // stamp what you make from a received event
```

A producer that stamps with `hlc` gives every child a timestamp above its
parents', so Inorderly never has to hold a child back.
