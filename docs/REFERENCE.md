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
| `-in FILE` or `-in URL` | standard input | An input file or broker. Repeat it to read several a line at a time in turn: one line from the first, one from the second, and so on. `-` is standard input. |
| `-out FILE` or `-out URL` | standard output | The ordered output: a file or a broker. |
| `-window N` | 0 | How far, in event-time units, an event may arrive behind the newest time seen. |
| `-window-for PRODUCER=N` | none | A window of N for one producer, in place of `-window`. Repeat it for more producers. Release waits for the largest window. |
| `-hold N` | the window | How long past its own time an event waits for a parent. |
| `-remember N` | 0 (always) | How long behind the watermark released events are remembered, for duplicates and parents. |
| `-rejects FILE` | none | Late, conflicting and invalid lines, one JSON object each. |
| `-late FILE` | none | Late events alone, each as its canonical line, ready to be read again by another run. |
| `-state DIR` | none | Checkpoints. A run killed part way carries on from the last checkpoint when started again with the same flags. Needs `-in` files or brokers and an `-out` file or broker. |
| `-checkpoint-every N` | 10000 | Input lines between checkpoints. |
| `-checkpoint-interval D` | 0 (off) | Also checkpoint when this long, such as `30s`, has passed since the last one. |
| `-follow` | off | Keep reading broker inputs as messages come, never flushing. SIGINT or SIGTERM writes a checkpoint and stops. Needs `-state`, and brokers for every input. |
| `-stats FILE` | standard error | Where the summary goes. |
| `-strict` | off | Exit with status 3 if any line was late, conflicting or invalid. |

### Input

One event per line, as JSON. Blank lines are skipped. Lines may end in `\n`
or `\r\n`, and the last line needs no line ending. A line longer than 16 MiB
is invalid. Unknown fields, a missing `producer` or `time`, a fraction where
an integer belongs, a key repeated in one `data` object, or anything after the
event on the same line make the line invalid. Keys must be spelled exactly,
each once, and identities, producers and parents must be valid UTF-8.

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
{"reason":"late","input":"a.ndjson","line":4021,"watermark":40,"after":3577,"event":{"id":"x","producer":"p","time":12}}
{"reason":"conflict","input":"b.ndjson","line":77,"event":{"id":"o-1001","producer":"checkout","time":1760000000123}}
{"reason":"invalid","input":"-","line":9,"error":"time is missing","raw":"{\"id\":\"y\",\"producer\":\"p\"}"}
```

`raw` holds at most the first 1,000 bytes of the line. A late line gives
`watermark`, the time its producer's events had to reach when it came, and
`after`, how many lines had been written by then. A line from a broker gives
`at`, where it was: `entry 1700000000000-3`, `seq 57` or
`partition 2 offset 51`.

### Late events

`-late FILE` gets each late event as its canonical line, the same form the
output uses without `pos` and `missing`, so the file can be the input of
another run: with a wider window, say, or merged into a correction.

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
| `stopped` | Present and true when a `-follow` run was stopped by a signal. |
| `config` | The window, producer windows, hold and remember used. |

### Exit status

0 when the run finished; 1 for an error such as an unreadable file or a
checkpoint that doesn't match the flags; 2 for bad flags; 3 with `-strict`
when any line was refused.

### The state directory

`-state DIR` holds `checkpoint.json` (where each input had got to, how much
output was written, and the held events) and a record log,
`released-N.ndjson`, with one line per event written, for spotting duplicates
and parents after a restart. Every checkpoint first makes the output, the
rejects and the record log durable, then replaces `checkpoint.json` by writing
a new file and renaming it over the old one. With `-remember`, the record log
is rewritten from time to time with only what is still remembered, under the
next number, and the old one is removed once a checkpoint names the new one.

Started again, `inorderly` cuts each file back to the checkpoint and carries
on; anything written after the checkpoint is written again, the same. It
refuses to carry on if an input file's size or modification time has changed
since the checkpoint, or if the inputs, outputs or settings differ. A finished
run started again prints its summary and does nothing else, unless the output
file has changed since, which is refused too.

`-out`, `-rejects` and `-late` can't be the same file, and none of them can
be an input. A UTF-8 byte-order mark at the start of an input is skipped.

## Brokers

`-in` and `-out` take broker URLs as well as files. Inorderly talks to Redis
Streams, NATS JetStream and Kafka itself, with clients written against Go's
standard library; there is nothing else to install.

| URL | What it reads or writes |
|---|---|
| `redis://[user:pass@]host[:6379]/stream` | A Redis stream. Each entry keeps the line in the field `event`, or the one `?field=` names. `?db=N` selects a database. Redis 6.2 or newer, or Valkey. |
| `nats://[user:pass@]host[:4222]/stream` | A JetStream stream. `?subject=` reads only that subject, and names the subject an output is written to when the stream has more than one. A user without a password is a token. |
| `kafka://[user:pass@]host[:9092]/topic` | A Kafka topic, every partition. With a user, SASL PLAIN, or `?sasl=scram-sha-256` or `scram-sha-512`. Kafka 2.1 or newer. |

`rediss://`, `tls://` and `kafkas://` use TLS, checking the server's
certificate unless `?insecure=true`. Passwords are never written to a
checkpoint, a rejects file or a message.

**Reading.** Each message is one input line. Inorderly reads without a
consumer group: it keeps where each input had got to (the Redis entry ID, the
JetStream stream sequence, the next offset of each Kafka partition) in its own
checkpoint. A Kafka input takes its partitions a message at a time in turn.
Without `-follow`, a run reads up to the end each input had when it started,
then flushes, as with files, and a restarted run reads to that same end. With
`-follow`, it keeps reading and releasing as the watermark moves, and never
flushes: what is still held waits in the checkpoint for the next run.

**Writing.** One message per output line, in order: a Redis entry with the ID
`<pos>-1`, so Redis itself refuses a line written twice; a JetStream message
on one subject, acked, with the `Nats-Msg-Id` `<subject>:<pos>`; a Kafka record
with `acks=all`, on a topic of exactly one partition, so its order is the
output's. A run without a checkpoint refuses an output that already holds
messages.

**Restarting.** A broker output can't be cut back to the checkpoint the way
a file is. Instead, a restarted run reads the output's last message, which
names its `pos`, and doesn't write the lines up to there again. The line it
makes at that `pos` must be the one stored, or it stops: something else
writes there. So a run killed at any moment, with brokers on both ends,
leaves the output it would have left without the kill, each line once.

**What it doesn't do.** Kafka records are read uncommitted, without
transaction markers; batches compressed with gzip are read, and snappy, lz4
or zstd batches are refused by name. Outputs are written uncompressed. A
Kafka partition added after a run started isn't read by that run. Inorderly
doesn't create streams or topics.

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
| `(*Orderer).Stats()`, `Gaps()`, `Watermark()` | Counts, sequence gaps and the watermark (newest time less the widest window). |
| `(*Orderer).WatermarkFor(producer)` | The time below which that producer's events are late. |
| `Config.Windows` | Producer windows: `[]ProducerWindow{{Producer: "slow", Window: 5000}}`. |
| `(*Orderer).Snapshot()` | The whole state, to save. |
| `(*Orderer).Remembered()` | The records of the released events still remembered. |
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
