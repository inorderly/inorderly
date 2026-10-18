# Inorderly

Deterministic event ordering. Inorderly takes events that arrive late, twice
or out of order and turns them into one sequence that comes out the same
every time, byte for byte, however the events arrived.

Same events in, same sequence out.

It's one small Go library and one command, `inorderly`, that reads NDJSON and
writes NDJSON. Any language can feed it.

## Why

Every pipeline that takes events from more than one place ends up writing the
same fix. A webhook gets retried, so the same order shows up twice. Two
servers' clocks disagree, so a payment is stamped before the order it pays
for. A feed handler sees tick 88 before tick 87. Someone writes a dedupe
table, a sort buffer and a timeout, and it mostly works.

Then you run the same day's events again and get a slightly different
answer. Nobody can tell you why.

Inorderly is that fix, written once, with the rules written down. It holds
events for a bounded window, drops repeats, waits a bounded time for parents
that should come first, and releases everything in a published order. When
the same events arrive in a different order, the output doesn't change. The
replay hash proves it: it's the SHA-256 of the output, so two runs compare in
one line.

## Quick start

Download the binary for your system from the
[latest release](https://github.com/inorderly/inorderly/releases/latest), or
build it with Go:

```sh
go install github.com/inorderly/inorderly/cmd/inorderly@latest
```

Feed it events, one JSON object per line:

```sh
printf '%s\n' \
  '{"id":"o-1001","producer":"checkout","time":1760000000123,"data":{"total":42.5}}' \
  '{"producer":"feed-a","seq":88,"time":1760000000124,"logical":1}' \
  '{"id":"pay-77","producer":"payments","time":1760000000120,"parents":["o-1001"]}' \
  | inorderly -window 500
```

```json
{"pos":1,"id":"o-1001","producer":"checkout","time":1760000000123,"data":{"total":42.5}}
{"pos":2,"id":"pay-77","producer":"payments","time":1760000000120,"parents":["o-1001"]}
{"pos":3,"producer":"feed-a","seq":88,"time":1760000000124,"logical":1}
```

The payment's clock was 3 ms behind the checkout's. It names the order as its
parent, so it waited for it and went straight after it. A summary with the
replay hash and the counts goes to standard error.

Shuffle those three lines, repeat any of them, and the output stays the same.

## The rules, short

- **Events.** Each has a `producer`, a `time` (an integer, in units you
  choose) and an identity: an `id`, or a `seq` number within its producer.
  Optional: `logical` (a hybrid logical clock counter), `parents` (identities
  that must come first) and `data` (any JSON).
- **Window.** The watermark is the newest time seen minus `-window`. Events
  below it go out. An event that arrives below it is late: it's refused and
  reported, never slipped in somewhere new.
- **Order.** By time, then logical counter, then producer, seq and id.
- **Parents.** An event that names a parent waits for it, up to `-hold` past
  its own time, and then goes straight after it. A parent that doesn't show
  up in time is reported in the event's `missing` field.
- **Duplicates.** The same event again is dropped. Two different events with
  the same identity are a conflict: the second is refused and reported.
- **Canonical form.** Field order, whitespace, key order inside `data` and
  string escaping are all fixed, so the same event from two different JSON
  encoders is the same bytes.

[docs/CONTRACT.md](docs/CONTRACT.md) states every rule exactly, including
cycles of parents and what counts as missing.

## Things it's for

- **Merging redundant feeds.** Two copies of a market data feed, each missing
  different ticks: read both with `-in a.ndjson -in b.ndjson`, and every tick
  that reached either copy comes out once, in order. Ticks lost from both are
  reported as gaps.
- **Replayable processing.** Run yesterday's events through again and get
  yesterday's output, with the hash to prove it.
- **Webhooks and retries.** Dedupe and order deliveries from providers that
  retry and don't promise order.
- **Causal order across services.** An order, its payment and its shipment
  from three services come out parent first, even when the clocks disagree.

## Long runs and crashes

With `-state DIR`, Inorderly checkpoints as it goes. Kill it at any moment,
start it again with the same flags, and it carries on from the last
checkpoint. The finished output is byte for byte what one uninterrupted run
would have written.

```sh
inorderly -in day.ndjson -out ordered.ndjson -window 500 -state ordered.state
```

## From Go

```go
o, _ := inorderly.New(inorderly.Config{Window: 500, Hold: 500})
status, released, _ := o.Push(inorderly.Event{ID: "a", Producer: "p", Time: 1760000000123})
for _, r := range released {
    fmt.Println(string(r.Line))
}
for _, r := range o.Flush() {
    fmt.Println(string(r.Line))
}
fmt.Println(status, o.Hash())
```

Package `hlc` is a hybrid logical clock for producers. Stamp events with it
and every child sorts after its parents without waiting.

[docs/REFERENCE.md](docs/REFERENCE.md) lists every flag, file format and
function.

## How it's tested

Every number here comes from the files in [test/results](test/results),
written by `sh scripts/record-tests.sh`.

- **The same events in any order.** 10,000 random tangles of events, with parents in cycles, parents that never come, clocks that disagree and shared times, each fed in 20 arrival orders with repeats and snapshot-and-restore along the way: 200,000 orders, one output per set.
- **Killed and resumed.** A run of 240,351 lines from two input files, with late and invalid lines, is killed with SIGKILL 30 times, each time at a random moment after a fresh checkpoint, and started again. The output, the rejects and the replay hash match a run that was never interrupted.
- **The demo.** `inorderly demo` checks 100,000 events in
  20 shuffled arrival orders, with every event sent one to five
  times: one output, one hash. It also restores from snapshots part way,
  refuses 1,000 late events without changing anything else, keeps
  13,916 children after their parents (3,919 of them
  with clocks behind their parent's), reports all 1,044 orphans,
  and merges two copies of a 100,000-tick feed, each missing about 5%,
  reporting exactly the 247 ticks lost from both.
- **Another language.** A Python producer with its own JSON encoder, key
  order and escapes gives the same output from every arrival order.
- **Speed.** On a two-core 2.1 GHz Xeon cloud machine, one Orderer takes about 428,000 arriving events a second (2.3 µs each, two-thirds of them repeats) and writes about 143,000 ordered events a second.

The tests run on Linux, macOS and Windows on every push, with the race
detector on Linux.

## Building and releasing

```sh
go test ./...
sh scripts/record-tests.sh   # every test, the long search, the demo, the smoke check
```

Releases are automated; [RELEASING.md](RELEASING.md) has the procedure.

## License

Apache License 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
