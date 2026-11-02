# Notes for Claude

This file says where the next session starts and what comes next. What comes
next is decided with the owner before it goes here, and work starts only when
the owner says go.

## Releases

Releases are automated. To release, raise `Version` in `inorderly.go` and push
to `main`; never ask the owner to publish one. [RELEASING.md](RELEASING.md) has
the procedure and the checks to run afterwards.

## Where things stand

v0.1.0 is the first release (October 2026): Apache 2.0, CI on Linux, macOS and
Windows, and automated releases with smoke-tested binaries for Linux, macOS and
Windows. Pure Go, no dependencies. The code is the Orderer (bounded window,
fixed order key, causal parents with a hold and the MaxChain cap, duplicate and
conflict detection, a canonical JSON form, Snapshot/SnapshotHeld/Restore), the
`hlc` clock, and the `inorderly` command (run with round-robin inputs, rejects,
`-state` checkpoints with a compacting record log, `-strict`; `version`;
`demo`). Read [docs/CONTRACT.md](docs/CONTRACT.md) before changing behaviour:
every rule there is a promise the tests check. [docs/REFERENCE.md](docs/REFERENCE.md)
lists the flags, files and functions.

An independent review on October 18 2026 found no determinism bug for valid
input; its fixes are in (UTF-8 identities, exact input keys, waiter counting,
MaxChain, freeing released nodes, log compaction, resume checks, smoke kills).

v0.1.1 (October 18 2026) fixes a crash-safety bug the kill test caught once:
a run killed between compacting its record log and the checkpoint that
names the new log left that file behind, and the restarted run deleted it
when it compacted into the same name, so the next restart refused to resume.
TestCompactionAfterAnInterruptedCompaction reproduces it. The same release
adds four worked use cases in `examples/usecases` (code in
`internal/usecase`): retried webhooks, two redundant market feeds, sensor
telemetry and a ledger audit, each checked in several arrival orders against
a hand-written consumer, run through the command by a test, and recorded in
`test/results/usecases.txt`. inorderly.com has a post on each, tagged
"use cases".

Out of scope unless the owner decides otherwise: ordering across several
processes or machines, a server mode, storing events after they are written,
changing the window part way through a stream.

## Test results

`sh scripts/record-tests.sh` runs every test, the long determinism search
(10,000 random event sets, 20 arrival orders each), the race detector, the
release plan's cases, the demo and the smoke check on a static binary, and
writes them to `test/results/`. Run it on a clean checkout so the header names
the commit. Every number on the site and in the README comes from those files;
record again after any change that could move them. It needs Go and Python 3.

## Site

Inorderly.com is in `photozeitgeist/inorderly.com`: the Hugo Launch template
copied unchanged, with content and config values only. Pages: about, contact,
quick start, download, reference. Posts: the launch, deterministic event
ordering explained, how it was tested, the name, and four use cases
(webhooks, feeds, telemetry, audit) under the "use cases" tag, which the main
menu links. Images are drawn into
static/images. Every number comes from `test/results/`; when a recording
changes a number, update the site too.

## In progress: Inorderly 0.2 (softdev-plan item 32, approved by YK 2026-11-02)

What and why: the pipelines that need deterministic ordering live on brokers,
not files, and today a killed run resumes exactly only because it can re-read
input files and cut its output file back. A slow producer also forces one
large window on everyone, and late events can only be counted, not
reconciled.

Design decisions:
- Broker mode, native and pure Go (YK chose this over pipe recipes and client
  libraries): small built-in clients for Redis Streams, NATS JetStream and
  Kafka, still no dependencies. `-in` and `-out` take URLs as well as files:
  `redis://[user:pass@]host:port/stream`, `nats://[user:pass@]host:port/stream`
  (JetStream stream, optional `?subject=`), `kafka://host:port/topic` (every
  partition of the topic). `rediss://`, `tls://` and `kafkas://` use TLS.
  Auth: Redis AUTH, NATS user/password or token, Kafka SASL PLAIN and
  SCRAM-SHA-256/512. Each message value is one input line; inputs are read
  round-robin as files are.
- No consumer groups: Inorderly is the only reader of its inputs and keeps the
  positions in its own checkpoint (Redis entry ID, JetStream stream sequence,
  Kafka offset per partition), so a restart reads on from exactly there.
- Output: one message per ordered line, in order. On restart the sink's last
  message is read back (XREVRANGE, JetStream last message on the subject,
  the last record of a one-partition Kafka topic); lines up to its `pos` are
  not written again, and the line at that `pos` must match the one the run
  recomputes, or the run refuses (someone else writes there). Redis entries
  get the ID `<pos>-1`, so a repeat is refused by Redis itself; JetStream
  messages carry `Nats-Msg-Id`. So a killed run neither loses nor repeats an
  output line.
- Two ways to run against a broker: by default read up to the end each input
  had when the run started, then flush, as with files; with `-follow`, keep
  reading and releasing as the watermark moves, never flush, and on SIGINT or
  SIGTERM write a checkpoint and stop, so the next run carries on.
  `-checkpoint-interval` adds a time-based checkpoint for quiet streams.
- Kafka scope: Metadata, ListOffsets, Fetch and Produce with record batch v2,
  uncompressed or gzip on read (other codecs refused by name), uncompressed
  with acks=all on write; output topic must have one partition.
- Phase B, per-producer windows: `-window-for PRODUCER=N` gives one producer a
  larger window than `-window`. Its events are late only below newest time
  minus its own window; the others keep the tight one. Release waits for the
  largest window, which keeps the output the same whatever the arrival order;
  CONTRACT.md gets the rule first. Late events to a side file: `-late FILE`
  writes each late event as its canonical line, so the file can be fed to a
  later run as it is; the rejects file gives each late line `watermark` (the
  time it had to reach) and `after` (the lines written when it came).
- Upkeep: go.mod to go 1.26. Version 0.2.0.
- Testing: protocol encoders and decoders against byte fixtures; real servers
  where they can run (Redis from source here; Redis, NATS and Kafka as service
  containers in a CI job on Linux); kill-and-resume with a broker on both ends.

Steps:
1. [x] Upkeep: go.mod 1.26.
2. [x] Input and output layer (cmd/inorderly/runner.go: lineReader takes a broker.Source, output is a file or a broker sink); checkpoint keeps positions and the bounded end per broker input, and out_pos; a first checkpoint is written at start so a run killed early carries on; -follow (needs -state, brokers only), SIGINT/SIGTERM checkpoint and stop with "stopped": true; -checkpoint-interval.
3. [x] Redis Streams in and out (internal/broker/redis.go), tests in cmd/inorderly/broker_test.go against real Redis 7.2.17: same output as files, output stream equals the file output, a full output refused, 10 SIGKILLs with brokers on both ends end with the uninterrupted output, -follow over two sessions, a foreign last entry refused.
4. [x] NATS JetStream in and out (internal/broker/nats.go): pull consumer with AckAll acked per batch; positions are stream sequences read from each delivery's ack subject, which also says how many are pending (a bounded read ends there). Deliveries keep their original subject and come on the inbox subscription, matched by ack subject; found with a server trace in CI.
5. [x] Kafka in and out (internal/broker/kafka.go): Metadata v7, ListOffsets v4, Fetch v10, Produce v7, CreateTopics v3, record batch v2 (CRC32C), gzip read, TLS, SASL PLAIN and SCRAM (RFC 7677 vector). Three-partition input test; one-partition output enforced.
6. [x] CI job `brokers` (Redis 8, Valkey 9, NATS 2.12 with JetStream, Kafka 4.1.0 KRaft): in and out, 10 SIGKILLs with brokers on both ends ending with the uninterrupted output, -follow over two sessions, foreign last message refused, all green on each.
7. [x] Phase B: Config.Windows / -window-for (late per producer, release at the widest window; 200 random sets x 20 orders with snapshots), -late FILE (late events as canonical lines; read back in); late rejects carry watermark and after. Plan change: `after` is the number of lines written when the late event came (well defined and cheap), not a position search.
8. [x] README (brokers, slow producers), CONTRACT.md (promise and watermark with producer windows), REFERENCE.md (flags, broker URLs, reading, writing, restarting, limits), smoke.py (-window-for, -late, unreachable broker).
9. [x] Independent review, two rounds. Round 1: the orderer and Kafka layouts checked sound; three blockers, all fixed with tests: Kafka produce was retried after a timeout (could duplicate; now only on not-leader); a broker output with a gap or another writer's lines was accepted on restart (now each checkpoint keeps out_mark, the output's own position, and a restart reads back every line after it, requiring pos to follow on and each line to match; a finished run reads back what it wrote; Redis writes each batch in MULTI/EXEC); NATS 409/408 could end a bounded read early (now ends only when CONSUMER.INFO says nothing is pending; 409 recreates or retries). Should-fix, all fixed: Kafka partition turn in the position (`offsets@turn`), NATS token redacted, Kafka read_committed with aborted transactions left out, a deleted JetStream consumer made again, duplicate JetStream acks stop the run, docs no longer overclaim (follow mode with late events and a kill can stop a restart). Nits fixed: reply size and nesting caps, user without password refused for Redis and Kafka, leaderless partition retried, outputs written on their own context. Round 2: one blocker, a regression, fixed: an output emptied since the checkpoint (DEL, purge, Redis restart without persistence) was accepted on resume; now, with nothing past the mark, the output's last line must be the checkpoint's pos (test: TestBrokerOutputCheckedOnRestart/emptied). Round 2 nits fixed: a cap on empty NATS pulls, recreate decided by CONSUMER.INFO not the 409 text, read-back cost in REFERENCE.md.
10. [ ] 0.2.0 released by the workflow, downloads checked.
11. [ ] inorderly.com post with its own SVG, pages; softdevstudio.com note; softdev-plan log row.

Local note: this cloud session has Go 1.24 only, so local tests run on a
scratch go.mod; CI on the go.mod version is the real check. No local NATS or
Kafka server can be fetched here, so those run in CI.
