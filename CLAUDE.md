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
ordering explained, how it was tested, the name. Images are drawn into
static/images. Every number comes from `test/results/`; when a recording
changes a number, update the site too.

## Next

Nothing is decided. Agree it with the owner before it goes here.
