#!/bin/sh
# Copyright Inorderly.com 2026
# SPDX-License-Identifier: Apache-2.0
#
# Runs every test, the long determinism search, the race detector, the
# release plan's cases, the self-checking demo and the smoke check on a
# static release-style binary, and writes the output to test/results/ with
# a note of the machine they ran on. The numbers in the README and on
# inorderly.com come from these files.
#
#   sh scripts/record-tests.sh
#
# Needs Go and Python 3.
set -eu
cd "$(dirname "$0")/.."
out=test/results
mkdir -p "$out"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

# Whether the checkout was clean before anything was recorded.
state="at $(git rev-parse --short HEAD 2>/dev/null || echo 'no commit')$(test -z "$(git status --porcelain 2>/dev/null)" || echo ' plus uncommitted changes')"

header() {
	echo "# $1"
	echo "# recorded $(date -u '+%Y-%m-%d %H:%M UTC') $state, Inorderly $(sed -n 's/^const Version = "\(.*\)"$/\1/p' inorderly.go), $(go version | cut -d' ' -f3-)"
	cpu=$(grep -m1 'model name' /proc/cpuinfo 2>/dev/null | cut -d: -f2- | sed 's/^ //' || true)
	test -n "$cpu" || cpu=$(sysctl -n machdep.cpu.brand_string 2>/dev/null || echo 'unknown CPU')
	echo "# $(uname -sr), $cpu, $(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo '?') CPUs"
	echo "# command: $2"
	echo
}

go vet ./...

cmd="go test -count=1 -v ./..."
{ header "Every test, verbose" "INORDERLY_RECORD=1 $cmd"; INORDERLY_RECORD=1 $cmd 2>&1; } > "$out/tests.txt"

# The determinism property test at length: random tangled event sets, each
# fed in 20 arrival orders with duplicates and snapshots along the way.
cmd="go test -count=1 -run TestSameEventsSameOutput -v ."
{ header "The determinism search: 10,000 random event sets, 20 arrival orders each" "INORDERLY_ROUNDS=10000 $cmd"; INORDERLY_ROUNDS=10000 $cmd 2>&1; } > "$out/determinism.txt"

cmd="go test -race -short -count=1 ./..."
{ header "Every test with the race detector (short determinism search)" "$cmd"; $cmd 2>&1; } > "$out/race.txt"

cmd="python3 -B -m unittest discover -s scripts -p *_test.py -v"
{ header "The release plan's cases" "$cmd"; $cmd 2>&1; } > "$out/release-plan.txt"

# The demo prints progress on stderr and a JSON report on stdout; it fails
# on any failed check. The report carries its own environment details and
# is the raw numbers.
go build -o "$work/inorderly" ./cmd/inorderly
"$work/inorderly" demo > "$out/demo.json" 2>"$work/demo.log" || { cat "$work/demo.log" >&2; exit 1; }

mkdir "$work/bin"
CGO_ENABLED=0 go build -trimpath -ldflags '-s -w' -o "$work/bin/inorderly" ./cmd/inorderly
cmd="python3 scripts/smoke.py BIN"
{ header "The release smoke check on a static Linux binary built as the release workflow builds it" "$cmd"; python3 scripts/smoke.py "$work/bin" 2>&1; } > "$out/smoke.txt"

grep -E '^(ok|FAIL|--- FAIL|RECORD)' "$out/tests.txt" "$out/race.txt" "$out/determinism.txt" || true
tail -n 1 "$out/release-plan.txt" "$out/smoke.txt"
grep -E '"(passed|identical|pushes_per_second)"' "$out/demo.json"
echo "results written to $out/"
