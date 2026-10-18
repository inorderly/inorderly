#!/usr/bin/env python3
# Copyright Inorderly.com 2026
# SPDX-License-Identifier: Apache-2.0
"""Checks a built inorderly binary on the machine that runs it.

    python3 scripts/smoke.py BIN_DIR [EXPECTED_VERSION]

BIN_DIR holds inorderly (inorderly.exe on Windows). It must report the
expected version (inorderly.Version from the checkout unless given). Then,
in a private temporary directory, with readings made by
examples/python/producer.py: four shuffled arrival orders with duplicates
must give the same output as the sorted one, and the replay hash must be
the output's SHA-256; late, conflicting and invalid lines must land in the
rejects file and -strict must exit 3; a run with -state must be killed part
way, started again and finish with the same output; and the self-checking
demo must pass. The release workflow runs it on every binary it ships, on
the machine type that binary is for; any failure stops the release.
"""
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parent.parent
WINDOWS = os.name == "nt"
PRODUCER = ROOT / "examples" / "python" / "producer.py"


class Failure(Exception):
    pass


def require(condition, message):
    if not condition:
        raise Failure(message)


def run(args, stdin=None, timeout=300, ok=(0,)):
    result = subprocess.run([str(a) for a in args], input=stdin, capture_output=True, timeout=timeout)
    require(result.returncode in ok, f"{Path(str(args[0])).name} {' '.join(str(a) for a in args[1:])} exited {result.returncode}:\n{result.stderr.decode(errors='replace')[-4000:]}")
    return result


def source_version():
    text = (ROOT / "inorderly.go").read_text(encoding="utf-8")
    found = re.findall(r'^const Version = "(.*)"$', text, re.M)
    require(len(found) == 1, "inorderly.go has no single Version constant")
    return found[0]


def produce(*args):
    return run([sys.executable, PRODUCER, "--events", "30000", "--window", "500", *args]).stdout


def smoke(bin_dir, version):
    exe = Path(bin_dir).resolve() / ("inorderly.exe" if WINDOWS else "inorderly")
    require(exe.is_file(), f"missing {exe}")
    info = json.loads(run([exe, "version"]).stdout)
    require(info["version"] == version, f"inorderly reports version {info['version']}, expected {version}")
    print(f"inorderly: {info['version']} {info['os']}/{info['arch']} revision {info['revision'][:12]}", flush=True)

    sorted_out = run([exe, "-window", "500"], stdin=produce("--sorted"))
    summary = json.loads(sorted_out.stderr)
    require(summary["ordered"] == 30000, f"ordered {summary['ordered']} of 30000")
    require(summary["hash"] == "sha256:" + hashlib.sha256(sorted_out.stdout).hexdigest(), "the replay hash isn't the output's SHA-256")
    for seed in ("1", "2", "3", "4"):
        r = run([exe, "-window", "500"], stdin=produce("--seed", seed))
        s = json.loads(r.stderr)
        require(r.stdout == sorted_out.stdout, f"arrival order {seed} gave different output")
        require(s["duplicates"] > 0 and s["late"] == 0 and s["conflicts"] == 0, f"arrival order {seed}: {s}")
    print("four shuffled orders with duplicates, one output: passed", flush=True)

    with tempfile.TemporaryDirectory(prefix="inorderly-smoke-") as temporary:
        d = Path(temporary)
        bad = b'{"id":"a","producer":"p","time":100}\nnot json\n{"id":"b","producer":"p","time":1}\n{"id":"a","producer":"p","time":100,"data":1}\n'
        rejects = d / "rejects.ndjson"
        r = run([exe, "-window", "10", "-rejects", rejects, "-strict"], stdin=bad, ok=(3,))
        reasons = [json.loads(line)["reason"] for line in rejects.read_text().splitlines()]
        require(reasons == ["invalid", "late", "conflict"], f"rejects {reasons}")
        print("rejects and -strict: passed", flush=True)

        events = d / "events.ndjson"
        events.write_bytes(produce("--seed", "5", "--events", "200000"))
        clean, killed = d / "clean.ndjson", d / "killed.ndjson"
        run([exe, "-in", events, "-out", clean, "-window", "500", "-state", d / "clean-state"])
        flags = [exe, "-in", events, "-out", killed, "-window", "500", "-state", d / "state", "-checkpoint-every", "2000"]
        kills = 0
        for delay in (0.3, 0.6, 0.9):
            p = subprocess.Popen([str(a) for a in flags], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            time.sleep(delay)
            if p.poll() is None:
                p.kill()  # SIGKILL, or TerminateProcess on Windows
                kills += 1
            p.wait()
        run(flags)
        require(clean.read_bytes() == killed.read_bytes(), f"after {kills} kills the output differs")
        print(f"killed {kills} times and resumed, same output: passed", flush=True)

    report = json.loads(run([exe, "demo", "-events", "20000", "-orders", "4", "-ticks", "20000"]).stdout)
    require(report["passed"], f"the demo failed: {report['failures']}")
    print("self-checking demo: passed", flush=True)


def main():
    if len(sys.argv) not in (2, 3):
        sys.exit("usage: python3 scripts/smoke.py BIN_DIR [EXPECTED_VERSION]")
    try:
        smoke(sys.argv[1], sys.argv[2] if len(sys.argv) == 3 else source_version())
    except (Failure, subprocess.TimeoutExpired, KeyError, ValueError, OSError) as e:
        print(f"smoke check failed: {e}", file=sys.stderr)
        sys.exit(1)
    print("smoke check passed")


if __name__ == "__main__":
    main()
