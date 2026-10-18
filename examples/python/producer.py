#!/usr/bin/env python3
# Copyright Inorderly.com 2026
# SPDX-License-Identifier: Apache-2.0
"""A producer in another language: sensor readings as NDJSON for inorderly.

    python3 producer.py [--events N] [--window W] [--seed S] [--sorted] | inorderly -window W

It makes the same readings for any seed, from a fixed list of sensors, then
writes them in an order that depends on the seed: each reading one to three
times, each copy up to W time units late. --sorted writes each reading once
in time order instead. Every order gives inorderly's output byte for byte.
"""
import argparse
import json
import random
import sys


def readings(count):
    rng = random.Random(42)  # the readings themselves never change
    out = []
    for i in range(count):
        sensor = f"sensor-{rng.randrange(8)}"
        out.append({
            "id": f"r{i}",
            "producer": sensor,
            "time": 1_760_000_000_000 + i * 5 + rng.randrange(5),
            "logical": rng.randrange(2),
            "data": {"celsius": round(rng.uniform(-20, 40), 1), "battery": rng.randrange(101)},
        })
        if i > 10 and rng.random() < 0.1:
            # An alert caused by an earlier reading of a nearby sensor.
            out[-1]["parents"] = [f"r{i - 1 - rng.randrange(10)}"]
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", type=int, default=10000)
    ap.add_argument("--window", type=int, default=500)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--sorted", action="store_true")
    a = ap.parse_args()
    events = readings(a.events)
    rng = random.Random(a.seed)
    arrivals = []
    for e in events:
        copies = 1 if a.sorted else rng.randint(1, 3)
        for _ in range(copies):
            late = 0 if a.sorted else rng.randint(0, a.window)
            arrivals.append((e["time"] + late, rng.random(), e))
    arrivals.sort(key=lambda x: (x[0], x[1]))
    out = sys.stdout
    for _, _, e in arrivals:
        # Key order and spacing differ from inorderly's own; it doesn't matter.
        out.write(json.dumps(e, sort_keys=a.seed % 2 == 0) + "\n")


if __name__ == "__main__":
    main()
