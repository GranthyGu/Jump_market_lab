#!/usr/bin/env python3
"""Run queue and durable-engine comparisons sequentially, saving raw samples."""

import argparse
import csv
import hashlib
import json
from pathlib import Path
import platform
import statistics
import subprocess
import time


POLICIES = ["mutex", "spsc_park", "spsc_yield"]
ENGINE_FIELDS = [
    "queue", "shards", "commands", "active_orders", "seconds",
    "commands_per_second", "cpu_cores", "accepted_p50_us", "accepted_p99_us",
    "order_syncs", "trade_syncs", "context_switches",
]


def run_checked(command, log, timeout=180):
    result = subprocess.run(
        [str(part) for part in command],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=timeout,
        check=False,
    )
    log.write_text(result.stdout)
    if result.returncode != 0:
        raise RuntimeError(f"Command failed ({result.returncode}); see {log}")
    return result.stdout


def summarize(path, dimensions, rate):
    with path.open(newline="") as source:
        rows = list(csv.DictReader(source))
    groups = {}
    for row in rows:
        key = tuple(row[field] for field in dimensions)
        groups.setdefault(key, []).append(row)
    result = []
    for key, samples in groups.items():
        item = dict(zip(dimensions, key))
        values = [float(row[rate]) for row in samples]
        item.update(
            samples=len(samples),
            rate_median=statistics.median(values),
            rate_min=min(values),
            rate_max=max(values),
            cpu_cores_median=statistics.median(float(row["cpu_cores"]) for row in samples),
        )
        if "accepted_p99_us" in samples[0]:
            item["accepted_p99_us_median"] = statistics.median(
                float(row["accepted_p99_us"]) for row in samples
            )
            item["accepted_p50_us_median"] = statistics.median(
                float(row["accepted_p50_us"]) for row in samples
            )
        result.append(item)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-dir", type=Path, default=Path("build/queue-study"))
    parser.add_argument("--output", type=Path, default=Path("build/queue-study/results"))
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--queue-items", type=int, default=2000000)
    parser.add_argument("--commands", type=int, default=1000000)
    parser.add_argument("--active-orders", type=int, default=100000)
    parser.add_argument("--shards", type=int, nargs="+", default=[1, 2, 4, 8])
    parser.add_argument("--skip-queue", action="store_true")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    binary = args.build_dir.resolve() / "bin"
    metadata = {
        "date": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "platform": platform.platform(),
        "repeats": args.repeats,
        "commands": args.commands,
        "active_orders": args.active_orders,
        "shards": args.shards,
        "queue_items": args.queue_items,
        "capacity": 128,
        "sync_group": 64,
        "total_symbols": 256,
        "notes": "Sequential runs; rotated policy order; real write/fsync; checkpoint outside timing",
    }
    cache = args.build_dir / "CMakeCache.txt"
    metadata["build_flags"] = [
        line for line in cache.read_text().splitlines()
        if line.startswith(("CMAKE_BUILD_TYPE:", "CMAKE_CXX_COMPILER:",
                            "CMAKE_CXX_FLAGS:", "CMAKE_CXX_FLAGS_RELEASE:"))
    ]
    metadata["source_sha256"] = {
        str(path): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in [
            *Path(__file__).parent.glob("*.hpp"),
            *Path(__file__).parent.glob("*.cpp"),
            Path(__file__),
            Path(__file__).parent / "CMakeLists.txt",
            Path("src/matching_engine/persistence/queue.hpp"),
            Path("src/matching_engine/persistence/durable_engine.cpp"),
        ]
    }
    (args.output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    run_checked([binary / "persistence_queue_benchmark", "--check"],
                args.output / "queue-check.log")
    if not args.skip_queue:
        print("Measuring queues (sequential policies)...", flush=True)
        run_checked([binary / "persistence_queue_benchmark", args.queue_items, args.repeats],
                    args.output / "queue.csv", timeout=600)

    engine_csv = args.output / "engine.csv"
    with engine_csv.open("w", newline="") as output:
        writer = csv.writer(output)
        writer.writerow(["repeat", *ENGINE_FIELDS])
        # One small warmup for each policy, excluded from the reported samples.
        for policy in POLICIES:
            run_checked([binary / f"persistence_engine_benchmark_{policy}",
                         1, 10000, 10000, args.output / "work"],
                        args.output / f"warmup-{policy}.log")
        for repeat in range(args.repeats):
            for shards in args.shards:
                for offset in range(len(POLICIES)):
                    policy = POLICIES[(repeat + offset) % len(POLICIES)]
                    name = f"engine-{repeat}-{shards}-{policy}"
                    text = run_checked(
                        [binary / f"persistence_engine_benchmark_{policy}", shards,
                         args.commands, args.active_orders, args.output / "work"],
                        args.output / f"{name}.log",
                    )
                    lines = [line for line in text.splitlines() if line.startswith("RESULT,")]
                    if len(lines) != 1:
                        raise RuntimeError(f"Missing unique RESULT in {name}")
                    values = lines[0].split(",")[1:]
                    writer.writerow([repeat, *values])
                    output.flush()
                    print(f"{name}: {float(values[5]):,.0f} commands/s, "
                          f"{float(values[6]):.2f} CPU cores", flush=True)

    summary = {"engine": summarize(engine_csv, ["queue", "shards"], "commands_per_second")}
    if (args.output / "queue.csv").exists():
        summary["queue"] = summarize(args.output / "queue.csv", ["queue", "pairs"],
                                     "items_per_second")
    (args.output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(f"Saved raw samples and summary to {args.output}", flush=True)


if __name__ == "__main__":
    main()
