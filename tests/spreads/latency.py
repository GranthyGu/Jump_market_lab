#!/usr/bin/env python3
"""Compare ordinary and spread order latency using opt-in native timing probes."""

import argparse
import csv
from datetime import datetime
import json
import math
import os
from pathlib import Path
import platform
import signal
import subprocess
import time


SCENARIOS = ("ordinary-hot", "ordinary-balanced", "group-direct", "group-implied",
             "group-butterfly-in", "group-butterfly-out", "group-butterfly-b", "group-shared")
METRICS = ("memory_us", "client_us", "send_to_start_us", "start_to_memory_us", "after_memory_us")


def check(condition, message):
    if not condition:
        raise RuntimeError(message)


def rows(path):
    with path.open(newline="") as stream:
        return [{key: int(value) for key, value in row.items()} for row in csv.DictReader(stream)]


def statistics(values):
    values = sorted(values)
    check(bool(values), "No latency samples")
    return {
        "samples": len(values), "mean_us": math.fsum(values) / len(values),
        "p50_us": values[math.ceil(0.50 * len(values)) - 1],
        "p95_us": values[math.ceil(0.95 * len(values)) - 1],
        "p99_us": values[math.ceil(0.99 * len(values)) - 1], "max_us": values[-1],
    }


def analyze(directory):
    run = json.loads((directory / "run.json").read_text())
    check(run.get("clock") == "CLOCK_MONOTONIC_RAW" and run.get("clock_resolution_ns", 0) > 0,
          "Use a raw-clock benchmark build with recorded clock resolution")
    clients = rows(directory / "client.csv")
    requests = {row["request"] for row in clients}
    check(len(clients) == len(requests) == run["count"], "Missing/duplicate client samples")
    check(run["scenario"] in SCENARIOS, "Unknown latency scenario")
    ordinary = run["scenario"].startswith("ordinary-")
    starts, memories, group_memories = {}, {}, {}
    for path in sorted((directory / "probes").glob("probe-*.csv")):
        for row in rows(path):
            kind, request, sequence = row["kind"], row["request"], row["sequence"]
            check(kind in range(4), "Unknown probe kind")
            if kind == 3:
                check(sequence not in group_memories, "Duplicate group memory probe")
                group_memories[sequence] = row
            elif request in requests:
                check(kind in ((0, 1) if ordinary else (2,)), "Wrong execution path")
                target = memories if kind == 1 else starts
                check(request not in target, "Duplicate request probe")
                target[request] = row
    check(set(starts) == requests, "Missing execution start probe; use a probe-enabled build")
    if ordinary:
        check(set(memories) == requests, "Missing ordinary memory probe")
    samples = []
    for index, client in enumerate(clients):
        start = starts[client["request"]]
        memory = memories.get(client["request"]) if ordinary else group_memories.get(start["sequence"])
        check(memory is not None, "Missing group memory probe")
        check(start["shard"] == client["shard"] and start["sequence"] == memory["sequence"],
              "Probe request/sequence/shard mismatch")
        expected_shard = {"group-butterfly-in": 5, "group-butterfly-out": 6,
                          "group-butterfly-b": 7, "group-shared": 6}.get(run["scenario"])
        if expected_shard:
            check(client["shard"] == expected_shard, "Wrong butterfly aggressor owner")
        if ordinary:
            check(memory["shard"] == client["shard"], "Ordinary completion shard mismatch")
        else:
            scenario = run["scenario"]
            if scenario == "group-direct":
                mask = 1 << (client["shard"] - 5)
            elif scenario == "group-implied":
                mask = 7 if index % 2 == 0 else 11
            elif scenario == "group-shared":
                mask = 7 if index + run["warmup"] < run["calendar_orders"] else 15
            else:
                check(scenario.startswith("group-butterfly-"), "Unknown latency scenario")
                mask = 15
            check(memory["participants"] == mask, "Unexpected direct/implied participant mask")
        sent, begun, done, received = (client["send_ns"], start["timestamp_ns"],
                                       memory["timestamp_ns"], client["done_ns"])
        check(sent <= begun <= done <= received, "Cross-process timestamps are out of order")
        samples.append({
            **client, "sequence": start["sequence"], "start_ns": begun, "memory_ns": done,
            "participants": memory["participants"], "memory_us": (done - sent) / 1000,
            "client_us": (received - sent) / 1000, "send_to_start_us": (begun - sent) / 1000,
            "start_to_memory_us": (done - begun) / 1000, "after_memory_us": (received - done) / 1000,
        })
    run["validated_orders"] = len(samples)
    run["orders_per_second"] = len(samples) / run["seconds"]
    run["latency"] = {name: statistics([sample[name] for sample in samples]) for name in METRICS}
    with (directory / "samples.csv").open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(samples[0]))
        writer.writeheader()
        writer.writerows(samples)
    (directory / "validated.json").write_text(json.dumps(run, indent=2) + "\n")
    return run, samples


def execute(command, log):
    with log.open("w") as stream, subprocess.Popen(
            command, stdout=stream, stderr=subprocess.STDOUT, start_new_session=True) as process:
        try:
            result = process.wait(timeout=180)
        except BaseException:
            # The deployment's children share this isolated process group.
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=8)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
            raise
    check(result == 0, f"Native benchmark exited {result}; see {log}")


def prepare_build(base, log):
    """Build probes separately; never instrument the caller's normal services."""
    root = Path(__file__).resolve().parents[2]
    base = base.resolve()
    if base.name == "bin":
        base = base.parent
    cache = base / "CMakeCache.txt"
    check(cache.is_file(), f"Missing normal CMake build: {base}")
    compiler = next((line.partition("=")[2] for line in cache.read_text().splitlines()
                     if line.startswith("CMAKE_CXX_COMPILER:")), "")
    build = base / "latency-probes"
    configure = ["cmake", "-S", str(root / "src"), "-B", str(build),
                 "-DCMAKE_BUILD_TYPE=Release", "-DMARKETLAB_BUILD_TESTS=ON",
                 "-DMARKETLAB_LATENCY_PROBES=ON", "-DMARKETLAB_BUILD_SQLITE_BENCHMARK=OFF"]
    if compiler:
        configure.append("-DCMAKE_CXX_COMPILER=" + compiler)
    with log.open("w") as stream:
        for command in (configure, ["cmake", "--build", str(build), "--parallel", "4",
                                    "--target", "spread_latency_tests"]):
            subprocess.run(command, stdout=stream, stderr=subprocess.STDOUT, timeout=600, check=True)
    return build / "bin"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    builds = parser.add_mutually_exclusive_group(required=True)
    builds.add_argument("--build-dir", type=Path, help="Existing probe-enabled bin directory")
    builds.add_argument("--prepare-build", type=Path, help="Normal build; prepare an isolated Release probe build automatically")
    parser.add_argument("--output", type=Path, required=True, help="New output directory")
    parser.add_argument("--count", type=int, default=5000)
    parser.add_argument("--warmup", type=int, default=200)
    parser.add_argument("--windows", type=int, nargs="+", default=[1, 32, 128, 5000])
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--scenarios", nargs="+", choices=SCENARIOS, default=list(SCENARIOS))
    args = parser.parse_args()
    if not (1 <= args.count <= 30000 and 1 <= args.warmup <= 30000 and args.repeats >= 1):
        parser.error("Invalid counts/repeats")
    if any(window < 1 or window > args.count for window in args.windows):
        parser.error("Windows must be in [1, count]")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    preparation = time.monotonic()
    binary = (prepare_build(args.prepare_build, output / "build.log") if args.prepare_build
              else args.build_dir.resolve())
    build_seconds = time.monotonic() - preparation
    for name in ("spread_latency_tests", "market_matching_engine", "market_engine_router",
                 "market_group_coordinator", "market_gateway"):
        if not (binary / name).is_file():
            parser.error(f"Missing binary: {binary / name}")
    started = time.monotonic()
    summary = {
        "created": datetime.now().astimezone().isoformat(), "platform": platform.platform(),
        "build_dir": str(binary), "build_seconds": build_seconds, "count_per_run": args.count, "warmup": args.warmup,
        "windows": args.windows, "repeats": args.repeats, "trace": False,
        "clock": "CLOCK_MONOTONIC_RAW nanoseconds, same host", "results": {},
    }
    totals = {}
    for repeat in range(args.repeats):
        # Alternate scenario order between repeats; deployments never overlap.
        scenarios = args.scenarios if repeat % 2 == 0 else list(reversed(args.scenarios))
        for window in args.windows:
            for scenario in scenarios:
                key = f"{scenario}/window-{window}"
                directory = output / key / f"run-{repeat + 1:02d}"
                directory.mkdir(parents=True)
                print(f"RUN {key} repeat={repeat + 1}/{args.repeats}", flush=True)
                before = time.monotonic()
                execute([str(binary / "spread_latency_tests"), str(binary), str(directory), scenario,
                         str(args.count), str(window), str(args.warmup)], directory / "process.log")
                run, samples = analyze(directory)
                run["wall_seconds"] = time.monotonic() - before
                total = totals.setdefault(key, {"runs": [], "values": {name: [] for name in METRICS}})
                total["runs"].append(run)
                for name in METRICS:
                    total["values"][name].extend(sample[name] for sample in samples)
                print(f"PASS orders={run['validated_orders']} memory_start_mean_us="
                      f"{run['latency']['start_to_memory_us']['mean_us']:.3f} client_mean_us="
                      f"{run['latency']['client_us']['mean_us']:.3f} "
                      f"orders/s={run['orders_per_second']:.2f}", flush=True)
    for key, total in totals.items():
        runs = total["runs"]
        count = sum(run["validated_orders"] for run in runs)
        summary["results"][key] = {
            "validated_orders": count, "runs": runs,
            "orders_per_second": count / sum(run["seconds"] for run in runs),
            "latency": {name: statistics(values) for name, values in total["values"].items()},
        }
    summary["wall_seconds"] = time.monotonic() - started
    summary["validated_orders"] = sum(result["validated_orders"] for result in summary["results"].values())
    (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(f"PASS total_orders={summary['validated_orders']} wall_seconds={summary['wall_seconds']:.3f} "
          f"report={output / 'summary.json'}", flush=True)


if __name__ == "__main__":
    main()
