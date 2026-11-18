#!/usr/bin/env python3
"""Repeat the native spread benchmark, optionally alternating two builds."""

import argparse
import csv
import json
import math
from pathlib import Path
import subprocess
import time


def aggregate(runs):
    metrics = {}
    for directory in runs:
        report = json.loads((directory / "suite.json").read_text())
        if not report["passed"]:
            raise RuntimeError(f"Benchmark failed: {directory}")
        for metric in report["metrics"]:
            total = metrics.setdefault(metric["name"], {
                "operations": 0, "seconds": 0.0, "values": [],
                "boundary": metric["boundary"], "runs": [],
            })
            total["operations"] += metric["operations"]
            total["seconds"] += metric["seconds"]
            total["runs"].append(metric)
        with (directory / "samples.csv").open(newline="") as stream:
            for row in csv.DictReader(stream):
                metrics[row["metric"]]["values"].append(float(row["latency_us"]))
    for metric in metrics.values():
        values = sorted(metric.pop("values"))
        metric["samples"] = len(values)
        metric["mean_us"] = sum(values) / len(values)
        for label, percentile in (("p50_us", 0.50), ("p95_us", 0.95), ("p99_us", 0.99)):
            metric[label] = values[math.ceil(percentile * len(values)) - 1]
        metric["max_us"] = values[-1]
        metric["operations_per_second"] = metric["operations"] / metric["seconds"]
    return metrics


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-dir", type=Path, required=True)
    parser.add_argument("--reference-dir", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--repeats", type=int, default=5)
    args = parser.parse_args()
    if args.repeats < 1:
        parser.error("--repeats must be positive")
    builds = {"current": args.build_dir.resolve()}
    if args.reference_dir:
        builds = {"reference": args.reference_dir.resolve(), **builds}
    for directory in builds.values():
        for name in ("spread_process_tests", "market_matching_engine", "market_engine_router",
                     "market_group_coordinator", "market_gateway"):
            if not (directory / name).is_file():
                parser.error(f"Missing binary: {directory / name}")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    runs = {label: [] for label in builds}
    durations = {label: [] for label in builds}
    started = time.monotonic()
    for repeat in range(args.repeats):
        order = list(builds)
        if repeat % 2:
            order.reverse()
        for label in order:
            directory = output / label / f"run-{repeat + 1:02d}"
            directory.mkdir(parents=True, exist_ok=True)
            command = [str(builds[label] / "spread_process_tests"), str(builds[label]),
                       str(directory), "--benchmark"]
            print(f"RUN {label} {repeat + 1}/{args.repeats}", flush=True)
            begin = time.monotonic()
            with (directory / "process.log").open("w") as log:
                result = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, timeout=180)
            if result.returncode:
                raise RuntimeError(f"Benchmark exited {result.returncode}: {directory / 'process.log'}")
            runs[label].append(directory)
            durations[label].append(time.monotonic() - begin)
    summary = {
        "trace": False, "warmup_per_latency_scenario": 200,
        "samples_per_latency_scenario_per_run": 2000, "repeats": args.repeats,
        "wall_seconds": time.monotonic() - started,
        "runs_seconds": durations,
        "builds": {label: str(path) for label, path in builds.items()},
        "results": {label: aggregate(paths) for label, paths in runs.items()},
    }
    (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    for name, metric in summary["results"]["current"].items():
        print(f"CURRENT {name}: mean_us={metric['mean_us']:.3f} "
              f"p50_us={metric['p50_us']:.3f} p99_us={metric['p99_us']:.3f} "
              f"ops/s={metric['operations_per_second']:.2f}", flush=True)
    if "reference" in builds:
        comparison = {}
        for name, current in summary["results"]["current"].items():
            reference = summary["results"]["reference"][name]
            comparison[name] = {
                f"{field}_reduction_pct": 100 * (1 - current[field] / reference[field])
                for field in ("mean_us", "p50_us", "p99_us")
            }
            comparison[name]["throughput_increase_pct"] = 100 * (
                current["operations_per_second"] / reference["operations_per_second"] - 1)
        (output / "comparison.json").write_text(json.dumps(comparison, indent=2) + "\n")
    print(f"RESULT PASS: {output / 'summary.json'}", flush=True)


if __name__ == "__main__":
    main()
