#!/usr/bin/env python3
"""Paired, fixed-work B+ tree experiments. Keep raw runs and source provenance."""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import platform
import statistics
import subprocess
import time


def capture(command, cwd):
    result = subprocess.run(command, cwd=cwd, text=True, capture_output=True, check=False)
    return {"command": command, "exit_code": result.returncode,
            "stdout": result.stdout.strip(), "stderr": result.stderr.strip()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument("--suite", choices=("all", "cache", "concurrency"), default="all")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--entries", type=int, default=1_000_000)
    parser.add_argument("--cache-queries", type=int, default=3_000)
    parser.add_argument("--inserts", type=int, default=240_000)
    parser.add_argument("--queries", type=int, default=120_000)
    args = parser.parse_args()
    if min(args.repeats, args.entries, args.cache_queries, args.inserts, args.queries) < 1:
        parser.error("all counts must be positive")
    repo = Path(__file__).resolve().parents[2]
    binary = args.binary.resolve()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    work = args.work_dir.resolve()
    work.mkdir(parents=True, exist_ok=True)
    sources = sorted((repo / "src/matching_engine/trade_history").rglob("*"))
    sources += [Path(__file__).resolve(), repo / "tests/bpt/cache_concurrency_benchmark.cpp",
                repo / "tests/CMakeLists.txt", repo / "src/CMakeLists.txt"]
    hashes = {str(p.relative_to(repo)): hashlib.sha256(p.read_bytes()).hexdigest()
              for p in sources if p.is_file()}
    provenance = {
        "started_at": datetime.datetime.now().astimezone().isoformat(),
        "platform": platform.platform(), "machine": platform.machine(),
        "logical_cpus": os.cpu_count(), "arguments": {k: str(v) if isinstance(v, Path) else v
                                                      for k, v in vars(args).items()},
        "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
        "source_sha256": hashes,
        "git_revision": capture(["git", "rev-parse", "HEAD"], repo),
        "git_status": capture(["git", "status", "--short"], repo),
        "hardware": capture(["sysctl", "-n", "machdep.cpu.brand_string", "hw.memsize"], repo),
        "method": {
            "page_bytes": 8192, "frame_capacity_both_cache_modes": 1024,
            "no_cache": "write dirty pages and evict all unpinned pages after every operation; no retained pages",
            "os_cache": "enabled; not purged; file calls are not physical SSD I/O",
            "serial": "same tree and page latches plus one exclusive mutex per operation",
            "concurrent": "same tree and page latches, without the external mutex",
            "sync": "same final flush+fsync; no per-operation fsync",
            "query": "first <=256 positions for user; no trade body reads; 20% hot user requests",
            "concurrent_boundary": "seed prefix only, identical query result set despite new inserts",
            "latency": "every 64th insert per writer, every query; serial mutex wait and no-cache eviction included",
            "throughput": "fixed work / wall time, includes scheduling and per-query oracle checks; excludes final sync",
            "verification": "every timed query, full paged scan after work and after clean reopen; outside timed phases",
            "cache_warmup": "none beyond timed insertion and final flush",
            "concurrency_warmup": "prefill + flush + 3000 queries, excluded from timing",
            "aggregation": "median of each metric across runs; no pooled percentiles; order reversed on alternate repetitions",
        },
    }
    build = binary.parent.parent
    for name in ("CMakeCache.txt", "bin/demo-build.json"):
        path = build / name
        if path.exists():
            (output / path.name).write_bytes(path.read_bytes())
    (output / "provenance.json").write_text(json.dumps(provenance, indent=2) + "\n")
    runs = []
    for repeat in range(1, args.repeats + 1):
        configs = []
        if args.suite in ("all", "cache"):
            configs.append([(mode, mode, [args.entries, args.cache_queries])
                            for mode in ("cache-none", "cache-8m")])
        if args.suite in ("all", "concurrency"):
            for writers, readers in ((1, 2), (6, 4)):
                configs.append([(f"{mode}-{writers}w{readers}r", mode,
                                 [args.entries, args.inserts, args.queries, writers, readers])
                                for mode in ("serial", "concurrent")])
        for pair in configs:
            for name, mode, counts in pair[::1 if repeat % 2 else -1]:
                label = f"{name}-{repeat}"
                command = [str(binary), str(work / label), mode, *map(str, counts)]
                print(f"START {label}", flush=True)
                start = time.monotonic()
                process = subprocess.run(command, text=True, capture_output=True, check=False)
                elapsed = time.monotonic() - start
                (output / f"{label}.stdout.log").write_text(process.stdout)
                (output / f"{label}.stderr.log").write_text(process.stderr)
                run = {"name": name, "repeat": repeat, "command": command,
                       "wall_seconds_including_validation": elapsed, "exit_code": process.returncode}
                if process.returncode:
                    (output / "failed-run.json").write_text(json.dumps(run, indent=2) + "\n")
                    raise RuntimeError(f"{label} failed; inspect {output}")
                run["metrics"] = json.loads(process.stdout)
                if run["metrics"].get("correct") is not True:
                    raise RuntimeError(f"{label} did not pass correctness verification")
                runs.append(run)
                (output / "runs.json").write_text(json.dumps(runs, indent=2) + "\n")
                print(f"PASS {label}: {elapsed:.2f}s including validation", flush=True)
    summary = {}
    for name in sorted({run["name"] for run in runs}):
        samples = [run["metrics"] for run in runs if run["name"] == name]
        summary[name] = {"runs": len(samples)}
        for key in samples[0]:
            if key in ("mode", "correct"):
                continue
            values = [sample[key] for sample in samples]
            summary[name][key] = {"median": statistics.median(values),
                                  "min": min(values), "max": max(values)}
    (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(f"Results: {output / 'summary.json'}", flush=True)


if __name__ == "__main__":
    main()
