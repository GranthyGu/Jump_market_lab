#!/usr/bin/env python3
"""Run every registered regression plus standalone benchmarks, CMU checks and the fault audit."""
import argparse
from dataclasses import dataclass
import datetime
import json
import os
from pathlib import Path
import platform
import re
import shlex
import signal
import subprocess
import sys
import tempfile
import threading
import time

ROOT = Path(__file__).resolve().parent.parent


@dataclass
class Task:
    name: str
    command: list
    timeout: float
    category: str
    missing: str = ""


def build_root(path):
    path = path.resolve()
    if path.name == "bin" and (path.parent / "CMakeCache.txt").is_file():
        path = path.parent
    return path


def inventory(build):
    if not (build / "CTestTestfile.cmake").is_file():
        raise ValueError(f"No CTest configuration in {build}. Run ./tests/build.sh '{build}' first.")
    proc = subprocess.run(["ctest", "--test-dir", str(build), "--show-only=json-v1"],
                          text=True, capture_output=True, check=True)
    tests = json.loads(proc.stdout)["tests"]
    if not tests:
        raise ValueError("CTest registered zero tests; refusing an empty successful run.")
    return tests


def cache_value(build, key):
    for line in (build / "CMakeCache.txt").read_text().splitlines():
        if line.startswith(key + ":"):
            return line.partition("=")[2]
    return ""


def plan(args, build, directory, tests):
    tasks = []
    for test in tests:
        name = test["name"]
        # CTest names in this repository are filenames as well as display labels.
        if not re.fullmatch(r"[A-Za-z0-9_.-]+", name):
            raise ValueError(f"Unsupported test name for report directory: {name}")
        props = {p["name"]: p["value"] for p in test.get("properties", [])}
        limit = args.timeout or float(props.get("TIMEOUT", 300)) + 30
        command = ["ctest", "--test-dir", str(build), "--output-on-failure", "--no-tests=error",
                   "-R", "^" + re.escape(name) + "$", "--output-junit", str(directory / name / "ctest.xml")]
        tasks.append(Task(name, command, limit, "regression",
                          "CTest cannot locate this test executable; run tests/build.sh again."
                          if not test.get("command") else ""))
    if args.regression_only:
        return tasks
    binary = build / "bin"
    py = sys.executable
    def extra(name, command, timeout, executables=()):
        missing = [str(binary / exe) for exe in executables if not os.access(binary / exe, os.X_OK)]
        tasks.append(Task(name, list(map(str, command)), args.timeout or timeout, "standalone",
                          "Missing executables: " + ", ".join(missing) if missing else ""))
    for policy in ("spsc_park", "spsc_yield"):
        exe = "persistence_queue_recovery_" + policy
        extra(exe, [binary / exe, directory / exe / "data"], 180, [exe])
    queue = "persistence_queue_comparison"
    cmd = [py, ROOT / "tests/recovery/bench/run.py", "--build-dir", build,
           "--output", directory / queue / "artifacts"]
    if args.quick:
        cmd += ["--repeats", 1, "--queue-items", 10000, "--commands", 10000, "--active-orders", 1000]
    extra(queue, cmd, 7200, ["persistence_queue_benchmark", *[
        "persistence_engine_benchmark_" + p for p in ("mutex", "spsc_park", "spsc_yield")]])
    name = "snapshot_contention_benchmark"
    extra(name, [binary / name, directory / name / "artifacts",
                 1 if args.quick else 3, 1000 if args.quick else 3000], 600, [name])
    name = "trade_history_benchmark"
    extra(name, [binary / name, directory / name / "data", 10000 if args.quick else 1000000], 600, [name])
    name = "bpt_cache_concurrency_comparison"
    cmd = [py, ROOT / "tests/bpt/run_cache_concurrency.py", "--binary", binary / "bpt_cache_concurrency_benchmark",
           "--output", directory / name / "artifacts", "--work-dir", directory / name / "data"]
    if args.quick:
        cmd += ["--repeats", 1, "--entries", 12000, "--cache-queries", 300,
                "--inserts", 6000, "--queries", 3000]
    extra(name, cmd, 1800, ["bpt_cache_concurrency_benchmark"])
    name = "bpt_sqlite_comparison"
    cmd = [py, ROOT / "tests/bpt/run_sqlite_comparison.py", "--binary", binary / "bpt_sqlite_benchmark",
           "--output", directory / name / "artifacts", "--work-dir", directory / name / "data"]
    if args.quick: cmd += ["--repeats", 1, "--entries", 12000, "--queries", 300]
    extra(name, cmd, 1800, ["bpt_sqlite_benchmark"])
    if cache_value(build, "MARKETLAB_BUILD_SQLITE_BENCHMARK") != "ON":
        tasks[-1].missing = "SQLite tests are disabled in this build. Run tests/build.sh to enable and build them."
    name = "cmu_public"
    extra(name, [py, ROOT / "tests/bpt/run_cmu_public.py", "--source", args.cmu_source.resolve(),
                 "--build", build / "cmu-public", "--output", directory / name / "artifacts",
                 "--cxx", cache_value(build, "CMAKE_CXX_COMPILER") or "c++", "--jobs", args.jobs,
                 "--duration", 1000 if args.quick else 30000], 1800)
    name = "persistence_recovery_audit"
    extra(name, [binary / name, directory / name / "data"], 180, [name])
    return tasks


def stop_group(process):
    def send(sig):
        try:
            os.killpg(process.pid, sig)
        except ProcessLookupError:
            return
        except PermissionError:
            # On macOS a sandbox can report EPERM after the child/group has
            # already exited. Do not replace an in-flight KeyboardInterrupt
            # with that cleanup error; a live child is still an actual error.
            if process.poll() is None:
                raise
    send(signal.SIGTERM)
    time.sleep(0.2)
    send(signal.SIGKILL)


def execute(task, directory, env, verbose=False):
    target = directory / task.name
    target.mkdir()
    log = target / "console.log"
    start = time.monotonic()
    result = {"name": task.name, "category": task.category, "command": task.command,
              "timeout_seconds": task.timeout, "log": str(log.relative_to(directory)),
              "status": "FAIL", "exit_code": None, "timed_out": False, "error": task.missing}
    if task.missing:
        log.write_text(task.missing + "\n")
    else:
        child_env = dict(env, MARKETLAB_TEST_OUTPUT_ROOT=str(target / "artifacts"))
        expired = threading.Event()
        with log.open("w") as output:
            process = None
            try:
                process = subprocess.Popen(task.command, cwd=ROOT, env=child_env,
                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, errors="replace",
                    bufsize=1, start_new_session=True)
                def timeout():
                    expired.set()
                    stop_group(process)
                watchdog = threading.Timer(task.timeout, timeout)
                watchdog.start()
                try:
                    for line in process.stdout:
                        output.write(line)
                        output.flush()
                        if verbose: print(line, end="", flush=True)
                    result["exit_code"] = process.wait()
                finally:
                    watchdog.cancel()
                    watchdog.join()
                    stop_group(process)
                    process.wait()
                    process.stdout.close()
            except OSError as error:
                result["error"] = str(error)
                output.write(str(error) + "\n")
            result["timed_out"] = expired.is_set()
        if result["exit_code"] == 0 and not result["timed_out"]:
            result["status"] = "PASS"
    result["wall_seconds"] = time.monotonic() - start
    return result


def save_report(directory, report, started):
    report["wall_seconds"] = time.monotonic() - started
    report["passed"] = (len(report["results"]) == len(report["selected_tests"])
                         and all(r["status"] == "PASS" for r in report["results"])
                         and not report.get("interrupted", False))
    (directory / "summary.json").write_text(json.dumps(report, indent=2) + "\n")
    rows = ["# MarketLab test run", "", f"Scope: {report['scope']}; quick benchmarks: {report['quick']}",
            f"Result: {'PASS' if report['passed'] else 'INCOMPLETE / FAIL'}", "",
            "| Test | Result | Seconds | Log |", "|---|---|---:|---|"]
    by_name = {r["name"]: r for r in report["results"]}
    for name in report["selected_tests"]:
        r = by_name.get(name)
        rows.append(f"| {name} | {r['status'] if r else 'NOT RUN'} | "
                    + (f"{r['wall_seconds']:.2f} | [log]({r['log']}) |" if r else "— | — |"))
    if report["excluded"]:
        rows += ["", "Excluded explicitly by --regression-only: " + ", ".join(report["excluded"])]
    rows += ["", "Fault-audit failures are failures, not expected passes. Quick mode reduces benchmark sizes only.", ""]
    (directory / "report.md").write_text("\n".join(rows))


def run_tasks(tasks, directory, report, env, verbose=False):
    started = time.monotonic()
    save_report(directory, report, started)
    try:
        for i, task in enumerate(tasks, 1):
            print(f"[{i}/{len(tasks)}] START {task.name}", flush=True)
            result = execute(task, directory, env, verbose)
            report["results"].append(result)
            save_report(directory, report, started)
            print(f"  {result['status']} {result['wall_seconds']:.2f}s — {result['log']}", flush=True)
    except KeyboardInterrupt:
        report["interrupted"] = True
    finally:
        save_report(directory, report, started)
    return 0 if report["passed"] else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-dir", type=Path, default=ROOT / "build/demo-tests")
    parser.add_argument("--output", type=Path, default=ROOT / "build/test-results")
    parser.add_argument("--regression-only", action="store_true", help="Only registered CTest regressions; explicitly omit standalone experiments/audit/CMU")
    parser.add_argument("--quick", action="store_true", help="Run every selected task, with smaller standalone benchmark workloads; not a full performance measurement")
    parser.add_argument("--list", action="store_true", help="List selected tasks and commands without running them")
    parser.add_argument("--verbose", action="store_true", help="Stream task output as well as saving it")
    parser.add_argument("--scale", type=int, choices=(1, 2, 3), default=1, help="Workload scale for the four component suites")
    parser.add_argument("--timeout", type=float, help="Override each task's outer deadline (seconds); default uses per-task limits")
    parser.add_argument("--jobs", type=int, default=4, help="CMU build parallelism; test tasks always run sequentially")
    parser.add_argument("--cmu-source", type=Path, default=ROOT / "build/vendor/bustub")
    args = parser.parse_args()
    if (args.timeout is not None and args.timeout <= 0) or args.jobs < 1:
        parser.error("timeout and jobs must be positive")
    build = build_root(args.build_dir)
    try:
        tests = inventory(build)
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        parser.error(str(error))
    # Listing does not create result directories or invoke any test/download/build.
    if args.list:
        tasks = plan(args, build, args.output.resolve() / "<run>", tests)
        for task in tasks:
            print(f"{task.name} [{task.category}, timeout={task.timeout:g}s]")
            print("  " + shlex.join(task.command))
            if task.missing: print("  NOT READY: " + task.missing)
        print(f"Selected {len(tasks)} tasks. Scope: {'regression' if args.regression_only else 'all'}.")
        return 0
    args.output.mkdir(parents=True, exist_ok=True)
    directory = Path(tempfile.mkdtemp(prefix=datetime.datetime.now().strftime("%Y%m%d-%H%M%S-"),
                                     dir=args.output.resolve()))
    tasks = plan(args, build, directory, tests)
    selected = [task.name for task in tasks]
    if args.regression_only:
        args.regression_only = False
        excluded = [t.name for t in plan(args, build, directory, tests) if t.name not in selected]
        args.regression_only = True
    else:
        excluded = []
    report = {"started_at": datetime.datetime.now().astimezone().isoformat(), "platform": platform.platform(),
              "scope": "regression" if args.regression_only else "all", "quick": args.quick,
              "build_directory": str(build), "selected_tests": selected, "excluded": excluded, "results": []}
    print(f"RESULT_DIR={directory}", flush=True)
    env = dict(os.environ, MARKETLAB_TEST_SCALE=str(args.scale))
    def interrupted(*_):
        raise KeyboardInterrupt()
    previous_signals = {sig: signal.signal(sig, interrupted) for sig in (signal.SIGINT, signal.SIGTERM)}
    try:
        code = run_tasks(tasks, directory, report, env, args.verbose)
    finally:
        for sig, handler in previous_signals.items():
            signal.signal(sig, handler)
    print(f"OVERALL {'PASS' if code == 0 else 'FAIL'} ({report['scope']})", flush=True)
    print(f"Report: {directory / 'report.md'}\nJSON: {directory / 'summary.json'}", flush=True)
    return code


if __name__ == "__main__":
    sys.exit(main())
