#!/usr/bin/env python3
"""Run already compiled suites, preserve logs, enforce process-group deadlines."""
import argparse
import datetime
import json
import os
import platform
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import threading
import time

ROOT = Path(__file__).resolve().parent.parent
PARTS = ("gateway", "publisher", "engine", "integration")


def stop_group(process):
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    time.sleep(0.5)
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("suite", choices=(*PARTS, "all"))
    parser.add_argument("--build-dir", type=Path, default=ROOT / "build/demo-tests/bin",
                        help="Directory containing demo_*_tests and service executables; a CMake build root also works")
    parser.add_argument("--output", type=Path, default=ROOT / "build/demo-results",
                        help="Parent of a fresh result directory; previous results are preserved")
    parser.add_argument("--scale", type=int, choices=(1, 2, 3), default=int(os.environ.get("MARKETLAB_TEST_SCALE", "1")),
                        help="Multiply request counts/query time; products <= 2000 and shards <= 32 remain fixed")
    parser.add_argument("--timeout", type=int, default=150, help="Per-suite deadline in seconds")
    args = parser.parse_args()
    if args.timeout < 1:
        parser.error("--timeout must be positive")
    binaries = args.build_dir.resolve()
    if (binaries / "bin").is_dir():
        binaries /= "bin"
    parts = PARTS if args.suite == "all" else (args.suite,)
    needed = [f"demo_{part}_tests" for part in parts]
    if any(part != "engine" for part in parts):
        needed += ["market_gateway", "market_matching_engine", "market_broadcast"]
    missing = [str(binaries / name) for name in needed if not os.access(binaries / name, os.X_OK)]
    if missing:
        parser.error("Missing compiled executables:\n" + "\n".join(missing) + "\nBuild once with ./tests/build.sh, then rerun this script.")
    args.output = Path(os.environ.get("MARKETLAB_TEST_OUTPUT_ROOT", args.output)).resolve()
    args.output.mkdir(parents=True, exist_ok=True)
    stamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S-")
    directory = Path(tempfile.mkdtemp(prefix=stamp, dir=args.output))
    metadata = {}
    if (binaries / "demo-build.json").exists():
        metadata = json.loads((binaries / "demo-build.json").read_text())
    report = {"started_at": datetime.datetime.now().astimezone().isoformat(),
              "platform": platform.platform(), "machine": platform.machine(), "build": metadata,
              "binary_directory": str(binaries), "scale": args.scale, "expected_suites": len(parts), "suites": []}
    started = time.monotonic()
    current = None

    def interrupted(signum, _frame):
        if current is not None:
            stop_group(current)
        raise KeyboardInterrupt(f"signal {signum}")

    signal.signal(signal.SIGINT, interrupted)
    signal.signal(signal.SIGTERM, interrupted)
    print(f"RESULT_DIR={directory}", flush=True)
    try:
        for part in parts:
            target = directory / part
            target.mkdir()
            command = [str(binaries / f"demo_{part}_tests"), str(binaries), str(target), str(args.scale)]
            print(f"\n=== {part.upper()} ===", flush=True)
            tick = time.monotonic()
            with (target / "console.log").open("w") as log:
                current = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                           text=True, bufsize=1, start_new_session=True)
                expired = threading.Event()

                def timeout(process=current):
                    expired.set()
                    stop_group(process)

                watchdog = threading.Timer(args.timeout, timeout)
                watchdog.start()
                try:
                    for line in current.stdout:
                        print(line, end="", flush=True)
                        log.write(line)
                        log.flush()
                    code = current.wait()
                finally:
                    watchdog.cancel()
                    # Remove any descendant that escaped normal C++ RAII teardown.
                    stop_group(current)
                    current.wait()
                    current = None
            data = {}
            if (target / "suite.json").exists():
                try:
                    data = json.loads((target / "suite.json").read_text())
                except (ValueError, OSError) as error:
                    data = {"report_error": str(error)}
            data.update(name=part, command=command, exit_code=code, timeout=expired.is_set(),
                        wall_seconds=time.monotonic() - tick,
                        passed=code == 0 and not expired.is_set() and data.get("passed") is True)
            report["suites"].append(data)
            write_report(directory, report, started)
    except KeyboardInterrupt:
        report["interrupted"] = True
    finally:
        write_report(directory, report, started)
    passed = report["passed"]
    print(f"\nOVERALL {'PASS' if passed else 'FAIL'} elapsed={report['wall_seconds']:.2f}s")
    print(f"Report: {directory / 'report.md'}")
    print(f"JSON:   {directory / 'summary.json'}")
    return 0 if passed else 1


def write_report(directory, report, started):
    report["wall_seconds"] = time.monotonic() - started
    report["passed"] = len(report["suites"]) == report["expected_suites"] and all(s["passed"] for s in report["suites"]) and not report.get("interrupted", False)
    (directory / "summary.json").write_text(json.dumps(report, indent=2) + "\n")
    lines = ["# MarketLab demonstration results", "", f"Platform: {report['platform']}",
             f"Build: {report['build']}", f"Scale: {report['scale']}; wall time: {report['wall_seconds']:.2f} s", "",
             "| Suite | Result | Cases | Seconds |", "| --- | --- | ---: | ---: |"]
    for suite in report["suites"]:
        lines.append(f"| {suite['name']} | {'PASS' if suite['passed'] else 'FAIL'} | {len(suite.get('cases', []))} | {suite['wall_seconds']:.2f} |")
    for suite in report["suites"]:
        lines += ["", f"## {suite['name']}", "", f"[Console log]({suite['name']}/console.log) · [Metrics CSV]({suite['name']}/metrics.csv) · [Raw samples]({suite['name']}/samples.csv)", "",
                  "| Case | Result | Milliseconds | Detail |", "| --- | --- | ---: | --- |"]
        if not suite.get("cases"):
            lines.append(f"| Suite execution | FAIL | — | exit={suite['exit_code']}, timeout={suite['timeout']}; see console log |")
        for case in suite.get("cases", []):
            detail = case["error"].replace("|", "/").replace("\n", " ")
            lines.append(f"| {case['name']} | {'PASS' if case['passed'] else 'FAIL'} | {case['duration_ms']:.2f} | {detail} |")
        lines += ["", "| Metric | Samples | Ops/s | p50 µs | p95 µs | p99 µs | Max µs |", "| --- | ---: | ---: | ---: | ---: | ---: | ---: |"]
        for metric in suite.get("metrics", []):
            lines.append(f"| {metric['name']} | {metric['samples']} | {metric['operations_per_second']:.1f} | {metric['p50_us']:.3f} | {metric['p95_us']:.3f} | {metric['p99_us']:.3f} | {metric['max_us']:.3f} |")
        for metric in suite.get("metrics", []):
            lines += ["", f"- `{metric['name']}`: {metric['boundary']}"]
    lines += ["", "Percentiles use nearest rank on raw samples; µs means microseconds. Rates and latencies have the boundaries recorded above. Performance is observational; correctness failures and timeouts return a nonzero exit code.", ""]
    (directory / "report.md").write_text("\n".join(lines))


if __name__ == "__main__":
    sys.exit(main())
