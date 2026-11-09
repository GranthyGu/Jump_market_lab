#!/usr/bin/env python3
"""Run unmodified CMU public tests against the production B+ tree sources."""
import argparse
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess

REVISION = "e42a66f171093c138464b8302f5c3677a08e2ce4"
TESTS = ["arc_replacer", "arc_replacer_performance", "page_guard", "disk_scheduler", "buffer_pool_manager",
         "b_plus_tree_insert", "b_plus_tree_delete", "b_plus_tree_tombstone",
         "b_plus_tree_concurrent", "b_plus_tree_sequential_scale"]


def main():
    repo = Path(__file__).resolve().parents[2]
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, default=repo / "build/vendor/bustub")
    parser.add_argument("--build", type=Path, default=repo / "build/bustub-public")
    parser.add_argument("--cxx", default=os.environ.get("CXX", shutil.which("clang++") or "c++"))
    parser.add_argument("--jobs", type=int, default=8)
    parser.add_argument("--duration", type=int, default=30000)
    args = parser.parse_args()
    source, build = args.source.resolve(), args.build.resolve()
    if not source.exists():
        source.parent.mkdir(parents=True, exist_ok=True)
        subprocess.run(["git", "clone", "--depth", "1", "https://github.com/cmu-db/bustub.git", str(source)], check=True)
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=source, text=True).strip()
    if revision != REVISION:
        raise SystemExit(f"Expected CMU revision {REVISION}; checkout contains {revision}. Use a dedicated checkout at the pinned revision.")
    if subprocess.check_output(["git", "diff", "--", "test", "tools/btree_bench"], cwd=source):
        raise SystemExit("Refusing modified upstream tests/benchmark")
    # Only the disposable clone receives links. Tracked production files remain
    # the single source of truth, and the tests/benchmark remain upstream originals.
    for original in (repo / "src/matching_engine/bpt/cmu/src").rglob("*"):
        if not original.is_file():
            continue
        target = source / "src" / original.relative_to(repo / "src/matching_engine/bpt/cmu/src")
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists() or target.is_symlink():
            target.unlink()
        target.symlink_to(original)
    build.mkdir(parents=True, exist_ok=True)
    results = build / "public-results"
    results.mkdir(exist_ok=True)
    with (results / "build.log").open("w") as log:
        subprocess.run(["cmake", "-S", str(source), "-B", str(build), "-DCMAKE_BUILD_TYPE=Release",
                        f"-DCMAKE_CXX_COMPILER={args.cxx}", "-DCMAKE_CXX_FLAGS=-include cstdlib"], stdout=log, stderr=subprocess.STDOUT, check=True)
        subprocess.run(["cmake", "--build", str(build), "--target"] + [t + "_test" for t in TESTS] +
                       ["btree-bench", "-j", str(args.jobs)], stdout=log, stderr=subprocess.STDOUT, check=True)
    summary = {"upstream_revision": REVISION, "platform": platform.platform(), "compiler": args.cxx, "suites": []}
    for name in TESTS:
        output = results / (name + ".json")
        with (results / (name + ".log")).open("w") as log:
            subprocess.run([str(build / "test" / (name + "_test")), "--gtest_also_run_disabled_tests",
                            "--gtest_output=json:" + str(output)], cwd=results, stdout=log, stderr=subprocess.STDOUT, check=True, timeout=180)
        report = json.loads(output.read_text())
        # GoogleTest still counts DISABLED_ names as 'disabled' in the summary
        # even when run explicitly; insist every case actually reports COMPLETED.
        cases = [case for suite in report["testsuites"] for case in suite["testsuite"]]
        if not cases or any(case.get("status") != "RUN" or case.get("result") != "COMPLETED" for case in cases):
            raise SystemExit("A CMU public test was skipped")
        summary["suites"].append({"name": name, "tests": report["tests"], "failures": report["failures"], "time": report["time"]})
        print(name, report["tests"], "PASS", flush=True)
    with (results / "benchmark.log").open("w") as log:
        subprocess.run([str(build / "bin/bustub-btree-bench"), "--duration", str(args.duration)],
                       cwd=results, stdout=log, stderr=subprocess.STDOUT, check=True, timeout=180)
    summary["benchmark_duration_ms"] = args.duration
    summary["official_leaderboard_submission"] = False
    (results / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print("RESULTS", results, flush=True)


if __name__ == "__main__":
    main()
