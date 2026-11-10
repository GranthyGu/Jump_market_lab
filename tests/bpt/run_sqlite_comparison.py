#!/usr/bin/env python3
"""Measure BPT and two SQLite transaction sizes using the same C++ harness."""
import argparse
import datetime
import hashlib
import json
from pathlib import Path
import platform
import statistics
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--work-dir', type=Path, required=True)
    parser.add_argument('--entries', type=int, default=1_000_000)
    parser.add_argument('--queries', type=int, default=3000)
    parser.add_argument('--repeats', type=int, default=3)
    args = parser.parse_args()
    if args.entries < 11 or args.queries < 1 or args.repeats < 1:
        parser.error('invalid workload counts')
    repo = Path(__file__).resolve().parents[2]
    binary = args.binary.resolve()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    work = args.work_dir.resolve()
    work.mkdir(parents=True, exist_ok=True)
    paths = list((repo / 'src/matching_engine/bpt').rglob('*'))
    paths += [Path(__file__).resolve(), repo / 'tests/bpt/sqlite_benchmark.cpp',
              repo / 'tests/CMakeLists.txt']
    provenance = {
        'date': datetime.datetime.now().astimezone().isoformat(),
        'platform': platform.platform(),
        'args': {k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()},
        'source_sha256': {str(p.relative_to(repo)): hashlib.sha256(p.read_bytes()).hexdigest()
                          for p in paths if p.is_file()},
        'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
        'git_head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=repo, text=True).strip(),
        'scope': 'Index positions only; no trade log, network, concurrency, or physical power-loss test.',
        'cache': '8192-byte pages, 1024-frame BPT pool, SQLite cache_size=1024 (suggested target), mmap off; OS cache enabled.',
        'sqlite': 'Native C API; prepared statements; WITHOUT ROWID primary key (user_id,record_offset); WAL + FULL; fsync not F_FULLFSYNC; auto-checkpoint 1000 pages.',
        'transactions': 'sqlite-bulk: one transaction; sqlite-batch: 1000 entries per transaction. Both final TRUNCATE checkpoint. BPT: final flush+fsync, no atomic index transaction or index.snapshot copy.',
        'durability_limit': 'SQLite supplies transaction atomicity; BPT pages are a rebuildable derived index. Build times are not equivalent end-to-end durability guarantees.',
        'files': 'Main index file after final flush/checkpoint; excludes authoritative trades, BPT index.snapshot, SQLite shared-memory metadata and transient WAL peak.',
        'timing': 'Build includes final flush/checkpoint; query phase includes oracle checks, returns <=256 positions; full hot-user time includes all continuation pages; independent median per metric.',
        'checks': 'Every query, full scans after writing and reopening, duplicates and upper bound; SQLite integrity_check and EXPLAIN QUERY PLAN.',
    }
    (output / 'provenance.json').write_text(json.dumps(provenance, indent=2) + '\n')
    for relative in ('CMakeCache.txt', 'bin/demo-build.json'):
        path = binary.parent.parent / relative
        if path.exists(): (output / path.name).write_bytes(path.read_bytes())
    runs = []
    modes = ['bpt', 'sqlite-bulk', 'sqlite-batch']
    for repeat in range(args.repeats):
        order = modes[repeat % 3:] + modes[:repeat % 3]
        for mode in order:
            label = f'{mode}-{repeat+1}'
            command = [str(binary), str(work / label), mode, str(args.entries), str(args.queries)]
            print('START', label, flush=True)
            proc = subprocess.run(command, text=True, capture_output=True, timeout=600)
            (output / f'{label}.log').write_text(proc.stdout + proc.stderr)
            if proc.returncode:
                raise RuntimeError(f'{label} failed: {proc.stderr}')
            metrics = json.loads(proc.stdout)
            if not metrics['correct']: raise RuntimeError(f'{label}: verification failed')
            runs.append({'mode': mode, 'repeat': repeat + 1, 'command': command, 'metrics': metrics})
            (output / 'runs.json').write_text(json.dumps(runs, indent=2) + '\n')
            print(f"PASS {label}: build {metrics['build_s']:.3f}s, query P99 {metrics['query_p99_us']:.3f}us", flush=True)
    summary = {}
    for mode in modes:
        samples = [r['metrics'] for r in runs if r['mode'] == mode]
        summary[mode] = {'runs': len(samples)}
        for key, value in samples[0].items():
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                values = [m[key] for m in samples]
                summary[mode][key] = {'median': statistics.median(values), 'min': min(values), 'max': max(values)}
    (output / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    print('Results:', output / 'summary.json', flush=True)


if __name__ == '__main__':
    main()
