# Planned restart regression and measurements

Build with `cmake -S src -B build/planned-restart -DCMAKE_BUILD_TYPE=Release -DMARKETLAB_BUILD_TESTS=ON`, then `cmake --build build/planned-restart -j 6`.

```sh
ctest --test-dir build/planned-restart --output-on-failure
python3 tests/restart/process.py build/planned-restart/bin build/planned-restart/pressure \
  --resting 20000 --rate 20000 --copy-rate 0 --warmup 2 --switch-gap 2
```

`replication_correctness` checks manual/automatic checkpoint freeze, a checkpoint already in publication when freeze arrives, contiguous replay, duplicate/gap rejection, mismatched parallel logs, partial-fill FIFO and durable recovery followed by new matching. It opens no network ports.

`planned_restart_process` starts real Gateway, Router, four engines, standby processes and Publisher on temporary ports with isolated data. It performs A→B→A for every shard while a bounded client pipelines self-trades on separate symbols. It checks each acceptance and both participant notifications, history contents and cross-switch cursors, partial-fill FIFO, cancels, full login snapshots, unchanged public market ports and publisher epoch, UDP delivery, the stock CLI, failed replacement rollback, and reopening a mixed A/B topology.

The default baseline copy limit is **0.25 MiB/s** so the test can repeatedly hash the source checkpoint during copying. The source must advance while its checkpoint remains unchanged. `--copy-rate 0` removes this artificial limit for timing runs; fast copies may finish between filesystem observations, so the slower regression remains the freeze-race check.

Each run prints its artifact directory and saves `results.json`, service logs, persisted data and `restart-results.json`. Metrics distinguish copy, bootstrap, catch-up, drain, final sync, complete routing pause, queue peak, total maintenance time and client latency. Target request rate is a pacing target, not a capacity claim; compute achieved throughput from `accepted / elapsed_s`. Tests keep at most 128 workload requests outstanding to stay within admission limits.

The process test requires local TCP/UDP bind permissions. Results from macOS do not establish Linux performance; rerun the same commands on the intended deployment host. No production config data directories are used.
