# Persistence and recovery tests

| Group | Coverage |
| --- | --- |
| Logs and commit barriers | Order/trade synchronization, notification ordering, bounded buffers, write failures |
| Snapshots and replay | Partial fills, FIFO preservation, log rotation, incomplete tails, corruption detection |
| State and queries | Copy-on-write pages, asynchronous snapshots, cancellation reuse, file compatibility |
| Process recovery | Forced termination followed by order, commit-boundary, and market recovery |

From the repository root:

```sh
./tests/build.sh
ctest --test-dir build/demo-tests --output-on-failure \
  -R '^(persistence_correctness|persistence_optimizations|recovery_process)$'
```

See [queue benchmarks](bench/README.md) for performance comparisons and the [test runner](../README.md) for the full suite.
