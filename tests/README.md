# Tests

Run all commands from the repository root.

## Test groups

| Directory | Coverage |
| --- | --- |
| [Engine](engine/README.md) | Price/time priority, cancellation, order pools, reference models, concurrent snapshots |
| [Gateway](gateway/README.md) | Login, permissions, session replacement, TCP framing, routing, concurrent requests |
| [Publisher](publisher/README.md) | Snapshot/delta encoding, multicast, concurrent queries, source failures |
| [Integration](integration/README.md) | Real trades and market data, cross-shard notifications, snapshot recovery |
| [Recovery](recovery/README.md) | Dual logs, commit barriers, snapshots, corruption detection, crash recovery |
| [Queue benchmarks](recovery/bench/README.md) | Queue ordering, concurrent shutdown, waiting strategies, persistence performance |
| [History index](bpt/) | B+ trees, concurrent caches, history queries, SQLite comparisons |
| [Planned restart](restart/README.md) | Replication, shard replacement, history continuity, failed-switch fallback |
| [Client](client/README.md) | Reconnection, orders with unknown outcomes, warnings, history display |
| [Spreads](spreads/README.md) | Twenty implied routes, shared liquidity, cross-shard execution, recovery, latency |
| [Test runner](runner/) | Test discovery, failure propagation, timeouts, process cleanup |
| [Web Console](../web_console/tests/) | Protocols, deployment, demo replay, frontend data, real-service integration |

## Build and run

Requirements: a C++20 compiler, CMake, Python 3, and SQLite development files.

```sh
./tests/build.sh

# All project tests with smaller standalone benchmark workloads.
./tests/run_all.sh --quick --skip-cmu

# Full-size regressions and benchmarks.
./tests/run_all.sh --skip-cmu

# Registered regressions only.
./tests/run_all.sh --regression-only

# List selected tests without running them.
./tests/run_all.sh --list --skip-cmu
```

The default build directory is `build/demo-tests/`. `--quick` reduces standalone benchmark workloads only. `--skip-cmu` excludes external CMU tests, which need additional source and dependencies. Reports, timings, and logs go to `build/test-results/`.

```sh
# Run a specific group; see each group's README for more commands.
ctest --test-dir build/demo-tests --output-on-failure -R '^demo_engine$'
ctest --test-dir build/demo-tests --output-on-failure -L spreads

# Console tests run separately, after the console's first native build.
./web_console/tests/run.sh
```
