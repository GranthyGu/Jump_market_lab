# Spread tests

| Group | Files | Coverage |
| --- | --- | --- |
| Derivation and shared liquidity | `depth.cpp`, `shared_depth.cpp` | Top-three depth, twenty routes, source allocation, ratios, rounding, boundaries |
| Selection and execution | `execution.cpp`, `session.cpp`, `butterfly_session.cpp` | Price/time priority, cross-shard pairing, multiple rounds, butterflies, reference models |
| Communication and coordination | `network.cpp`, `bootstrap.cpp`, `coordinator.cpp` | Framing, joins, versions, reordering, source/view barriers |
| Persistence and market data | `persistence.cpp`, `publication.cpp`, `market.cpp` | Batch encoding, commit collection, direct/implied views, publication boundaries |
| Processes and recovery | `process.cpp`, `lifecycle.py` | Real group trades, restarts, shard replacement, interrupted execution |
| Performance and timing | `benchmark.py`, `latency.py`, `latency_analysis_test.py` | Core load, single-in-flight and concurrent latency, timestamp validation |

From the repository root:

```sh
./tests/build.sh
ctest --test-dir build/demo-tests --output-on-failure -L spreads

# Process benchmarks.
python3 -B tests/spreads/benchmark.py --build-dir build/demo-tests/bin \
  --output build/spread-benchmark --repeats 3

# Per-order latency; builds separate instrumented binaries automatically.
python3 -B tests/spreads/latency.py --prepare-build build/demo-tests \
  --output build/spread-latency --count 5000 --warmup 200 \
  --windows 1 128 --repeats 3
```

Benchmark output directories must be new. Window 1 waits for each order's complete response before sending the next; window 128 adds load. Reports measure execution-start to memory completion and client-send to complete response.

Use `latency.py --reference-dir <baseline-bin-directory>` to alternate two builds. See the [test runner](../README.md) for the full suite.
