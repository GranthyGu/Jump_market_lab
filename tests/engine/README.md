# Engine tests

| Group | Coverage |
| --- | --- |
| Matching and cancellation | Input limits, price/time priority, partial fills, self-trades, order-pool reuse |
| Reference model | Random commands checked against independent order, trade, and depth results |
| Concurrent snapshots | Mailbox wakeup, timeout, cancellation, shutdown, consistent reads |
| Load | Multiple products, new/cancel loops, sweeping orders, snapshot contention |

From the repository root:

```sh
./tests/build.sh
ctest --test-dir build/demo-tests --output-on-failure -R '^(demo_engine|snapshot_mailbox)$'

# Standalone snapshot contention benchmark.
./build/demo-tests/bin/snapshot_contention_benchmark build/snapshot-results 3 3000
```

See the [test runner](../README.md) for the full suite.
