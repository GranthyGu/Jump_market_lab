# Client tests

| Group | Coverage |
| --- | --- |
| Reconnection | Login snapshots, retry deadline, offline input, no replay of orders with unknown outcomes |
| Service interruption | Repeated engine restarts, deduplicated warnings, recovery notifications, resumed trading |
| History display | CLI trade queries, field verification, deduplication, complete identifiers |

From the repository root:

```sh
./tests/build.sh
ctest --test-dir build/demo-tests --output-on-failure \
  -R '^(client_reconnect_process|trade_history_process)$'
```

See the [test runner](../README.md) for the full suite.
