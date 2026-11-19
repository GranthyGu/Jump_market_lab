# Integration tests

| Group | Coverage |
| --- | --- |
| Trading and market data | Real Gateway, shards, and Publisher; trades, remaining orders, published depth |
| Concurrency and sharding | Multiple users, unique order IDs, both counterparties, concurrent sampling and queries |
| Market recovery | Packet loss, reordering, late snapshots, Publisher restart, stale views |
| Read transports | TCP/shared-memory consistency and shutdown |

From the repository root; tests launch their own isolated services:

```sh
./tests/build.sh
ctest --test-dir build/demo-tests --output-on-failure -R '^demo_integration$'
```

See the [test runner](../README.md) for the full suite.
