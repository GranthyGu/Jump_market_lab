# Publisher tests

| Group | Coverage |
| --- | --- |
| Encoding | Snapshots, net deltas, empty books, frame sizes, malformed messages |
| Publication and queries | Periodic snapshots, size-based delta selection, multicast subscribers, concurrent TCP queries |
| Failures and load | Source recovery, epoch changes, multiple products and shards |

From the repository root; tests launch their own isolated services:

```sh
./tests/build.sh
ctest --test-dir build/demo-tests --output-on-failure -R '^demo_publisher$'
```

See the [test runner](../README.md) for the full suite.
