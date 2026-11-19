# Gateway tests

| Group | Coverage |
| --- | --- |
| Accounts and sessions | Login, invalid credentials, session replacement, snapshots, cancel permissions |
| Networking and routing | Fragmented/coalesced frames, invalid messages, request correlation, per-shard ordering |
| Failures and load | Backend recovery, multiple users, products, and shards |

From the repository root; tests launch their own isolated services:

```sh
./tests/build.sh
ctest --test-dir build/demo-tests --output-on-failure -R '^demo_gateway$'
```

See the [test runner](../README.md) for the full suite.
