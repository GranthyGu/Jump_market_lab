# Matching engine

The module owns order matching, persistence, market snapshots, shard routing, and spread execution. Runtime processes use the durable engine; core tests can call the same matching engine directly.

## Components

| Directory | Responsibility |
| --- | --- |
| `core/` | Single-threaded price/time matching, active-order lookup, result collection, and real-book operations for spread execution |
| `runtime/` | Process startup, order reactor, command adapters, notifications, and spread-worker integration |
| `market_data/` | Snapshot mailbox, direct/implied depth merging, and immutable publication candidates |
| `persistence/` | Order/trade log encoding, background writes, synchronization, and batch publication barriers |
| `recovery/` | Copy-on-write state, snapshots, log rotation, and contiguous-prefix recovery |
| `trade_history/` | Asynchronous indexing of committed trades and bounded history queries |
| `trade_history/index/` | B+ tree, page storage, buffer pool, and cache; `cmu/` retains upstream source and licenses |
| `cluster/` | Router, spread command scheduling and result collection, replication, and planned shard replacement |
| `spreads/` | Top-three depth, twenty derivation routes, shared-source allocation, execution plans, and multi-round sessions |
| `spreads/transport/` | Binary frames, source/result encoding, connections, heartbeats, and recovery joins |
| `spreads/coordinator/` | Independent Coordinator process, source/view synchronization, and group memory-completion tracking |

Shards 1–4 use the ordinary path. Shards 5–8 reuse the matching core through spread sessions. Each participating shard seals one batch per command; group memory completion and durable publication remain separate barriers. Terminal batch sealing can overlap final implied-view installation, while the Coordinator still waits for both before reporting memory completion.

The default deployment has 24 ordinary products and six spread-group products. The Coordinator stores price-level summaries, not individual orders. Trade history is a rebuildable index; the trade log remains its source. Group peers use protocol version 3 and must run compatible binaries.

## Build and run

From the repository root:

```sh
./src/scripts/build.sh
./src/scripts/run_matching_engine.sh
```

The launcher manages the Router, Coordinator, and shard processes. For the full exchange and browser interface, use `./web_console/start.sh`; see the [project README](../../README.md).

## Tests

```sh
./tests/build.sh
./tests/run_all.sh --regression-only
```

See [test groups and commands](../../tests/README.md), [recovery tests](../../tests/recovery/README.md), and [spread tests](../../tests/spreads/README.md).
