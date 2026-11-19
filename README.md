# MarketLab

A C++20 exchange with sharded order matching, calendar and butterfly spreads, durable recovery, market data, and a browser console connected to the real trading services.

## Components

| Component | Responsibility |
| --- | --- |
| [Gateway](src/gateway/) | Authenticates clients, manages sessions, forwards requests, and delivers order and trade notifications. |
| [Matching engine](src/matching_engine/) | Owns each shard's order books and matches orders by price and time priority. |
| [Router](src/matching_engine/cluster/) | Routes orders by product, schedules spread-group commands, collects results, and coordinates planned shard replacement. |
| [Spread execution](src/matching_engine/spreads/) | Reuses the matching engine to execute real legs across shards and assemble complete trade records. |
| [Coordinator](src/matching_engine/spreads/coordinator/) | Derives implied quotes from real top-three price levels, allocates shared liquidity, and synchronizes refreshed views. |
| [Persistence and recovery](src/matching_engine/persistence/) | Writes order and trade logs, gates publication on durable batches, and restores state from snapshots and logs. |
| [Trade history](src/matching_engine/trade_history/) | Builds a recoverable B+ tree index over trade logs for paginated account queries. |
| [Publisher](src/publisher/) | Publishes market snapshots or deltas through multicast and serves TCP snapshot queries. |
| [Client and shared code](src/client/) | Provides terminal trading and market-data recovery; [shared utilities](src/shared_data/) contain protocols, networking, and runtime support. |
| [Web Console](web_console/) | Displays books, trades, charts, and history; supports manual trading, demo replay, and service controls. |

The default configuration assigns 24 ordinary products to shards 1–4, six per shard. Shards 5–8 handle A, B, C, SP_AB, SP_AC, and BF_ABC. Both paths use the same matching core. Spread commands execute sequentially within the group; the Coordinator stores aggregated price levels rather than individual orders.

## Start the Web Console

Requirements: Python 3.10+, CMake 3.20+, a C++20 compiler, and macOS or Linux. Run all commands from the repository root.

```sh
./web_console/start.sh
```

The script builds native services when needed, starts the exchange, and opens **http://127.0.0.1:8080**. The web layer uses Python's standard library and plain HTML/CSS/JavaScript; no frontend package installation is needed.

Once services are ready, load the demo or trade through the account panels. Keep the launch terminal open. Closing the browser does not stop the exchange.

```sh
# Stop the console and its owned services gracefully.
./web_console/stop.sh

# Retain orders and trades between sessions.
./web_console/start.sh --persistent

# Rebuild the current native source and start.
./web_console/start.sh --rebuild
```

A default session starts with empty demo data and removes that session's data on normal shutdown. Persistent mode retains it. Console builds, logs, and data live under `web_console/runtime/`. See [Web Console options](web_console/README.md).

## Build and run components separately

The console builds services automatically. For terminal use or individual component startup:

```sh
./src/scripts/build.sh
```

Executables are written to `build/bin/`; build and service logs go to `build/logs/`. Run these commands in separate terminals, in the order below. If the console already owns the deployment, use its running services.

| Order | Command | Starts |
| --- | --- | --- |
| 1 | `./src/scripts/run_matching_engine.sh` | Router, Coordinator, and all matching shards |
| 2 | `./src/scripts/run_gateway.sh` | Trading Gateway |
| 3 | `./src/scripts/run_publisher.sh` | Market-data Publisher |
| 4 | `./src/scripts/run_client.sh` | Interactive trading client with login prompts |
| Optional | `./src/scripts/run_market_client.sh` | Market-data subscriber with snapshot recovery |
| Optional | `./src/scripts/run_snapshot_client.sh` | Market snapshot query client |

Each script supports `--help`. Accounts, products, and trading endpoints are configured in `src/config/exchange.conf`; market data in `src/config/publisher.conf`; cluster management ports in `src/config/engine_cluster.json`.

## Tests

The test build additionally requires SQLite development files for index comparisons.

```sh
./tests/build.sh
./tests/run_all.sh --quick --skip-cmu

# After the console's first native build:
./web_console/tests/run.sh
```

See [test groups and commands](tests/README.md). Native test artifacts stay under `build/`; console test artifacts stay under `web_console/runtime/tests/`.
