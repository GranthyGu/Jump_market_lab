# Web Console

A browser interface to the existing C++ exchange, with account trading, books, executed-price charts, history queries, demo replay, and engine controls.

## Components

| Files | Responsibility |
| --- | --- |
| `server.py`, `static/` | Local HTTP server, interface, and browser interactions |
| `bridge.py`, `protocol.py` | Trading protocol, market-data reception, reconnection, history queries |
| `services.py`, `deployment.py`, `service_host.py` | Native builds, configuration isolation, service startup and shutdown |
| `demo.py`, `demo_scenario.py` | Real demo order flow and replay progress |
| `tests/` | Protocol, deployment, demo, and real-service integration tests |

The console uses the existing Gateway, Router, matching shards, Coordinator, and Publisher. See the [exchange components](../README.md#components).

## Start and stop

Requirements: Python 3.10+, CMake 3.20+, and a C++20 compiler. From the repository root:

```sh
./web_console/start.sh
```

The script builds native services when needed and opens **http://127.0.0.1:8080**. Keep the launch terminal open. Once services are ready, load the demo or trade manually.

```sh
./web_console/stop.sh
```

Alternatively, press `Ctrl-C` in the launch terminal. Closing the page does not stop services. Each default session starts with empty demo data; normal shutdown removes that session's data and retains logs.

| Startup option | Purpose |
| --- | --- |
| `--persistent` | Retain orders and trades for the next session |
| `--rebuild` | Rebuild the current C++ source |
| `--attach` | Connect to running services without owning their lifecycle |
| `--no-start` | Open the console and start services later from the page |
| `--no-open` | Skip automatic browser opening |
| `--port 8081` | Change the web port; pass the same option when stopping |
| `--build-dir build/bin` | Use an existing native build |

Builds, data, and logs default to `web_console/runtime/`. Accounts and products come from `src/config/exchange.conf`; market and cluster settings come from `publisher.conf` and `engine_cluster.json` in the same directory.

## Demo replay

The demo submits real orders through the Gateway, including ordinary, calendar, and butterfly trades. Use the page controls or:

```sh
./web_console/demo.sh
./web_console/demo.sh --duration 15
./web_console/demo.sh --cancel
```

Cancelling replay keeps accepted orders and leaves services running. Stopping the default demo session cleans up its data. Demo loading is disabled in persistent mode.

## Tests

After the first startup has built the native services, run from the repository root:

```sh
./web_console/tests/run.sh

# Unit tests and demo integration only.
./web_console/tests/run.sh --demo-only
```

Tests use isolated ports and data directories, with results under `web_console/runtime/tests/`. If Node.js is installed, the script also runs frontend history-index tests; Node.js is not required to run the console.
