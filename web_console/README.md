# MarketLab local console

A local browser interface for the existing C++ exchange. The layout follows `IMG_9700.PNG`: market books and engine controls on the left; Gateway output and independent Alice / Bob panels on the right. Everything added for the console lives in this directory. Existing source and configuration files are read without modification.

## Run

From the repository root:

```sh
./web_console/start.sh
```

The script opens **http://127.0.0.1:8080**, builds the current C++ services into `web_console/runtime/build` when needed, and starts the existing supervisor, eight matching engines, router, coordinator, Gateway and Publisher. Initial compilation can take a minute; follow **Engine control → System log**. The default workspace is now a **fresh, disposable demo**: every launch starts with empty books, orders, trade history and price curves. Python 3.10+, CMake 3.20+ and a C++20 compiler are required. The web layer uses Python's standard library and plain HTML/CSS/JavaScript, with no package install or CDN.

Keep the launch terminal open. Closing the browser tab does not stop the services. To stop gracefully, press **Ctrl-C** in that terminal, or run:

```sh
./web_console/stop.sh
```

The stop script waits for owned native services to finish and for demo data cleanup to complete. It prints **Stopped. Demo data cleared.** Only the marked directory created for this demo session is removed. Previous persistent data is preserved. Attached processes belonging to another terminal are left running. The console binds only to `127.0.0.1`.

Each owned service runs under a small guardian connected to the console by a pipe. If the console exits unexpectedly, the guardian detects the closed pipe and asks the native supervisor to drain and shut down gracefully. Native output is written directly to log files so losing the console does not break a service's output stream. Startup completes only after all shard routes are connected, the spread group is ready, and Publisher snapshots have been verified. Failed startup attempts stop the services they just created.

## Demonstrate in 12 seconds

1. Run `./web_console/start.sh` and wait for **Exchange live**. The books are empty.
2. In the header, choose **10 seconds**, **12 seconds** or **15 seconds**, then click **Load demo**. Alice and Bob log in automatically. The prepared scenario submits real Gateway orders at a controlled rate, with progress visible in the header and activity logs.
3. The replay opens with five bid/ask levels and **five separate orders per level** across all 30 symbols. It then continuously mixes executions and replacement quotes: Alice and Bob trade against existing resting orders, and new quotes replenish the consumed liquidity. Order sizes, buy/sell direction, product order and submission intervals vary on every run. Outright quotes follow bounded, correlated random walks within configured price limits; spread/leg quotes retain consistent anchors while their direction, size and timing vary. Real implied and butterfly executions are interleaved with this flow. The prepared scenario sends **20,000 real orders**: **18,500 finish fully filled**, and **1,500 remain resting**. The console checks these counts against actual order responses before handing control back. Each symbol receives hundreds of price points, and all committed history pages are loaded automatically. Matched buy/sell orders share a trade, so the number of filled orders is not the number of trades. Timing is a target; a bounded request pipeline paces submissions and waits for every acknowledgement, so a slow exchange can extend the run. The random seed is recorded in the demo log for diagnosis.
4. Click any **symbol name** to open its live executed-price curve. Switch symbols inside the chart, hover over points or use the left/right arrow keys to inspect time, price and quantity. Clicking a **book price** still fills the trading ticket. Curves contain actual executions observed by the connected accounts, deduplicated across counterparties and history queries; they are not fabricated chart data.
5. When **Demo loaded** appears, automatic orders have finished. Trade, cancel, query history, restart or interrupt engines manually. Engine maintenance keeps this session's data. **Stop loading** cancels only the replay; accepted orders remain and engines keep running. Loading is one-shot per session to prevent accidental duplicate data.
6. Close the browser whenever you like; the exchange keeps running. Run `./web_console/stop.sh` in another terminal, or press Ctrl-C in the launch terminal. The session's matching-engine data is removed only after service shutdown. The next normal start is empty again.

The same replay can be started or stopped from a terminal while watching the page:

```sh
./web_console/demo.sh                 # 12-second replay
./web_console/demo.sh --duration 15   # Slower replay; allowed range: 10–15
./web_console/demo.sh --cancel        # Stop injection, keep the exchange running
```

The replay includes **1,000 native Calendar spread executions** (500 each for SP_AB and SP_AC) and **200 Butterfly executions**. Throughout the run, batches of three to seven real orders on each required leg are matched by a spread order; Alice and Bob alternate as spread buyers, sellers and leg liquidity providers. Quantities, episode order and timing vary. These orders share the existing 20,000-order budget and leave the same 1,500 resting orders. In **History**, each account has 500 Calendar spread records. A calendar execution also creates records for its two legs, so the Implied leg count is naturally higher. Direct counterparties trading SP_AB or SP_AC are classified as Direct by the native engine and are separate from Calendar spread records.

Use `--port 8081` with these scripts if the console uses another HTTP port. Demo loading temporarily disables manual trading and engine maintenance, so inputs cannot interleave with the prepared scenario. No credentials, order books, trade files or recovery flags in the original project are changed. Diagnostic logs are retained in `runtime/logs/`; trade state and in-memory curves begin empty on the next demo launch. If shutdown cannot complete, data is retained and the stop script reports the failure instead of deleting files still in use. An abrupt console kill can likewise leave its old disposable folder on disk; the next launch creates a new empty session rather than reusing it.

For durable work instead of demonstrations, use `./web_console/start.sh --persistent` or an explicit compatible `--data-dir`. These modes preserve orders and history on Stop and disable automatic demo loading.

## Use

- **Log in:** Connect Alice and Bob independently. The default option reads the selected account's password from the local exchange configuration on the server. Uncheck it to enter a password. One exchange account can have only one connected session.
- **Trade:** Select a symbol, Buy / Sell, integer limit price and quantity. Click a market price to fill the most recently focused account's ticket. An order is accepted only after its real Gateway response; rejection and fill events appear in the account log. Clicking the symbol name opens its price chart.
- **Orders:** Shows the complete login snapshot plus subsequent exchange updates. Cancel by the row button or by the full order ID. IDs remain decimal strings throughout the browser/API, preserving all 64 bits.
- **History:** **Query trades** automatically reads subsequent native history pages. Filter the queried records by **All symbols**, execution type, or **Trade / order ID**; filters combine and pagination follows the matching records. **Implied-related** includes implied legs, calendar spreads and butterflies. An exact ID lookup also finds referenced leg orders, without converting 64-bit IDs to JavaScript numbers. The footer shows matching versus loaded records. Open a row to inspect execution details; CSV export contains all loaded history. Live session notifications are also available under **Fills**. Trade and log timestamps use UTC; the header clock uses browser local time.
- **Market depth:** Twenty-four ordinary symbols, with six per ordinary shard. Each page shows eight books in two columns of four: ITEM0001–0008, ITEM0009–0016, then ITEM0017–0024. The adjacent panel shows all six spread-related books together: A/B/C in the left column and SP_AB/SP_AC/BF_ABC in the right. Each book retains all five levels. Search filters both panels. The interface polls every 500 ms; the existing Publisher samples every second. Empty books remain empty until actual orders are placed.
- **Compact workspace:** The desktop workspace fits the browser window without page or panel scrolling. The Publisher/market column receives about 65% of the desktop width and the two clients share the rest. Direct and implied quantities occupy separate lines, showing full numbers with purple implied values. Publisher and Gateway each show three compact, single-line log entries with aligned timestamps. Market cards use half their group's width and share the available height; on short windows their type tightens to preserve five complete levels. Ordinary market pages always contain up to eight books. Orders and filtered history use adaptive pagination. All retained records remain available through the page arrows.
- **Logs:** Publisher, Gateway and each trading session have their own terminal-font output, showing the latest entries that fit. Expand a log to filter by source, severity and text, page through older entries, pause updates, or download all filtered events. Hover over an entry for its full text. Green indicates success, purple trades, amber warnings, red errors. Engine process output and build logs are retained under `runtime/logs/`.

## Engine operations

| Control | Actual operation |
| --- | --- |
| Restart → shard / All shards | Sends the existing supervisor's planned A/B restart command, preserving its snapshot, replay, drain and promotion workflow. |
| Shutdown engines | Gracefully stops the owned cluster. Gateway and Publisher remain available for recovery. |
| Recover engines | Starts the cluster using the same data directory, refreshes public snapshots and reconnects previously logged-in accounts. |
| Interrupt → Pause / Resume shard | Sends SIGSTOP / SIGCONT to a specific owned matching-engine PID. |
| Interrupt → Crash recovery test | Sends SIGKILL to a selected spread-group member (shards 5–8); the existing supervisor performs recovery. |

Operations are serialized and display progress and actual failures. The console does not invent recovery semantics: an unresolved cross-shard execution can remain paused under the engine's existing rules. Engine maintenance never clears recovery markers, journals or persisted boundaries. A complete console Stop discards its disposable demo directory only after the owned services have exited; persistent workspaces are retained. Requests with uncertain outcomes are never automatically replayed; inspect active orders and committed history after reconnection.

Observed native-engine limitation: pausing a spread-group member beyond its connection timeout after a planned group restart can leave the group `PAUSED` after SIGCONT, with `join progress timeout` in the native log. The console displays this state; resuming a process does not bypass the engine's boundary checks. Ordinary-shard pause/resume, idle spread-member crash recovery, planned restarts, and graceful whole-cluster shutdown/recovery passed the integration run. Fixing this separate native recovery case would require changes outside this directory.

## Local data and configuration

Defaults are read from:

- `src/config/exchange.conf` — accounts, symbols, shard assignments and order endpoints.
- `src/config/publisher.conf` — multicast and TCP market snapshots.
- `src/config/engine_cluster.json` — supervisor and engine ports.

Default demos use a unique **`web_console/runtime/demo/session-<id>/data`** directory. It survives engine maintenance within that console session, then is deleted by normal console shutdown. The initial **persistent-mode** data directory is **`web_console/runtime/data`**. The console stores a symbol-configuration manifest alongside native state. When the symbol list, order or shard assignments change in persistent mode, it uses a stable **`web_console/runtime/data-<configuration-id>`** directory and preserves the previous directory. Subsequent persistent launches with that configuration reuse its orders and history. This prevents the old 20-item data from being replayed with the new 24-item symbol IDs. The terminal and Workspace details dialog show the selected directory. An explicit `--data-dir` with a conflicting manifest is rejected.

Configuration is read when the console starts. It does not import data under `src/config/.marketlab-data`. For legacy console state without a manifest, the console identifies the old symbol mapping from its retained generated Router configuration; an ambiguous mapping requires an explicitly selected, verified compatible directory. Native recovery flags and pending transitions are never cleared by this process.

```sh
# Keep state between complete console sessions; demo loading is disabled.
./web_console/start.sh --persistent

# Attach to a matching exchange already running on the configured ports.
# Trading, market data, history and supervisor planned restarts remain available.
# Process shutdown, pause and crash controls require ownership by this console.
./web_console/start.sh --attach

# Recover compatible existing state (with its previous deployment stopped).
./web_console/start.sh --data-dir /absolute/path/to/compatible/cluster-data

# Use another HTTP port; the native exchange ports still come from configuration.
./web_console/start.sh --port 8081
./web_console/stop.sh --port 8081

# Rebuild current native source explicitly; all build output stays in this folder.
./web_console/start.sh --rebuild

# Open only the console, then start/recover engines from the page.
./web_console/start.sh --no-start

# Suppress automatic browser opening.
./web_console/start.sh --no-open
```

`--config`, `--market-config`, `--cluster-config` and `--runtime` select an alternate deployment. `--build-dir /path/to/bin` explicitly uses existing native binaries; the caller is responsible for their source version. Without this flag, the console checks source timestamps and builds its own binaries. Separate concurrent deployments require distinct native ports and data directories, in addition to distinct HTTP ports.

In persistent mode, if a supervisor is already listening, startup attaches to it after checking its symbol configuration when available. A disposable demo refuses to attach to an existing deployment, because that would mix existing data into a supposedly empty session. Engine logs can be tailed from an attached supervisor's reported log directory; Gateway and Publisher stdout are captured only for processes launched by this console. The Workspace details dialog shows the actual connected engine roots and log location.

If an older console left orphaned services behind, startup can stop those services before starting the current deployment. Cleanup requires an orphan parent PID, an exact native executable path under this runtime's `build/bin`, and a matching console configuration path. The process identity is checked again before SIGTERM; an audit is saved under `runtime/logs/orphan-cleanup-*.json`. Other processes occupying the configured ports are left alone and reported as a startup error. Cleanup retains all native data and does not override an unclean-shutdown recovery boundary.

## Implementation

```text
Browser (same-origin HTTP; 500 ms state updates)
  └─ server.py — loopback HTTP, static files, local request token
       ├─ bridge.py / protocol.py
       │    ├─ authenticated Gateway TCP: orders, snapshots, cancels, history
       │    └─ Publisher UDP + authoritative TCP snapshot recovery
       ├─ demo.py — paced scenario, actual execution curves, disposable session
       └─ services.py
            ├─ deployment.py — configuration identity and owned-orphan cleanup
            └─ service_host.py guardians
                 └─ existing cluster/run.py supervisor and native services
```

The market adapter verifies publisher epochs and contiguous sequence numbers, buffers bounded UDP reordering and falls back to TCP snapshots after gaps. Stale books are labelled. Account reconnection waits for the full active-order snapshot before enabling submissions. Passwords stay in server memory; configured passwords are not included in browser state. Mutations require a per-process request token and a matching local origin/Host.

The live journal retains 4,000 events, live fills retain 300 per session, and history loads 128 records per shard/request with a 20,000-record soft limit per account. Query trades follows all available pages up to that limit. The browser indexes the loaded history by symbol, execution type and referenced IDs. It requests unchanged history by version, avoiding retransmission of the full history every 500 ms. Display pagination adapts to the viewport independently of these storage and query limits. These limits do not truncate the exchange's on-disk histories.

## Verify

After a normal first launch has built the native programs:

```sh
./web_console/tests/run.sh
```

Protocol tests cover packed frame sizes, fragmented reads, signed prices, 64-bit IDs, typed history and market recovery. Deployment tests cover configuration isolation, retained recovery flags, cleanup ownership checks, PID reuse and guardian shutdown. Integration tests run the real C++ services on isolated ports with fresh data beneath `runtime/tests/`, exercise trading and recovery, and kill/restart the test console to verify persisted orders and history in persistent mode. Demo checks verify resting liquidity alongside executions throughout the replay, every depth book and curve, complete history queries, versioned history responses, manual trading, engine restart, stop-time deletion, the next empty native session, cancellation and stopping during replay. `./web_console/tests/run.sh --demo-only` runs just the focused demo integration checks after unit tests. When Node is installed, the script also checks combined history filters and full-width trade/order IDs; Node is not required to run the console. Results and diagnostic logs are retained there. Override `--build-dir` only to test a particular native build.

Browser visual verification was deferred because the browser-control host was unavailable; the user chose to review the page directly. The latest integration result and completed checks are recorded in each test run's `results.json`.

## Files

`start.sh` / `stop.sh` are the entry points; `demo.sh` / `demo_cli.py` control replay; `demo_scenario.py` prepares the bounded random order flow; `server.py`, `bridge.py`, `protocol.py`, `services.py`, `deployment.py`, `demo.py` and `service_host.py` are the local adapter and service lifecycle support; `static/` contains the frontend and history index; `tests/` contains verification. Generated binaries, data and logs are isolated in the ignored `runtime/` directory.
