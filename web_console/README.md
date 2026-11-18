# MarketLab local console

A local browser interface for the existing C++ exchange. The layout follows `IMG_9700.PNG`: market books and engine controls on the left; Gateway output and independent Alice / Bob panels on the right. Everything added for the console lives in this directory. Existing source and configuration files are read without modification.

## Run

From the repository root:

```sh
./web_console/start.sh
```

The script opens **http://127.0.0.1:8080**, builds the current C++ services into `web_console/runtime/build` when needed, and starts the existing supervisor, eight matching engines, router, coordinator, Gateway and Publisher. Initial compilation can take a minute; follow **Engine control → System log**. Later launches reuse the build and persistent state. Python 3.10+, CMake 3.20+ and a C++20 compiler are required. The web layer uses Python's standard library and plain HTML/CSS/JavaScript, with no package install or CDN.

Keep the launch terminal open. Closing the browser tab does not stop the services. To stop gracefully, press **Ctrl-C** in that terminal, or run:

```sh
./web_console/stop.sh
```

All processes started by this console are stopped; attached processes belonging to another terminal are left running. Orders and trade history remain on disk. The console binds only to `127.0.0.1`.

Each owned service runs under a small guardian connected to the console by a pipe. If the console exits unexpectedly, the guardian detects the closed pipe and asks the native supervisor to drain and shut down gracefully. Native output is written directly to log files so losing the console does not break a service's output stream. Startup completes only after all shard routes are connected, the spread group is ready, and Publisher snapshots have been verified. Failed startup attempts stop the services they just created.

## Use

- **Log in:** Connect Alice and Bob independently. The default option reads the selected account's password from the local exchange configuration on the server. Uncheck it to enter a password. One exchange account can have only one connected session.
- **Trade:** Select a symbol, Buy / Sell, integer limit price and quantity. Click a market price to fill the most recently focused account's ticket. An order is accepted only after its real Gateway response; rejection and fill events appear in the account log.
- **Orders:** Shows the complete login snapshot plus subsequent exchange updates. Cancel by the row button or by the full order ID. IDs remain decimal strings throughout the browser/API, preserving all 64 bits.
- **History:** Query committed execution records, load additional pages, inspect direct / implied / calendar / butterfly details, or export the loaded records as CSV. Live session notifications are also available under **Fills**. Trade and log timestamps use UTC; the header clock uses browser local time.
- **Market depth:** Twenty-four ordinary symbols, with six per ordinary shard. Each page shows eight books in two columns of four: ITEM0001–0008, ITEM0009–0016, then ITEM0017–0024. The adjacent panel shows all six spread-related books together: A/B/C in the left column and SP_AB/SP_AC/BF_ABC in the right. Each book retains all five levels. Search filters both panels. The interface polls every 500 ms; the existing Publisher samples every second. Empty books remain empty until actual orders are placed.
- **Compact workspace:** The desktop workspace fits the browser window without page or panel scrolling. Market cards use half their group's width and share the available height; on short windows their type tightens to preserve five complete levels. Ordinary market pages always contain up to eight books. Orders and history use adaptive pagination. All retained records remain available through the page arrows.
- **Logs:** Publisher, Gateway and each trading session have their own terminal-font output, showing the latest entries that fit. Expand a log to filter by source, severity and text, page through older entries, pause updates, or download all filtered events. Hover over an entry for its full text. Green indicates success, purple trades, amber warnings, red errors. Engine process output and build logs are retained under `runtime/logs/`.

## Engine operations

| Control | Actual operation |
| --- | --- |
| Restart → shard / All shards | Sends the existing supervisor's planned A/B restart command, preserving its snapshot, replay, drain and promotion workflow. |
| Shutdown engines | Gracefully stops the owned cluster. Gateway and Publisher remain available for recovery. |
| Recover engines | Starts the cluster using the same data directory, refreshes public snapshots and reconnects previously logged-in accounts. |
| Interrupt → Pause / Resume shard | Sends SIGSTOP / SIGCONT to a specific owned matching-engine PID. |
| Interrupt → Crash recovery test | Sends SIGKILL to a selected spread-group member (shards 5–8); the existing supervisor performs recovery. |

Operations are serialized and display progress and actual failures. The console does not invent recovery semantics: an unresolved cross-shard execution can remain paused under the engine's existing rules. No recovery markers, journals or persisted boundaries are erased. Requests with uncertain outcomes are never automatically replayed; inspect active orders and committed history after reconnection.

Observed native-engine limitation: pausing a spread-group member beyond its connection timeout after a planned group restart can leave the group `PAUSED` after SIGCONT, with `join progress timeout` in the native log. The console displays this state; resuming a process does not bypass the engine's boundary checks. Ordinary-shard pause/resume, idle spread-member crash recovery, planned restarts, and graceful whole-cluster shutdown/recovery passed the integration run. Fixing this separate native recovery case would require changes outside this directory.

## Local data and configuration

Defaults are read from:

- `src/config/exchange.conf` — accounts, symbols, shard assignments and order endpoints.
- `src/config/publisher.conf` — multicast and TCP market snapshots.
- `src/config/engine_cluster.json` — supervisor and engine ports.

The initial default data directory is **`web_console/runtime/data`**. The console stores a symbol-configuration manifest alongside native state. When the symbol list, order or shard assignments change, it uses a stable **`web_console/runtime/data-<configuration-id>`** directory and preserves the previous directory. Subsequent launches with that configuration reuse its orders and history. This prevents the old 20-item data from being replayed with the new 24-item symbol IDs. The terminal and Workspace details dialog show the selected directory. An explicit `--data-dir` with a conflicting manifest is rejected.

Configuration is read when the console starts. It does not import data under `src/config/.marketlab-data`. For legacy console state without a manifest, the console identifies the old symbol mapping from its retained generated Router configuration; an ambiguous mapping requires an explicitly selected, verified compatible directory. Native recovery flags and pending transitions are never cleared by this process.

```sh
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

If a supervisor is already listening, startup attaches to it after checking its symbol configuration when available. Engine logs can be tailed from its reported log directory; Gateway and Publisher stdout are captured only for processes launched by this console. The Workspace details dialog shows the actual connected engine roots and log location.

If an older console left orphaned services behind, startup can stop those services before starting the current deployment. Cleanup requires an orphan parent PID, an exact native executable path under this runtime's `build/bin`, and a matching console configuration path. The process identity is checked again before SIGTERM; an audit is saved under `runtime/logs/orphan-cleanup-*.json`. Other processes occupying the configured ports are left alone and reported as a startup error. Cleanup retains all native data and does not override an unclean-shutdown recovery boundary.

## Implementation

```text
Browser (same-origin HTTP; 500 ms state updates)
  └─ server.py — loopback HTTP, static files, local request token
       ├─ bridge.py / protocol.py
       │    ├─ authenticated Gateway TCP: orders, snapshots, cancels, history
       │    └─ Publisher UDP + authoritative TCP snapshot recovery
       └─ services.py
            ├─ deployment.py — configuration identity and owned-orphan cleanup
            └─ service_host.py guardians
                 └─ existing cluster/run.py supervisor and native services
```

The market adapter verifies publisher epochs and contiguous sequence numbers, buffers bounded UDP reordering and falls back to TCP snapshots after gaps. Stale books are labelled. Account reconnection waits for the full active-order snapshot before enabling submissions. Passwords stay in server memory; configured passwords are not included in browser state. Mutations require a per-process request token and a matching local origin/Host.

The live journal retains 4,000 events, live fills retain 300 per session, and history loads 128 records per shard/request with a 10,000-record soft limit. Display pagination adapts to the viewport independently of these storage and query limits. These limits do not truncate the exchange's on-disk histories.

## Verify

After a normal first launch has built the native programs:

```sh
./web_console/tests/run.sh
```

Protocol tests cover packed frame sizes, fragmented reads, signed prices, 64-bit IDs, typed history and market recovery. Deployment tests cover configuration isolation, retained recovery flags, cleanup ownership checks, PID reuse and guardian shutdown. Integration tests run the real C++ services on isolated ports with fresh data beneath `runtime/tests/`, exercise the HTTP API through trading and recovery, kill the test console to verify automatic native shutdown, and restart it to verify persisted orders and history. Results and diagnostic logs are retained there. Override `--build-dir` only to test a particular native build.

Browser visual verification was deferred because the browser-control host was unavailable; the user chose to review the page directly. The latest integration result and completed checks are recorded in each test run's `results.json`.

## Files

`start.sh` / `stop.sh` are the entry points; `server.py`, `bridge.py`, `protocol.py`, `services.py`, `deployment.py` and `service_host.py` are the local adapter and service lifecycle support; `static/` contains the frontend; `tests/` contains verification. Generated binaries, data and logs are isolated in the ignored `runtime/` directory.
