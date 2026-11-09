# MarketLab 2026: Build an Exchange

Status: approved instructor specification.

Implementation layout, file migration records, and independent component builds:

[Build and run guide](docs/EXCHANGE_RUN.md), [network interfaces and port ownership](docs/NETWORK_INTERFACES.md).

The default C++ deployment runs four independent matching-engine processes for
1,000 symbols, with 250 symbols per shard. Symbol ID `n` belongs to shard
`1 + (n - 1) % 4`. The gateway serves concurrent clients over one persistent
order connection per shard (TCP 7201–7204); each shard processes orders in
sequence to preserve price-time priority. The publisher samples all four shards
in parallel over TCP 9201–9204. Start all four with
`./src/scripts/run_matching_engine.sh`.

Each matching thread exclusively owns its live books. Market-data adapter threads
request coherent snapshots through a bounded atomic mailbox; the matching thread
copies top-five levels into preallocated buffers between commands. See the
[snapshot mailbox design and latency comparison](docs/matching_engine/SNAPSHOT_MAILBOX_DESIGN_AND_RESULTS.md).

Current C++ deployment publishes public market data once per message to UDP multicast
`239.10.10.1:9001`. Every subscriber joins the same group; there is no per-client
UDP destination list. The default interface is loopback for local demonstrations;
use `--interface LOCAL_IPV4` on the publisher and each observer for LAN deployment.
The TCP snapshot query service remains on port 9100.

All shell entry points are in `src/scripts/`:

- `build.sh`: build runtime programs.
- `run_matching_engine.sh`, `run_gateway.sh`, `run_publisher.sh`: start services.
- `run_market_client.sh`: maintain public books with UDP reorder buffering and automatic TCP recovery.
- `run_snapshot_client.sh`: interactive snapshot queries over publisher TCP (default port 9100).
- `run_client.sh`: terminal login with hidden password input, followed by trading.

Run each service in its own terminal. Start a market client with
`./src/scripts/run_market_client.sh` and a trading client with
`./src/scripts/run_client.sh`. Both support `--help` and `--build-dir`;
use `--interface LOCAL_IPV4` for the market client or `--host GATEWAY_IPV4`
for the trading client when running across machines.

Open `./src/scripts/run_snapshot_client.sh`, then enter `query 1` or a symbol ID.
Enter `quit` to exit. Add `--host PUBLISHER_IPV4 --port 9100` for a remote publisher;
`--symbol 1` remains available for a single query followed by exit.

`src/CMakeLists.txt` is the full-exchange build entry point and supplies shared
C++20, thread, dependency and output-directory settings. Component CMakeLists
files own their targets and can be configured independently. Optional tests are
disabled by the normal `./src/scripts/build.sh` build. Shared settings formerly
in `src/cmake/MarketLab.cmake` are now integrated into `src/CMakeLists.txt`.
Runtime prompts and log messages use English.

The four presentation test suites (gateway, publisher, engine, and integration)
are described in [tests/README.md](tests/README.md). Build them once with
`./tests/build.sh`, then run `./tests/run_all.sh` or a component's `run.sh`.
Each run saves correctness results, raw latency samples, metrics and logs.

## Project objective

Implement an exchange with a TCP order gateway, matching engines sharded by symbol, and a UDP market-data publisher. Students work individually and may implement all core components in C++20 or Python. The required runtime platform is Linux.

A central learning objective is to design communication between processes. Students choose their own IPC mechanisms, libraries, internal message formats, and component implementation. Components must support deployment across remote machines.

Performance should inform the design, but there are no hard throughput or latency targets.

## Architecture

The gateway, matching engines, and market-data publisher must run in separate processes.

### Gateway

- Manage client TCP sessions and authenticate users against preconfigured usernames and passwords.
- Receive limit-order and cancellation requests and route them to the appropriate matching-engine shard.
- Deliver request responses, order-status changes, and trade notifications to clients.
- Return all of a user's active orders after every successful login.
- One gateway process is sufficient.

### Matching engines

- Support multiple matching-engine processes, with symbols partitioned across shards.
- Configure symbol-to-shard assignments before startup; dynamic resharding is not required.
- Maintain order books and implement the matching and cancellation rules below.

### Market-data publisher

- Publish market data over UDP using a protocol designed by the student.
- Publish the top five bid price levels and top five ask price levels for each product every second, even when the book has not changed.
- Each price level contains price, total remaining quantity, and number of active orders.
- Students may use snapshots, deltas, or a combination. They must explain and implement their packet-loss recovery design.
- Students choose UDP unicast or multicast, receiver configuration, and how to represent fewer than five levels.
- The supplied starter package does not define a market-data wire protocol or an internal IPC protocol.

## Trading rules

### Supported operations

- Limit orders and full cancellation of an order's remaining quantity.
- Unfilled quantities remain on the book until canceled. No expiry or additional order types are required.
- The exchange assigns uint64 order IDs that are unique across all shards.

### Matching

- Match at the best available price first, then FIFO within a price level.
- Execute trades at the resting order's price.
- Allow partial fills; unfilled remaining quantity stays on the book.
- Self-trading is allowed. Crossing orders from the same user match normally.
- Every valid new order receives an acceptance before any fill notifications, even when it immediately fills completely.
- Each fill produces a trade message and an updated order status for each participating order. Order status includes original, filled, and remaining quantities.

### Validation and price limits

- Order prices and quantities are positive int32 values; symbols must be configured.
- Each symbol has a preconfigured previous-day close price.
- The lower price limit is truncation of 90% of the previous close; the upper price limit is truncation of 110% of the previous close.
- Both limits are inclusive.
- Balance, position, and margin checks are not required.

### Cancellation

- Users may cancel only their own orders.
- A successful cancellation removes the entire remaining quantity.
- Reject cancellation of an unknown, already filled, already canceled, or other user's order.

## Sessions and login snapshots

- Credentials are preconfigured.
- Each user may have only one active logged-in TCP session. A successful new login disconnects the existing session for that user.
- Open orders remain active when a client's TCP connection closes.
- After a successful login response, send all of that user's active orders, including partially filled orders with their remaining quantities.
- Active-order snapshot messages contain an isEnd flag. Always finish with a message containing isEnd=true and no order data, including when the user has no active orders.
- Login snapshots use a distinct message type and may be interleaved with live updates; live delivery need not wait for the snapshot to finish.
- Reconciliation of interleaved snapshot records and live updates is not a requirement for this project.
- Restart recovery is not required; in-memory exchange state is sufficient.

## Gateway binary protocol

Supply matching protocol definitions in C++20 and Python. Use packed structs, fixed-width fields, and little-endian encoding. Students may extend the basic protocol.

### Basic types

| Value | Representation | Meaning |
|---|---|---|
| SymbolName | char[32] | Null-terminated symbol name |
| Username | char[32] | Null-terminated username |
| Password | char[32] | Null-terminated password |
| Price | int32 | Positive integer order or trade price |
| Quantity | int32 | Positive submitted quantity; status fields may also contain zero |
| OrderId | uint64 | Assigned by the exchange, unique across shards |
| RequestId | uint64 | Chosen by the client for request/response correlation |
| TradeId | uint64 | Assigned by the exchange |
| Timestamp | uint64 | Unix time in nanoseconds, for every timestamp in the project |

### Common header

The common header contains these fields:

| Field | Type | Meaning |
|---|---|---|
| messageType | uint16 | Message kind |
| messageLength | uint32 | Total message length, including the header |
| requestId | uint64 | Client request identifier; unsolicited notifications use zero |

Responses echo the corresponding requestId. Timestamps appear only in server-message payloads, not in the common header or client-request payloads.

The packed header is 14 bytes. TCP is a byte stream: a message may span multiple reads, and a read may contain multiple messages. Use messageLength to frame complete messages.

Detailed handling of malformed or bad input is not required; implementations may drop bad messages or disconnect the client.

### Required message behavior

- Login request and success/failure response. The protocol uses LoginAccepted for success and RequestRejected for failure.
- Active-order snapshot messages with an isEnd flag.
- New limit-order request and acceptance/rejection response.
- Full remaining-quantity cancel request and success/rejection response.
- Order-status notifications for changes, including partial and complete fills.
- Trade notifications containing tradeId, symbol, the recipient's orderId and side, trade price, trade quantity, and exchange timestamp.
- A self-trade generates a trade notification for each participating order.

OrderUpdate is shared by acceptance, partial fill, full fill, and successful cancellation. Its status field identifies the change. Acceptance and successful cancellation echo the requestId; fill updates use requestId=0. Rejections use RequestRejected with the rejected request's requestId.

### wire conventions

The C++20 and Python definitions in [src/shared_data/orders/protocol.h](src/shared_data/orders/protocol.h) and protocol.py use these conventions:

- Serialize all fields in the exact order shown, without alignment padding. All multi-byte integers are little-endian.
- side, status, and isEnd occupy one byte each. isEnd is encoded as 0 or 1.
- Fixed-size character fields hold a null-terminated string and are padded with zero bytes.
- Client request IDs are nonzero; zero is reserved for unsolicited notifications.
- All server payloads begin with timestamp: uint64, expressed as Unix nanoseconds. On an order event this identifies the exchange event time; snapshot messages identify the time the snapshot record is produced.
- Snapshot records and the terminal snapshot marker echo the login requestId so clients can identify their snapshot stream while live messages are interleaved.
- A snapshot record has isEnd=0. The terminal marker has isEnd=1 and its entire OrderState is zero-filled and ignored. This retains one fixed-size snapshot struct, including for an empty snapshot.
- All sizes below include the 14-byte header where stated. No native C++ bool or platform-dependent integer sizes appear on the wire.

### numeric codes

| Message type | uint16 code | Direction |
|---|---|---|
| LoginRequest | 1 | Client to gateway |
| LoginAccepted | 2 | Gateway to client |
| NewOrderRequest | 3 | Client to gateway |
| CancelRequest | 4 | Client to gateway |
| OrderUpdate | 5 | Gateway to client |
| Trade | 6 | Gateway to client |
| ActiveOrderSnapshot | 7 | Gateway to client |
| RequestRejected | 8 | Gateway to client |

| Enum | Representation | Values |
|---|---|---|
| Side | uint8 | Buy=1, Sell=2 |
| OrderStatus | uint8 | Accepted=1, PartiallyFilled=2, Filled=3, Canceled=4 |
| RejectReason | uint16 | InvalidCredentials=1, InvalidOrder=2, CancelFailed=3 |

These are the basic codes; students may extend them. Generic InvalidOrder includes invalid symbols, nonpositive price/quantity, and prices outside the configured limits. CancelFailed covers the cancellation failures specified above. Invalid credentials must not disconnect an existing authenticated session for that username.

### shared OrderState layout

OrderState is a packed 58-byte record embedded in OrderUpdate and ActiveOrderSnapshot. It does not have its own header.

| Field, in wire order | Type | Meaning |
|---|---|---|
| orderId | uint64 | Exchange-assigned order ID |
| symbol | char[32] | Symbol name |
| side | uint8 | Side enum |
| price | int32 | Limit price |
| originalQuantity | int32 | Original submitted quantity |
| filledQuantity | int32 | Cumulative executed quantity |
| remainingQuantity | int32 | Quantity still active on the book |
| status | uint8 | OrderStatus enum |

For an accepted order, filledQuantity=0 and remainingQuantity=originalQuantity. For a filled order, filledQuantity=originalQuantity and remainingQuantity=0. For a canceled order, remainingQuantity=0 and filledQuantity retains the executed amount; the canceled amount is originalQuantity-filledQuantity. Active-order snapshots contain only Accepted or PartiallyFilled orders.

### message payload layouts

Each message is the common header followed by the payload below. Field lists are in wire order.

| Message | Payload fields | Payload bytes | Total bytes |
|---|---|---|---|
| LoginRequest | username: char[32], password: char[32] | 64 | 78 |
| LoginAccepted | timestamp: uint64 | 8 | 22 |
| NewOrderRequest | symbol: char[32], side: uint8, price: int32, quantity: int32 | 41 | 55 |
| CancelRequest | orderId: uint64 | 8 | 22 |
| OrderUpdate | timestamp: uint64, order: OrderState | 66 | 80 |
| Trade | timestamp: uint64, tradeId: uint64, orderId: uint64, symbol: char[32], side: uint8, price: int32, quantity: int32 | 65 | 79 |
| ActiveOrderSnapshot | timestamp: uint64, isEnd: uint8, order: OrderState | 67 | 81 |
| RequestRejected | timestamp: uint64, reason: uint16 | 10 | 24 |

Trade uses requestId=0. Each participant receives the trade details for its own order. A self-trading user receives both participant notifications. The distinct ActiveOrderSnapshot message type permits snapshots and live notifications to be delivered concurrently.

## Starter package and repository sharing

The starter package contains only this specification and the C++20/Python gateway protocol definitions, including basic types. No example clients, build files, or component implementations are supplied.

Students will share repositories forked from the instructor's repository. No separate submission checklist is specified here.

The market client now confirms epochs through TCP, buffers out-of-order deltas,
and recovers missing or silent feeds automatically. Recovery settings live in
`src/config/publisher.conf` (`client_*`); default reorder wait is 200 ms, LATE
is 1500 ms, STALE is 3000 ms, and pending data is limited to 64 batches per
symbol and a 16 MiB global budget. UDP full snapshots default to every 10
batches (10 seconds), with only a snapshot sent in those batches. In other
batches, the publisher compares encoded sizes and sends a snapshot only when
the delta is larger; equal-size and empty deltas are still sent. Each symbol
gets exactly one UDP message per batch. `run_market_client.sh --raw` retains
packet diagnostics.
