# MarketLab 2026 — Task 2: Recovery and Planned Restarts

Status: approved instructor specification.

Extend the Task 1 exchange with trade persistence, recovery, and planned matching-engine restarts. Task 1 requirements remain in effect, except that this task adds recovery requirements where restart recovery was previously out of scope.

## Trade persistence and recovery

- Write completed trades to disk. Each trade must be recorded before its trade notifications are sent, so no reported trade disappears after a matching-engine process crash.
- Recovery of completed trades is mandatory.
- Each stored trade includes trade ID, timestamp, symbol, price, quantity, and both participants' order IDs and usernames.
- Recovered trades retain their original trade IDs and timestamps. New trades must not reuse existing trade IDs. All timestamps remain uint64 Unix nanoseconds.
- Recovery of active orders after an unexpected crash is a bonus. Students may instead cancel all active orders after that crash, but clients must receive cancellation notifications.
- Students freely choose the storage format or database and replication mechanism.

## Trade-query API

- Provide a user-facing API for querying completed trades, including recovered trades.
- Recovery must be transparent to users: the same API works before and after a matching-engine restart. Users do not need to inspect storage files or use a separate recovery interface.
- Query results preserve the original trade records, IDs, and timestamps.
- The exact query API and its wire format are not prescribed by this specification; students must document their interface.

## Planned matching-engine restarts

- Support zero downtime during a planned matching-engine restart, using a duplicate/backup matching-engine instance and a router to route traffic.
- Requests may briefly queue during switchover, but must not be lost, duplicated, or rejected because of the restart.
- Preserve active orders, remaining quantities, and FIFO priority during a planned restart. Canceling all active orders is not allowed in this case.
- Zero downtime during an unexpected crash is not required.

## Failure scope

- Handle matching-engine process crashes, including kill -9.
- Crashes of the gateway, publisher, or router, machine failures, and power loss are outside the required scope.

## Required demonstrations

1. **Unexpected crash:** execute trades, kill the matching-engine process with kill -9, restart it, and use the normal trade-query API to show that reported trades remain available with their original contents, IDs, and timestamps. Demonstrate cancellation notifications for discarded active orders, or active-order recovery for the bonus.
2. **Planned restart:** restart a matching engine while order requests continue arriving. Demonstrate switchover through the router without losing, duplicating, or rejecting requests because of the restart, and verify that active orders, remaining quantities, and FIFO priority are preserved.

Students explain their persistence, replication, recovery, and switchover designs. No component implementation, storage schema, replication protocol, or recovery algorithm is supplied.