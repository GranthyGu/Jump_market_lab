# MarketLab 2026 — Task 3: Calendar Spreads and Implied-Out Trading

Status: approved instructor specification. This is the final task.

Extend the exchange from Tasks 1 and 2 to support front-month/far-month calendar spreads and implied-out trading. Read the CME documentation below and apply its relevant calendar-spread mechanics; students design the implementation and any protocol extensions.

> **MANDATORY: The front-month and far-month futures MUST run on DIFFERENT matching-engine shards. Implied-out trading must work across those shards. A single-shard implementation does not satisfy Task 3.**
>
> **Market data: merge direct and implied liquidity into one price level, but report quantity (direct only) and impliedQuantity (implied only) separately, without overlap.**

## Spread symbols

- Support a tradable 1:1 calendar-spread symbol S = A - B, where A is the front-month future and B is the far-month future of the same product.
- Buying S means buying A and selling B; selling S means selling A and buying B, in equal quantities.
- Accept limit orders and cancellations on spread symbols and support direct spread-to-spread matching.
- Spread prices may be positive, zero, or negative. Adapt Task 1's positive-price validation for spread symbols and document the applicable spread price-limit rules.

## Cross-shard matching

- **A and B must run on different matching-engine shards. Placing both underlying futures on one shard does not satisfy this task.**
- Students choose where the spread book runs and design the cross-shard communication and coordination needed for implied execution.
- Coordinated execution must protect source quantities across shards and preserve the Task 2 persistence, recovery, and planned-restart requirements.

## Implied-out trading

- Use resting spread orders together with resting orders in one underlying future to generate executable implied bids and asks in the other underlying future. Support both A and B as target books.
- Implied prices and available quantities must be reflected in the underlying futures books and their published market data, including the top-five-level output required by Task 1.
- Merge direct and implied liquidity at the same price into one published level. Each underlying level contains price, quantity, impliedQuantity, and numOfOrders. quantity is the remaining direct-order quantity only; impliedQuantity is the executable implied quantity only. These fields are mutually exclusive categories: do not include implied quantity in quantity. Their sum is the level's total executable quantity.
- Implied liquidity must be executable by incoming futures orders, not merely displayed.
- Source-order fills and cancellations must update or remove all affected implied liquidity. Sources share their remaining quantities across direct and implied execution paths; the same quantity must not be consumed twice.
- Execute the related legs together, without leaving a partially executed spread. Follow the relevant CME matching-priority rules when direct and implied liquidity compete. Implied bids must not match implied asks.
- Keep order notifications, trade records, trade queries, and Task 2 recovery behavior consistent with spread and implied executions. Document how implied liquidity contributes to the published numOfOrders field.

Required implication scope is implied-out for the 1:1 calendar spread described above. Implied-in and more complex spread strategies are extensions.

## Example and demonstration

With S = A - B, a resting sell order for 10 spreads at -3 and a resting sell order for 6 contracts of B at 83 create an implied ask in A at 80 for 6 contracts.

If an incoming order buys 4 contracts of A through this path, the spread source has 6 remaining, the B source has 2 remaining, and the implied ask in A has 2 remaining. Canceling either remaining source removes this implied path.

Demonstrate direct spread trading, implied bids and asks in both futures on different shards, partial fills, source cancellation, and merged underlying market-data levels with separate direct and implied quantities. Explain how shared source quantities are protected from duplicate execution across shards.

## Required CME reading

- [Implied Orders](https://cmegroupclientsite.atlassian.net/wiki/spaces/EPICSANDBOX/pages/457095346/Implied%2BOrders) — implied liquidity, restrictions, and publication behavior.
- [Implied Orders — Examples](https://cmegroupclientsite.atlassian.net/wiki/spaces/EPICSANDBOX/pages/457705043/Implied%2BOrders%2B-%2BExamples) — implied-out construction and coordinated execution across markets.
- [Futures Implied Order Matching Priority](https://cmegroupclientsite.atlassian.net/wiki/spaces/EPICSANDBOX/pages/457096650/Futures%2BImplied%2BOrder%2BMatching%2BPriority) — priority when liquidity sources compete.

Use these documents for the relevant trading behavior. The project's existing gateway and student-designed market-data protocols remain the interfaces; implementing CME's wire protocols is not required.