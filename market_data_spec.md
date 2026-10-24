# Market-Data Output

For each product, publish the top **five bid levels** and **five ask levels** every second over UDP.

- Every one-second publication cycle must refresh **all configured symbols**, including symbols with unchanged or empty books.
- Bids are ordered from highest to lowest price; asks from lowest to highest.
- Each level contains **price**, **quantity** (total remaining quantity), and **numOfOrders** (number of active orders at that price).
- If fewer than five levels exist, students choose how to represent missing levels.

Students design the wire format and may use snapshots, deltas, or both.
