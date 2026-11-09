#pragma once

// MarketLab 2026 gateway wire definitions (C++20).
// See README.md for behavior, field meanings, and extension rules.
// All integers are little-endian; structs contain no alignment padding.
// These are wire records, not an implementation of TCP framing or exchange logic.

#include <bit>
#include <cstddef>
#include <cstdint>
#include <type_traits>

namespace marketlab {

// Direct struct serialization is valid only on a little-endian host.
static_assert(std::endian::native == std::endian::little);

using SymbolName = char[32];
using Username = char[32];
using Password = char[32];
using Price = std::int32_t;
using Quantity = std::int32_t;
using OrderId = std::uint64_t;
using RequestId = std::uint64_t;
using TradeId = std::uint64_t;
using Timestamp = std::uint64_t;  // Unix nanoseconds, never monotonic clock time.

enum class MessageType : std::uint16_t {
  LoginRequest = 1,
  LoginAccepted = 2,
  NewOrderRequest = 3,
  CancelRequest = 4,
  OrderUpdate = 5,
  Trade = 6,
  ActiveOrderSnapshot = 7,
  RequestRejected = 8,
  TradeHistoryRequest = 9,
  TradeHistoryRecord = 10,
  TradeHistoryEnd = 11,
};

enum class Side : std::uint8_t { Buy = 1, Sell = 2 };

enum class OrderStatus : std::uint8_t {
  Accepted = 1,
  PartiallyFilled = 2,
  Filled = 3,
  Canceled = 4,
};

enum class RejectReason : std::uint16_t {
  InvalidCredentials = 1,
  InvalidOrder = 2,
  CancelFailed = 3,
  InvalidHistoryQuery = 4,
  HistoryUnavailable = 5,
  HistoryBusy = 6,
};

#pragma pack(push, 1)

struct Header {
  MessageType messageType;
  std::uint32_t messageLength;  // Includes Header; use sizeof(the full message).
  RequestId requestId;         // Echo requests; unsolicited notifications use 0.
};

// Embedded record with no header of its own.
struct OrderState {
  OrderId orderId;
  SymbolName symbol;
  Side side;
  Price price;
  Quantity originalQuantity;
  Quantity filledQuantity;
  Quantity remainingQuantity;
  OrderStatus status;
};

struct LoginRequest {
  Header header;
  Username username;
  Password password;
};

struct LoginAccepted {
  Header header;
  Timestamp timestamp;
};

struct NewOrderRequest {
  Header header;
  SymbolName symbol;
  Side side;
  Price price;
  Quantity quantity;
};

struct CancelRequest {
  Header header;
  OrderId orderId;
};

struct OrderUpdate {
  Header header;
  Timestamp timestamp;
  OrderState order;
};

struct Trade {
  Header header;
  Timestamp timestamp;
  TradeId tradeId;
  OrderId orderId;  // Recipient's participating order.
  SymbolName symbol;
  Side side;       // Recipient's side.
  Price price;
  Quantity quantity;
};

struct ActiveOrderSnapshot {
  Header header;        // Echo the login requestId, including on the end marker.
  Timestamp timestamp;
  std::uint8_t isEnd;    // 0 = record, 1 = terminal marker (not a native bool).
  OrderState order;     // All bytes zero and ignored when isEnd == 1.
};

struct RequestRejected {
  Header header;
  Timestamp timestamp;
  RejectReason reason;
};

// Query only the authenticated account. shard=0 starts a query across all
// shards; subsequent pages use the shard/throughOffset returned in the marker.
// Offsets are opaque, exclusive cursors in each shard's retained trade log.
inline constexpr std::uint16_t trade_history_page_limit = 256;
inline constexpr std::uint16_t history_more = 1, history_all_shards = 2;
struct TradeHistoryRequest {
  Header header;
  std::uint16_t shard;
  std::uint16_t limit;
  std::uint64_t afterOffset;
  std::uint64_t throughOffset;  // 0 captures the current committed prefix.
};
struct TradeHistoryRecord {
  Header header;
  Timestamp timestamp;  // Original trade time, not query time.
  TradeId tradeId;
  SymbolName symbol;
  Price price;
  Quantity quantity;
  OrderId buyOrderId;
  OrderId sellOrderId;
  Username buyer;
  Username seller;
};
struct TradeHistoryEnd {
  Header header;
  Timestamp timestamp;
  std::uint16_t shard;
  std::uint16_t flags;
  std::uint32_t count;
  std::uint64_t nextOffset;
  std::uint64_t throughOffset;
};

#pragma pack(pop)

static_assert(sizeof(Header) == 14);
static_assert(offsetof(Header, messageType) == 0);
static_assert(offsetof(Header, messageLength) == 2);
static_assert(offsetof(Header, requestId) == 6);
static_assert(sizeof(OrderState) == 58);
static_assert(sizeof(LoginRequest) == 78);
static_assert(sizeof(LoginAccepted) == 22);
static_assert(sizeof(NewOrderRequest) == 55);
static_assert(sizeof(CancelRequest) == 22);
static_assert(sizeof(OrderUpdate) == 80);
static_assert(sizeof(Trade) == 79);
static_assert(sizeof(ActiveOrderSnapshot) == 81);
static_assert(sizeof(RequestRejected) == 24);
static_assert(sizeof(TradeHistoryRequest) == 34);
static_assert(sizeof(TradeHistoryRecord) == 150);
static_assert(sizeof(TradeHistoryEnd) == 46);

template <typename T>
inline constexpr bool is_wire_record =
    std::is_standard_layout_v<T> && std::is_trivially_copyable_v<T> &&
    alignof(T) == 1;

static_assert(is_wire_record<Header> && is_wire_record<OrderState> &&
              is_wire_record<LoginRequest> && is_wire_record<LoginAccepted> &&
              is_wire_record<NewOrderRequest> && is_wire_record<CancelRequest> &&
              is_wire_record<OrderUpdate> && is_wire_record<Trade> &&
              is_wire_record<ActiveOrderSnapshot> &&
              is_wire_record<RequestRejected> && is_wire_record<TradeHistoryRequest> &&
              is_wire_record<TradeHistoryRecord> && is_wire_record<TradeHistoryEnd>);

// Value-initialize records to zero before assigning fields. Character fields
// require a null terminator and zero padding. Set messageType/messageLength
// explicitly. Receive bytes into a suitably sized record (e.g. with memcpy);
// packed fields may be unaligned, so do not expose pointers to their members.

}  // namespace marketlab
