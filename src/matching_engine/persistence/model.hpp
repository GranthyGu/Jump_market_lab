#pragma once
#include <atomic>
#include <chrono>
#include <filesystem>
#include <functional>
#include <limits>
#include <variant>

#include "shared_data/orders/engine_link.hpp"

namespace marketlab::orders::persistence {
using Name = std::array<char, 32>;

struct StoredOrder {
  OrderState state{};
  Name username{};
  std::uint64_t arrival = 0;
};

// Process-local identities keep change buffers and COW pages compact. The
// persistent format still contains names, expanded by the immutable dictionary.
struct CompactOrder {
  OrderId id = 0;
  std::uint64_t arrival = 0;
  Price price = 0;
  Quantity original = 0;
  Quantity filled = 0;
  Quantity remaining = 0;
  std::uint32_t user = 0;
  std::uint16_t symbol = 0;
  Side side{};
  OrderStatus status{};
};

inline CompactOrder compact_order(const OrderState& state,
                                  std::uint32_t user,
                                  std::uint16_t symbol,
                                  std::uint64_t arrival) {
  return {state.orderId,
          arrival,
          state.price,
          state.originalQuantity,
          state.filledQuantity,
          state.remainingQuantity,
          user,
          symbol,
          state.side,
          state.status};
}

struct TradeRecord {
  std::uint64_t id = 0, timestamp = 0;
  Name symbol{};
  Price price = 0;
  Quantity quantity = 0;
  OrderId buy_order = 0, sell_order = 0;
  Name buyer{}, seller{};
  bool operator==(const TradeRecord&) const = default;
};

struct ExecutionKey {
  OrderId aggressor = 0;
  std::uint32_t fill = 0;
  bool operator==(const ExecutionKey&) const = default;
};

struct ImpliedLegTrade : TradeRecord {
  ExecutionKey execution;
  bool operator==(const ImpliedLegTrade&) const = default;
};

struct SpreadLeg {
  OrderId order = 0;
  Name user{};
  Price price = 0;
  bool operator==(const SpreadLeg&) const = default;
};

struct SpreadExecution {
  TradeId id = 0;
  Timestamp timestamp = 0;
  Name symbol{};
  Price price = 0;
  Quantity quantity = 0;
  ExecutionKey execution;
  OrderId order = 0;
  Name user{};
  Side side{};
  SpreadLeg a, x;
  bool operator==(const SpreadExecution&) const = default;
};

// Each B identity contributes quantity contracts; identical identities represent
// two contracts from the same order. Half-tick B targets use adjacent prices.
struct ButterflyExecution : SpreadExecution {
  SpreadLeg b_second, c;
  bool operator==(const ButterflyExecution&) const = default;
};

enum class TradeType : std::uint8_t {
  Direct = 1,
  Leg = 2,
  Spread = 3,
  Butterfly = 4
};
using ExecutionRecord =
    std::variant<TradeRecord, ImpliedLegTrade, SpreadExecution, ButterflyExecution>;

inline TradeType trade_type(const ExecutionRecord& record) {
  return static_cast<TradeType>(record.index() + 1);
}

struct Change {
  bool erase = false;
  CompactOrder order{};
};

struct Event {
  LinkKind kind{};
  LinkHeader context{};
  // History responses bypass matching batches. Keep the hot matching event
  // slots at their original size even though history adds a larger wire record.
  std::array<std::uint8_t, sizeof(ActiveOrderSnapshot)> payload{};
  std::size_t size = 0;
};

struct Result {
  std::vector<TradeRecord> trades;
  // Group commands use typed records; ordinary commands keep their original
  // compact vector and wire format. Never mix the two representations in a batch.
  std::vector<ExecutionRecord> executions;
  std::vector<Change> changes;
  std::vector<Event> events;
  std::size_t limit = 8 * 1024 * 1024;

  std::size_t bytes() const {
    return trades.capacity() * sizeof(TradeRecord) +
           executions.capacity() * sizeof(ExecutionRecord) + changes.capacity() * sizeof(Change) +
           events.capacity() * sizeof(Event);
  }

  template <class T>
  void add(std::vector<T>& target, const T& value) {
    if (target.size() == target.capacity()) {
      const auto next = std::max<std::size_t>(8, target.capacity() * 2);
      if (next < target.capacity() ||
          (next - target.capacity()) > (limit - std::min(limit, bytes())) / sizeof(T))
        throw std::runtime_error("Single command result exceeds persistence buffer budget");
      target.reserve(next);
    }
    target.push_back(value);
  }

  void clear() {
    trades.clear();
    executions.clear();
    changes.clear();
    events.clear();
  }

  std::size_t trade_count() const {
    return trades.size() + executions.size();
  }
};
enum class Stage {
  BeforeTradeWrite,
  BeforeOrderWrite,
  TradeWritten,
  OrderWritten,
  BeforeTradeSync,
  BeforeOrderSync,
  TradeSynced,
  OrderSynced,
  BeforeSnapshotPublish,
  SnapshotPublished,
  BeforePrune,
  Emitting
};

struct Options {
  std::filesystem::path directory;
  std::size_t pending_slots = 128;
  std::size_t pending_bytes = 64 * 1024 * 1024;
  std::size_t command_bytes = 8 * 1024 * 1024;
  std::size_t sync_group = 64;
  std::size_t checkpoint_bytes = 256 * 1024 * 1024;
  std::size_t cow_bytes = 32 * 1024 * 1024;
  std::size_t query_slots = 64;
  std::size_t query_bytes = 64 * 1024 * 1024;
  std::size_t history_cache_pages = 1024;  // 8 MiB of B+ tree pages per shard.
  std::uint64_t history_checkpoint_bytes = 64 * 1024 * 1024;
  std::chrono::milliseconds checkpoint_interval{600000};
  std::function<void(Stage, std::uint64_t)> hook;
};

struct Boundary {
  std::uint64_t seq = 0, next_order = 1, next_trade = 1, trade_end = 0, trade_count = 0;
};
}  // namespace marketlab::orders::persistence
