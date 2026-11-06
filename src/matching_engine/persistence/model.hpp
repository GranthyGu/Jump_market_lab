#pragma once
#include <atomic>
#include <chrono>
#include <filesystem>
#include <functional>
#include <limits>

#include "shared_data/orders/engine_link.hpp"

namespace marketlab::orders::persistence {
using Name = std::array<char, 32>;
struct StoredOrder {
  OrderState state{};
  Name username{};
  std::uint64_t arrival = 0;
};
struct TradeRecord {
  std::uint64_t id = 0, timestamp = 0;
  Name symbol{};
  Price price = 0;
  Quantity quantity = 0;
  OrderId buy_order = 0, sell_order = 0;
  Name buyer{}, seller{};
  bool operator==(const TradeRecord&) const = default;
};
struct Change {
  bool erase = false;
  StoredOrder order{};
};
struct Event {
  LinkKind kind{};
  LinkHeader context{};
  std::array<std::uint8_t, max_client_frame> payload{};
  std::size_t size = 0;
};
struct Result {
  std::vector<TradeRecord> trades;
  std::vector<Change> changes;
  std::vector<Event> events;
  std::size_t limit = 8 * 1024 * 1024;
  std::size_t bytes() const {
    return trades.capacity() * sizeof(TradeRecord) + changes.capacity() * sizeof(Change) +
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
    changes.clear();
    events.clear();
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
  std::chrono::milliseconds checkpoint_interval{600000};
  std::function<void(Stage, std::uint64_t)> hook;
};
struct Boundary {
  std::uint64_t seq = 0, next_order = 1, next_trade = 1, trade_end = 0, trade_count = 0;
};
}  // namespace marketlab::orders::persistence
