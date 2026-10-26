#pragma once

#include "market_data/io.hpp"

#include <mutex>

namespace marketlab::md {
inline constexpr auto publication_period = std::chrono::seconds(1);

struct Batch {
  Epoch epoch;
  std::uint64_t index;
  std::uint64_t timestamp;
  std::map<std::uint16_t, Book> books;
  std::map<std::uint16_t, Bytes> snapshots;
};

// Matching engines keep these aggregates as part of their own order books.
struct PriceLevelTotals { std::uint64_t quantity; std::uint64_t num_orders; };

// Bids must iterate from highest to lowest price; asks from lowest to highest.
// Call under the engine's state ownership/lock, then release before any socket I/O.
template <typename Bids, typename Asks>
Book copy_top_five(std::uint16_t symbol_id, const Bids& bids, const Asks& asks) {
  Book book{symbol_id, {}};
  book.levels.reserve(2 * depth);
  auto copy = [&](Side side, const auto& levels) {
    std::size_t count = 0;
    for (auto it = levels.begin(); it != levels.end() && count < depth; ++it, ++count) {
      const auto& [price, totals] = *it;
      book.levels.push_back({side, price, totals.quantity, totals.num_orders});
    }
  };
  copy(Side::Buy, bids);
  copy(Side::Sell, asks);
  validate_book(book);
  return book;
}

class Publisher {
 public:
  explicit Publisher(Config config);
  void run(const std::atomic_bool& stop);
  std::shared_ptr<const Batch> latest() const {
    std::lock_guard lock(latest_mutex_);
    return latest_;
  }
 private:
  void set_latest(std::shared_ptr<const Batch> batch) {
    std::lock_guard lock(latest_mutex_);
    latest_ = std::move(batch);
  }
  void sample_loop(const std::atomic_bool& stop, const std::atomic_bool& cancelled);
  Config config_;
  UdpSender sender_;
  std::vector<std::unique_ptr<ShardSource>> sources_;
  mutable std::mutex latest_mutex_;
  std::shared_ptr<const Batch> latest_;
};

// This simulator only supplies aggregate views; it does not execute orders.
class MockShard {
 public:
  MockShard(const Config& config, ShardConfig shard, bool is_static);
  void run(const std::atomic_bool& stop);
 private:
  ShardView generate(std::uint64_t step) const;
  Config config_;
  ShardConfig shard_;
  Epoch epoch_;
  bool static_;
  std::mutex mutex_;
  ShardView current_;
};
}  // namespace marketlab::md
