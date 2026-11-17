#pragma once
#include <functional>

#include "shared_data/market_data/market_data.hpp"
#include "shared_data/orders/engine_link.hpp"

namespace marketlab::orders {
// A snapshot slot has no pointers into the engine and allocates only at startup.
struct MarketBookSnapshot {
  std::uint16_t symbol_id = 0;
  std::uint8_t count = 0;
  bool extended = false;
  std::array<md::Level, 2 * md::depth> levels{};
};

struct MarketSnapshot {
  explicit MarketSnapshot(std::size_t symbols)
      : books(symbols) {
  }

  md::Epoch epoch{};
  std::vector<MarketBookSnapshot> books;

  // Called by the reader while it owns the completed slot. Allocations here
  // cannot hold up the matching thread or change the source snapshot.
  md::ShardView materialize() const {
    md::ShardView result{epoch, {}};
    for (const auto& book : books)
      result.books.emplace(book.symbol_id,
                           md::Book{book.symbol_id,
                                    {book.levels.begin(), book.levels.begin() + book.count},
                                    book.extended});
    return result;
  }
};

// One owner calls command(), copy_market() and market_view(). Other threads
// request snapshots through SnapshotMailbox; they never access live books.
// The receiver must copy emitted spans before returning. Durable backends may
// retain a copy of Emit until poll()/drain(); captures must outlive that work.
// Callbacks must not reenter command(). Destruction never invokes callbacks.
class EngineBackend {
 public:
  using Emit = std::function<void(LinkKind, const LinkHeader&, std::span<const std::uint8_t>)>;
  virtual ~EngineBackend() = default;
  virtual void command(const LinkHeader&, std::span<const std::uint8_t>, const Emit&) = 0;

  virtual int notification_fd() const noexcept {
    return -1;
  }

  virtual void poll() {
  }

  virtual void drain() {
  }

  virtual std::size_t symbol_count() const noexcept = 0;
  // The destination must have exactly symbol_count() preallocated entries.
  virtual void copy_market(MarketSnapshot&) noexcept = 0;

  md::ShardView market_view() {
    MarketSnapshot snapshot(symbol_count());
    copy_market(snapshot);
    return snapshot.materialize();
  }
};
}  // namespace marketlab::orders
