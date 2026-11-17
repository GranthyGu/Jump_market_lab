#pragma once

#include "matching_engine/core/engine.hpp"

namespace marketlab::orders::spreads {
struct Slice {
  Quantity quantity = 0;
  std::uint8_t route = no_slot;  // no_slot means the local real book.
};

// At most ten implied segments, separated by at most eleven direct prefixes.
// Adjacent direct prices share a slice; their actual prices stay in FIFO pieces.
struct Plan {
  std::array<Slice, 2 * 2 * route_depth + 1> slices{};
  std::array<Quantity, 12> prefixes{};
  Quantity direct = 0, remaining = 0;
  std::uint8_t count = 0;
  bool refill = false;
};

Plan select(const MatchingEngine&,
            std::uint8_t target,
            const Views&,
            const persistence::CompactOrder& incoming);

struct SourcePiece {
  OrderId order = 0;
  std::uint32_t user = 0;
  Price price = 0;
  Quantity quantity = 0;
};

// A cursor is round-local, and advances in source FIFO order even if responses
// arrived in a different order. It never retains pointers into mutable orders.
class PieceCursor {
 public:
  explicit PieceCursor(std::span<const SourcePiece> pieces)
      : pieces_(pieces) {
  }

  const SourcePiece& head() const;
  Quantity remaining() const;
  void consume(Quantity);

  std::uint32_t index() const {
    return index_;
  }

  bool empty() const {
    return index_ == pieces_.size();
  }

 private:
  std::span<const SourcePiece> pieces_;
  std::uint32_t index_ = 0;
  Quantity consumed_ = 0;
};

struct Identity {
  OrderId order = 0;
  std::uint32_t user = 0;
  Price price = 0;
};

struct Combination {
  Identity spread, a, x;
  persistence::ExecutionKey key;
  Timestamp timestamp = 0;
  Quantity quantity = 0;
  std::uint8_t route = 0;
  Side spread_side{};
};

Combination pair(std::uint8_t route,
                 const persistence::CompactOrder& incoming,
                 const SourcePiece& first,
                 const SourcePiece& second,
                 Quantity quantity,
                 persistence::ExecutionKey,
                 Timestamp);

// Update a local FIFO piece after a suballocation. Splitting one source order
// against several counterparties must emit incremental owned-order updates.
persistence::CompactOrder allocated(const MatchingEngine::FillPiece&, Quantity used);

// One record and notifications only for the actual local order. Remote order
// identities are references, never inserted into the active-order index.
void record(MatchingEngine&,
            persistence::Result&,
            const Topology&,
            const Combination&,
            const persistence::CompactOrder& local_after);
}  // namespace marketlab::orders::spreads
