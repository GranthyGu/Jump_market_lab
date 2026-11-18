#pragma once

#include "matching_engine/core/engine.hpp"

namespace marketlab::orders::spreads {
struct Slice {
  Quantity quantity = 0;
  std::uint8_t route = no_slot;  // no_slot means the local real book.
};

// Each route contributes bounded segments, interleaved with direct prefixes.
// Adjacent direct prices share a slice; their actual prices stay in FIFO pieces.
struct Plan {
  std::array<Slice, 2 * maximum_routes_per_side * route_depth + 1> slices{};
  std::array<Quantity, route_count> prefixes{};
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
  PieceCursor() = default;

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
  bool operator==(const Identity&) const = default;
};

struct Combination {
  Identity spread, a, x;
  Identity b_second, c;
  persistence::ExecutionKey key;
  Timestamp timestamp = 0;
  Quantity quantity = 0;
  std::uint8_t route = 0;
  Side spread_side{};
  bool operator==(const Combination&) const = default;
};

const Identity& participant(const Combination&, unsigned slot, unsigned part = 0);
void set_participant(Combination&, unsigned slot, const Identity&, unsigned part = 0);
void complete_butterfly(Combination&, const persistence::CompactOrder& incoming);
Quantity participant_quantity(const Combination&, unsigned slot, unsigned part = 0);

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
            const persistence::CompactOrder& local_after,
            unsigned part = 0);
}  // namespace marketlab::orders::spreads
