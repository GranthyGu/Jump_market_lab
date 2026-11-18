#pragma once
#include "matching_engine/core/engine.hpp"

namespace marketlab::orders::spreads {
// Reuse captured real five-level prices, merge at most two routes, then truncate.
// The returned value owns every byte, so a later publish permit cannot expose
// working state from a newer command.
MarketBookSnapshot merged_market(const MarketBookSnapshot& real, std::uint8_t target, const Views&);

// Owner-thread immutable market candidates. All storage is allocated at startup;
// a later permit can never sample the newer working order book.
class MarketPublication {
 public:
  MarketPublication(MatchingEngine&, const OrderConfig&, std::uint16_t shard);
  void reset();
  void capture(std::uint64_t sequence, const Views&);
  void permit(std::uint64_t sequence);
  void copy(MarketSnapshot&) const noexcept;

  std::size_t pending() const {
    return size_;
  }

 private:
  struct Candidate {
    std::uint64_t seq = 0;
    MarketSnapshot snapshot;

    explicit Candidate(std::size_t symbols)
        : snapshot(symbols) {
    }
  };

  MatchingEngine& core_;
  Topology topology_;
  MarketSnapshot visible_;
  // Compare the latest capture's inputs; retained candidates own their merged
  // books. A new sequence always populates its own slot, even for equal inputs.
  MarketSnapshot real_, last_real_;
  Views last_views_{};
  std::vector<Candidate> slots_;
  std::size_t head_ = 0, size_ = 0;
  std::uint64_t permitted_ = 0;
};
}  // namespace marketlab::orders::spreads
