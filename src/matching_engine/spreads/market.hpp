#pragma once
#include "matching_engine/core/engine.hpp"

namespace marketlab::orders::spreads {
// Read aggregated real prices, merge at most two route views, then truncate.
// The returned value owns every byte, so a later publish permit cannot expose
// working state from a newer command.
MarketBookSnapshot merged_market(const MatchingEngine&,
                                 std::uint16_t symbol,
                                 std::uint8_t target,
                                 const Views&);
}  // namespace marketlab::orders::spreads
