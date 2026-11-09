#pragma once

#include "shared_data/market_data/market_data.hpp"

namespace marketlab::md {
// Matching engines keep these aggregates as part of their own order books.
struct PriceLevelTotals {
  std::uint64_t quantity;
  std::uint64_t num_orders;
};

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
}  // namespace marketlab::md
