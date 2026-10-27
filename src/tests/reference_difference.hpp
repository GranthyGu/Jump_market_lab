#pragma once

#include "market_publisher/protocol/market_data.hpp"

#include <algorithm>
#include <set>
#include <stdexcept>

namespace marketlab::md::test {
inline bool wire_order(const Level& a, const Level& b) {
  if (a.side != b.side) return a.side < b.side;
  return a.side == Side::Buy ? a.price > b.price : a.price < b.price;
}

// Keep the previous implementation as an independent test oracle and benchmark baseline.
inline void reference_validate_levels(const std::vector<Level>& levels, bool delta) {
  if (levels.size() > (delta ? 4 : 2) * depth) throw std::runtime_error("Invalid reference depth");
  std::set<std::pair<Side, std::int32_t>> seen;
  std::size_t buys = 0, sells = 0;
  for (const auto& x : levels) {
    if ((x.side != Side::Buy && x.side != Side::Sell) || x.price <= 0 ||
        !((x.quantity > 0 && x.num_orders > 0) || (delta && x.quantity == 0 && x.num_orders == 0)) ||
        !seen.emplace(x.side, x.price).second)
      throw std::runtime_error("Invalid reference level");
    x.side == Side::Buy ? ++buys : ++sells;
  }
  if ((!delta && (buys > depth || sells > depth)) || !std::is_sorted(levels.begin(), levels.end(), wire_order))
    throw std::runtime_error("Invalid reference ordering/depth");
}

inline void reference_validate(const Book& book) {
  if (book.symbol_id == 0) throw std::runtime_error("Invalid reference symbol");
  reference_validate_levels(book.levels, false);
}

inline std::vector<Level> reference_map_difference(const Book& before, const Book& after) {
  if (before.symbol_id != after.symbol_id) throw std::runtime_error("Different symbols");
  using Key = std::pair<Side, std::int32_t>;
  std::map<Key, Level> old, now;
  for (const auto& x : before.levels) old.emplace(Key{x.side, x.price}, x);
  for (const auto& x : after.levels) now.emplace(Key{x.side, x.price}, x);
  std::vector<Level> changes;
  for (const auto& [key, x] : old) if (!now.contains(key)) changes.push_back({x.side, x.price, 0, 0});
  for (const auto& [key, x] : now) if (!old.contains(key) || old.at(key) != x) changes.push_back(x);
  std::sort(changes.begin(), changes.end(), wire_order);
  return changes;
}

inline std::vector<Level> reference_difference(const Book& before, const Book& after) {
  reference_validate(before);
  reference_validate(after);
  return reference_map_difference(before, after);
}

// Ablation baseline: keep the old map algorithm but use the new linear validator.
inline std::vector<Level> map_with_linear_validation(const Book& before, const Book& after) {
  validate_book(before);
  validate_book(after);
  return reference_map_difference(before, after);
}
}  // namespace marketlab::md::test
