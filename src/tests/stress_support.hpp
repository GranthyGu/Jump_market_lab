#pragma once

#include "common/io.hpp"
#include "tests/reference_difference.hpp"

#include <algorithm>
#include <limits>
#include <random>
#include <stdexcept>

namespace marketlab::md::test {
inline void require(bool condition, const char* message) {
  if (!condition) throw std::runtime_error(message);
}

template <typename F> bool accepts(F fn) {
  try { fn(); return true; } catch (const std::exception&) { return false; }
}

inline Book full_book(std::uint16_t id, std::int32_t shift = 0, std::uint64_t step = 0) {
  Book book{id, {}};
  for (const auto side : {Side::Buy, Side::Sell})
    for (int i = 0; i < 5; ++i)
      book.levels.push_back({side, (side == Side::Buy ? 100 - i : 110 + i) + shift,
                            10 + step, 2 + step % 3});
  return book;
}

inline Book random_book(std::mt19937_64& random, std::uint16_t id = 1) {
  Book book{id, {}};
  for (const auto side : {Side::Buy, Side::Sell}) {
    std::array<std::int32_t, 16> prices{};
    for (std::size_t i = 0; i < prices.size(); ++i) prices[i] = 100 + static_cast<std::int32_t>(i);
    std::shuffle(prices.begin(), prices.end(), random);
    const auto count = random() % 6;
    for (std::size_t i = 0; i < count; ++i) {
      const auto quantity = random();
      const auto orders = random();
      book.levels.push_back({side, prices[i], quantity ? quantity : 1, orders ? orders : 1});
    }
  }
  std::sort(book.levels.begin(), book.levels.end(), wire_order);
  return book;
}

inline Book apply_changes(Book before, const std::vector<Level>& delta) {
  for (const auto& x : delta) {
    std::erase_if(before.levels, [&](const Level& y) { return x.side == y.side && x.price == y.price; });
    if (x.quantity) before.levels.push_back(x);
  }
  std::sort(before.levels.begin(), before.levels.end(), wire_order);
  validate_book(before);
  return before;
}

inline double percentile(std::vector<double> values, double fraction) {
  if (values.empty()) return 0;
  std::sort(values.begin(), values.end());
  return values[static_cast<std::size_t>(fraction * static_cast<double>(values.size() - 1))];
}
}  // namespace marketlab::md::test
