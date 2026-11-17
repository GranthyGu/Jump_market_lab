#include "matching_engine/spreads/market.hpp"

namespace marketlab::orders::spreads {
MarketBookSnapshot merged_market(const MatchingEngine& core,
                                 std::uint16_t symbol,
                                 std::uint8_t target,
                                 const Views& views) {
  MarketBookSnapshot out;
  out.symbol_id = symbol;
  out.extended = true;
  auto add = [](std::uint64_t& total, std::uint64_t amount) {
    if (amount > UINT64_MAX - total)
      throw std::overflow_error("Merged market quantity overflow");
    total += amount;
  };
  for (const auto side : {Bid, Ask}) {
    const auto wire_side = side == Bid ? md::Side::Buy : md::Side::Sell;
    std::array<md::Level, md::depth> real{};
    std::uint8_t real_count = 0, real_at = 0;
    core.visit_levels(symbol,
                      side == Bid ? Side::Buy : Side::Sell,
                      [&](Price price, std::uint64_t quantity, std::uint64_t count) {
                        real[real_count++] = {wire_side, price, quantity, count};
                        return real_count < md::depth;
                      });
    std::array<std::uint8_t, 2> candidates{}, at{};
    std::uint8_t count = 0;
    for (std::uint8_t i = 0; i < routes.size(); ++i)
      if (routes[i].target == target && routes[i].side == side)
        candidates[count++] = i;
    for (unsigned level = 0; level < md::depth; ++level) {
      std::optional<Price> price;
      auto consider = [&](Price value) {
        if (!price || (side == Bid ? value > *price : value < *price))
          price = value;
      };
      if (real_at < real_count)
        consider(real[real_at].price);
      for (unsigned i = 0; i < count; ++i)
        if (at[i] < views[candidates[i]].count)
          consider(views[candidates[i]].price[at[i]]);
      if (!price)
        break;
      md::Level merged{wire_side, *price, 0, 0};
      if (real_at < real_count && real[real_at].price == *price)
        merged = real[real_at++];
      for (unsigned i = 0; i < count; ++i) {
        const auto& view = views[candidates[i]];
        if (at[i] < view.count && view.price[at[i]] == *price)
          add(merged.implied_quantity, view.quantity[at[i]++]);
      }
      auto total = merged.quantity;
      add(total, merged.implied_quantity);
      out.levels[out.count++] = merged;
    }
  }
  return out;
}
}  // namespace marketlab::orders::spreads
