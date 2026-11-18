#include "matching_engine/market_data/spread_market.hpp"
#include "matching_engine/spreads/transport/protocol.hpp"
#include <algorithm>

namespace marketlab::orders::spreads {
MarketBookSnapshot merged_market(const MarketBookSnapshot& real,
                                 std::uint8_t target,
                                 const Views& views) {
  MarketBookSnapshot out;
  out.symbol_id = real.symbol_id;
  out.extended = true;
  auto add = [](std::uint64_t& total, std::uint64_t amount) {
    if (amount > UINT64_MAX - total)
      throw std::overflow_error("Merged market quantity overflow");
    total += amount;
  };
  const auto levels = std::span(real.levels).first(real.count);
  std::size_t bids = 0;
  while (bids < levels.size() && levels[bids].side == md::Side::Buy)
    ++bids;
  for (const auto side : {Bid, Ask}) {
    const auto wire_side = side == Bid ? md::Side::Buy : md::Side::Sell;
    const auto prices = side == Bid ? levels.first(bids) : levels.subspan(bids);
    std::size_t real_at = 0;
    std::array<std::uint8_t, maximum_routes_per_side> candidates{}, at{};
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
      if (real_at < prices.size())
        consider(prices[real_at].price);
      for (unsigned i = 0; i < count; ++i)
        if (at[i] < views[candidates[i]].count)
          consider(views[candidates[i]].price[at[i]]);
      if (!price)
        break;
      md::Level merged{wire_side, *price, 0, 0};
      if (real_at < prices.size() && prices[real_at].price == *price)
        merged = prices[real_at++];
      for (unsigned i = 0; i < count; ++i) {
        const auto& view = views[candidates[i]];
        if (at[i] < view.count && view.price[at[i]] == *price) {
          add(merged.implied_quantity, view.quantity[at[i]++]);
          add(merged.num_orders, 1);
        }
      }
      auto total = merged.quantity;
      add(total, merged.implied_quantity);
      out.levels[out.count++] = merged;
    }
  }
  return out;
}

MarketPublication::MarketPublication(MatchingEngine& core,
                                     const OrderConfig& config,
                                     std::uint16_t shard)
    : core_(core),
      topology_(config),
      visible_(core.symbol_count()),
      real_(core.symbol_count()),
      last_real_(core.symbol_count()) {
  if (topology_.member(shard) >= 4)
    throw std::invalid_argument("Unknown group market owner");
  slots_.reserve(command_window + 1);
  for (unsigned i = 0; i < command_window + 1; ++i)
    slots_.emplace_back(core.symbol_count());
}

void MarketPublication::reset() {
  head_ = size_ = 0;
  permitted_ = 0;
}

void MarketPublication::capture(std::uint64_t seq, const Views& views) {
  std::size_t at;
  const bool same_command = size_ && slots_[(head_ + size_ - 1) % slots_.size()].seq == seq;
  if (same_command) {
    at = (head_ + size_ - 1) % slots_.size();
  } else {
    if (size_ == slots_.size() || (seq && seq <= permitted_) ||
        (size_ && seq < slots_[(head_ + size_ - 1) % slots_.size()].seq))
      throw std::runtime_error("Market candidate window/order exhausted");
    at = (head_ + size_++) % slots_.size();
  }
  auto& candidate = slots_[at];
  candidate.seq = seq;
  core_.copy_market(real_);
  candidate.snapshot.epoch = real_.epoch;
  std::uint8_t changed_views = 0;
  for (unsigned route = 0; route < routes.size(); ++route)
    if (views[route] != last_views_[route]) {
      changed_views |= 1U << routes[route].target;
      last_views_[route] = views[route];
    }
  for (std::size_t i = 0; i < real_.books.size(); ++i) {
    const auto& real = real_.books[i];
    auto& previous = last_real_.books[i];
    const bool changed_real =
        real.symbol_id != previous.symbol_id || real.extended != previous.extended ||
        real.count != previous.count ||
        !std::equal(real.levels.begin(), real.levels.begin() + real.count, previous.levels.begin());
    const auto slot = topology_.slot(real.symbol_id);
    const bool changed_implied = slot < product_count && (changed_views & (1U << slot));
    if (!same_command || changed_real || changed_implied)
      candidate.snapshot.books[i] = slot < product_count ? merged_market(real, slot, views) : real;
    if (changed_real)
      previous = real;
  }
}

void MarketPublication::permit(std::uint64_t seq) {
  if (seq < permitted_)
    throw std::runtime_error("Stale market publication permit");
  while (size_ && slots_[head_].seq <= seq) {
    const auto& snapshot = slots_[head_].snapshot;
    visible_.epoch = snapshot.epoch;
    std::copy(snapshot.books.begin(), snapshot.books.end(), visible_.books.begin());
    head_ = (head_ + 1) % slots_.size();
    --size_;
  }
  permitted_ = seq;
}

void MarketPublication::copy(MarketSnapshot& out) const noexcept {
  out.epoch = visible_.epoch;
  std::copy(visible_.books.begin(), visible_.books.end(), out.books.begin());
}
}  // namespace marketlab::orders::spreads
