#include "matching_engine/market_data/spread_market.hpp"
#include "matching_engine/spreads/transport/protocol.hpp"

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
      visible_(core.symbol_count()) {
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
  if (size_ && slots_[(head_ + size_ - 1) % slots_.size()].seq == seq) {
    at = (head_ + size_ - 1) % slots_.size();
  } else {
    if (size_ == slots_.size() || (seq && seq <= permitted_) ||
        (size_ && seq < slots_[(head_ + size_ - 1) % slots_.size()].seq))
      throw std::runtime_error("Market candidate window/order exhausted");
    at = (head_ + size_++) % slots_.size();
  }
  auto& candidate = slots_[at];
  candidate.seq = seq;
  core_.copy_market(candidate.snapshot);
  for (auto& book : candidate.snapshot.books) {
    const auto slot = topology_.slot(book.symbol_id);
    if (slot < 5)
      book = merged_market(core_, book.symbol_id, slot, views);
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
