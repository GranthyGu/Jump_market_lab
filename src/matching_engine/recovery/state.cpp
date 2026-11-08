#include "matching_engine/recovery/state.hpp"

#include <algorithm>
#include <cassert>

namespace marketlab::orders::persistence {
CommittedState::CommittedState(const OrderConfig& c, std::uint16_t shard) : epoch_(md::new_epoch()) {
  symbol_books_.resize(c.products.size() + 1, std::numeric_limits<std::size_t>::max());
  for (std::size_t i = 0; i < c.products.size(); ++i) {
    if (c.products[i].shard == shard) {
      symbol_books_[i + 1] = books_.size();
      books_.push_back({static_cast<std::uint16_t>(i + 1), {}, {}});
    }
  }
}
Page& CommittedState::writable(std::size_t p) {
  if (pages_[p].use_count() != 1) {
    pages_[p] = std::make_shared<Page>(*pages_[p]);
    copied_bytes_ += sizeof(Page);
  }
  return *pages_[p];
}
bool CommittedState::needs_copy(const Change& c) const {
  const auto id = c.order.id;
  auto it = positions_.find(id);
  if (it != positions_.end()) return pages_[it->second / Page::slots].use_count() != 1;
  return !c.erase && !free_.empty() && pages_[free_.back() / Page::slots].use_count() != 1;
}
std::map<Price, CommittedState::Level>& CommittedState::levels(const CompactOrder& order) {
  auto& book = books_.at(symbol_books_.at(order.symbol));
  return order.side == Side::Buy ? book.bids : book.asks;
}

void CommittedState::aggregate(const CompactOrder& order, bool add) {
  auto& prices = levels(order);
  if (add) {
    auto& level = prices[order.price];
    level.quantity += order.remaining;
    ++level.count;
  } else {
    auto it = prices.find(order.price);
    if (it == prices.end()) {
      throw std::logic_error("Missing committed level");
    }
    it->second.quantity -= order.remaining;
    if (!--it->second.count) {
      prices.erase(it);
    }
  }
}

void CommittedState::replace(const CompactOrder& previous, const CompactOrder& next) {
  if (previous.symbol == next.symbol && previous.side == next.side &&
      previous.price == next.price) {
    auto& prices = levels(previous);
    auto it = prices.find(previous.price);
    if (it == prices.end()) {
      throw std::logic_error("Missing committed level during update");
    }
    // A partial fill changes quantity, not the order count or the map node.
    // Separate unsigned operations avoid underflow in (next - previous).
    it->second.quantity -= previous.remaining;
    it->second.quantity += next.remaining;
    return;
  }
  aggregate(previous, false);
  aggregate(next, true);
}

void CommittedState::apply(const Change& c) {
  const auto id = c.order.id;
  auto it = positions_.find(id);
  if (it != positions_.end()) {
    const auto slot = it->second;
    auto& entry = writable(slot / Page::slots).orders[slot % Page::slots];
    if (c.erase) {
      aggregate(*entry, false);
      entry.reset();
      positions_.erase(it);
      free_.push_back(slot);
    } else {
      replace(*entry, c.order);
      entry = c.order;
    }
    return;
  }
  if (c.erase) return;
  if (free_.empty()) {
    const auto start = pages_.size() * Page::slots;
    pages_.push_back(std::make_shared<Page>());
    for (std::size_t i = Page::slots; i > 0; --i) free_.push_back(start + i - 1);
  }
  const auto slot = free_.back();
  free_.pop_back();
  writable(slot / Page::slots).orders[slot % Page::slots] = c.order;
  positions_.emplace(id, slot);
  aggregate(c.order, true);
}
FrozenPages CommittedState::freeze() const {
  return {pages_.begin(), pages_.end()};
}
void CommittedState::market(MarketSnapshot& view) const noexcept {
  assert(view.books.size() == books_.size());
  view.epoch = epoch_;
  for (std::size_t i = 0; i < books_.size(); ++i) {
    const auto& book = books_[i];
    auto& top = view.books[i];
    top.symbol_id = book.id;
    top.count = 0;
    auto copy = [&](auto begin, auto end, md::Side side) {
      for (unsigned n = 0; begin != end && n < md::depth; ++begin, ++n)
        top.levels[top.count++] = {side, begin->first, begin->second.quantity, begin->second.count};
    };
    copy(book.bids.rbegin(), book.bids.rend(), md::Side::Buy);
    copy(book.asks.begin(), book.asks.end(), md::Side::Sell);
  }
}
}  // namespace marketlab::orders::persistence
