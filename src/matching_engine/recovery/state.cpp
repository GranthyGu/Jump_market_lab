#include "matching_engine/recovery/state.hpp"

#include <algorithm>
#include <cassert>

namespace marketlab::orders::persistence {
CommittedState::CommittedState(const OrderConfig& c, std::uint16_t shard) : epoch_(md::new_epoch()) {
  users_.resize(c.accounts.size() + 1);
  for (std::size_t i = 0; i < c.accounts.size(); ++i)
    std::copy(c.accounts[i].name.begin(), c.accounts[i].name.end(), users_[i + 1].begin());
  for (std::size_t i = 0; i < c.products.size(); ++i)
    if (c.products[i].shard == shard) {
      symbols_.emplace(c.products[i].symbol, books_.size());
      books_.push_back({static_cast<std::uint16_t>(i + 1), {}, {}});
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
  const auto id = OrderId(c.order.state.orderId);
  auto it = positions_.find(id);
  if (it != positions_.end()) return pages_[it->second / Page::slots].use_count() != 1;
  return !c.erase && !free_.empty() && pages_[free_.back() / Page::slots].use_count() != 1;
}
void CommittedState::aggregate(const StoredOrder& o, bool add) {
  auto& book = books_.at(symbols_.at(fixed_string(o.state.symbol)));
  const auto price = Price(o.state.price);
  auto& levels = o.state.side == Side::Buy ? book.bids : book.asks;
  if (add) {
    auto& l = levels[price];
    l.quantity += o.state.remainingQuantity;
    ++l.count;
  } else {
    auto it = levels.find(price);
    if (it == levels.end()) throw std::logic_error("Missing committed level");
    it->second.quantity -= o.state.remainingQuantity;
    if (!--it->second.count) levels.erase(it);
  }
}
void CommittedState::apply(const Change& c) {
  const auto id = OrderId(c.order.state.orderId);
  auto it = positions_.find(id);
  if (it != positions_.end()) {
    const auto slot = it->second;
    auto& entry = writable(slot / Page::slots).orders[slot % Page::slots];
    aggregate(*entry, false);
    if (c.erase) {
      entry.reset();
      positions_.erase(it);
      free_.push_back(slot);
    } else {
      entry = c.order;
      aggregate(c.order, true);
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
FrozenPages CommittedState::freeze() const { return {pages_.begin(), pages_.end()}; }
std::vector<StoredOrder> CommittedState::orders() const {
  std::vector<StoredOrder> out;
  out.reserve(positions_.size());
  for (const auto& p : pages_)
    for (const auto& o : p->orders)
      if (o) out.push_back(*o);
  return out;
}
void CommittedState::snapshot(const LinkHeader& h, const Header& request,
                              const EngineBackend::Emit& emit) const {
  if (!h.user || h.user >= users_.size()) throw std::runtime_error("Invalid snapshot user");
  for (const auto& p : pages_)
    for (const auto& o : p->orders)
      if (o && o->username == users_[h.user]) {
        auto r = server_record<ActiveOrderSnapshot>(MessageType::ActiveOrderSnapshot, request.requestId);
        r.order = o->state;
        emit(LinkKind::Event, h, record_bytes(r));
      }
  auto end = server_record<ActiveOrderSnapshot>(MessageType::ActiveOrderSnapshot, request.requestId);
  end.isEnd = 1;
  emit(LinkKind::SnapshotEnd, h, record_bytes(end));
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
