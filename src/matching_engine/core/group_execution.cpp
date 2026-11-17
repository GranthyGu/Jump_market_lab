#include "matching_engine/core/engine.hpp"

#include <algorithm>
#include <utility>

namespace marketlab::orders {
MatchingEngine::Capture::Capture(MatchingEngine& owner, persistence::Result& result)
    : engine(owner) {
  if (engine.capture_)
    throw std::logic_error("Reentrant matching capture");
  engine.capture_ = &result;
}

MatchingEngine::Capture::~Capture() {
  engine.capture_ = nullptr;
}

const MatchingEngine::Book& MatchingEngine::find_book(std::uint16_t symbol) const {
  const auto it = std::find_if(books_.begin(), books_.end(), [&](const auto& b) {
    return b.symbol_id == symbol;
  });
  if (it == books_.end())
    throw std::runtime_error("Book is not owned by this shard");
  return *it;
}

MatchingEngine::Book& MatchingEngine::find_book(std::uint16_t symbol) {
  return const_cast<Book&>(std::as_const(*this).find_book(symbol));
}

MatchingEngine::Order MatchingEngine::expand_order(const persistence::CompactOrder& value) {
  Order out;
  out.book = &find_book(value.symbol);
  out.user = value.user;
  if ((value.id >> 48) != shard_ || !value.user || value.user >= users_.size())
    throw std::runtime_error("Invalid local group order identity");
  out.state.orderId = value.id;
  std::memcpy(out.state.symbol, out.book->symbol, sizeof(SymbolName));
  out.state.side = value.side;
  out.state.price = value.price;
  out.state.originalQuantity = value.original;
  out.state.filledQuantity = value.filled;
  out.state.remainingQuantity = value.remaining;
  out.state.status = value.status;
  return out;
}

std::optional<persistence::CompactOrder> MatchingEngine::begin_group(const LinkHeader& h,
                                                                     const NewOrderRequest& request,
                                                                     persistence::Result& result) {
  Capture capture(*this, result);
  auto order = accept(h, request);
  if (!order)
    return std::nullopt;
  return persistence::compact_order(
      order->state, order->user, order->book->symbol_id, order->state.orderId);
}

void MatchingEngine::finish_group(const persistence::CompactOrder& value,
                                  persistence::Result& result) {
  Capture capture(*this, result);
  auto order = expand_order(value);
  if (order.state.remainingQuantity)
    rest(order);
  change(order, !order.state.remainingQuantity);
}

void MatchingEngine::take_prefix(std::uint16_t symbol,
                                 Side side,
                                 Quantity quantity,
                                 std::vector<FillPiece>& pieces,
                                 persistence::Result& result) {
  if (quantity <= 0 || (side != Side::Buy && side != Side::Sell))
    throw std::runtime_error("Invalid real quantity prefix");
  Capture capture(*this, result);
  auto& book = find_book(symbol);
  auto& levels = side == Side::Buy ? book.bids : book.asks;
  // Verify availability before applying any piece. At most three source prices
  // are visited for a peer prefix; direct execution may span more prices.
  std::uint64_t available = 0;
  visit_levels(symbol, side, [&](Price, std::uint64_t q, std::uint64_t) {
    available += std::min<std::uint64_t>(q, std::uint64_t(quantity) - available);
    return available < std::uint64_t(quantity);
  });
  if (available < std::uint64_t(quantity))
    throw std::runtime_error("Real prefix is unavailable");
  while (quantity) {
    auto best = side == Side::Buy ? std::prev(levels.end()) : levels.begin();
    auto* order = best->second.head;
    const auto amount = std::min(quantity, Quantity(order->state.remainingQuantity));
    order->state.filledQuantity += amount;
    order->state.remainingQuantity -= amount;
    order->state.status =
        order->state.remainingQuantity ? OrderStatus::PartiallyFilled : OrderStatus::Filled;
    pieces.push_back(
        {persistence::compact_order(order->state, order->user, symbol, order->state.orderId),
         amount});
    change(*order, !order->state.remainingQuantity);
    finish_resting_fill(order, amount);
    quantity -= amount;
  }
}

void MatchingEngine::fill_events(const persistence::CompactOrder& value,
                                 TradeId id,
                                 Price price,
                                 Quantity quantity,
                                 Timestamp timestamp,
                                 persistence::Result& result) {
  Capture capture(*this, result);
  const auto order = expand_order(value);
  trade(order, id, price, quantity, timestamp);
  update(order, 0, {0, LinkKind::Event, 0, value.user, 0}, timestamp);
}

persistence::TradeRecord MatchingEngine::trade_record(std::uint16_t symbol,
                                                      Price price,
                                                      Quantity quantity,
                                                      Timestamp timestamp,
                                                      OrderId buy,
                                                      std::uint32_t buyer,
                                                      OrderId sell,
                                                      std::uint32_t seller) {
  persistence::TradeRecord t;
  t.id = allocate_trade();
  t.timestamp = timestamp;
  t.price = price;
  t.quantity = quantity;
  const auto& book = find_book(symbol);
  std::copy(std::begin(book.symbol), std::end(book.symbol), t.symbol.begin());
  t.buy_order = buy;
  t.sell_order = sell;
  t.buyer = usernames_.at(buyer);
  t.seller = usernames_.at(seller);
  return t;
}

spreads::DepthSide MatchingEngine::source_depth(std::uint16_t symbol, Side side) const {
  spreads::DepthSide out;
  visit_levels(symbol, side, [&](Price price, std::uint64_t quantity, std::uint64_t) {
    if (out.count == spreads::source_depth) {
      out.more = true;
      return false;
    }
    out.price[out.count] = price;
    out.quantity[out.count++] = quantity;
    return true;
  });
  return out;
}

}  // namespace marketlab::orders
