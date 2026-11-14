#include "matching_engine/core/engine.hpp"

#include <algorithm>
#include <cassert>
#include <limits>

namespace marketlab::orders {
namespace {
constexpr std::uint64_t sequence_limit = (1ULL << 48) - 1;

std::uint64_t allocate(std::uint16_t shard, std::uint64_t& sequence) {
  if (sequence > sequence_limit)
    throw std::overflow_error("Shard ID sequence exhausted");
  return (static_cast<std::uint64_t>(shard) << 48) | sequence++;
}
}  // namespace

MatchingEngine::MatchingEngine(const OrderConfig& config, std::uint16_t shard)
    : shard_(shard),
      epoch_(md::new_epoch()),
      users_(config.accounts.size() + 1, nullptr),
      user_order_counts_(config.accounts.size() + 1, 0) {
  if (!shard || config.products.size() > 65535)
    throw std::runtime_error("Invalid shard/products");
  const auto count =
      std::count_if(config.products.begin(), config.products.end(), [&](const auto& p) {
        return p.shard == shard;
      });
  if (!count)
    throw std::runtime_error("Shard has no products");
  books_.reserve(static_cast<std::size_t>(count));
  symbols_.reserve(static_cast<std::size_t>(count));
  for (std::size_t i = 0; i < config.products.size(); ++i) {
    const auto& product = config.products[i];
    if (product.shard != shard)
      continue;
    books_.emplace_back(&price_memory_);
    auto& book = books_.back();
    put_string(book.symbol, product.symbol);
    book.symbol_id = static_cast<std::uint16_t>(i + 1);
    book.lower = static_cast<std::int64_t>(product.previous_close) * 90 / 100;
    book.upper = static_cast<std::int64_t>(product.previous_close) * 110 / 100;
    symbols_.emplace(std::string_view(book.symbol, product.symbol.size()), books_.size() - 1);
  }
  usernames_.resize(config.accounts.size() + 1);
  for (std::size_t i = 0; i < config.accounts.size(); ++i)
    std::copy(
        config.accounts[i].name.begin(), config.accounts[i].name.end(), usernames_[i + 1].begin());
  outgoing_.reserve(64);
}

MatchingEngine::Order* MatchingEngine::Pool::acquire() {
  if (!free_) {
    auto block = std::make_unique<Order[]>(block_size);
    for (std::size_t i = 0; i + 1 < block_size; ++i)
      block[i].next = &block[i + 1];
    auto* first = block.get();
    blocks_.push_back(std::move(block));
    free_ = first;
  }
  auto* order = free_;
  free_ = free_->next;
  *order = Order{};
  return order;
}

void MatchingEngine::Pool::release(Order* order) noexcept {
  order->next = free_;
  free_ = order;
}

template <typename T>
void MatchingEngine::queue(LinkKind kind, const LinkHeader& context, const T& record) {
  static_assert(sizeof(T) <= sizeof(Event::payload));
  Event event{};
  event.kind = kind;
  event.context = context;
  event.size = sizeof(T);
  std::memcpy(event.payload.data(), &record, sizeof(T));
  if (capture_)
    capture_->add(capture_->events, event);
  else
    outgoing_.push_back(event);
}

void MatchingEngine::command(const LinkHeader& h,
                             std::span<const std::uint8_t> bytes,
                             const Emit& emit) {
  // The reactor exclusively owns all live state; readers use copied snapshots.
  outgoing_.clear();
  if (!h.user || h.user >= users_.size())
    throw std::runtime_error("Unknown engine user");
  if (h.kind == LinkKind::NewOrder)
    new_order(h, copy_record<NewOrderRequest>(bytes));
  else if (h.kind == LinkKind::Cancel)
    cancel(h, copy_record<CancelRequest>(bytes));
  else if (h.kind == LinkKind::Snapshot)
    snapshot(h, copy_record<Header>(bytes));
  else if (h.kind == LinkKind::History)
    queue(LinkKind::Event,
          h,
          rejected(copy_record<TradeHistoryRequest>(bytes).header.requestId,
                   RejectReason::HistoryUnavailable));
  else
    throw std::runtime_error("Unexpected engine command");
  for (const auto& event : outgoing_)
    emit(event.kind, event.context, {event.payload.data(), event.size});
}

void MatchingEngine::execute(const LinkHeader& h,
                             std::span<const std::uint8_t> bytes,
                             persistence::Result& result) {
  if (capture_)
    throw std::logic_error("Reentrant matching command");
  capture_ = &result;
  try {
    command(h, bytes, [](auto, const auto&, auto) {
    });
    capture_ = nullptr;
  } catch (...) {
    capture_ = nullptr;
    throw;
  }
}

void MatchingEngine::change(const Order& o, bool erase) {
  if (!capture_)
    return;
  persistence::Change c;
  c.erase = erase;
  c.order = persistence::compact_order(o.state, o.user, o.book->symbol_id, o.state.orderId);
  capture_->add(capture_->changes, c);
}

void MatchingEngine::restore(std::span<const persistence::StoredOrder> orders,
                             std::uint64_t next_order,
                             std::uint64_t next_trade) {
  if (next_order_ != 1 || next_trade_ != 1)
    throw std::logic_error("Restore into nonempty core");
  std::uint64_t previous = 0;
  for (const auto& stored : orders) {
    const auto id = OrderId(stored.state.orderId);
    const auto symbol = std::string_view(stored.state.symbol);
    const auto found = symbols_.find(symbol);
    const auto user = std::find(usernames_.begin() + 1, usernames_.end(), stored.username);
    if (found == symbols_.end() || user == usernames_.end() || stored.arrival <= previous ||
        (id >> 48) != shard_ || (id & sequence_limit) >= next_order)
      throw std::runtime_error("Invalid recovered order identity/FIFO");
    previous = stored.arrival;
    Order o;
    o.state = stored.state;
    o.user = static_cast<std::uint32_t>(user - usernames_.begin());
    o.book = &books_[found->second];
    if (o.state.price < o.book->lower || o.state.price > o.book->upper)
      throw std::runtime_error("Recovered order outside configured price limits");
    rest(o);
  }
  next_order_ = next_order;
  next_trade_ = next_trade;
}

void MatchingEngine::replicate(std::span<const persistence::Change> changes,
                               const persistence::OrderDictionary& dictionary,
                               const persistence::Boundary& boundary) {
  for (const auto& change : changes) {
    auto* existing = active_.find(change.order.id);
    if (change.erase) {
      if (!existing)
        continue;  // An incoming order may have filled without ever resting.
      auto& levels =
          existing->state.side == Side::Buy ? existing->book->bids : existing->book->asks;
      const auto position = existing->position;
      position->second.quantity -= existing->state.remainingQuantity;
      unlink(*existing);
      if (!position->second.count)
        levels.erase(position);
      active_.erase(change.order.id);
      pool_.release(existing);
      continue;
    }
    const auto stored = dictionary.expand(change.order);
    if (existing) {
      if (existing->user != change.order.user || existing->state.price != stored.state.price ||
          existing->state.side != stored.state.side ||
          existing->book->symbol_id != change.order.symbol)
        throw std::runtime_error("Replication changed resting order identity");
      auto& quantity = existing->position->second.quantity;
      quantity -= existing->state.remainingQuantity;
      quantity += stored.state.remainingQuantity;
      existing->state = stored.state;  // Preserve the price-time queue position.
    } else {
      const auto found = symbols_.find(std::string_view(stored.state.symbol));
      if (found == symbols_.end() || (change.order.id >> 48) != shard_)
        throw std::runtime_error("Replication order belongs to another shard");
      Order order;
      order.state = stored.state;
      order.user = change.order.user;
      order.book = &books_[found->second];
      rest(order);
    }
  }
  next_order_ = boundary.next_order;
  next_trade_ = boundary.next_trade;
}

void MatchingEngine::update(const Order& order,
                            RequestId request,
                            const LinkHeader& context,
                            Timestamp time) {
  OrderUpdate record{};
  record.header = {MessageType::OrderUpdate, sizeof(record), request};
  record.timestamp = time ? time : unix_ns();
  record.order = order.state;
  auto destination = context;
  destination.user = order.user;
  queue(LinkKind::Event, destination, record);
}

void MatchingEngine::trade(
    const Order& order, TradeId id, Price price, Quantity quantity, Timestamp time) {
  Trade record{};
  record.header = {MessageType::Trade, sizeof(record), 0};
  record.timestamp = time;
  record.tradeId = id;
  record.orderId = order.state.orderId;
  std::memcpy(record.symbol, order.state.symbol, sizeof(SymbolName));
  record.side = order.state.side;
  record.price = price;
  record.quantity = quantity;
  queue(LinkKind::Event, {0, LinkKind::Event, 0, order.user, 0}, record);
}

void MatchingEngine::new_order(const LinkHeader& h, const NewOrderRequest& request) {
  // Validate fixed-width strings without allocating a temporary std::string.
  const auto end = std::find(std::begin(request.symbol), std::end(request.symbol), '\0');
  if (end == std::end(request.symbol) || !std::all_of(end, std::end(request.symbol), [](char c) {
        return c == 0;
      }))
    throw std::runtime_error("Invalid symbol wire field");
  const auto found = symbols_.find(
      std::string_view(request.symbol, static_cast<std::size_t>(end - request.symbol)));
  if (found == symbols_.end() || (request.side != Side::Buy && request.side != Side::Sell) ||
      request.price <= 0 || request.quantity <= 0 || request.price < books_[found->second].lower ||
      request.price > books_[found->second].upper || next_order_ > sequence_limit ||
      next_trade_ > sequence_limit) {
    queue(LinkKind::Event, h, rejected(request.header.requestId, RejectReason::InvalidOrder));
    return;
  }
  auto& book = books_[found->second];
  Order incoming{};
  incoming.user = h.user;
  incoming.book = &book;
  incoming.state.orderId = allocate(shard_, next_order_);
  std::memcpy(incoming.state.symbol, book.symbol, sizeof(SymbolName));
  incoming.state.side = request.side;
  incoming.state.price = request.price;
  incoming.state.originalQuantity = incoming.state.remainingQuantity = request.quantity;
  incoming.state.status = OrderStatus::Accepted;
  update(incoming, request.header.requestId, h);
  match(incoming, request.side == Side::Buy ? book.asks : book.bids);
  if (incoming.state.remainingQuantity)
    rest(incoming);
  change(incoming, !incoming.state.remainingQuantity);
}

void MatchingEngine::match(Order& incoming, Levels& opposite) {
  while (incoming.state.remainingQuantity && !opposite.empty()) {
    auto best = incoming.state.side == Side::Buy ? opposite.begin() : std::prev(opposite.end());
    const auto price = best->first;
    if ((incoming.state.side == Side::Buy && price > incoming.state.price) ||
        (incoming.state.side == Side::Sell && price < incoming.state.price))
      break;
    auto* resting = best->second.head;  // No order-ID lookup in the fill loop.
    const Quantity incoming_remaining = incoming.state.remainingQuantity;
    const Quantity resting_remaining = resting->state.remainingQuantity;
    const Quantity quantity = std::min(incoming_remaining, resting_remaining);
    const auto id = allocate(shard_, next_trade_);
    const auto time = unix_ns();
    if (capture_) {
      const auto* buy = incoming.state.side == Side::Buy ? &incoming : resting;
      const auto* sell = incoming.state.side == Side::Sell ? &incoming : resting;
      persistence::TradeRecord t;
      t.id = id;
      t.timestamp = time;
      std::copy(
          std::begin(incoming.state.symbol), std::end(incoming.state.symbol), t.symbol.begin());
      t.price = price;
      t.quantity = quantity;
      t.buy_order = buy->state.orderId;
      t.sell_order = sell->state.orderId;
      t.buyer = usernames_[buy->user];
      t.seller = usernames_[sell->user];
      capture_->add(capture_->trades, t);
    }
    for (auto* order : {&incoming, resting}) {
      order->state.filledQuantity += quantity;
      order->state.remainingQuantity -= quantity;
      order->state.status =
          order->state.remainingQuantity ? OrderStatus::PartiallyFilled : OrderStatus::Filled;
      trade(*order, id, price, quantity, time);
      update(*order, 0, {0, LinkKind::Event, 0, order->user, 0}, time);
    }
    change(*resting, !resting->state.remainingQuantity);
    best->second.quantity -= static_cast<std::uint64_t>(quantity);
    if (!resting->state.remainingQuantity) {
      unlink(*resting);
      active_.erase(resting->state.orderId);
      pool_.release(resting);
      if (!best->second.count)
        opposite.erase(best);
    }
  }
}

void MatchingEngine::rest(const Order& incoming) {
  auto* order = pool_.acquire();
  *order = incoming;
  auto& levels = order->state.side == Side::Buy ? order->book->bids : order->book->asks;
  const Price price = order->state.price;  // Never bind STL references to packed fields.
  const auto [position, inserted] = levels.try_emplace(price);
  (void)inserted;
  auto& level = position->second;
  active_.insert(order->state.orderId, order);
  order->position = position;
  order->prev = level.tail;
  if (level.tail)
    level.tail->next = order;
  else
    level.head = order;
  level.tail = order;
  level.quantity += static_cast<std::uint64_t>(order->state.remainingQuantity);
  ++level.count;
  order->user_next = users_[order->user];
  if (order->user_next)
    order->user_next->user_prev = order;
  users_[order->user] = order;
  ++user_order_counts_[order->user];
}

void MatchingEngine::unlink(Order& order) {
  auto& level = order.position->second;
  if (order.prev)
    order.prev->next = order.next;
  else
    level.head = order.next;
  if (order.next)
    order.next->prev = order.prev;
  else
    level.tail = order.prev;
  --level.count;
  if (order.user_prev)
    order.user_prev->user_next = order.user_next;
  else
    users_[order.user] = order.user_next;
  if (order.user_next)
    order.user_next->user_prev = order.user_prev;
  --user_order_counts_[order.user];
}

void MatchingEngine::cancel(const LinkHeader& h, const CancelRequest& request) {
  auto* order = active_.find(request.orderId);
  if (!order || order->user != h.user) {
    queue(LinkKind::Event, h, rejected(request.header.requestId, RejectReason::CancelFailed));
    return;
  }
  auto& levels = order->state.side == Side::Buy ? order->book->bids : order->book->asks;
  const auto position = order->position;
  position->second.quantity -= static_cast<std::uint64_t>(order->state.remainingQuantity);
  unlink(*order);
  if (!position->second.count)
    levels.erase(position);
  order->state.remainingQuantity = 0;
  order->state.status = OrderStatus::Canceled;
  update(*order, request.header.requestId, h);
  change(*order, true);
  active_.erase(request.orderId);
  pool_.release(order);
}

std::size_t MatchingEngine::active_order_count(std::uint32_t user) const {
  if (!user || user >= users_.size()) {
    throw std::runtime_error("Unknown snapshot user");
  }
  return user_order_counts_[user];
}

void MatchingEngine::copy_active_orders(std::uint32_t user,
                                        std::span<OrderState> destination) const {
  if (destination.size() != active_order_count(user)) {
    throw std::logic_error("Snapshot buffer does not match user order count");
  }
  std::size_t index = 0;
  for (const auto* order = users_[user]; order; order = order->user_next) {
    destination[index++] = order->state;
  }
  assert(index == destination.size());
}

void MatchingEngine::snapshot(const LinkHeader& h, const Header& request) {
  for (auto* order = users_[h.user]; order; order = order->user_next) {
    auto record =
        server_record<ActiveOrderSnapshot>(MessageType::ActiveOrderSnapshot, request.requestId);
    record.order = order->state;
    queue(LinkKind::Event, h, record);
  }
  auto end =
      server_record<ActiveOrderSnapshot>(MessageType::ActiveOrderSnapshot, request.requestId);
  end.isEnd = 1;
  queue(LinkKind::SnapshotEnd, h, end);
}

void MatchingEngine::copy_market(MarketSnapshot& view) noexcept {
  assert(view.books.size() == books_.size());
  view.epoch = epoch_;
  std::size_t index = 0;
  for (const auto& book : books_) {
    auto& top = view.books[index++];
    top.symbol_id = book.symbol_id;
    top.count = 0;
    auto copy = [&](auto begin, auto end, md::Side side) {
      for (unsigned count = 0; begin != end && count < md::depth; ++begin, ++count)
        top.levels[top.count++] = {side, begin->first, begin->second.quantity, begin->second.count};
    };
    copy(book.bids.rbegin(), book.bids.rend(), md::Side::Buy);
    copy(book.asks.begin(), book.asks.end(), md::Side::Sell);
  }
}
}  // namespace marketlab::orders
