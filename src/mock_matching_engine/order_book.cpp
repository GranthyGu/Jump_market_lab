#include "mock_matching_engine/order_book.hpp"
#include <algorithm>

namespace marketlab::orders {
MockOrderBook::MockOrderBook(const OrderConfig& c,std::uint16_t shard) : shard_(shard), epoch_(md::new_epoch()) {
  for (std::size_t i = 0; i < c.products.size(); ++i) {
    const auto& p = c.products[i];
    if (p.shard == shard) books_.emplace(p.symbol,Book{p,static_cast<std::uint16_t>(i + 1),{},{}});
  }
  if (books_.empty()) throw std::runtime_error("Shard has no products");
  active_.reserve(65536); by_user_.reserve(c.accounts.size());
}
void MockOrderBook::command(const LinkHeader& h,std::span<const std::uint8_t> bytes,const Emit& emit) {
  // Collect encoded events while owning state; socket writes occur after unlock.
  struct Event { LinkKind kind; LinkHeader h; Bytes bytes; };
  std::vector<Event> outgoing;
  {
    std::lock_guard lock(mutex_);
    const Emit collect = [&](auto kind,const auto& context,const auto& payload) { outgoing.push_back({kind,context,payload}); };
    if (h.kind == LinkKind::NewOrder) new_order(h,copy_record<NewOrderRequest>(bytes),collect);
    else if (h.kind == LinkKind::Cancel) cancel(h,copy_record<CancelRequest>(bytes),collect);
    else if (h.kind == LinkKind::Snapshot) snapshot(h,copy_record<Header>(bytes),collect);
    else throw std::runtime_error("Unexpected engine command");
  }
  for (const auto& e : outgoing) emit(e.kind,e.h,e.bytes);
}
void MockOrderBook::order_update(const Order& order,RequestId request,const LinkHeader& h,const Emit& emit) {
  auto update = server_record<OrderUpdate>(MessageType::OrderUpdate,request);
  update.order = order.state;
  LinkHeader context = h; context.user = order.user;
  emit(LinkKind::Event,context,record_bytes(update));
}
void MockOrderBook::trade(const Order& order,TradeId trade_id,Price price,Quantity quantity,Timestamp time,const Emit& emit) {
  auto value = server_record<Trade>(MessageType::Trade,0);
  value.timestamp = time; value.tradeId = trade_id; value.orderId = order.state.orderId;
  std::memcpy(value.symbol,order.state.symbol,32);
  value.side = order.state.side; value.price = price; value.quantity = quantity;
  emit(LinkKind::Event,{0,LinkKind::Event,0,order.user,0},record_bytes(value));
}
template <typename Levels> void MockOrderBook::match(Order& incoming,Levels& levels,const Emit& emit) {
  while (incoming.state.remainingQuantity && !levels.empty()) {
    auto best = levels.begin();
    const auto price = best->first;
    if ((incoming.state.side == Side::Buy && price > incoming.state.price) ||
        (incoming.state.side == Side::Sell && price < incoming.state.price)) break;
    auto& resting = active_.at(best->second.fifo.front());
    const auto incoming_remaining = incoming.state.remainingQuantity, resting_remaining = resting.state.remainingQuantity;
    const auto quantity = std::min(incoming_remaining,resting_remaining);
    const auto trade_id = (static_cast<std::uint64_t>(shard_) << 48) | next_trade_++;
    const auto time = unix_ns();
    for (auto* order : {&incoming,&resting}) {
      order->state.filledQuantity += quantity;
      order->state.remainingQuantity -= quantity;
      order->state.status = order->state.remainingQuantity ? OrderStatus::PartiallyFilled : OrderStatus::Filled;
      trade(*order,trade_id,price,quantity,time,emit);
      auto update = server_record<OrderUpdate>(MessageType::OrderUpdate,0);
      update.timestamp = time; update.order = order->state;
      emit(LinkKind::Event,{0,LinkKind::Event,0,order->user,0},record_bytes(update));
    }
    best->second.quantity -= static_cast<std::uint64_t>(quantity);
    if (!resting.state.remainingQuantity) {
      const auto id = resting.state.orderId;
      by_user_[resting.user].erase(id);
      best->second.fifo.pop_front(); active_.erase(id);
      if (best->second.fifo.empty()) levels.erase(best);
    }
  }
}
void MockOrderBook::new_order(const LinkHeader& h,const NewOrderRequest& request,const Emit& emit) {
  const auto found = books_.find(fixed_string(request.symbol));
  if (found == books_.end() || (request.side != Side::Buy && request.side != Side::Sell) ||
      request.price <= 0 || request.quantity <= 0 ||
      request.price < static_cast<std::int64_t>(found->second.product.previous_close) * 90 / 100 ||
      request.price > static_cast<std::int64_t>(found->second.product.previous_close) * 110 / 100 ||
      next_order_ >= (1ULL << 48) || next_trade_ >= (1ULL << 48)) {
    emit(LinkKind::Event,h,record_bytes(rejected(request.header.requestId,RejectReason::InvalidOrder))); return;
  }
  Order incoming{};
  incoming.user = h.user;
  incoming.state.orderId = (static_cast<std::uint64_t>(shard_) << 48) | next_order_++;
  std::memcpy(incoming.state.symbol,request.symbol,32);
  incoming.state.side = request.side; incoming.state.price = request.price;
  incoming.state.originalQuantity = incoming.state.remainingQuantity = request.quantity;
  incoming.state.status = OrderStatus::Accepted;
  order_update(incoming,request.header.requestId,h,emit);
  auto& book = found->second;
  if (request.side == Side::Buy) match(incoming,book.asks,emit); else match(incoming,book.bids,emit);
  if (incoming.state.remainingQuantity) {
    const auto incoming_id = incoming.state.orderId;
    const auto incoming_price = incoming.state.price;
    auto add = [&](auto& levels) {
      auto& level = levels[incoming_price];
      level.fifo.push_back(incoming_id);
      incoming.position = std::prev(level.fifo.end());
      level.quantity += static_cast<std::uint64_t>(incoming.state.remainingQuantity);
    };
    if (incoming.state.side == Side::Buy) add(book.bids); else add(book.asks);
    by_user_[h.user].insert(incoming_id);
    active_.emplace(incoming_id,std::move(incoming));
  }
}
void MockOrderBook::cancel(const LinkHeader& h,const CancelRequest& request,const Emit& emit) {
  const auto order_id = request.orderId;
  const auto found = active_.find(order_id);
  if (found == active_.end() || found->second.user != h.user) {
    emit(LinkKind::Event,h,record_bytes(rejected(request.header.requestId,RejectReason::CancelFailed))); return;
  }
  auto& order = found->second;
  const auto order_price = order.state.price;
  auto& book = books_.at(fixed_string(order.state.symbol));
  auto remove = [&](auto& levels) {
    auto level = levels.find(order_price);
    level->second.quantity -= static_cast<std::uint64_t>(order.state.remainingQuantity);
    level->second.fifo.erase(order.position);
    if (level->second.fifo.empty()) levels.erase(level);
  };
  if (order.state.side == Side::Buy) remove(book.bids); else remove(book.asks);
  order.state.remainingQuantity = 0; order.state.status = OrderStatus::Canceled;
  order_update(order,request.header.requestId,h,emit);
  by_user_[h.user].erase(order_id); active_.erase(found);
}
void MockOrderBook::snapshot(const LinkHeader& h,const Header& request,const Emit& emit) {
  const auto found = by_user_.find(h.user);
  if (found != by_user_.end()) for (const auto id : found->second) {
    auto value = server_record<ActiveOrderSnapshot>(MessageType::ActiveOrderSnapshot,request.requestId);
    value.order = active_.at(id).state;
    emit(LinkKind::Event,h,record_bytes(value));
  }
  auto end = server_record<ActiveOrderSnapshot>(MessageType::ActiveOrderSnapshot,request.requestId);
  end.isEnd = 1;
  emit(LinkKind::SnapshotEnd,h,record_bytes(end));
}
md::ShardView MockOrderBook::market_view() {
  std::lock_guard lock(mutex_);
  md::ShardView view{epoch_,{}};
  // Global symbol IDs follow configuration order and remain stable across shards.
  // The adapter has no socket I/O while holding the order-book lock.
  for (const auto& [symbol,book] : books_) {
    (void)symbol;
    md::Book top{book.symbol_id,{}};
    auto copy = [&](md::Side side,const auto& levels) {
      std::size_t n = 0;
      for (const auto& [price,level] : levels) {
        if (n++ == 5) break;
        top.levels.push_back({side,price,level.quantity,level.fifo.size()});
      }
    };
    copy(md::Side::Buy,book.bids); copy(md::Side::Sell,book.asks);
    view.books.emplace(top.symbol_id,std::move(top));
  }
  return view;
}
}  // namespace marketlab::orders
