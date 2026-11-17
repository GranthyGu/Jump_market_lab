#include "matching_engine/spreads/execution.hpp"

#include <algorithm>

namespace marketlab::orders::spreads {
Plan select(const MatchingEngine& core,
            std::uint8_t target,
            const Views& views,
            const persistence::CompactOrder& incoming) {
  if (target >= 5 || incoming.remaining <= 0)
    throw std::invalid_argument("Invalid group selection target/quantity");
  const bool buy = incoming.side == Side::Buy;
  const auto side = buy ? Ask : Bid;
  std::array<std::uint8_t, 2> candidates{no_slot, no_slot}, at{};
  std::uint8_t count = 0;
  for (std::uint8_t route = 0; route < routes.size(); ++route)
    if (routes[route].target == target && routes[route].side == side)
      candidates[count++] = route;
  auto better = [&](Price a, Price b) {
    return buy ? a < b : a > b;
  };
  auto crosses = [&](Price price) {
    return buy ? price <= incoming.price : price >= incoming.price;
  };
  Plan plan;
  plan.remaining = incoming.remaining;
  auto append = [&](std::uint8_t route, Quantity quantity) {
    if (plan.count && plan.slices[plan.count - 1].route == route)
      plan.slices[plan.count - 1].quantity += quantity;
    else {
      if (plan.count == plan.slices.size())
        throw std::logic_error("Selection exceeded fixed segment bound");
      plan.slices[plan.count++] = {quantity, route};
    }
    (route == no_slot ? plan.direct : plan.prefixes[route]) += quantity;
    plan.remaining -= quantity;
  };
  // Consume implied heads that precede the current real level. Equal prices
  // stay with the real level, irrespective of source order timestamps.
  auto implied_before = [&](std::optional<Price> direct) {
    while (plan.remaining) {
      auto best = no_slot;
      for (std::uint8_t i = 0; i < count; ++i) {
        const auto route = candidates[i];
        if (at[i] == views[route].count)
          continue;
        if (best == no_slot ||
            better(views[route].price[at[i]], views[candidates[best]].price[at[best]]) ||
            (views[route].price[at[i]] == views[candidates[best]].price[at[best]] &&
             routes[route].priority < routes[candidates[best]].priority))
          best = i;
      }
      if (best == no_slot)
        return true;
      const auto route = candidates[best];
      const auto& view = views[route];
      const auto price = view.price[at[best]];
      if (direct && !better(price, *direct))
        return true;
      if (!crosses(price))
        return false;
      const auto quantity =
          static_cast<Quantity>(std::min<std::uint64_t>(plan.remaining, view.quantity[at[best]]));
      append(route, quantity);
      ++at[best];  // A partial level can only exhaust the incoming order.
      if (plan.remaining && at[best] == view.count && view.more) {
        plan.refill = true;
        return false;
      }
    }
    return false;
  };
  bool stopped = false;
  core.visit_levels(
      incoming.symbol,
      buy ? Side::Sell : Side::Buy,
      [&](Price price, std::uint64_t quantity, std::uint64_t) {
        if (!implied_before(price) || !crosses(price)) {
          stopped = true;
          return false;
        }
        append(no_slot, static_cast<Quantity>(std::min<std::uint64_t>(plan.remaining, quantity)));
        if (!plan.remaining) {
          stopped = true;
          return false;
        }
        return true;
      });
  if (!stopped)
    implied_before(std::nullopt);
  return plan;
}

const SourcePiece& PieceCursor::head() const {
  if (empty())
    throw std::runtime_error("Incomplete source details");
  const auto& piece = pieces_[index_];
  if (!piece.order || !piece.user || piece.quantity <= 0)
    throw std::runtime_error("Invalid source details");
  return piece;
}

Quantity PieceCursor::remaining() const {
  return head().quantity - consumed_;
}

void PieceCursor::consume(Quantity quantity) {
  if (quantity <= 0 || quantity > remaining())
    throw std::runtime_error("Source allocation exceeds FIFO piece");
  consumed_ += quantity;
  if (consumed_ == head().quantity) {
    ++index_;
    consumed_ = 0;
  }
}

Combination pair(std::uint8_t route_id,
                 const persistence::CompactOrder& incoming,
                 const SourcePiece& first,
                 const SourcePiece& second,
                 Quantity quantity,
                 persistence::ExecutionKey key,
                 Timestamp timestamp) {
  if (route_id >= routes.size() || quantity <= 0 || quantity > first.quantity ||
      quantity > second.quantity || key.aggressor != incoming.id || !key.fill || !timestamp)
    throw std::runtime_error("Invalid combination allocation");
  const auto& route = routes[route_id];
  if (incoming.side != (route.side == Ask ? Side::Buy : Side::Sell))
    throw std::runtime_error("Combination direction mismatch");
  const auto price = std::int64_t(first.price) +
                     (route.add ? std::int64_t(second.price) : -std::int64_t(second.price));
  if (price < INT32_MIN || price > INT32_MAX ||
      (incoming.side == Side::Buy ? price > incoming.price : price < incoming.price))
    throw std::runtime_error("Combination price exceeds incoming limit");
  Combination out;
  out.route = route_id;
  out.quantity = quantity;
  out.key = key;
  out.timestamp = timestamp;
  auto install = [&](std::uint8_t slot, const Identity& identity) {
    if (slot == A)
      out.a = identity;
    else if (slot >= AB)
      out.spread = identity;
    else
      out.x = identity;
  };
  install(route.target, {incoming.id, incoming.user, static_cast<Price>(price)});
  install(route.first / 2, {first.order, first.user, first.price});
  install(route.second / 2, {second.order, second.user, second.price});
  if (route.target >= AB)
    out.spread_side = incoming.side;
  else {
    const auto source = route.first / 2 >= AB ? route.first : route.second;
    out.spread_side = source % 2 == Bid ? Side::Buy : Side::Sell;
  }
  return out;
}

persistence::CompactOrder allocated(const MatchingEngine::FillPiece& piece, Quantity used) {
  if (used <= 0 || used > piece.quantity)
    throw std::runtime_error("Invalid local piece allocation");
  auto order = piece.after;
  order.filled -= piece.quantity - used;
  order.remaining += piece.quantity - used;
  order.status = order.remaining ? OrderStatus::PartiallyFilled : OrderStatus::Filled;
  return order;
}

void record(MatchingEngine& core,
            persistence::Result& result,
            const Topology& topology,
            const Combination& fill,
            const persistence::CompactOrder& local) {
  const auto slot = topology.slot(local.symbol);
  const auto& route = routes.at(fill.route);
  const auto spread = route.spread;
  const auto x = spread == AB ? B : C;
  const Identity* identity = slot == spread ? &fill.spread
                             : slot == A    ? &fill.a
                             : slot == x    ? &fill.x
                                            : nullptr;
  if (!identity || identity->order != local.id || identity->user != local.user ||
      local.side != (slot == x        ? (fill.spread_side == Side::Buy ? Side::Buy : Side::Sell)
                     : slot == spread ? fill.spread_side
                                      : (fill.spread_side == Side::Buy ? Side::Sell : Side::Buy)) ||
      fill.quantity <= 0 || fill.quantity > local.filled ||
      std::int64_t(fill.spread.price) != std::int64_t(fill.a.price) - fill.x.price)
    throw std::runtime_error("Combination does not match owned order");
  TradeId trade;
  if (slot == spread) {
    persistence::SpreadExecution execution;
    trade = execution.id = core.allocate_trade();
    execution.timestamp = fill.timestamp;
    execution.symbol = core.symbol_name(local.symbol);
    execution.price = fill.spread.price;
    execution.quantity = fill.quantity;
    execution.execution = fill.key;
    execution.order = local.id;
    execution.user = core.username(local.user);
    execution.side = local.side;
    execution.a = {fill.a.order, core.username(fill.a.user), fill.a.price};
    execution.x = {fill.x.order, core.username(fill.x.user), fill.x.price};
    result.add(result.executions, persistence::ExecutionRecord(execution));
  } else {
    const bool local_buy = local.side == Side::Buy;
    persistence::ImpliedLegTrade execution;
    static_cast<persistence::TradeRecord&>(execution) =
        core.trade_record(local.symbol,
                          identity->price,
                          fill.quantity,
                          fill.timestamp,
                          local_buy ? local.id : fill.spread.order,
                          local_buy ? local.user : fill.spread.user,
                          local_buy ? fill.spread.order : local.id,
                          local_buy ? fill.spread.user : local.user);
    execution.execution = fill.key;
    trade = execution.id;
    result.add(result.executions, persistence::ExecutionRecord(execution));
  }
  core.fill_events(local, trade, identity->price, fill.quantity, fill.timestamp, result);
}
}  // namespace marketlab::orders::spreads
