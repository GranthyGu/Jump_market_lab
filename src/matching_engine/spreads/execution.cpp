#include "matching_engine/spreads/execution.hpp"

#include <algorithm>

namespace marketlab::orders::spreads {
Plan select(const MatchingEngine& core,
            std::uint8_t target,
            const Views& views,
            const persistence::CompactOrder& incoming) {
  if (target >= product_count || incoming.remaining <= 0)
    throw std::invalid_argument("Invalid group selection target/quantity");
  const bool buy = incoming.side == Side::Buy;
  const auto side = buy ? Ask : Bid;
  std::array<std::uint8_t, maximum_routes_per_side> candidates{}, at{};
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
  bool single_b = false;
  auto implied_before = [&](std::optional<Price> direct) {
    while (plan.remaining) {
      if (target == B && plan.remaining == 1 && !single_b) {
        if (std::any_of(plan.prefixes.begin(), plan.prefixes.end(), [](auto q) {
              return q != 0;
            })) {
          plan.refill = true;  // Rebuild the one-contract fallback after actual consumption.
          return false;
        }
        single_b = true;
        at.fill(0);
      }
      auto current = [&](unsigned i) -> const Implied& {
        return single_b ? views.single_b[side] : views[candidates[i]];
      };
      auto best = no_slot;
      for (std::uint8_t i = 0; i < count; ++i) {
        const auto route = candidates[i];
        if ((single_b && route != 4 + side) || at[i] == current(i).count)
          continue;
        if (best == no_slot || better(current(i).price[at[i]], current(best).price[at[best]]) ||
            (current(i).price[at[i]] == current(best).price[at[best]] &&
             routes[route].priority < routes[candidates[best]].priority))
          best = i;
      }
      if (best == no_slot)
        return true;
      const auto route = candidates[best];
      const auto& view = current(best);
      const auto price = view.price[at[best]];
      if (direct && !better(price, *direct))
        return true;
      if (!crosses(price))
        return false;
      const auto& definition = routes[route];
      std::uint64_t budget = INT32_MAX;
      for (unsigned leg = 0; leg < definition.legs(); ++leg)
        budget = std::min(
            budget, std::uint64_t(INT32_MAX / definition.weight(leg)) * definition.target_weight);
      budget -= plan.prefixes[route];
      auto quantity = std::min({std::uint64_t(plan.remaining), view.quantity[at[best]], budget});
      quantity -= quantity % definition.target_weight;
      if (!quantity) {
        plan.refill = true;
        return false;
      }
      append(route, static_cast<Quantity>(quantity));
      if (quantity == view.quantity[at[best]])
        ++at[best];
      else if (plan.remaining) {
        plan.refill = true;
        return false;
      }
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

namespace {
template <class Fill>
auto& identity(Fill& fill, unsigned slot, unsigned part) {
  const auto spread = routes.at(fill.route).spread;
  if (slot == spread && !part)
    return fill.spread;
  if (slot == A && !part)
    return fill.a;
  if (spread == BF) {
    if (slot == B && part <= 1)
      return part ? fill.b_second : fill.x;
    if (slot == C && !part)
      return fill.c;
  } else if (slot == (spread == AB ? B : C) && !part) {
    return fill.x;
  }
  throw std::runtime_error("Combination has no such participant");
}
}  // namespace

const Identity& participant(const Combination& fill, unsigned slot, unsigned part) {
  return identity(fill, slot, part);
}

void set_participant(Combination& fill, unsigned slot, const Identity& value, unsigned part) {
  identity(fill, slot, part) = value;
}

Quantity participant_quantity(const Combination& fill, unsigned slot, unsigned part) {
  if (routes.at(fill.route).spread == BF && slot == B && fill.x == fill.b_second) {
    if (part)
      throw std::runtime_error("Identical B pieces must be recorded together");
    return fill.quantity * 2;
  }
  return fill.quantity;
}

void complete_butterfly(Combination& fill, const persistence::CompactOrder& incoming) {
  const auto& r = routes.at(fill.route);
  if (r.spread != BF || fill.quantity <= 0 || fill.quantity > INT32_MAX / 2 ||
      fill.key.aggressor != incoming.id || !fill.key.fill || !fill.timestamp ||
      incoming.side != (r.side == Ask ? Side::Buy : Side::Sell))
    throw std::runtime_error("Invalid butterfly allocation");
  auto install = [&](unsigned part, std::int64_t value) {
    if (value < INT32_MIN || value > INT32_MAX ||
        (incoming.side == Side::Buy ? value > incoming.price : value < incoming.price))
      throw std::runtime_error("Butterfly price exceeds incoming limit");
    set_participant(fill, r.target, {incoming.id, incoming.user, static_cast<Price>(value)}, part);
  };
  switch (r.target) {
    case BF:
      install(0, std::int64_t(fill.a.price) - fill.x.price - fill.b_second.price + fill.c.price);
      break;
    case A:
      install(0,
              std::int64_t(fill.spread.price) + fill.x.price + fill.b_second.price - fill.c.price);
      break;
    case C:
      install(0,
              std::int64_t(fill.spread.price) - fill.a.price + fill.x.price + fill.b_second.price);
      break;
    case B: {
      const auto total = std::int64_t(fill.a.price) + fill.c.price - fill.spread.price;
      install(0, total / 2 - (total % 2 < 0));
      install(1, total / 2 + (total % 2 > 0));
      break;
    }
    default:
      throw std::runtime_error("Invalid butterfly target");
  }
  fill.spread_side = incoming.side;
  if (r.target != BF)
    for (unsigned leg = 0; leg < r.legs(); ++leg)
      if (r.source(leg) / 2 == BF)
        fill.spread_side = r.source(leg) % 2 == Bid ? Side::Buy : Side::Sell;
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
            const persistence::CompactOrder& local,
            unsigned part) {
  const auto slot = topology.slot(local.symbol);
  const auto spread = routes.at(fill.route).spread;
  const auto& own = participant(fill, slot, part);
  const auto amount = participant_quantity(fill, slot, part);
  const bool negative_leg = spread == BF ? slot == B : slot == (spread == AB ? B : C);
  const auto expected_side = slot == spread || negative_leg  ? fill.spread_side
                             : fill.spread_side == Side::Buy ? Side::Sell
                                                             : Side::Buy;
  const auto implied_price =
      spread == BF ? std::int64_t(fill.a.price) - fill.x.price - fill.b_second.price + fill.c.price
                   : std::int64_t(fill.a.price) - fill.x.price;
  if (own.order != local.id || own.user != local.user || local.side != expected_side ||
      amount <= 0 || amount > local.filled || fill.spread.price != implied_price)
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
    if (spread == BF) {
      persistence::ButterflyExecution butterfly;
      static_cast<persistence::SpreadExecution&>(butterfly) = execution;
      butterfly.b_second = {
          fill.b_second.order, core.username(fill.b_second.user), fill.b_second.price};
      butterfly.c = {fill.c.order, core.username(fill.c.user), fill.c.price};
      result.add(result.executions, persistence::ExecutionRecord(butterfly));
    } else {
      result.add(result.executions, persistence::ExecutionRecord(execution));
    }
  } else {
    const bool local_buy = local.side == Side::Buy;
    persistence::ImpliedLegTrade execution;
    static_cast<persistence::TradeRecord&>(execution) =
        core.trade_record(local.symbol,
                          own.price,
                          amount,
                          fill.timestamp,
                          local_buy ? local.id : fill.spread.order,
                          local_buy ? local.user : fill.spread.user,
                          local_buy ? fill.spread.order : local.id,
                          local_buy ? fill.spread.user : local.user);
    execution.execution = fill.key;
    trade = execution.id;
    result.add(result.executions, persistence::ExecutionRecord(execution));
  }
  core.fill_events(local, trade, own.price, amount, fill.timestamp, result);
}
}  // namespace marketlab::orders::spreads
