#include "matching_engine/persistence/order_dictionary.hpp"

#include <algorithm>

namespace marketlab::orders::persistence {
namespace {

Name checked_name(const std::string& value) {
  Name result{};
  if (value.empty() || value.size() >= result.size() || value.find('\0') != std::string::npos) {
    throw std::invalid_argument("Invalid order dictionary name");
  }
  std::copy(value.begin(), value.end(), result.begin());
  return result;
}

}  // namespace

OrderDictionary::OrderDictionary(const OrderConfig& config) {
  if (config.spreads)
    topology_.emplace(config);
  if (config.products.size() > std::numeric_limits<std::uint16_t>::max() ||
      config.accounts.size() > std::numeric_limits<std::uint32_t>::max()) {
    throw std::invalid_argument("Order dictionary ID range exceeded");
  }
  users_.resize(config.accounts.size() + 1);
  symbols_.resize(config.products.size() + 1);
  policies_.resize(config.products.size() + 1);
  for (std::size_t i = 0; i < config.accounts.size(); ++i) {
    const auto& name = config.accounts[i].name;
    users_[i + 1] = checked_name(name);
    if (!user_ids_.emplace(name, static_cast<std::uint32_t>(i + 1)).second) {
      throw std::invalid_argument("Duplicate order dictionary user");
    }
  }
  for (std::size_t i = 0; i < config.products.size(); ++i) {
    const auto& symbol = config.products[i].symbol;
    symbols_[i + 1] = checked_name(symbol);
    policies_[i + 1] = {config.products[i].shard,
                        product_limits(config, i),
                        config.spreads && (symbol == config.spread_symbols[3] ||
                                           symbol == config.spread_symbols[4] ||
                                           symbol == config.spread_symbols[5])};
    if (!symbol_ids_.emplace(symbol, static_cast<std::uint16_t>(i + 1)).second) {
      throw std::invalid_argument("Duplicate order dictionary symbol");
    }
  }
}

CompactOrder OrderDictionary::compact(const StoredOrder& order) const {
  const auto user = user_ids_.at(fixed_string(order.username));
  const auto symbol = symbol_ids_.at(fixed_string(order.state.symbol));
  const auto& policy = policies_[symbol];
  if ((order.state.orderId >> 48) != policy.shard ||
      (!policy.signed_price && order.state.price <= 0) || order.state.price < policy.limits.lower ||
      order.state.price > policy.limits.upper)
    throw std::runtime_error("Persisted order violates product ownership/price limits");
  return compact_order(order.state, user, symbol, order.arrival);
}

StoredOrder OrderDictionary::expand(const CompactOrder& order) const {
  if (!order.user || !order.symbol) {
    throw std::logic_error("Missing compact order identity");
  }
  StoredOrder result;
  auto& state = result.state;
  state.orderId = order.id;
  const auto& symbol = symbols_.at(order.symbol);
  std::copy(symbol.begin(), symbol.end(), std::begin(state.symbol));
  state.side = order.side;
  state.price = order.price;
  state.originalQuantity = order.original;
  state.filledQuantity = order.filled;
  state.remainingQuantity = order.remaining;
  state.status = order.status;
  result.username = users_.at(order.user);
  result.arrival = order.arrival;
  return result;
}

void OrderDictionary::validate_execution(const ExecutionRecord& record,
                                         const Boundary& boundary,
                                         std::uint16_t owner) const {
  auto id = [&](OrderId value, std::uint16_t expected, bool trade = false) {
    const auto counter = value & ((1ULL << 48) - 1);
    if (!counter || (value >> 48) != expected ||
        (expected == owner && counter >= (trade ? boundary.next_trade : boundary.next_order)))
      throw std::runtime_error("Trade ID/reference outside product owner/counter");
  };
  auto known_user = [&](const Name& name) {
    (void)user_ids_.at(fixed_string(name));
  };
  std::visit(
      [&](const auto& t) {
        using T = std::decay_t<decltype(t)>;
        const auto symbol = symbol_ids_.at(fixed_string(t.symbol));
        const auto& policy = policies_[symbol];
        if (policy.shard != owner || (!policy.signed_price && t.price <= 0))
          throw std::runtime_error("Trade product/price mismatch");
        id(t.id, owner, true);
        if constexpr (std::is_same_v<T, TradeRecord>) {
          id(t.buy_order, owner);
          id(t.sell_order, owner);
          known_user(t.buyer);
          known_user(t.seller);
        } else {
          if (!topology_)
            throw std::runtime_error("Typed implied trade without spread configuration");
          const auto slot = topology_->slot(symbol);
          const auto aggressor_owner = static_cast<std::uint16_t>(t.execution.aggressor >> 48);
          if (!t.execution.fill || topology_->member(aggressor_owner) == spreads::no_slot)
            throw std::runtime_error("Invalid execution key");
          id(t.execution.aggressor, aggressor_owner);
          const auto spread_owner = topology_->shards[0];
          if constexpr (std::is_same_v<T, ImpliedLegTrade>) {
            if (slot > spreads::C)
              throw std::runtime_error("Implied leg must name an outright");
            const auto buy_shard = static_cast<std::uint16_t>(t.buy_order >> 48);
            const auto sell_shard = static_cast<std::uint16_t>(t.sell_order >> 48);
            if (!((buy_shard == owner && sell_shard == spread_owner) ||
                  (sell_shard == owner && buy_shard == spread_owner)))
              throw std::runtime_error("Implied leg lacks real/spread counterparties");
            id(t.buy_order, buy_shard);
            id(t.sell_order, sell_shard);
            known_user(t.buyer);
            known_user(t.seller);
          } else {
            if (slot != spreads::AB && slot != spreads::AC && slot != spreads::BF)
              throw std::runtime_error("Combination record must name a spread");
            id(t.order, spread_owner);
            id(t.a.order, topology_->shards[1]);
            id(t.x.order, topology_->shards[slot == spreads::AC ? 3 : 2]);
            known_user(t.user);
            known_user(t.a.user);
            known_user(t.x.user);
            bool participating = t.execution.aggressor == t.order ||
                                 t.execution.aggressor == t.a.order ||
                                 t.execution.aggressor == t.x.order;
            if constexpr (std::is_same_v<T, ButterflyExecution>) {
              if (slot != spreads::BF)
                throw std::runtime_error("Butterfly record names a calendar spread");
              id(t.b_second.order, topology_->shards[2]);
              id(t.c.order, topology_->shards[3]);
              known_user(t.b_second.user);
              known_user(t.c.user);
              participating |=
                  t.execution.aggressor == t.b_second.order || t.execution.aggressor == t.c.order;
            } else if (slot == spreads::BF) {
              throw std::runtime_error("Calendar record names a butterfly");
            }
            if (!participating)
              throw std::runtime_error("Execution key does not identify a participating order");
          }
        }
      },
      record);
}

}  // namespace marketlab::orders::persistence
