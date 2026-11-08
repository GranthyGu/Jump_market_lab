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
  if (config.products.size() > std::numeric_limits<std::uint16_t>::max() ||
      config.accounts.size() > std::numeric_limits<std::uint32_t>::max()) {
    throw std::invalid_argument("Order dictionary ID range exceeded");
  }
  users_.resize(config.accounts.size() + 1);
  symbols_.resize(config.products.size() + 1);
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
    if (!symbol_ids_.emplace(symbol, static_cast<std::uint16_t>(i + 1)).second) {
      throw std::invalid_argument("Duplicate order dictionary symbol");
    }
  }
}

CompactOrder OrderDictionary::compact(const StoredOrder& order) const {
  const auto user = user_ids_.at(fixed_string(order.username));
  const auto symbol = symbol_ids_.at(fixed_string(order.state.symbol));
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

}  // namespace marketlab::orders::persistence
