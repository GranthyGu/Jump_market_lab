#pragma once
#include "harness.hpp"
#include "matching_engine/persistence/durable_engine.hpp"

namespace spread_test {
namespace m = marketlab;
namespace o = m::orders;
namespace p = o::persistence;
using demo::require;

inline o::OrderConfig config() {
  o::OrderConfig c;
  c.direct = false;
  c.spreads = true;
  c.accounts = {{"Alice", "demo"}, {"Bob", "demo"}, {"Carol", "demo"}};
  c.products = {{"A", 120, 6},    {"B", 110, 7},    {"C", 105, 8},
                {"SP_AB", 10, 5}, {"SP_AC", 15, 5}, {"BF_ABC", 5, 5}};
  for (unsigned id = 5; id <= 8; ++id)
    c.shards.push_back({static_cast<std::uint16_t>(id),
                        {"127.0.0.1", static_cast<std::uint16_t>(7200 + id)}});
  return c;
}

inline p::Options options(const std::filesystem::path &root) {
  p::Options options;
  options.directory = root;
  options.history_cache_pages = 32;
  options.checkpoint_interval = std::chrono::hours(1);
  return options;
}

inline m::NewOrderRequest order(unsigned request, const char *symbol,
                                m::Side side, m::Price price,
                                m::Quantity quantity) {
  m::NewOrderRequest q{};
  q.header = {m::MessageType::NewOrderRequest, sizeof(q), request};
  o::put_string(q.symbol, symbol);
  q.side = side;
  q.price = price;
  q.quantity = quantity;
  return q;
}

inline o::LinkHeader context(unsigned request, unsigned user = 1) {
  return {0, o::LinkKind::NewOrder, request, user, 1};
}

inline m::OrderId id(unsigned shard, unsigned counter) {
  return (std::uint64_t(shard) << 48) | counter;
}

inline p::Name name(const char *text) {
  p::Name value{};
  std::copy_n(text, std::strlen(text), value.begin());
  return value;
}

inline void fill(p::CompactOrder &order, m::Quantity quantity) {
  require(quantity > 0 && quantity <= order.remaining,
          "Invalid test fill quantity");
  order.remaining -= quantity;
  order.filled += quantity;
  order.status = order.remaining ? m::OrderStatus::PartiallyFilled
                                 : m::OrderStatus::Filled;
}

template <class F> void rejects(F fn) {
  bool caught = false;
  try {
    fn();
  } catch (const std::exception &) {
    caught = true;
  }
  require(caught, "Invalid input was accepted");
}
} // namespace spread_test
