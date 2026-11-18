#include "matching_engine/spreads/depth.hpp"

#include <algorithm>
#include <limits>
#include <set>
#include <stdexcept>

namespace marketlab::orders::spreads {
bool DepthSide::same_levels(const DepthSide& other) const {
  if (count != other.count || more != other.more)
    return false;
  for (std::size_t i = 0; i < count; ++i)
    if (price[i] != other.price[i] || quantity[i] != other.quantity[i])
      return false;
  return true;
}

std::uint64_t DepthSide::total() const {
  std::uint64_t sum = 0;
  for (std::size_t i = 0; i < count; ++i) {
    if (quantity[i] > std::numeric_limits<std::uint64_t>::max() - sum)
      throw std::overflow_error("Source depth quantity overflow");
    sum += quantity[i];
  }
  return sum;
}

Topology::Topology(const OrderConfig& c) {
  if (!c.spreads || c.direct)
    throw std::invalid_argument("Spread group requires router routing");
  for (std::size_t slot = 0; slot < symbols.size(); ++slot) {
    const auto it = std::find_if(c.products.begin(), c.products.end(), [&](const auto& p) {
      return p.symbol == c.spread_symbols[slot];
    });
    if (it == c.products.end())
      throw std::invalid_argument("Missing spread product: " + c.spread_symbols[slot]);
    const auto index = static_cast<std::size_t>(it - c.products.begin());
    symbols[slot] = static_cast<std::uint16_t>(index + 1);
    limits[slot] = product_limits(c, index);
    if (slot == AC && shards[0] != it->shard)
      throw std::invalid_argument("Spread books must share their owner shard");
    shards[owner(slot)] = it->shard;
  }
  if (std::set(symbols.begin(), symbols.end()).size() != 5 ||
      std::set(shards.begin(), shards.end()).size() != 4)
    throw std::invalid_argument("Spread group requires five distinct products on four shards");
  auto hash = [&](std::uint64_t value) {
    for (unsigned n = 0; n < 8; ++n) {
      fingerprint ^= (value >> (8 * n)) & 255;
      fingerprint *= 1099511628211ULL;
    }
  };
  hash(1);  // Group topology/wire schema version.
  hash(c.accounts.size());
  for (const auto& account : c.accounts) {
    hash(account.name.size());
    for (unsigned char ch : account.name)
      hash(ch);
  }
  for (std::size_t i = 0; i < symbols.size(); ++i) {
    hash(c.spread_symbols[i].size());
    for (unsigned char ch : c.spread_symbols[i])
      hash(ch);
    hash(symbols[i]);
    hash(shards[owner(i)]);
    hash(limits[i].lower);
    hash(limits[i].upper);
  }
}

std::uint8_t Topology::slot(std::uint16_t symbol) const {
  for (std::uint8_t i = 0; i < symbols.size(); ++i)
    if (symbols[i] == symbol)
      return i;
  return no_slot;
}

std::uint8_t Topology::member(std::uint16_t shard) const {
  for (std::uint8_t i = 0; i < shards.size(); ++i)
    if (shards[i] == shard)
      return i;
  return no_slot;
}

void validate(const DepthSide& side, BookSide direction) {
  if (side.count > source_depth || (side.more && side.count != source_depth))
    throw std::runtime_error("Invalid source depth count/more");
  for (std::size_t i = 0; i < side.count; ++i) {
    if (!side.quantity[i] || (i && (direction == Bid ? side.price[i - 1] <= side.price[i]
                                                     : side.price[i - 1] >= side.price[i])))
      throw std::runtime_error("Invalid source depth ordering/quantity");
  }
  (void)side.total();
}

Implied derive(const Route& route, const Sources& sources) {
  const auto& a = sources[route.first];
  const auto& b = sources[route.second];
  const auto ta = a.total(), tb = b.total(), q = std::min(ta, tb);
  Implied out;
  out.more = (ta > q || a.more) && (tb > q || b.more);
  std::size_t i = 0, j = 0;
  std::uint64_t qa = a.count ? a.quantity[0] : 0, qb = b.count ? b.quantity[0] : 0;
  while (i < a.count && j < b.count) {
    const auto amount = std::min(qa, qb);
    const auto value = std::int64_t(a.price[i]) +
                       (route.add ? std::int64_t(b.price[j]) : -std::int64_t(b.price[j]));
    if (value < INT32_MIN || value > INT32_MAX)
      throw std::overflow_error("Implied price outside int32");
    if (out.count && out.price[out.count - 1] == value)
      out.quantity[out.count - 1] += amount;
    else {
      out.price[out.count] = static_cast<std::int32_t>(value);
      out.quantity[out.count++] = amount;
    }
    qa -= amount;
    qb -= amount;
    if (!qa && ++i < a.count)
      qa = a.quantity[i];
    if (!qb && ++j < b.count)
      qb = b.quantity[j];
  }
  return out;
}

std::uint8_t recompute(const Sources& sources, std::uint16_t dirty, Views& views) {
  std::uint16_t affected = 0;
  for (unsigned i = 0; i < sources.size(); ++i)
    if (dirty & (1U << i))
      affected |= dependencies[i];
  std::uint8_t changed = 0;
  for (unsigned i = 0; i < routes.size(); ++i) {
    if (!(affected & (1U << i)))
      continue;
    auto next = derive(routes[i], sources);
    if (next != views[i]) {
      views[i] = next;
      changed |= std::uint8_t(1U << routes[i].target);
    }
  }
  return changed;
}
}  // namespace marketlab::orders::spreads
