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
    if (it == c.products.end() && slot == BF)
      continue;  // Existing five-product deployments need no synthetic butterfly book.
    if (it == c.products.end())
      throw std::invalid_argument("Missing spread product: " + c.spread_symbols[slot]);
    const auto index = static_cast<std::size_t>(it - c.products.begin());
    symbols[slot] = static_cast<std::uint16_t>(index + 1);
    limits[slot] = product_limits(c, index);
    if (slot >= AC && shards[0] != it->shard)
      throw std::invalid_argument("Spread books must share their owner shard");
    shards[owner(slot)] = it->shard;
  }
  if (std::set(symbols.begin(), symbols.end()).size() != product_count ||
      std::set(shards.begin(), shards.end()).size() != 4)
    throw std::invalid_argument("Spread group requires distinct products on four shards");
  auto hash = [&](std::uint64_t value) {
    for (unsigned n = 0; n < 8; ++n) {
      fingerprint ^= (value >> (8 * n)) & 255;
      fingerprint *= 1099511628211ULL;
    }
  };
  hash(2);  // Shared-source allocation and ratio routes.
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

namespace {
struct Cursor {
  std::uint8_t level = 0;
  std::uint64_t left = 0;
};

using Cursors = std::array<Cursor, source_count>;

enum class Availability {
  Empty,
  Ready,
  Refill
};

struct Candidate {
  Availability state = Availability::Ready;
  std::int32_t price = 0;
  std::uint64_t quantity = UINT64_MAX;
};

Cursors cursors(const Sources& sources) {
  Cursors result{};
  for (unsigned i = 0; i < sources.size(); ++i)
    result[i].left = sources[i].count ? sources[i].quantity[0] : 0;
  return result;
}

Candidate peek(const Route& route, const Sources& sources, const Cursors& positions) {
  Candidate out;
  std::int64_t price = 0;
  for (unsigned leg = 0; leg < route.legs(); ++leg) {
    const auto source = route.source(leg);
    const auto& depth = sources[source];
    auto cursor = positions[source];
    const auto weight = route.weight(leg);
    unsigned needed = weight;
    std::int64_t cost = 0;
    // At most two real contracts make one combination unit. A ratio leg may
    // span levels; flooring each individual level would lose valid pairs.
    while (needed && cursor.level < depth.count) {
      const auto take = std::min<std::uint64_t>(needed, cursor.left);
      cost += std::int64_t(depth.price[cursor.level]) * static_cast<std::int64_t>(take);
      needed -= take;
      if (needed && ++cursor.level < depth.count)
        cursor.left = depth.quantity[cursor.level];
    }
    if (needed) {
      if (!depth.more)
        return {Availability::Empty, 0, 0};
      out.state = Availability::Refill;
    } else {
      price += route.sign(leg) * cost;
      const auto capacity = positions[source].left / weight;
      out.quantity = std::min(out.quantity, capacity ? capacity : 1);
    }
  }
  if (out.state != Availability::Ready)
    return out;
  const auto denominator = route.target_weight;
  const auto rounded = price / denominator +
                       (route.side == Ask ? (price % denominator > 0) : -(price % denominator < 0));
  if (rounded < INT32_MIN || rounded > INT32_MAX)
    throw std::overflow_error("Implied price outside int32");
  out.price = static_cast<std::int32_t>(rounded);
  out.quantity = std::min(out.quantity, UINT64_MAX / denominator);
  return out;
}

void consume(const Route& route,
             std::uint64_t quantity,
             const Sources& sources,
             Cursors& positions) {
  for (unsigned leg = 0; leg < route.legs(); ++leg) {
    const auto source = route.source(leg);
    auto& cursor = positions[source];
    auto left = quantity * route.weight(leg);
    while (left) {
      const auto take = std::min(left, cursor.left);
      left -= take;
      cursor.left -= take;
      if (!cursor.left && ++cursor.level < sources[source].count)
        cursor.left = sources[source].quantity[cursor.level];
    }
  }
}

void append(Implied& out, const Candidate& next) {
  if (out.count && out.price[out.count - 1] == next.price) {
    if (next.quantity > UINT64_MAX - out.quantity[out.count - 1])
      throw std::overflow_error("Implied quantity overflow");
    out.quantity[out.count - 1] += next.quantity;
  } else {
    if (out.count == route_depth)
      throw std::logic_error("Implied segment bound exceeded");
    out.price[out.count] = next.price;
    out.quantity[out.count++] = next.quantity;
  }
}

void derive_side(unsigned target, unsigned side, const Sources& sources, Views& views) {
  std::array<std::uint8_t, maximum_routes_per_side> ids{};
  unsigned count = 0;
  for (std::uint8_t route = 0; route < routes.size(); ++route)
    if (routes[route].target == target && routes[route].side == side) {
      ids[count++] = route;
      views[route] = {};
    }
  auto positions = cursors(sources);
  std::uint8_t last = no_slot;
  for (;;) {
    Candidate best;
    auto selected = no_slot;
    bool refill = false;
    for (unsigned i = 0; i < count; ++i) {
      const auto id = ids[i];
      const auto next = peek(routes[id], sources, positions);
      refill |= next.state == Availability::Refill;
      if (next.state != Availability::Ready)
        continue;
      if (selected == no_slot ||
          (side == Ask ? next.price < best.price : next.price > best.price) ||
          (next.price == best.price && routes[id].priority < routes[selected].priority)) {
        selected = id;
        best = next;
      }
    }
    // Unknown deeper liquidity may precede another route. Attach the barrier
    // to the final emitted segment, so the owner refreshes before continuing.
    if (refill) {
      if (last == no_slot)
        throw std::logic_error("Fresh source window cannot form its first ratio unit");
      views[last].more = true;
      return;
    }
    if (selected == no_slot)
      return;
    const auto units = best.quantity;
    best.quantity *= routes[selected].target_weight;
    append(views[selected], best);
    consume(routes[selected], units, sources, positions);
    last = selected;
  }
}
}  // namespace

Implied derive(const Route& route, const Sources& sources) {
  auto positions = cursors(sources);
  Implied out;
  for (;;) {
    auto next = peek(route, sources, positions);
    if (next.state != Availability::Ready) {
      out.more = next.state == Availability::Refill;
      return out;
    }
    const auto units = next.quantity;
    next.quantity *= route.target_weight;
    append(out, next);
    consume(route, units, sources, positions);
  }
}

std::uint8_t recompute(const Sources& sources, std::uint16_t dirty, Views& views) {
  const auto affected = affected_routes(dirty);
  std::uint16_t sides = 0;
  for (unsigned i = 0; i < routes.size(); ++i)
    if (affected & (1U << i))
      sides |= 1U << (2 * routes[i].target + routes[i].side);
  auto next = views;
  for (unsigned i = 0; i < source_count; ++i)
    if (sides & (1U << i))
      derive_side(i / 2, i % 2, sources, next);
  std::uint8_t changed = 0;
  for (unsigned side = 0; side < 2; ++side)
    if (sides & (1U << (2 * B + side))) {
      next.single_b[side] = derive(routes[4 + side], sources);
      if (next.single_b[side] != views.single_b[side]) {
        views.single_b[side] = next.single_b[side];
        changed |= 1U << B;
      }
    }
  for (unsigned i = 0; i < routes.size(); ++i)
    if (next[i] != views[i]) {
      views[i] = next[i];
      changed |= 1U << routes[i].target;
    }
  return changed;
}
}  // namespace marketlab::orders::spreads
