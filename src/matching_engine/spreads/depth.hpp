#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include "shared_data/orders/config.hpp"

namespace marketlab::orders::spreads {
inline constexpr std::size_t source_depth = 3, route_depth = 9;
inline constexpr std::size_t product_count = 6, source_count = 2 * product_count;
inline constexpr std::size_t route_count = 20, maximum_routes_per_side = 3;
inline constexpr std::uint16_t all_sources = (1U << source_count) - 1;
inline constexpr std::uint8_t all_targets = (1U << product_count) - 1;
inline constexpr std::uint8_t no_slot = 255;

enum Slot : std::uint8_t {
  A,
  B,
  C,
  AB,
  AC,
  BF
};

enum BookSide : std::uint8_t {
  Bid,
  Ask
};

// Separate price/quantity arrays avoid padding every 12-byte level to 16 bytes.
struct DepthSide {
  std::uint64_t version = 0;
  std::array<std::uint64_t, source_depth> quantity{};
  std::array<std::int32_t, source_depth> price{};
  std::uint8_t count = 0;
  bool more = false;
  bool same_levels(const DepthSide&) const;
  std::uint64_t total() const;
};

static_assert(sizeof(DepthSide) == 48);
using Sources = std::array<DepthSide, source_count>;

struct Route {
  std::uint8_t target, side, first, second, spread, priority;
  bool add;
  std::uint8_t third = no_slot, second_weight = 1, third_weight = 1;
  bool third_add = true;
  std::uint8_t target_weight = 1;

  constexpr unsigned legs() const {
    return third == no_slot ? 2 : 3;
  }

  constexpr std::uint8_t source(unsigned leg) const {
    return leg == 0 ? first : leg == 1 ? second : third;
  }

  constexpr unsigned weight(unsigned leg) const {
    return leg == 0 ? 1 : leg == 1 ? second_weight : third_weight;
  }

  constexpr int sign(unsigned leg) const {
    return leg == 0 || (leg == 1 ? add : third_add) ? 1 : -1;
  }
};

inline constexpr std::array<Route, route_count> routes{{
    {AB, Bid, 2 * A + Bid, 2 * B + Ask, AB, 0, false},
    {AB, Ask, 2 * A + Ask, 2 * B + Bid, AB, 0, false},
    {A, Bid, 2 * AB + Bid, 2 * B + Bid, AB, 0, true},
    {A, Ask, 2 * AB + Ask, 2 * B + Ask, AB, 0, true},
    {B, Bid, 2 * A + Bid, 2 * AB + Ask, AB, 0, false},
    {B, Ask, 2 * A + Ask, 2 * AB + Bid, AB, 0, false},
    {AC, Bid, 2 * A + Bid, 2 * C + Ask, AC, 1, false},
    {AC, Ask, 2 * A + Ask, 2 * C + Bid, AC, 1, false},
    {A, Bid, 2 * AC + Bid, 2 * C + Bid, AC, 1, true},
    {A, Ask, 2 * AC + Ask, 2 * C + Ask, AC, 1, true},
    {C, Bid, 2 * A + Bid, 2 * AC + Ask, AC, 1, false},
    {C, Ask, 2 * A + Ask, 2 * AC + Bid, AC, 1, false},
    {BF, Bid, 2 * A + Bid, 2 * B + Ask, BF, 2, false, 2 * C + Bid, 2},
    {BF, Ask, 2 * A + Ask, 2 * B + Bid, BF, 2, false, 2 * C + Ask, 2},
    {A, Bid, 2 * BF + Bid, 2 * B + Bid, BF, 2, true, 2 * C + Ask, 2, 1, false},
    {A, Ask, 2 * BF + Ask, 2 * B + Ask, BF, 2, true, 2 * C + Bid, 2, 1, false},
    {C, Bid, 2 * BF + Bid, 2 * A + Ask, BF, 2, false, 2 * B + Bid, 1, 2},
    {C, Ask, 2 * BF + Ask, 2 * A + Bid, BF, 2, false, 2 * B + Ask, 1, 2},
    {B, Bid, 2 * A + Bid, 2 * BF + Ask, BF, 2, false, 2 * C + Bid, 1, 1, true, 2},
    {B, Ask, 2 * A + Ask, 2 * BF + Bid, BF, 2, false, 2 * C + Ask, 1, 1, true, 2},
}};
inline constexpr auto dependencies = [] {
  std::array<std::uint32_t, source_count> masks{};
  for (std::size_t i = 0; i < routes.size(); ++i)
    for (unsigned leg = 0; leg < routes[i].legs(); ++leg)
      masks[routes[i].source(leg)] |= 1U << i;
  return masks;
}();

// Include competitors at compile time: a raw route change can release quantity.
inline constexpr auto allocation_dependencies = [] {
  auto masks = dependencies;
  for (unsigned source = 0; source < source_count; ++source)
    for (unsigned i = 0; i < routes.size(); ++i)
      if (dependencies[source] & (1U << i))
        for (unsigned j = 0; j < routes.size(); ++j)
          if (routes[j].target == routes[i].target && routes[j].side == routes[i].side)
            masks[source] |= 1U << j;
  return masks;
}();

constexpr std::uint32_t affected_routes(std::uint16_t dirty) {
  std::uint32_t affected = 0;
  for (unsigned source = 0; source < source_count; ++source)
    if (dirty & (1U << source))
      affected |= allocation_dependencies[source];
  return affected;
}

struct Implied {
  std::array<std::uint64_t, route_depth> quantity{};
  std::array<std::int32_t, route_depth> price{};
  std::uint8_t count = 0;
  bool more = false;
  bool operator==(const Implied&) const = default;
};

static_assert(sizeof(Implied) == 112);

struct Views : std::array<Implied, route_count> {
  // A single remaining B cannot take a two-contract butterfly. These two tiny
  // fallback views expose the original AB path without hypothetical BF usage.
  std::array<Implied, 2> single_b{};
  bool operator==(const Views&) const = default;
};

struct Topology {
  explicit Topology(const OrderConfig&);
  std::array<std::uint16_t, product_count> symbols{};
  std::array<std::uint16_t, 4> shards{};  // S, A, B, C
  std::array<PriceLimits, product_count> limits{};
  std::uint64_t fingerprint = 1469598103934665603ULL;
  std::uint8_t slot(std::uint16_t symbol) const;
  std::uint8_t member(std::uint16_t shard) const;

  std::uint8_t owner(std::uint8_t slot) const {
    return slot >= AB ? 0 : slot + 1;
  }
};

constexpr std::uint8_t member_targets(std::uint8_t member) {
  return member == 0 ? (1U << AB) | (1U << AC) | (1U << BF) : member <= 3 ? 1U << (member - 1) : 0;
}

constexpr std::uint16_t member_sources(std::uint8_t member) {
  std::uint16_t mask = 0;
  const auto targets = member_targets(member);
  for (unsigned slot = 0; slot < product_count; ++slot)
    if (targets & (1U << slot))
      mask |= 3U << (2 * slot);
  return mask;
}

void validate(const DepthSide&, BookSide);
Implied derive(const Route&, const Sources&);
// Returns the changed target-symbol mask, including replacements with empty views.
std::uint8_t recompute(const Sources&, std::uint16_t dirty_sources, Views&);
}  // namespace marketlab::orders::spreads
