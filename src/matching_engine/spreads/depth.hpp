#pragma once

#include <array>
#include <cstdint>
#include <span>
#include "shared_data/orders/config.hpp"
#include "shared_data/orders/engine_link.hpp"

namespace marketlab::orders::spreads {
inline constexpr std::size_t source_depth = 3, route_depth = 5;
inline constexpr std::uint8_t no_slot = 255;

enum Slot : std::uint8_t {
  A,
  B,
  C,
  AB,
  AC
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
using Sources = std::array<DepthSide, 10>;

struct Route {
  std::uint8_t target, side, first, second, spread, priority;
  bool add;
};

inline constexpr std::array<Route, 12> routes{{
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
}};
inline constexpr auto dependencies = [] {
  std::array<std::uint16_t, 10> masks{};
  for (std::size_t i = 0; i < routes.size(); ++i) {
    masks[routes[i].first] |= std::uint16_t(1U << i);
    masks[routes[i].second] |= std::uint16_t(1U << i);
  }
  return masks;
}();

struct Implied {
  std::array<std::uint64_t, route_depth> quantity{};
  std::array<std::int32_t, route_depth> price{};
  std::uint8_t count = 0;
  bool more = false;
  bool operator==(const Implied&) const = default;
};

static_assert(sizeof(Implied) == 64);
using Views = std::array<Implied, 12>;

struct Topology {
  explicit Topology(const OrderConfig&);
  std::array<std::uint16_t, 5> symbols{};
  std::array<std::uint16_t, 4> shards{};  // S, A, B, C
  std::array<PriceLimits, 5> limits{};
  std::uint64_t fingerprint = 1469598103934665603ULL;
  std::uint8_t slot(std::uint16_t symbol) const;
  std::uint8_t member(std::uint16_t shard) const;

  std::uint8_t owner(std::uint8_t slot) const {
    return slot >= AB ? 0 : slot + 1;
  }
};

void validate(const DepthSide&, BookSide);
Implied derive(const Route&, const Sources&);
// Returns the changed target-symbol mask, including replacements with empty views.
std::uint8_t recompute(const Sources&, std::uint16_t dirty_sources, Views&);

enum class DepthEncoding : std::uint8_t {
  Snapshot = 1,
  Delta = 2
};
// One source-side record. Explicit LE fields; no native padding on the wire.
void encode_depth(Bytes&,
                  std::uint8_t source,
                  const DepthSide& before,
                  const DepthSide& after,
                  bool initial = false);
// Install only a fully validated record. Initial snapshots require a fresh join.
std::uint8_t apply_depth(std::span<const std::uint8_t>, Sources&, bool initial = false);
}  // namespace marketlab::orders::spreads
