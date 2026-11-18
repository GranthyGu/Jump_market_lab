#pragma once
#include "matching_engine/spreads/transport/protocol.hpp"

namespace marketlab::orders::spreads {
struct Recovery {
  std::uint64_t instance = 0;
  persistence::Boundary boundary;
  std::uint64_t indexed_end = 0;
};

struct Endpoint {
  Address address;
  std::uint64_t instance = 0;
};

using Directory = std::array<Endpoint, 4>;

constexpr std::uint8_t peer_mask(std::uint8_t member) {
  return member == 0 ? 14 : member == 1 ? 13 : member < 4 ? 3 : 0;
}

bool same_boundary(const persistence::Boundary&, const persistence::Boundary&);
bool same_recovery(const Recovery&, const Recovery&);
void put_recovery(Bytes&, const Recovery&);
Recovery read_recovery(persistence::Reader&);
void put_endpoint(Bytes&, const Endpoint&);
Endpoint read_endpoint(persistence::Reader&);
Directory directory(const OrderConfig&, const Topology&);
}  // namespace marketlab::orders::spreads
