#pragma once

#include "shared_data/market_data/transport.hpp"

namespace marketlab::md {
// Raw diagnostics remain available separately from the default stateful client.
void observe_udp(const Endpoint& endpoint, std::size_t count, const std::atomic_bool& stop);
void observe_multicast(const MulticastConfig& config,
                       std::size_t count,
                       const std::atomic_bool& stop);
void observe_market(const Config& config, std::size_t count, const std::atomic_bool& stop);
Message query_snapshot(const Endpoint& endpoint, std::uint16_t symbol, int timeout_ms);
}  // namespace marketlab::md
