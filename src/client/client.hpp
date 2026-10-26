#pragma once

#include "common/io.hpp"

namespace marketlab::md {
// Decode and print datagrams; this diagnostic client does not reconstruct a book.
void observe_udp(const Endpoint& endpoint, std::size_t count, const std::atomic_bool& stop);
Message query_snapshot(const Endpoint& endpoint, std::uint16_t symbol, int timeout_ms);
}  // namespace marketlab::md
