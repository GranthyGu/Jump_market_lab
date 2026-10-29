#include "client/market_data.hpp"

#include <poll.h>
#include <sys/socket.h>

#include <cerrno>
#include <cstring>
#include <iostream>
#include <stdexcept>

namespace marketlab::md {
void observe_udp(const Endpoint& endpoint, std::size_t count, const std::atomic_bool& stop) {
  auto fd = bind_udp(endpoint.host, endpoint.port);
  std::cout << "Listening UDP " << endpoint.host << ':' << endpoint.port << std::endl;
  std::size_t received = 0;
  while (!stop.load() && (!count || received < count)) {
    pollfd p{fd.get(), POLLIN, 0};
    if (poll(&p, 1, 100) <= 0) continue;
    std::array<std::uint8_t, 65536> data{};
    const auto n = recv(fd.get(), data.data(), data.size(), 0);
    if (n < 0) throw std::runtime_error(std::string("UDP recv: ") + std::strerror(errno));
    try { print_message(decode(std::span(data.data(), static_cast<std::size_t>(n)))); ++received; }
    catch (const std::exception& e) { std::cerr << "Ignored invalid datagram: " << e.what() << '\n'; }
  }
}
Message query_snapshot(const Endpoint& endpoint, std::uint16_t symbol, int timeout_ms) {
  const auto deadline = Clock::now() + std::chrono::milliseconds(timeout_ms);
  auto fd = connect_tcp(endpoint.host, endpoint.port, deadline);
  Bytes request;
  append_uint(request, symbol, 2);
  send_all(fd.get(), request, deadline);
  const auto prefix = receive_exact(fd.get(), 2, deadline);
  std::size_t offset = 0;
  const auto length = read_uint(prefix, offset, 2);
  if (length < header_size || length > max_snapshot_size) throw std::runtime_error("Invalid snapshot length");
  auto message = decode(receive_exact(fd.get(), length, deadline));
  if (message.kind != Kind::Snapshot || message.symbol_id != symbol) throw std::runtime_error("Unexpected snapshot");
  return message;
}
}  // namespace marketlab::md
