#include "shared_data/runtime/io.hpp"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstring>
#include <stdexcept>

namespace marketlab::runtime {
namespace {
using Bytes = std::vector<std::uint8_t>;

std::runtime_error system_error(const std::string& what) {
  return std::runtime_error(what + ": " + std::strerror(errno));
}

sockaddr_in address(const std::string& host, std::uint16_t port) {
  sockaddr_in out{};
  out.sin_family = AF_INET;
  out.sin_port = htons(port);
  if (inet_pton(AF_INET, host.c_str(), &out.sin_addr) != 1)
    throw std::runtime_error("Expected numeric IPv4 address: " + host);
  return out;
}

void nonblocking(int fd) {
  const int flags = fcntl(fd, F_GETFL, 0);
  if (flags < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0)
    throw system_error("fcntl");
}

void no_delay(int fd) {
  const int yes = 1;
  if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes)) < 0)
    throw system_error("TCP_NODELAY");
}

void wait_for(int fd, short events, Deadline deadline) {
  for (;;) {
    const auto remaining =
        std::chrono::duration_cast<std::chrono::milliseconds>(deadline - Clock::now()).count();
    if (remaining <= 0)
      throw std::runtime_error("I/O deadline exceeded");
    pollfd p{fd, events, 0};
    const int result = poll(&p, 1, static_cast<int>(std::min<std::int64_t>(remaining, 1000)));
    if (result < 0 && errno == EINTR)
      continue;
    if (result < 0)
      throw system_error("poll");
    if (!result)
      continue;
    if (p.revents & events)
      return;
    if (p.revents & (POLLERR | POLLHUP | POLLNVAL))
      throw std::runtime_error("Connection closed");
  }
}

}  // namespace

Fd::~Fd() {
  reset();
}

Fd::Fd(Fd&& other) noexcept
    : value_(other.value_) {
  other.value_ = -1;
}

Fd& Fd::operator=(Fd&& other) noexcept {
  if (this != &other) {
    reset();
    value_ = other.value_;
    other.value_ = -1;
  }
  return *this;
}

void Fd::reset(int value) {
  if (value_ >= 0)
    ::close(value_);
  value_ = value;
}

Fd connect_tcp(const std::string& host, std::uint16_t port, Deadline deadline) {
  const auto addr = address(host, port);
  Fd fd(socket(AF_INET, SOCK_STREAM, 0));
  if (!fd)
    throw system_error("socket");
  nonblocking(fd.get());
  no_delay(fd.get());
  if (connect(fd.get(), reinterpret_cast<const sockaddr*>(&addr), sizeof(addr)) < 0) {
    if (errno != EINPROGRESS)
      throw system_error("connect");
    wait_for(fd.get(), POLLOUT, deadline);
    int error = 0;
    socklen_t size = sizeof(error);
    if (getsockopt(fd.get(), SOL_SOCKET, SO_ERROR, &error, &size) < 0)
      throw system_error("SO_ERROR");
    if (error) {
      errno = error;
      throw system_error("connect");
    }
  }
  return fd;
}

Fd listen_tcp(const std::string& host, std::uint16_t port) {
  const auto addr = address(host, port);
  Fd fd(socket(AF_INET, SOCK_STREAM, 0));
  if (!fd)
    throw system_error("socket");
  const int yes = 1;
  if (setsockopt(fd.get(), SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) < 0)
    throw system_error("SO_REUSEADDR");
  if (bind(fd.get(), reinterpret_cast<const sockaddr*>(&addr), sizeof(addr)) < 0)
    throw system_error("bind");
  if (listen(fd.get(), 128) < 0)
    throw system_error("listen");
  nonblocking(fd.get());
  return fd;
}

void send_all(int fd, std::span<const std::uint8_t> data, Deadline deadline) {
  std::size_t offset = 0;
  while (offset < data.size()) {
    wait_for(fd, POLLOUT, deadline);
    const auto n = send(fd, data.data() + offset, data.size() - offset, 0);
    if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR))
      continue;
    if (n <= 0)
      throw system_error("send");
    offset += static_cast<std::size_t>(n);
  }
}

Bytes receive_exact(int fd, std::size_t length, Deadline deadline) {
  Bytes data(length);
  std::size_t offset = 0;
  while (offset < length) {
    wait_for(fd, POLLIN, deadline);
    const auto n = recv(fd, data.data() + offset, length - offset, 0);
    if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR))
      continue;
    if (n < 0)
      throw system_error("recv");
    if (n == 0)
      throw std::runtime_error("Peer closed connection");
    offset += static_cast<std::size_t>(n);
  }
  return data;
}

void serve_requests(Fd listener,
                    std::size_t request_size,
                    std::size_t response_limit,
                    int timeout_ms,
                    std::size_t connection_limit,
                    const RequestHandler& handler,
                    const std::atomic_bool& stop) {
  struct Client {
    Fd fd;
    Bytes input, output;
    std::size_t sent = 0;
    Deadline expiry;
  };

  std::vector<Client> clients;
  while (!stop.load()) {
    std::vector<pollfd> fds{{listener.get(), POLLIN, 0}};
    for (const auto& c : clients)
      fds.push_back({c.fd.get(), static_cast<short>(c.output.empty() ? POLLIN : POLLOUT), 0});
    const int ready = poll(fds.data(), fds.size(), 50);
    if (ready < 0 && errno == EINTR)
      continue;
    if (ready < 0)
      throw system_error("server poll");
    // Process existing clients before accepting new ones to preserve poll indices.
    for (std::size_t i = 0; i < clients.size(); ++i) {
      auto& c = clients[i];
      const auto events = fds[i + 1].revents;
      if (Clock::now() >= c.expiry || (events & (POLLERR | POLLNVAL))) {
        c.fd.reset();
        continue;
      }
      try {
        if (c.output.empty() && (events & POLLIN)) {
          std::array<std::uint8_t, 2> buffer{};
          const auto n = recv(c.fd.get(), buffer.data(), request_size - c.input.size(), 0);
          if (n == 0) {
            c.fd.reset();
            continue;
          }
          if (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR)
            throw system_error("server recv");
          if (n > 0) {
            c.input.insert(c.input.end(), buffer.begin(), buffer.begin() + n);
            if (c.input.size() == request_size) {
              c.output = handler(c.input);
              c.input.clear();
              if (c.output.empty() || c.output.size() > response_limit)
                throw std::runtime_error("No valid snapshot");
              c.expiry = Clock::now() + std::chrono::milliseconds(timeout_ms);
            }
          }
        } else if (!c.output.empty() && (events & POLLOUT)) {
          const auto n = send(c.fd.get(), c.output.data() + c.sent, c.output.size() - c.sent, 0);
          if (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR)
            throw system_error("server send");
          if (n > 0)
            c.sent += static_cast<std::size_t>(n);
          if (c.sent == c.output.size()) {
            c.output.clear();
            c.sent = 0;
            c.expiry = Clock::now() + std::chrono::milliseconds(timeout_ms);
          }
        }
        if ((events & POLLHUP) && !(events & (POLLIN | POLLOUT)))
          c.fd.reset();
      } catch (const std::exception&) {
        c.fd.reset();
      }
    }
    std::erase_if(clients, [](const Client& c) {
      return !c.fd;
    });
    if (fds[0].revents & POLLIN) {
      // Bound accept work so connection floods cannot starve established clients.
      for (unsigned i = 0; i < 32; ++i) {
        Fd fd(accept(listener.get(), nullptr, nullptr));
        if (!fd)
          break;
        if (clients.size() >= connection_limit)
          continue;
        try {
          nonblocking(fd.get());
          no_delay(fd.get());
          clients.push_back(
              {std::move(fd), {}, {}, 0, Clock::now() + std::chrono::milliseconds(timeout_ms)});
        } catch (const std::exception&) {
        }
      }
    }
  }
}

Fd bind_udp(const std::string& host, std::uint16_t port) {
  Fd fd(socket(AF_INET, SOCK_DGRAM, 0));
  if (!fd)
    throw system_error("UDP receiver");
  const auto addr = address(host, port);
  if (bind(fd.get(), reinterpret_cast<const sockaddr*>(&addr), sizeof(addr)) < 0)
    throw system_error("UDP bind");
  return fd;
}

}  // namespace marketlab::runtime
