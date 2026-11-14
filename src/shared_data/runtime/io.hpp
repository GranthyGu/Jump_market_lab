#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <span>
#include <string>
#include <vector>

namespace marketlab::runtime {
using Clock = std::chrono::steady_clock;
using Deadline = Clock::time_point;

class Fd {
 public:
  explicit Fd(int value = -1)
      : value_(value) {
  }

  ~Fd();
  Fd(const Fd&) = delete;
  Fd& operator=(const Fd&) = delete;
  Fd(Fd&& other) noexcept;
  Fd& operator=(Fd&& other) noexcept;

  int get() const {
    return value_;
  }

  explicit operator bool() const {
    return value_ >= 0;
  }

  void reset(int value = -1);

 private:
  int value_;
};

Fd connect_tcp(const std::string& host, std::uint16_t port, Deadline deadline);
Fd listen_tcp(const std::string& host, std::uint16_t port);
Fd bind_udp(const std::string& host, std::uint16_t port);
void send_all(int fd, std::span<const std::uint8_t> data, Deadline deadline);
std::vector<std::uint8_t> receive_exact(int fd, std::size_t length, Deadline deadline);

// A bounded nonblocking server for fixed requests of one or two bytes.
// The handler supplies the complete response, including its framing.
using RequestHandler = std::function<std::vector<std::uint8_t>(std::span<const std::uint8_t>)>;
void serve_requests(Fd listener,
                    std::size_t request_size,
                    std::size_t response_limit,
                    int timeout_ms,
                    std::size_t connection_limit,
                    const RequestHandler& handler,
                    const std::atomic_bool& stop);
}  // namespace marketlab::runtime
