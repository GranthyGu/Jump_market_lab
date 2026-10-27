#pragma once

#include "market_publisher/config.hpp"
#include "market_publisher/protocol/market_data.hpp"

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace marketlab::md {
using Clock = std::chrono::steady_clock;
using Deadline = Clock::time_point;

void sleep_until_or_stop(Deadline deadline, const std::atomic_bool& stop,
                         const std::atomic_bool& cancelled);

// A small joining thread keeps this C++20 program compatible with older libc++.
class StopThread {
 public:
  template <typename F>
  explicit StopThread(F fn) : stop_(std::make_shared<std::atomic_bool>(false)),
      thread_([flag = stop_, fn = std::move(fn)]() mutable { fn(*flag); }) {}
  ~StopThread() { stop_->store(true); if (thread_.joinable()) thread_.join(); }
  StopThread(const StopThread&) = delete;
  StopThread& operator=(const StopThread&) = delete;
 private:
  std::shared_ptr<std::atomic_bool> stop_;
  std::thread thread_;
};

class Log {
 public:
  explicit Log(std::ostream& output) : output_(output) {}
  ~Log();
  template <typename T> Log& operator<<(const T& value) { buffer_ << value; return *this; }
 private:
  std::ostream& output_;
  std::ostringstream buffer_;
};

class Fd {
 public:
  explicit Fd(int value = -1) : value_(value) {}
  ~Fd();
  Fd(const Fd&) = delete;
  Fd& operator=(const Fd&) = delete;
  Fd(Fd&& other) noexcept;
  Fd& operator=(Fd&& other) noexcept;
  int get() const { return value_; }
  explicit operator bool() const { return value_ >= 0; }
  void reset(int value = -1);
 private:
  int value_;
};

Fd connect_tcp(const std::string& host, std::uint16_t port, Deadline deadline);
Fd listen_tcp(const std::string& host, std::uint16_t port);
Fd bind_udp(const std::string& host, std::uint16_t port);
void send_all(int fd, std::span<const std::uint8_t> data, Deadline deadline);
Bytes receive_exact(int fd, std::size_t length, Deadline deadline);

// Both transports return an owned, immutable-by-convention top-five view.
class ShardSource {
 public:
  virtual ~ShardSource() = default;
  virtual ShardView read(Deadline deadline) = 0;
  virtual void disconnect() = 0;
};
std::unique_ptr<ShardSource> make_source(const ShardConfig& config, int max_age_ms);

// The writer owns a separate lifetime lock; data locks protect coherent copies.
class SharedViewWriter {
 public:
  SharedViewWriter(const std::string& path, std::size_t symbols);
  ~SharedViewWriter();
  SharedViewWriter(const SharedViewWriter&) = delete;
  SharedViewWriter& operator=(const SharedViewWriter&) = delete;
  void publish(const ShardView& view);
 private:
  Fd owner_;
  Fd file_;
  void* mapping_ = nullptr;
  std::size_t capacity_ = 0;
};

// A provider copies engine state quickly; the server performs socket writes later.
using SnapshotProvider = std::function<ShardView()>;
using RequestHandler = std::function<Bytes(std::span<const std::uint8_t>)>;
void serve_requests(Fd listener, std::size_t request_size, std::size_t response_limit,
                    int timeout_ms, std::size_t connection_limit,
                    const RequestHandler& handler, const std::atomic_bool& stop);
void serve_shard(Fd listener, SnapshotProvider provider, const std::atomic_bool& stop,
                 int timeout_ms = 3000, std::size_t connection_limit = 128);

class UdpSender {
 public:
  explicit UdpSender(const std::vector<Endpoint>& destinations);
  void send(std::span<const std::uint8_t> bytes) const;
 private:
  Fd socket_;
  struct Target;
  std::vector<std::shared_ptr<Target>> targets_;
};
}  // namespace marketlab::md
