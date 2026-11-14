#pragma once

#include "shared_data/market_data/config.hpp"
#include "shared_data/market_data/market_data.hpp"
#include "shared_data/runtime/io.hpp"

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace marketlab::md {
// Bind the shared UDP port and join the group on the selected local interface.
runtime::Fd join_multicast(const MulticastConfig& config);

// Both transports return an owned, immutable-by-convention top-five view.
class ShardSource {
 public:
  virtual ~ShardSource() = default;
  virtual ShardView read(runtime::Deadline deadline) = 0;
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
  runtime::Fd owner_;
  runtime::Fd file_;
  void* mapping_ = nullptr;
  std::size_t capacity_ = 0;
};

// A provider copies engine state quickly; the server performs socket writes later.
using SnapshotProvider = std::function<ShardView()>;
void serve_shard(runtime::Fd listener,
                 SnapshotProvider provider,
                 const std::atomic_bool& stop,
                 int timeout_ms = 3000,
                 std::size_t connection_limit = 128);

class UdpSender {
 public:
  explicit UdpSender(const std::vector<Endpoint>& destinations);
  explicit UdpSender(const MulticastConfig& config);
  explicit UdpSender(const Config& config);
  void send(std::span<const std::uint8_t> bytes,
            runtime::Deadline submission_deadline =
                runtime::Clock::now() + std::chrono::milliseconds(100)) const;

  // The publication thread owns sends and this local-submission counter.
  std::uint64_t submission_retries() const {
    return submission_retries_;
  }

 private:
  runtime::Fd socket_;
  mutable std::uint64_t submission_retries_ = 0;
  struct Target;
  std::vector<std::shared_ptr<Target>> targets_;
};
}  // namespace marketlab::md
