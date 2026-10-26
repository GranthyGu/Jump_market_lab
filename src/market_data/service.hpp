#pragma once

#include "common/io.hpp"

#include <mutex>

namespace marketlab::md {
inline constexpr auto publication_period = std::chrono::seconds(1);

struct Batch {
  Epoch epoch;
  std::uint64_t index;
  std::uint64_t timestamp;
  std::map<std::uint16_t, Book> books;
  std::map<std::uint16_t, Bytes> snapshots;
};

class Publisher {
 public:
  explicit Publisher(Config config);
  void run(const std::atomic_bool& stop);
  std::shared_ptr<const Batch> latest() const {
    std::lock_guard lock(latest_mutex_);
    return latest_;
  }
 private:
  void set_latest(std::shared_ptr<const Batch> batch) {
    std::lock_guard lock(latest_mutex_);
    latest_ = std::move(batch);
  }
  void sample_loop(const std::atomic_bool& stop, const std::atomic_bool& cancelled);
  Config config_;
  UdpSender sender_;
  std::vector<std::unique_ptr<ShardSource>> sources_;
  mutable std::mutex latest_mutex_;
  std::shared_ptr<const Batch> latest_;
};

}  // namespace marketlab::md
