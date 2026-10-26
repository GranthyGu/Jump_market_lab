#pragma once

#include "common/io.hpp"

namespace marketlab::md {
// This simulator only supplies aggregate views; it does not execute orders.
class MockShard {
 public:
  MockShard(const Config& config, ShardConfig shard, bool is_static);
  void run(const std::atomic_bool& stop);
 private:
  ShardView generate(std::uint64_t step) const;
  Config config_;
  ShardConfig shard_;
  Epoch epoch_;
  bool static_;
  std::mutex mutex_;
  ShardView current_;
};

// An empty shard name starts all configured mock shards in this process.
void run_mock(const Config& config, const std::string& shard_name, bool is_static,
              std::atomic_bool& stop);
}  // namespace marketlab::md
