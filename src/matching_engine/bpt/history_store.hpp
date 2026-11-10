#pragma once

#include <condition_variable>
#include <deque>
#include <thread>
#include <unordered_map>
#include "matching_engine/backend.hpp"
#include "matching_engine/persistence/files.hpp"
#include "shared_data/orders/config.hpp"
#include "trade_index.hpp"

namespace marketlab::orders::bpt {

// The log is authoritative. Only the matching owner publishes committed byte
// boundaries; one index worker tails those bytes, while query workers use B+ tree
// page read latches. All Emit callbacks run on the matching owner via poll().
class HistoryStore {
 public:
  using Emit = EngineBackend::Emit;
  HistoryStore(const std::filesystem::path& root,
               const OrderConfig&,
               std::uint16_t shard,
               std::uint64_t recovered_end,
               std::size_t cache_pages,
               std::size_t query_slots,
               std::function<void()> wakeup,
               std::uint64_t checkpoint_bytes = 64 * 1024 * 1024);
  ~HistoryStore();
  void publish(std::uint64_t committed_end);
  void request(const LinkHeader&,
               const TradeHistoryRequest&,
               std::uint64_t committed_end,
               const Emit&);
  void poll();
  bool pending() const;

  bool caught_up(std::uint64_t end) const {
    std::lock_guard lock(mutex_);
    return indexed_ >= end && !failure_;
  }

  void close() noexcept;

 private:
  struct Task {
    LinkHeader context;
    TradeHistoryRequest request;
    Emit emit;
  };

  struct Answer {
    Task task;
    std::vector<persistence::TradeRecord> records;
    std::uint64_t next = 0;
    bool more = false;
    RejectReason error{};
  };

  std::filesystem::path root_, log_path_, working_path_, snapshot_path_;
  std::uint16_t shard_;
  std::uint64_t fingerprint_ = 1469598103934665603ULL;
  std::unordered_map<std::string, std::uint32_t> users_;
  std::vector<std::string> names_;
  std::unique_ptr<TradeIndex> index_;
  std::function<void()> wakeup_;
  std::size_t query_slots_;
  std::uint64_t checkpoint_bytes_;
  mutable std::mutex mutex_;
  std::condition_variable ready_;
  std::uint64_t target_ = 0, indexed_ = 0;
  std::size_t outstanding_ = 0;
  bool stopping_ = false;
  std::exception_ptr failure_;
  std::deque<Task> requests_;
  std::deque<Answer> answers_;
  std::thread index_thread_;
  std::vector<std::thread> readers_;
  // Only the index worker mutates checkpoint metadata after construction.
  std::uint64_t checkpoint_end_ = 0, last_frame_ = 0;
  std::uint32_t first_crc_ = 0, last_crc_ = 0;
  bool restore(std::uint64_t recovered_end);
  void checkpoint(std::uint64_t end);
  void index_worker();
  void query_worker();
  void report_failure(std::exception_ptr);
};
}  // namespace marketlab::orders::bpt
