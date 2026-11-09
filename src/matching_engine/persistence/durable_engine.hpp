#pragma once
#include <deque>
#include <thread>

#include "matching_engine/engine.hpp"
#include "matching_engine/bpt/history_store.hpp"
#include "matching_engine/persistence/queue.hpp"
#include "matching_engine/persistence/wakeup.hpp"
#include "matching_engine/recovery/storage.hpp"

namespace marketlab::orders::persistence {
class DurableEngine final : public EngineBackend {
 public:
  DurableEngine(const OrderConfig&, std::uint16_t, Options);
  ~DurableEngine() override;
  void command(const LinkHeader&, std::span<const std::uint8_t>, const Emit&) override;
  int notification_fd() const noexcept override { return wakeup_.fd(); }
  void poll() override;
  void drain() override;
  void request_checkpoint();
  void wait_checkpoint();
  std::size_t symbol_count() const noexcept override { return committed_.symbol_count(); }
  void copy_market(MarketSnapshot& out) noexcept override { committed_.market(out); }
  Boundary boundary() const { return boundary_; }
  std::uint64_t checkpoint_sequence() const { return checkpoint_seq_; }
  const std::filesystem::path& directory() const { return storage_.root(); }

 private:
  struct Batch {
    Result result;
    Boundary b;
    Emit emit;
    std::atomic<unsigned> done{0};
    std::uint64_t trade_end = 0, trade_count = 0;
  };
  struct Cut {
    std::uint64_t seq = 0;
    std::atomic_bool done{false};
  };
  struct Job {
    Batch* batch = nullptr;
    Cut* cut = nullptr;
  };
  struct Snapshot {
    Boundary b;
    FrozenPages pages;
    std::atomic_bool done{false};
  };
  struct Query {
    std::uint64_t target = 0;
    LinkHeader context{};
    Header request{};
    std::unique_ptr<OrderState[]> orders;
    std::size_t count = 0;
    Emit emit;
  };
  Options options_;
  Storage storage_;
  MatchingEngine working_;
  CommittedState committed_;
  Boundary boundary_;
  Boundary initial_;
  Wakeup wakeup_;
  std::unique_ptr<bpt::HistoryStore> history_;
  std::uint64_t executed_ = 0, checkpoint_seq_ = 0;
  std::size_t pending_bytes_ = 0;
  std::deque<std::unique_ptr<Batch>> pending_;
  std::vector<std::unique_ptr<Batch>> free_;
  std::deque<Query> queries_;
  std::size_t query_bytes_ = 0;
  Queue<Job> trade_queue_, order_queue_;
  Queue<Snapshot*> snapshot_queue_{1};
  std::unique_ptr<Cut> cut_;
  std::unique_ptr<Snapshot> snapshot_;
  bool snapshot_sent_ = false;
  std::atomic<std::uint64_t> delta_bytes_{0};
  std::atomic_bool failed_{false};
  std::mutex completion_mutex_;
  std::condition_variable completion_;
  std::exception_ptr error_;
  std::thread trade_thread_, order_thread_, snapshot_thread_;
  std::chrono::steady_clock::time_point last_checkpoint_ = std::chrono::steady_clock::now();
  void writer(bool trades);
  void snapshot_worker();
  void service_checkpoint();
  void capture_query(const LinkHeader&, const Header&, const Emit&);
  void emit_queries();
  void fail(std::exception_ptr);
  void check_error();
  void wait_progress();
  void stop() noexcept;
  void hook(Stage, std::uint64_t);
};
}  // namespace marketlab::orders::persistence
