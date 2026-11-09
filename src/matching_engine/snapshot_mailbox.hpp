#pragma once
#include "matching_engine/backend.hpp"
#include "shared_data/market_data/transport.hpp"

namespace marketlab::orders {
// Bounded multi-reader/single-owner handoff. Payloads are ordinary memory:
// ownership, not speculative retries over racing reads, makes them safe.
class SnapshotMailbox {
 public:
  static constexpr unsigned capacity = 8;
  explicit SnapshotMailbox(std::size_t symbols);
  ~SnapshotMailbox();
  SnapshotMailbox(const SnapshotMailbox&) = delete;
  SnapshotMailbox& operator=(const SnapshotMailbox&) = delete;
  // Concurrent readers may call read(). Saturation, timeout and shutdown throw.
  md::ShardView read(md::Deadline deadline);
  // Sole engine owner; call between commands and on reactor wakeup. No waiting,
  // allocation, encoding, or network output. At most capacity copies per call.
  void service(EngineBackend& engine) noexcept;
  int request_fd() const noexcept;
  void drain_requests() noexcept;
  // Wake readers before joining their threads. Join before destroying mailbox.
  void close() noexcept;

 private:
  class Signal {
   public:
    Signal();
    void notify() noexcept;
    void drain() noexcept;

    int fd() const noexcept {
      return read_.get();
    }

   private:
    md::Fd read_, write_;
  };

  enum State : std::uint32_t {
    Free,
    Reserved,
    Requested,
    Writing,
    Ready,
    Abandoned
  };

  struct alignas(64) Slot {
    explicit Slot(std::size_t symbols)
        : snapshot(symbols) {
    }

    std::atomic<std::uint32_t> state{Free};
    MarketSnapshot snapshot;
    Signal completed;
  };

  static_assert(std::atomic<std::uint32_t>::is_always_lock_free);
  std::array<std::unique_ptr<Slot>, capacity> slots_;
  Signal requested_;
  alignas(64) std::atomic<std::uint32_t> pending_{0};
  alignas(64) std::atomic<std::uint32_t> cursor_{0}, closed_{0};
  static void abandon(Slot&) noexcept;
};
}  // namespace marketlab::orders
