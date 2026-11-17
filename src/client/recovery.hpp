#pragma once
#include "shared_data/runtime/io.hpp"
#include "shared_data/market_data/config.hpp"
#include "shared_data/market_data/market_data.hpp"
#include <functional>
#include <optional>
#include <set>
#include <string_view>

namespace marketlab::md {
enum class SyncStatus {
  Initializing,
  Synced,
  GapWait,
  Recovering,
  Late,
  Stale
};
const char* status_name(SyncStatus status);

struct MarketView {
  std::optional<Message> snapshot;
  std::uint64_t highest_seen = 0;
  std::size_t pending_count = 0;
  SyncStatus status = SyncStatus::Initializing;
};

struct SnapshotRequest {
  std::uint16_t symbol;
  std::uint64_t generation, token;
};

// All methods, including TCP completion, are called by one state owner.
class MarketRecovery {
 public:
  using Emit = std::function<void(std::string_view, std::uint16_t, const MarketView&)>;
  MarketRecovery(const std::map<std::uint16_t, std::string>& symbols,
                 RecoveryConfig config,
                 runtime::Deadline now,
                 Emit emit = {});
  void udp(const Message& message, runtime::Deadline now);
  void invalid(std::optional<std::uint16_t> symbol, runtime::Deadline now);
  void tick(runtime::Deadline now);
  std::optional<SnapshotRequest> next_request(runtime::Deadline now);
  bool valid(const SnapshotRequest& request) const;
  void complete(const SnapshotRequest& request, const Message& snapshot, runtime::Deadline now);
  void failed(const SnapshotRequest& request, runtime::Deadline now);
  const MarketView& view(std::uint16_t symbol) const;

  std::size_t buffered_bytes() const {
    return buffered_bytes_;
  }

  // A fixed-size record plus a conservative map-node allowance is charged per entry.
  static constexpr std::size_t pending_charge = 1024;

 private:
  struct Pending {
    Epoch epoch;
    std::uint64_t index, timestamp;
    std::array<Level, 4 * depth> levels{};
    std::size_t count;
    Kind kind;
    explicit Pending(const Message& message);
    Message message(std::uint16_t symbol) const;
  };

  static_assert(sizeof(Pending) + 128 <= pending_charge);

  struct State : MarketView {
    std::map<std::uint64_t, Pending> pending;
    std::optional<Message> last_delta;
    std::optional<runtime::Deadline> gap_since;
    runtime::Deadline advanced, retry_at;
    int retry_ms = 0;
    std::uint64_t token = 0;
    bool check_required = false;
    std::optional<Epoch> candidate;
    std::uint64_t candidate_high = 0;
  };

  RecoveryConfig config_;
  std::map<std::uint16_t, State> states_;
  std::optional<Epoch> epoch_;
  std::set<Epoch> retired_;
  bool probe_ = false;
  std::uint16_t probe_symbol_ = 0, last_requested_ = 0;
  std::uint64_t generation_ = 1, token_ = 0;
  std::size_t buffered_bytes_ = 0;
  Emit emit_;
  void emit(std::string_view event, std::uint16_t id);
  void refresh(std::uint16_t id, runtime::Deadline now);
  void clear_pending(State& state);
  void require_check(std::uint16_t id, runtime::Deadline now, std::string_view reason);
  void commit(std::uint16_t id, Message snapshot, runtime::Deadline now, bool advanced);
  bool apply(std::uint16_t id, const Message& delta, runtime::Deadline now);
  void drain(std::uint16_t id, runtime::Deadline now);
  void install(std::uint16_t id, const Message& snapshot, runtime::Deadline now, bool authoritative);
  void confirm_epoch(const Epoch& epoch, runtime::Deadline now);
  bool needs_query(const State& state, std::uint16_t id, runtime::Deadline now) const;
};
}  // namespace marketlab::md
