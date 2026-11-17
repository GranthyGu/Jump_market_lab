#include "shared_data/runtime/io.hpp"
#include "client/recovery.hpp"
#include <algorithm>
#include <limits>
#include <stdexcept>

namespace marketlab::md {
namespace {
bool same(const Message& a, const Message& b) {
  return a.kind == b.kind && a.epoch == b.epoch && a.symbol_id == b.symbol_id &&
         a.index == b.index && a.timestamp == b.timestamp && a.levels == b.levels;
}

bool ordered(const Level& a, const Level& b) {
  return a.side != b.side ? a.side < b.side
                          : (a.side == Side::Buy ? a.price > b.price : a.price < b.price);
}
}

const char* status_name(SyncStatus status) {
  switch (status) {
    case SyncStatus::Initializing:
      return "INITIALIZING";
    case SyncStatus::Synced:
      return "SYNCED";
    case SyncStatus::GapWait:
      return "GAP_WAIT";
    case SyncStatus::Recovering:
      return "RECOVERING";
    case SyncStatus::Late:
      return "LATE";
    case SyncStatus::Stale:
      return "STALE";
  }
  return "UNKNOWN";
}

MarketRecovery::Pending::Pending(const Message& m)
    : epoch(m.epoch),
      index(m.index),
      timestamp(m.timestamp),
      count(m.levels.size()),
      kind(m.kind) {
  std::copy(m.levels.begin(), m.levels.end(), levels.begin());
}

Message MarketRecovery::Pending::message(std::uint16_t id) const {
  return {kind, epoch, id, index, timestamp, {levels.begin(), levels.begin() + count}};
}

MarketRecovery::MarketRecovery(const std::map<std::uint16_t, std::string>& symbols,
                               RecoveryConfig config,
                               runtime::Deadline now,
                               Emit emit)
    : config_(config),
      emit_(std::move(emit)) {
  validate_recovery(config_);
  for (const auto& [id, name] : symbols) {
    (void)name;
    auto& state = states_[id];
    state.advanced = state.retry_at = now;
    state.retry_ms = config_.retry_initial_ms;
  }
}

const MarketView& MarketRecovery::view(std::uint16_t id) const {
  return states_.at(id);
}

void MarketRecovery::emit(std::string_view event, std::uint16_t id) {
  auto& state = states_.at(id);
  state.pending_count = state.pending.size();
  if (emit_)
    emit_(event, id, state);
}

void MarketRecovery::clear_pending(State& state) {
  buffered_bytes_ -= state.pending.size() * pending_charge;
  state.pending.clear();
  state.pending_count = 0;
}

void MarketRecovery::refresh(std::uint16_t id, runtime::Deadline now) {
  auto& s = states_.at(id);
  const bool gap = s.snapshot && s.highest_seen > s.snapshot->index;
  if (gap && !s.gap_since)
    s.gap_since = now;
  if (!gap)
    s.gap_since.reset();
  const auto age = now - s.advanced;
  auto next = SyncStatus::Synced;
  if (probe_ || s.check_required)
    next = SyncStatus::Recovering;
  else if (!s.snapshot)
    next = SyncStatus::Initializing;
  else if (gap)
    next = (!s.token && now - *s.gap_since < std::chrono::milliseconds(config_.reorder_wait_ms))
               ? SyncStatus::GapWait
               : SyncStatus::Recovering;
  else if (age >= std::chrono::milliseconds(config_.stale_after_ms))
    next = SyncStatus::Stale;
  else if (age >= std::chrono::milliseconds(config_.late_after_ms))
    next = SyncStatus::Late;
  if (next == SyncStatus::Synced)
    s.token = 0;  // UDP may finish an in-flight recovery.
  if (s.status != next) {
    s.status = next;
    emit("STATE", id);
  }
}

void MarketRecovery::require_check(std::uint16_t id,
                                   runtime::Deadline now,
                                   std::string_view reason) {
  auto& s = states_.at(id);
  clear_pending(s);
  if (!s.check_required) {
    s.token = 0;
    s.retry_at = now;
    s.check_required = true;
  }
  refresh(id, now);
  emit(reason, id);
}

void MarketRecovery::invalid(std::optional<std::uint16_t> id, runtime::Deadline now) {
  if (id) {
    if (states_.contains(*id))
      require_check(*id, now, "INVALID_MESSAGE");
  } else {
    for (const auto& [symbol, s] : states_) {
      (void)s;
      require_check(symbol, now, "INVALID_MESSAGE");
    }
  }
}

void MarketRecovery::commit(std::uint16_t id,
                            Message snapshot,
                            runtime::Deadline now,
                            bool advanced) {
  auto& s = states_.at(id);
  snapshot.kind = is_v2(snapshot.kind) ? Kind::SnapshotV2 : Kind::Snapshot;
  s.snapshot = std::move(snapshot);
  if (advanced) {
    s.advanced = now;
    s.retry_ms = config_.retry_initial_ms;
  }
  refresh(id, now);
  emit("BOOK", id);
}

bool MarketRecovery::apply(std::uint16_t id, const Message& delta, runtime::Deadline now) {
  auto& s = states_.at(id);
  auto result = *s.snapshot;
  if (is_v2(delta.kind))
    result.kind = Kind::SnapshotV2;
  for (const auto& change : delta.levels) {
    std::erase_if(result.levels, [&](const Level& level) {
      return level.side == change.side && level.price == change.price;
    });
    if (change.quantity || change.implied_quantity)
      result.levels.push_back(change);
  }
  std::sort(result.levels.begin(), result.levels.end(), ordered);
  try {
    validate_book({id, result.levels, is_v2(result.kind)});
  } catch (...) {
    require_check(id, now, "INVALID_RESULT");
    return false;
  }
  result.index = delta.index;
  result.timestamp = delta.timestamp;
  s.last_delta = delta;
  commit(id, std::move(result), now, true);
  return true;
}

void MarketRecovery::drain(std::uint16_t id, runtime::Deadline now) {
  auto& s = states_.at(id);
  while (s.snapshot && s.snapshot->index != std::numeric_limits<std::uint64_t>::max() &&
         !s.check_required) {
    auto found = s.pending.find(s.snapshot->index + 1);
    if (found == s.pending.end())
      break;
    const auto delta = found->second.message(id);
    s.pending.erase(found);
    buffered_bytes_ -= pending_charge;
    if (!apply(id, delta, now))
      break;
  }
  s.pending_count = s.pending.size();
  refresh(id, now);
}

void MarketRecovery::install(std::uint16_t id,
                             const Message& snapshot,
                             runtime::Deadline now,
                             bool authoritative) {
  auto& s = states_.at(id);
  if (s.snapshot && snapshot.index < s.snapshot->index)
    return;
  const bool advanced = !s.snapshot || snapshot.index > s.snapshot->index;
  if (!advanced &&
      (snapshot.levels != s.snapshot->levels || snapshot.timestamp != s.snapshot->timestamp)) {
    if (!authoritative) {
      require_check(id, now, "SNAPSHOT_CONFLICT");
      return;
    }
    emit("TCP_CORRECTED_CONFLICT", id);
  } else if (!advanced) {
    if (authoritative)
      s.check_required = false;
    drain(id, now);
    return;  // Duplicate snapshots never refresh age.
  }
  if (authoritative)
    s.check_required = false;
  if (s.check_required)
    return;
  for (auto it = s.pending.begin(); it != s.pending.end() && it->first <= snapshot.index;) {
    it = s.pending.erase(it);
    buffered_bytes_ -= pending_charge;
  }
  s.highest_seen = std::max(s.highest_seen, snapshot.index);
  s.last_delta.reset();
  commit(id, snapshot, now, advanced);
  drain(id, now);
}

void MarketRecovery::udp(const Message& message, runtime::Deadline now) {
  const auto id = message.symbol_id;
  if (!states_.contains(id))
    return;
  try {
    (void)encode(message);
  } catch (...) {
    invalid(id, now);
    return;
  }
  auto& s = states_.at(id);
  if (!epoch_ || message.epoch != *epoch_) {
    if (retired_.contains(message.epoch))
      return;
    if (!s.candidate || *s.candidate != message.epoch) {
      s.candidate = message.epoch;
      s.candidate_high = message.index;
    } else
      s.candidate_high = std::max(s.candidate_high, message.index);
    if (!probe_) {
      probe_ = true;
      probe_symbol_ = id;
      ++generation_;
      for (auto& [symbol, state] : states_) {
        state.token = 0;
        state.retry_at = std::min(state.retry_at, now);
        refresh(symbol, now);
      }
    }
    return;  // TCP confirms epochs; unknown UDP cannot replace the baseline.
  }
  s.highest_seen = std::max(s.highest_seen, message.index);
  if (is_snapshot(message.kind)) {
    install(id, message, now, false);
    refresh(id, now);
    return;
  }
  if (s.snapshot && message.index <= s.snapshot->index) {
    if (s.last_delta && message.index == s.last_delta->index && !same(message, *s.last_delta))
      require_check(id, now, "DELTA_CONFLICT");
    return;
  }
  if (s.check_required)
    return;
  if (s.snapshot && message.index - s.snapshot->index == 1) {
    if (apply(id, message, now))
      drain(id, now);
    return;
  }
  if (auto found = s.pending.find(message.index); found != s.pending.end()) {
    if (!same(message, found->second.message(id)))
      require_check(id, now, "DELTA_CONFLICT");
    return;
  }
  if (s.pending.size() >= config_.max_pending_batches ||
      buffered_bytes_ + pending_charge > config_.max_buffer_bytes) {
    require_check(id, now, "BUFFER_LIMIT");
    return;  // Keep highest_seen even after buffered data is discarded.
  }
  s.pending.emplace(message.index, Pending(message));
  buffered_bytes_ += pending_charge;
  s.pending_count = s.pending.size();
  refresh(id, now);
}

void MarketRecovery::confirm_epoch(const Epoch& epoch, runtime::Deadline now) {
  if (!epoch_ || *epoch_ != epoch) {
    if (epoch_)
      retired_.insert(*epoch_);
    while (retired_.size() > 64)
      retired_.erase(retired_.begin());
    epoch_ = epoch;
    ++generation_;
    for (auto& [id, s] : states_) {
      clear_pending(s);
      s.snapshot.reset();
      s.last_delta.reset();
      s.gap_since.reset();
      s.highest_seen = 0;
      s.token = 0;
      s.check_required = false;
      s.advanced = s.retry_at = now;
      s.retry_ms = config_.retry_initial_ms;
      (void)id;
    }
  }
  probe_ = false;
  for (auto& [id, s] : states_) {
    if (s.candidate && *s.candidate == epoch)
      s.highest_seen = std::max(s.highest_seen, s.candidate_high);
    s.candidate.reset();
    refresh(id, now);
  }
}

void MarketRecovery::tick(runtime::Deadline now) {
  for (const auto& [id, s] : states_) {
    (void)s;
    refresh(id, now);
  }
}

bool MarketRecovery::needs_query(const State& s, std::uint16_t id, runtime::Deadline now) const {
  return !s.snapshot || s.check_required || (probe_ && id == probe_symbol_) ||
         (s.highest_seen > s.snapshot->index &&
          (!s.gap_since ||
           now - *s.gap_since >= std::chrono::milliseconds(config_.reorder_wait_ms))) ||
         now - s.advanced >= std::chrono::milliseconds(config_.late_after_ms);
}

std::optional<SnapshotRequest> MarketRecovery::next_request(runtime::Deadline now) {
  if (states_.empty())
    return {};
  auto it = states_.upper_bound(last_requested_);
  for (std::size_t checked = 0; checked < states_.size(); ++checked) {
    if (it == states_.end())
      it = states_.begin();
    const auto id = it->first;
    auto& s = (it++)->second;
    if (s.token || now < s.retry_at || !needs_query(s, id, now))
      continue;
    last_requested_ = id;
    s.token = ++token_;
    refresh(id, now);
    emit("TCP_START", id);
    return SnapshotRequest{id, generation_, s.token};
  }
  return {};
}

bool MarketRecovery::valid(const SnapshotRequest& request) const {
  const auto found = states_.find(request.symbol);
  return request.generation == generation_ && found != states_.end() &&
         found->second.token == request.token;
}

void MarketRecovery::complete(const SnapshotRequest& request,
                              const Message& snapshot,
                              runtime::Deadline now) {
  if (!valid(request))
    return;
  if (!is_snapshot(snapshot.kind) || snapshot.symbol_id != request.symbol) {
    failed(request, now);
    return;
  }
  try {
    (void)encode(snapshot);
  } catch (...) {
    failed(request, now);
    return;
  }
  // Reject results from a retired epoch even if a peer supplies an old frame.
  if (epoch_ && snapshot.epoch != *epoch_ && retired_.contains(snapshot.epoch)) {
    failed(request, now);
    return;
  }
  auto& s = states_.at(request.symbol);
  s.token = 0;
  const auto before = s.snapshot;
  confirm_epoch(snapshot.epoch, now);
  install(request.symbol, snapshot, now, true);
  const bool progressed = s.snapshot && (!before || before->epoch != s.snapshot->epoch ||
                                         before->index < s.snapshot->index);
  if (progressed)
    s.retry_ms = config_.retry_initial_ms;
  s.retry_at = now + std::chrono::milliseconds(s.retry_ms);
  if (!progressed)
    s.retry_ms = static_cast<int>(
        std::min<std::int64_t>(config_.retry_max_ms, std::int64_t(s.retry_ms) * 2));
  refresh(request.symbol, now);
  emit("TCP_COMPLETE", request.symbol);
}

void MarketRecovery::failed(const SnapshotRequest& request, runtime::Deadline now) {
  if (!valid(request))
    return;
  auto& s = states_.at(request.symbol);
  s.token = 0;
  s.retry_at = now + std::chrono::milliseconds(s.retry_ms);
  s.retry_ms =
      static_cast<int>(std::min<std::int64_t>(config_.retry_max_ms, std::int64_t(s.retry_ms) * 2));
  refresh(request.symbol, now);
  emit("TCP_FAILED", request.symbol);
}
}  // namespace marketlab::md
