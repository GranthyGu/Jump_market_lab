#include "matching_engine/spreads/coordinator/coordinator.hpp"
#ifdef MARKETLAB_LATENCY_PROBES
#include "spreads/latency_probe.hpp"
#endif
#include <bit>

namespace marketlab::orders::spreads {
using persistence::Reader;
using persistence::integer;

Coordinator::Coordinator(std::uint64_t epoch, Send send)
    : epoch_(epoch),
      send_(std::move(send)) {
  if (!epoch_ || !send_)
    throw std::invalid_argument("Coordinator requires epoch and transport");
  scratch_.reserve(1024);
}

void Coordinator::initialize(const Sources& sources,
                             const std::array<std::uint64_t, 4>& batches,
                             std::uint8_t reinstall_targets) {
  if (active_seq_ || ready_ || initializing_)
    throw std::runtime_error("Cannot rebuild views across unresolved group work");
  if (initialized_ && batches != batches_)
    throw std::runtime_error("Recovered local batches differ from completed group boundary");
  for (unsigned i = 0; i < sources.size(); ++i)
    validate(sources[i], static_cast<BookSide>(i % 2));
  sources_ = sources;
  batches_ = batches;
  const auto changed = recompute(sources_, all_sources, views_);
  if (reinstall_targets & ~unsigned(all_targets))
    throw std::runtime_error("Invalid initial view target mask");
  initializing_ = true;
  initialized_ = true;
  install(reinstall_targets | changed, true);
  finish_views();
}

void Coordinator::pause(std::string_view reason) {
  ready_ = false;
  initializing_ = false;
  const auto text = reason.substr(0, 512);
  send_(router_destination,
        {MessageKind::Pause, epoch_, active_seq_, round_},
        {reinterpret_cast<const std::uint8_t*>(text.data()), text.size()});
}

void Coordinator::reset_round() {
  expected_ = done_ = awaiting_views_ = 0;
  dirty_ = 0;
  declared_ = round_ready_ = false;
}

void Coordinator::prepare_round(const Header& h) {
  if (!ready_ || !h.seq || !h.round)
    throw std::runtime_error("Group is not ready for a normal round");
  if (!active_seq_) {
    if (completed_ == UINT64_MAX || h.seq != completed_ + 1 || h.round != 1)
      throw std::runtime_error("Group sequence/first-round gap");
    active_seq_ = h.seq;
    round_ = h.round;
    initiator_ = no_slot;
    participants_ = ended_ = market_members_ = 0;
    reset_round();
  } else if (h.seq == active_seq_ && round_ready_ && round_ != UINT32_MAX &&
             h.round == round_ + 1) {
    if (ended_)
      throw std::runtime_error("New round after a participant sealed its batch");
    round_ = h.round;
    reset_round();
  }
  if (h.seq != active_seq_ || h.round != round_ || round_ready_)
    throw std::runtime_error("Stale/overlapping group round");
}

void Coordinator::install(std::uint8_t targets, bool initial) {
  awaiting_views_ = targets;
  for (std::uint8_t member = 0; member < 4; ++member) {
    const auto own = std::uint8_t(targets & member_targets(member));
    if (!own)
      continue;
    scratch_.clear();
    put_views(scratch_, own, views_);
    ++counters.view_installs;
    market_members_ |= 1U << member;
    send_(member,
          {MessageKind::ImpliedReplace,
           epoch_,
           active_seq_,
           round_,
           std::uint8_t(initial ? group::initial_view : 0)},
          scratch_);
  }
}

void Coordinator::finish_sources() {
  if (!declared_ || done_ != expected_)
    return;
  const auto affected = affected_routes(dirty_);
  counters.route_recomputations += std::popcount(affected);
  const auto changed = recompute(sources_, dirty_, views_);
  install(changed, false);
  finish_views();
}

void Coordinator::finish_views() {
  if (awaiting_views_)
    return;
  if (initializing_) {
    initializing_ = false;
    ready_ = true;
    market_members_ = 0;
    send_(router_destination, {MessageKind::Ready, epoch_, completed_}, {});
  } else if (ready_ && declared_ && done_ == expected_ && !round_ready_) {
    round_ready_ = true;
    ++counters.rounds;
    send_(initiator_, {MessageKind::RoundReady, epoch_, active_seq_, round_}, {});
  }
}

void Coordinator::memory_done() {
#ifdef MARKETLAB_LATENCY_PROBES
  testing::latency::record(testing::latency::Kind::GroupMemory, active_seq_, 0, 0, participants_);
#endif
  scratch_.clear();
  integer(scratch_, participants_, 1);
  integer(scratch_, market_members_, 1);
  for (unsigned i = 0; i < batches_.size(); ++i)
    if (participants_ & (1U << i))
      integer(scratch_, batches_[i], 8);
  const auto seq = active_seq_;
  const auto round = round_;
  completed_ = seq;
  active_seq_ = 0;
  round_ = 0;
  initiator_ = no_slot;
  reset_round();
  ++counters.memory_done;
  send_(router_destination, {MessageKind::MemDone, epoch_, seq, round}, scratch_);
}

void Coordinator::receive(std::uint8_t member,
                          const Header& h,
                          std::span<const std::uint8_t> bytes) {
  if (member >= 4 || h.epoch != epoch_)
    throw std::runtime_error("Coordinator message owner/epoch mismatch");
  Reader body{bytes};
  switch (h.kind) {
    case MessageKind::RoundBegin: {
      if (h.flags)
        throw std::runtime_error("Unexpected round flags");
      const auto expected = body.number(1);
      body.finish();
      if (expected & ~15U || !(expected & (1U << member)))
        throw std::runtime_error("Invalid round participant mask");
      prepare_round(h);
      if (declared_ || (initiator_ != no_slot && initiator_ != member) || (done_ & ~expected))
        throw std::runtime_error("Round initiator/participant conflict");
      initiator_ = member;
      expected_ = expected;
      participants_ |= expected;
      market_members_ |= expected;
      declared_ = true;
      finish_sources();
      return;
    }
    case MessageKind::SourceDone: {
      if (h.flags)
        throw std::runtime_error("Unexpected source completion flags");
      prepare_round(h);
      if ((done_ & (1U << member)) || (declared_ && !(expected_ & (1U << member))))
        throw std::runtime_error("Duplicate/unexpected round source");
      auto next = sources_;
      const auto dirty = read_sources(body, next, member_sources(member), false);
      body.finish();
      sources_ = next;
      dirty_ |= dirty;
      done_ |= 1U << member;
      finish_sources();
      return;
    }
    case MessageKind::ViewApplied: {
      const auto targets = body.number(1);
      body.finish();
      if ((!ready_ && !initializing_) || h.seq != active_seq_ || h.round != round_ ||
          h.flags != (initializing_ ? group::initial_view : 0) || !targets ||
          (targets & ~member_targets(member)) || (targets & awaiting_views_) != targets)
        throw std::runtime_error("Stale/foreign view acknowledgement");
      awaiting_views_ &= ~targets;
      finish_views();
      return;
    }
    case MessageKind::EndAck: {
      const auto batch = body.number(8);
      body.finish();
      if (!ready_ || !round_ready_ || h.flags || h.seq != active_seq_ || h.round != round_ ||
          !(participants_ & (1U << member)) || (ended_ & (1U << member)) ||
          batches_[member] == UINT64_MAX || batch != batches_[member] + 1)
        throw std::runtime_error("Invalid command-end/local-batch acknowledgement");
      batches_[member] = batch;
      ended_ |= 1U << member;
      if (ended_ == participants_)
        memory_done();
      return;
    }
    case MessageKind::LocalComplete: {
      const auto batch = body.number(8);
      body.finish();
      if (!ready_ || active_seq_ || h.flags || h.round || completed_ == UINT64_MAX ||
          h.seq != completed_ + 1 || batches_[member] == UINT64_MAX ||
          batch != batches_[member] + 1)
        throw std::runtime_error("Local short path conflicts with group progress");
      active_seq_ = h.seq;
      participants_ = market_members_ = 1U << member;
      batches_[member] = batch;
      ++counters.local_complete;
      memory_done();
      return;
    }
    default:
      throw std::runtime_error("Unexpected coordinator state-machine message");
  }
}
}  // namespace marketlab::orders::spreads
