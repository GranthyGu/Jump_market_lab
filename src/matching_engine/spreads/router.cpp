#include "matching_engine/spreads/router.hpp"
#include <bit>
#include <iostream>
#include <sstream>

namespace marketlab::orders::spreads {
using persistence::integer;
using persistence::Reader;

RouterGroup::RouterGroup(const OrderConfig& config, TcpReactor& io, Callbacks callbacks)
    : config_(config),
      topology_(config),
      io_(io),
      callbacks_(std::move(callbacks)),
      links_(io, config, {group::Role::Router, 0, random_token_seed(), topology_.fingerprint}),
      publication_(std::max<std::size_t>(64 * 1024 * 1024, config.max_buffer_bytes),
                   {[this](auto member, const auto& event) {
                      callbacks_.event(member, event);
                    },
                    [this](auto seq, auto mask) {
                      for (unsigned i = 0; i < 4; ++i)
                        if (mask & (1U << i))
                          if (!send(i, {MessageKind::Publish, epoch_, seq}, {}, {}))
                            pause("market permit send failed");
                    }}) {
  links_.allow = [&](auto id, const auto& identity) {
    return id == coordinator_ && identity.role == group::Role::Coordinator;
  };
  links_.on_lost = [&](auto id) {
    if (coordinator_ == id) {
      coordinator_ = 0;
      pause("Coordinator connection lost");
    }
  };
  links_.on_message = [&](auto, const auto& message) {
    try {
      control(message);
    } catch (const std::exception& error) {
      failed_ = true;
      pause(error.what());
      throw;
    }
  };
}

bool RouterGroup::send(std::uint8_t member,
                       const Header& h,
                       const LinkHeader& context,
                       std::span<const std::uint8_t> bytes) {
  auto envelope = context;
  envelope.kind = LinkKind::Group;
  return callbacks_.send(member, envelope, frame(h, bytes));
}

void RouterGroup::pause(std::string_view reason) {
  ready_ = prepared_ = false;
  std::cerr << "GROUP_PAUSED component=router dispatched=" << publication_.dispatched()
            << " memory=" << publication_.completed() << " published=" << publication_.published()
            << " reason=" << reason << std::endl;
}

void RouterGroup::worker_lost() {
  pause("worker business connection lost");
  const std::string_view reason = "Router lost worker business connection";
  links_.send(coordinator_,
              {MessageKind::Pause, epoch_, publication_.dispatched()},
              {reinterpret_cast<const std::uint8_t*>(reason.data()), reason.size()});
}

void RouterGroup::discard_queued() {
  queued_.clear();
  queued_bytes_ = 0;
}

bool RouterGroup::enqueue(std::uint8_t member,
                          const LinkHeader& h,
                          std::span<const std::uint8_t> bytes) {
  if (member >= 4)
    throw std::runtime_error("Unknown Router group owner");
  if (h.kind == LinkKind::Snapshot && !queued_.empty() && queued_.back().context.token == h.token) {
    if (queued_.back().context.kind != LinkKind::Snapshot)
      throw std::runtime_error("Query token reused");
    queued_.back().members |= 1U << member;
    return true;
  }
  if (queued_.size() >= config_.max_pending ||
      bytes.size() > config_.max_buffer_bytes - queued_bytes_)
    return false;
  queued_.push_back({std::uint8_t(1U << member), h, {bytes.begin(), bytes.end()}});
  queued_bytes_ += bytes.size();
  if (maintenance_) {
    peak_queued_ = std::max(peak_queued_, queued_.size());
    peak_bytes_ = std::max(peak_bytes_, queued_bytes_);
  }
  return true;
}

void RouterGroup::recovery_progress() {
  if (!recovering_ || failed_)
    return;
  for (unsigned i = 0; i < 4; ++i)
    if ((recovering_ & (1U << i)) && !(requested_ & (1U << i)))
      if (send(i, {MessageKind::Recovered, epoch_, join_}, {}, {}))
        requested_ |= 1U << i;
}

void RouterGroup::initial_publication() {
  scratch_.clear();
  integer(scratch_, join_, 8);
  for (unsigned i = 0; i < 4; ++i)
    if (!send(i,
              {MessageKind::Publish, epoch_, publication_.completed(), 0, group::initial_view},
              {},
              scratch_))
      throw std::runtime_error("Cannot publish recovered group boundary");
}

void RouterGroup::control(const FrameView& message) {
  const auto& h = message.header;
  if (h.kind == MessageKind::Recovered) {
    if (!h.epoch || !h.seq || h.flags || h.round || !message.body.empty() || !settled() ||
        (epoch_ && (epoch_ != h.epoch || h.seq <= join_)))
      throw std::runtime_error(
          "Recovery cannot resolve pending results/queries or change Coordinator epoch");
    ready_ = prepared_ = approval_sent_ = false;
    epoch_ = h.epoch;
    join_ = h.seq;
    recovering_ = 15;
    requested_ = 0;
    progress_ = Clock::now();
    recovery_progress();
    return;
  }
  if (!epoch_ || h.epoch != epoch_)
    throw std::runtime_error("Stale Router control epoch");
  if (h.kind == MessageKind::Pause) {
    pause("Coordinator paused group");
    return;
  }
  if (h.kind == MessageKind::Ready) {
    if (h.seq != join_ || h.round || h.flags || recovering_ || !approval_sent_ || !settled())
      throw std::runtime_error("Unexpected group recovery readiness");
    Reader reader{message.body};
    const auto completed = reader.number(8);
    std::array<persistence::Boundary, 4> boundaries{};
    for (unsigned i = 0; i < 4; ++i) {
      const auto recovery = read_recovery(reader);
      if (!same_recovery(recovered_[i], recovery))
        throw std::runtime_error("Ready selected a different business instance");
      boundaries[i] = recovery.boundary;
    }
    reader.finish();
    if (initialized_ && completed != publication_.completed())
      throw std::runtime_error("Recovered group command boundary differs from Router");
    publication_.initialize(epoch_, completed, boundaries);
    initialized_ = prepared_ = true;
    ready_ = !maintenance_;
    if (ready_)
      initial_publication();
    std::cout << "ROUTER_GROUP_READY epoch=" << epoch_ << " join=" << join_
              << " completed=" << completed << " maintenance=" << maintenance_ << std::endl;
    return;
  }
  if (h.kind == MessageKind::Fenced) {
    if (!fence_sent_ || !maintenance_ || h.seq != publication_.completed() || h.flags || h.round ||
        !message.body.empty())
      throw std::runtime_error("Unexpected group fence acknowledgment");
    fenced_ = true;
    return;
  }
  if (h.kind != MessageKind::MemDone || (!ready_ && !maintenance_))
    throw std::runtime_error("Unexpected Router control message");
  publication_.memory(h, message.body);
  progress_ = Clock::now();
}

void RouterGroup::result(std::uint8_t member,
                         const LinkHeader& context,
                         std::span<const std::uint8_t> bytes) {
  try {
    const auto message = parse_frame(bytes);
    const auto& h = message.header;
    if (member >= 4 || !epoch_ || h.epoch != epoch_)
      throw std::runtime_error("Stale group business result");
    if (h.kind == MessageKind::Recovered) {
      if (h.seq != join_ || h.round || h.flags || !(recovering_ & (1U << member)) ||
          !(requested_ & (1U << member)))
        throw std::runtime_error("Duplicate/unsolicited business recovery");
      Reader reader{message.body};
      const auto recovery = read_recovery(reader);
      reader.finish();
      if (initialized_ && !same_boundary(recovery.boundary, publication_.visible()[member]))
        throw std::runtime_error(
            "Recovered real state differs from the complete public group boundary");
      recovered_[member] = recovery;
      recovering_ &= ~(1U << member);
      if (!recovering_) {
        scratch_.clear();
        for (const auto& value : recovered_)
          put_recovery(scratch_, value);
        if (!links_.send(coordinator_, {MessageKind::Recovered, epoch_, join_}, scratch_))
          throw std::runtime_error("Cannot approve recovered business instances");
        approval_sent_ = true;
      }
    } else if (h.kind == MessageKind::Capture) {
      if (h.seq != capture_sequence_ || context.token != capture_token_ ||
          !(capture_mask_ & (1U << member)) || h.flags != group::final_part || h.round ||
          !message.body.empty())
        throw std::runtime_error("Unexpected order capture acknowledgment");
      capture_mask_ &= ~(1U << member);
    } else
      publication_.durable(member, h, message.body);
    progress_ = Clock::now();
  } catch (const std::exception& error) {
    failed_ = true;
    pause(error.what());
    throw;
  }
}

void RouterGroup::maintenance() {
  if (!ready_ || maintenance_)
    throw std::runtime_error("Group is not available for maintenance");
  maintenance_ = true;
  peak_queued_ = queued_.size();
  peak_bytes_ = queued_bytes_;
  ready_ = false;
  prepared_ = false;
  fence_sent_ = fenced_ = false;
}

void RouterGroup::fence() {
  if (!maintenance_ || !settled())
    throw std::runtime_error("Group fence before public/query drain");
  if (!fence_sent_) {
    if (!links_.send(coordinator_, {MessageKind::Fence, epoch_, publication_.completed()}))
      throw std::runtime_error("Cannot fence Coordinator");
    fence_sent_ = true;
  }
}

void RouterGroup::replace(std::uint8_t member, const Address& address) {
  if (!fenced_ || !settled() || member >= 4)
    throw std::runtime_error("Replacement requires a drained group");
  prepared_ = false;
  scratch_.clear();
  integer(scratch_, member, 1);
  put_endpoint(scratch_, {address, 0});
  if (!links_.send(
          coordinator_, {MessageKind::Topology, epoch_, publication_.completed()}, scratch_))
    throw std::runtime_error("Cannot prepare replacement group endpoint");
}

void RouterGroup::resume() {
  if (!maintenance_ || !prepared_ || !settled())
    throw std::runtime_error("Resume before replacement group is prepared");
  maintenance_ = fence_sent_ = fenced_ = false;
  ready_ = true;
  initial_publication();
}

void RouterGroup::restart_prepare() {
  if (!maintenance_ || !settled())
    throw std::runtime_error("Cannot abort unresolved group work");
  ready_ = prepared_ = fence_sent_ = fenced_ = false;
  failed_ = false;
  recovering_ = requested_ = 0;
  fence();
}

std::string RouterGroup::status() const {
  std::ostringstream out;
  out << (ready_ ? "READY" : "PAUSED") << ' ' << epoch_ << ' ' << join_ << ' '
      << publication_.dispatched() << ' ' << publication_.completed() << ' '
      << publication_.published() << ' ' << queued_.size();
  return out.str();
}

void RouterGroup::poll() try {
  const auto now = Clock::now();
  if (!coordinator_ && !failed_ && now >= reconnect_) {
    coordinator_ = io_.connect(config_.coordinator, Framing::Group);
    if (coordinator_)
      links_.outgoing(coordinator_, group::Role::Coordinator, 0);
    reconnect_ = now + std::chrono::milliseconds(config_.reconnect_ms);
  }
  recovery_progress();
  if (!failed_ && !settled() &&
      now - progress_ > std::chrono::milliseconds(config_.request_timeout_ms))
    throw std::runtime_error("Group memory/publication progress timeout");
  if (!ready_ || maintenance_ || failed_ || capture_mask_)
    return;
  for (unsigned count = 0; count < 64 && !queued_.empty(); ++count) {
    const auto& request = queued_.front();
    const auto kind = request.context.kind;
    if (kind != LinkKind::History && publication_.dispatched() != publication_.completed())
      return;
    const auto member = std::uint8_t(std::countr_zero(request.members));
    if (kind == LinkKind::Snapshot) {
      capture_mask_ = request.members;
      capture_token_ = request.context.token;
      capture_sequence_ = publication_.completed();
      callbacks_.capture(capture_token_, capture_sequence_);
      for (unsigned i = 0; i < 4; ++i)
        if (request.members & (1U << i))
          if (!send(i,
                    {MessageKind::Capture, epoch_, capture_sequence_},
                    request.context,
                    request.bytes))
            throw std::runtime_error("Cannot capture group order query");
    } else if (kind == LinkKind::History) {
      scratch_.clear();
      integer(scratch_, publication_.visible()[member].trade_end, 8);
      scratch_.insert(scratch_.end(), request.bytes.begin(), request.bytes.end());
      if (!send(member,
                {MessageKind::PublicHistory, epoch_, publication_.completed()},
                request.context,
                scratch_))
        throw std::runtime_error("Cannot send bounded public history query");
    } else {
      if (!publication_.capacity())
        return;
      const auto sequence = publication_.dispatched() + 1;
      publication_.start(sequence, member);
      if (!send(member, {MessageKind::Run, epoch_, sequence}, request.context, request.bytes))
        throw std::runtime_error("Cannot dispatch group RUN");
    }
    queued_bytes_ -= request.bytes.size();
    queued_.pop_front();
    progress_ = now;
    if (capture_mask_)
      return;
  }
} catch (const std::exception& error) {
  failed_ = true;
  pause(error.what());
  const std::string_view reason = "Router group dispatch/publication failed";
  links_.send(coordinator_,
              {MessageKind::Pause, epoch_, publication_.dispatched()},
              {reinterpret_cast<const std::uint8_t*>(reason.data()), reason.size()});
}
}  // namespace marketlab::orders::spreads
