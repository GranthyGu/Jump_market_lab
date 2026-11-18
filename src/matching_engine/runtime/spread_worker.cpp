#include "matching_engine/runtime/spread_worker.hpp"
#include <algorithm>
#include <iostream>

namespace marketlab::orders::spreads {
using persistence::integer;
using persistence::Reader;

Worker::Worker(persistence::DurableEngine& durable,
               const OrderConfig& config,
               std::uint16_t shard,
               TcpReactor& io,
               Business business,
               Emit emit,
               std::function<bool()> active)
    : durable_(durable),
      config_(config),
      topology_(config),
      shard_(shard),
      member_(topology_.member(shard)),
      io_(io),
      business_(std::move(business)),
      emit_(std::move(emit)),
      active_(std::move(active)),
      instance_(random_token_seed()),
      links_(io, config, {group::Role::Peer, shard, instance_, topology_.fingerprint}),
      market_(durable.working(), config, shard),
      session_(durable,
               config,
               shard,
               {[this](const auto& h, auto bytes) {
                  send_control(h, bytes);
                },
                [this](auto member, const auto& h, auto bytes) {
                  send_peer(member, h, bytes);
                },
                [this](auto seq, const auto& boundary, auto events) {
                  send_result({MessageKind::DurableResult, epoch_, seq},
                              boundary,
                              events,
                              results_,
                              [this](const auto& h, auto bytes) {
                                send_business(h, bytes);
                              });
                },
                [this](auto seq) {
                  market_.capture(seq, session_.views());
                }}) {
  const auto endpoints = directory(config, topology_);
  io_.listen(endpoints[member_].address, Framing::Group, 8);
  links_.allow = [&](auto id, const auto& remote) {
    if (remote.role == group::Role::Coordinator)
      return !coordinator_ || coordinator_ == id;
    if (remote.role != group::Role::Peer || !topology_ready_)
      return false;
    const auto member = topology_.member(remote.shard);
    return member < 4 && (peer_mask(member_) & (1U << member)) &&
           remote.instance == directory_[member].instance &&
           (peers_[member] ? peers_[member] == id : remote.shard < shard_);
  };
  links_.on_ready = [&](auto id, const auto& remote) {
    if (remote.role == group::Role::Coordinator)
      coordinator_ = id;
    else
      peers_[topology_.member(remote.shard)] = id;
    join_progress();
  };
  links_.on_lost = [&](auto id) {
    bool expected = false;
    if (id == coordinator_) {
      coordinator_ = 0;
      expected = true;
    }
    for (auto& peer : peers_)
      if (id == peer) {
        peer = 0;
        expected = true;
      }
    if (expected && (ready_ || reported_peers_ || session_.active()))
      pause("required group connection lost");
  };
  links_.on_message = [&](auto id, const auto& message) {
    try {
      progress_ = Clock::now();
      if (id == coordinator_)
        control(message);
      else
        peer(topology_.member(links_.identity(id).shard), message);
    } catch (const std::exception& error) {
      failed_ = true;
      pause(error.what());
      throw;
    }
  };
  std::cout << "GROUP_WORKER_LISTENING shard=" << shard_
            << " peer_port=" << endpoints[member_].address.port << " instance=" << instance_
            << std::endl;
}

void Worker::pause(std::string_view reason, bool notify) {
  ready_ = false;
  market_ready_.store(false);
  reported_peers_ = false;
  topology_ready_ = false;
  initial_permit_.reset();
  std::cerr << "GROUP_PAUSED component=shard shard=" << shard_ << " seq=" << session_.sequence()
            << " unresolved=" << session_.active() << " reason=" << reason << std::endl;
  if (notify && epoch_) {
    const auto text = reason.substr(0, 512);
    links_.send(coordinator_,
                {MessageKind::Pause, epoch_, session_.sequence()},
                {reinterpret_cast<const std::uint8_t*>(text.data()), text.size()});
  }
}

void Worker::business_lost() {
  pause("Router business connection lost");
}

void Worker::fence() {
  if (session_.active() || deferred_)
    throw std::runtime_error("Shard fence before group execution drained");
  pause("business instance fenced", false);
}

void Worker::send_control(const Header& h, std::span<const std::uint8_t> bytes) {
  auto wire = h;
  if (h.flags == group::initial_view)
    wire.seq = join_;
  if (!links_.send(coordinator_, wire, bytes))
    throw std::runtime_error("Coordinator send failed");
}

void Worker::send_peer(std::uint8_t member, const Header& h, std::span<const std::uint8_t> bytes) {
  if (!links_.send(peers_.at(member), h, bytes))
    throw std::runtime_error("Peer send failed");
}

void Worker::send_business(const Header& h,
                           std::span<const std::uint8_t> bytes,
                           const LinkHeader& context) {
  auto envelope = context;
  envelope.kind = LinkKind::Group;
  const auto wire = wire_header(h, bytes.size());
  if (!business_(envelope, {reinterpret_cast<const std::uint8_t*>(&wire), sizeof(wire)}, bytes))
    throw std::runtime_error("Router result send failed");
}

void Worker::recover() {
  if ((!control_recovery_ && !business_recovery_) || !durable_.idle() || session_.active() ||
      deferred_)
    return;
  const Recovery recovered{instance_, durable_.boundary(), durable_.boundary().trade_end};
  scratch_.clear();
  put_recovery(scratch_, recovered);
  if (control_recovery_) {
    send_control(*control_recovery_, scratch_);
    control_recovery_.reset();
  }
  if (business_recovery_) {
    send_business(*business_recovery_, scratch_);
    business_recovery_.reset();
  }
}

void Worker::control(const FrameView& message) {
  const auto& h = message.header;
  // Other members may still be connecting before the first recovery request.
  // A pause from the authenticated Coordinator does not establish a join.
  if (h.kind == MessageKind::Pause && !epoch_ &&
      h.epoch == links_.identity(coordinator_).instance) {
    pause("Coordinator paused before initial recovery", false);
    return;
  }
  if (h.kind == MessageKind::Recovered) {
    if (!h.epoch || !h.seq || h.round || h.flags || !message.body.empty() || failed_ ||
        (epoch_ && epoch_ != h.epoch) || session_.active() || deferred_ ||
        (epoch_ == h.epoch && h.seq <= join_))
      throw std::runtime_error("Recovery requested across unresolved/stale group work");
    ready_ = false;
    market_ready_.store(false);
    topology_ready_ = reported_peers_ = source_sent_ = false;
    epoch_ = h.epoch;
    join_ = h.seq;
    session_.epoch(epoch_);
    control_recovery_ = h;
    recover();
    return;
  }
  if (!epoch_ || h.epoch != epoch_)
    throw std::runtime_error("Stale worker control epoch");
  if (h.kind == MessageKind::Pause) {
    pause("Coordinator paused group", false);
    return;
  }
  if (h.kind == MessageKind::Topology) {
    if (h.seq != join_ || h.round || h.flags || topology_ready_ || session_.active() || failed_)
      throw std::runtime_error("Invalid group topology installation");
    Reader reader{message.body};
    const auto changed = reader.number(1);
    if (changed & ~15U)
      throw std::runtime_error("Invalid replacement member mask");
    Directory next;
    for (auto& endpoint : next) {
      endpoint = read_endpoint(reader);
      if (!endpoint.instance)
        throw std::runtime_error("Empty peer instance");
    }
    reader.finish();
    if (next[member_].instance != instance_)
      throw std::runtime_error("Topology selected another local instance");
    changed_ = changed & (1U << member_);
    view_ready_ = !changed_;
    if (changed_) {
      session_.recover_sources();
      market_.reset();
    }
    directory_ = std::move(next);
    for (unsigned i = 0; i < 4; ++i)
      if (peers_[i] && (!links_.ready(peers_[i]) ||
                        links_.identity(peers_[i]).instance != directory_[i].instance)) {
        const auto old = peers_[i];
        peers_[i] = 0;
        io_.close(old);
        reconnect_[i] = {};
      }
    topology_ready_ = true;
    join_progress();
    return;
  }
  if (h.kind == MessageKind::Ready) {
    Reader reader{message.body};
    const auto completed = reader.number(8);
    reader.finish();
    if (h.seq != join_ || h.round || h.flags || !reported_peers_ || !view_ready_ || failed_ ||
        session_.active() || completed < session_.sequence())
      throw std::runtime_error("Group ready before peer/view boundary");
    ready_ = true;
    std::cout << "GROUP_WORKER_READY shard=" << shard_ << " join=" << join_
              << " completed=" << completed << std::endl;
    return;
  }
  auto normalized = h;
  if (h.flags == group::initial_view) {
    if (!topology_ready_ || h.seq != join_ || !reported_peers_)
      throw std::runtime_error("Initial view from wrong join");
    normalized.seq = 0;
    // New workers have no earlier public candidate. Healthy workers retain
    // their last complete view until the replacement join is confirmed.
    if (!changed_)
      market_.reset();
    session_.control(normalized, message.body);
    view_ready_ = true;
  } else {
    if (!ready_ || !active_())
      throw std::runtime_error("Normal control message to paused/fenced instance");
    session_.control(normalized, message.body);
  }
}

void Worker::join_progress() {
  if (!topology_ready_ || reported_peers_ || failed_)
    return;
  for (unsigned i = 0; i < 4; ++i)
    if ((peer_mask(member_) & (1U << i)) && !links_.ready(peers_[i]))
      return;
  if (changed_ && !source_sent_) {
    scratch_.clear();
    put_sources(scratch_, member_sources(member_), {}, session_.sources(), true);
    send_control({MessageKind::InitialDepth, epoch_, join_}, scratch_);
    source_sent_ = true;
  }
  send_control({MessageKind::PeersReady, epoch_, join_});
  reported_peers_ = true;
}

void Worker::defer(std::uint8_t peer,
                   const Header& h,
                   const LinkHeader& context,
                   std::span<const std::uint8_t> body) {
  if (deferred_)
    throw std::runtime_error("Overlapping deferred group work");
  deferred_ = Deferred{peer, h, context, {body.begin(), body.end()}};
}

bool Worker::apply(std::uint8_t peer,
                   const Header& h,
                   const LinkHeader& context,
                   std::span<const std::uint8_t> body) {
  if (!ready_)
    return false;
  if (!active_())
    throw std::runtime_error("Group command to fenced/standby worker");
  if (peer != no_slot)
    return session_.peer(peer, h, body);
  if (h.kind == MessageKind::Run)
    return session_.run(h, context, body);
  if (h.kind != MessageKind::Capture || session_.active())
    throw std::runtime_error("Query outside a complete group memory boundary");
  if (!durable_.capture_group(context, copy_record<marketlab::Header>(body), emit_))
    return false;
  auto ack = h;
  ack.flags = group::final_part;
  send_business(ack, {}, context);
  return true;
}

void Worker::peer(std::uint8_t from, const FrameView& message) {
  if (from >= 4 || message.header.epoch != epoch_ || failed_ || !active_())
    throw std::runtime_error("Peer command to inactive/stale instance");
  if (message.header.kind == MessageKind::ExecutePrefix) {
    if (!apply(from, message.header, {}, message.body))
      defer(from, message.header, {}, message.body);
  } else {
    if (!ready_ || deferred_)
      throw std::runtime_error("Peer follow-up before execution admission");
    session_.peer(from, message.header, message.body);
  }
}

void Worker::business(const LinkHeader& context, std::span<const std::uint8_t> bytes) {
  try {
    if (context.kind != LinkKind::Group)
      throw std::runtime_error("Group worker requires Router group envelope");
    const auto message = parse_frame(bytes);
    const auto& h = message.header;
    if (h.kind == MessageKind::Recovered) {
      if (!h.epoch || !h.seq || h.round || h.flags || !message.body.empty() || business_recovery_ ||
          failed_ || session_.active() || deferred_)
        throw std::runtime_error("Invalid business recovery request");
      business_recovery_ = h;
      recover();
      return;
    }
    if (!epoch_ || h.epoch != epoch_ || failed_)
      throw std::runtime_error("Stale group business epoch");
    if (h.kind == MessageKind::Publish) {
      if (h.round)
        throw std::runtime_error("Invalid publication round");
      if (h.flags == group::initial_view) {
        Reader reader{message.body};
        if (reader.number(8) != join_)
          throw std::runtime_error("Stale initial market permit");
        reader.finish();
        initial_permit_ = h;
      } else {
        if (h.flags || !message.body.empty())
          throw std::runtime_error("Invalid normal market permit");
        market_.permit(h.seq);
      }
      return;
    }
    auto command = context;
    if (!command.user || command.user > config_.accounts.size() || !command.token ||
        !command.session)
      throw std::runtime_error("Invalid group business request identity");
    if (h.kind == MessageKind::PublicHistory) {
      if (!ready_ || h.round || h.flags)
        throw std::runtime_error("History on unready group");
      Reader reader{message.body};
      const auto visible = reader.number(8);
      const auto query = copy_record<TradeHistoryRequest>(reader.data.subspan(reader.position));
      command.kind = LinkKind::History;
      durable_.public_history(command, query, visible, emit_);
      return;
    }
    if ((h.kind != MessageKind::Run && h.kind != MessageKind::Capture) || h.round || h.flags)
      throw std::runtime_error("Unknown group business command");
    if (h.kind == MessageKind::Capture) {
      const auto request = copy_record<marketlab::Header>(message.body);
      if (request.messageType != MessageType::ActiveOrderSnapshot ||
          request.messageLength != sizeof(request) || !request.requestId)
        throw std::runtime_error("Invalid group order query");
      command.kind = LinkKind::Snapshot;
    } else {
      if (!valid_client_record(message.body, true))
        throw std::runtime_error("Invalid RUN client record");
      marketlab::Header request{};
      std::memcpy(&request, message.body.data(), sizeof(request));
      if (request.messageType == MessageType::NewOrderRequest)
        command.kind = LinkKind::NewOrder;
      else if (request.messageType == MessageType::CancelRequest)
        command.kind = LinkKind::Cancel;
      else
        throw std::runtime_error("Invalid RUN mutation kind");
    }
    if (!apply(no_slot, h, command, message.body))
      defer(no_slot, h, command, message.body);
    progress_ = Clock::now();
  } catch (const std::exception& error) {
    failed_ = true;
    pause(error.what());
    throw;
  }
}

void Worker::poll() try {
  if (failed_)
    return;
  recover();
  const auto now = Clock::now();
  if (topology_ready_)
    for (unsigned i = 0; i < 4; ++i)
      if ((peer_mask(member_) & (1U << i)) && shard_ < topology_.shards[i] && !peers_[i] &&
          now >= reconnect_[i]) {
        const auto id = io_.connect(directory_[i].address, Framing::Group);
        peers_[i] = id;
        if (id)
          links_.outgoing(id, group::Role::Peer, topology_.shards[i], directory_[i].instance);
        reconnect_[i] = now + std::chrono::milliseconds(config_.reconnect_ms);
      }
  join_progress();
  if (ready_ && initial_permit_) {
    market_.permit(initial_permit_->seq);
    initial_permit_.reset();
    market_ready_.store(true);
  }
  if (deferred_ && apply(deferred_->peer, deferred_->header, deferred_->context, deferred_->body))
    deferred_.reset();
  if ((session_.active() || deferred_) &&
      now - progress_ > std::chrono::milliseconds(config_.request_timeout_ms)) {
    failed_ = true;
    pause("group execution progress timeout");
  }
} catch (const std::exception& error) {
  failed_ = true;
  pause(error.what());
}
}  // namespace marketlab::orders::spreads
