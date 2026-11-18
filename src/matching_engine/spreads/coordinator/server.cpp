#include "matching_engine/spreads/coordinator/server.hpp"
#include "matching_engine/spreads/coordinator/coordinator.hpp"
#include "matching_engine/spreads/transport/join.hpp"
#include "matching_engine/spreads/transport/links.hpp"
#include <fstream>
#include <iostream>

namespace marketlab::orders::spreads {
namespace {
class Server {
 public:
  explicit Server(const OrderConfig& config)
      : config_(config),
        topology_(config),
        directory_(directory(config, topology_)),
        io_(config.max_buffer_bytes, config.partial_timeout_ms),
        epoch_(random_token_seed()),
        links_(io_, config_, {group::Role::Coordinator, 0, epoch_, topology_.fingerprint}),
        core_(epoch_, [this](auto destination, const auto& h, auto body) {
          send(destination, h, body);
        }) {
    if (!config.spreads)
      throw std::runtime_error("Coordinator requires spreads=enabled");
    io_.on_accept = [&](auto id) {
      links_.incoming(id);
    };
    io_.on_connect = [&](auto id) {
      links_.connected(id);
    };
    io_.on_frame = [&](auto id, auto bytes) {
      links_.receive(id, bytes);
    };
    io_.on_close = [&](auto id) {
      links_.lost(id);
    };
    links_.allow = [&](auto id, const auto& identity) {
      if (identity.role == group::Role::Router)
        return !router_ || router_ == id;
      if (identity.role != group::Role::Peer)
        return false;
      const auto member = topology_.member(identity.shard);
      return member < 4 && connections_[member] == id;
    };
    links_.on_ready = [&](auto id, const auto& identity) {
      if (identity.role == group::Role::Router)
        router_ = id;
      else
        directory_[topology_.member(identity.shard)].instance = identity.instance;
    };
    links_.on_lost = [&](auto id) {
      bool active = false;
      if (id == router_) {
        router_ = 0;
        active = true;
      }
      for (auto& connection : connections_)
        if (connection == id) {
          connection = 0;
          active = true;
        }
      if (active)
        pause("control connection lost", false);
    };
    links_.on_message = [&](auto id, const auto& message) {
      try {
        receive(id, message);
      } catch (const std::exception& error) {
        pause(error.what(), true);
        throw;
      }
    };
    io_.listen(config.coordinator, Framing::Group, 8);
    std::cout << "COORDINATOR_LISTENING port=" << config.coordinator.port << " epoch=" << epoch_
              << " fingerprint=" << topology_.fingerprint << std::endl;
  }

  void run(const std::atomic_bool& stop) {
    auto tick = Clock::now();
    while (!stop.load()) {
      const auto now = Clock::now();
      for (unsigned member = 0; member < 4; ++member)
        if (!connections_[member] && now >= reconnect_[member]) {
          const auto id = io_.connect(directory_[member].address, Framing::Group);
          connections_[member] = id;
          if (id)
            links_.outgoing(id, group::Role::Peer, topology_.shards[member]);
          reconnect_[member] = now + std::chrono::milliseconds(config_.reconnect_ms);
        }
      io_.step(10);
      if (!joining_ && !failed_ && !maintenance_ && !core_.ready() && !core_.in_flight() &&
          links_.ready(router_) &&
          std::all_of(connections_.begin(), connections_.end(), [&](auto id) {
            return links_.ready(id);
          }))
        begin_join();
      if (joining_ && now - progress_ > std::chrono::milliseconds(config_.request_timeout_ms))
        pause("join progress timeout", true);
      if (now - tick >= std::chrono::milliseconds(50)) {
        links_.tick();
        io_.expire();
        tick = now;
      }
    }
    pause("coordinator stopped", false);
  }

  void stats(const std::string& path) const {
    if (path.empty())
      return;
    std::ofstream out(path);
    const auto& n = core_.counters;
    out << "{\"epoch\":" << epoch_ << ",\"joins\":" << join_ << ",\"rounds\":" << n.rounds
        << ",\"local_complete\":" << n.local_complete << ",\"memory_done\":" << n.memory_done
        << ",\"route_recomputations\":" << n.route_recomputations
        << ",\"view_installs\":" << n.view_installs
        << ",\"paused\":" << (core_.ready() ? "false" : "true") << "}\n";
  }

 private:
  const OrderConfig& config_;
  Topology topology_;
  Directory directory_;
  TcpReactor io_;
  std::uint64_t epoch_, join_ = 0;
  Links links_;
  Coordinator core_;
  ConnectionId router_ = 0;
  std::array<ConnectionId, 4> connections_{};
  std::array<Clock::time_point, 4> reconnect_{};
  std::array<Recovery, 4> recovered_{}, router_recovered_{}, accepted_{};
  Sources sources_{};
  std::uint8_t recovered_mask_ = 0, peers_ = 0, depths_ = 0, changed_ = 15;
  bool joining_ = false, router_approved_ = false, topology_sent_ = false, installing_ = false,
       failed_ = false, maintenance_ = false, pausing_ = false;
  Clock::time_point progress_ = Clock::now();
  Bytes scratch_;

  void pause(std::string_view reason, bool failed) {
    if (pausing_)
      return;
    pausing_ = true;
    joining_ = false;
    failed_ |= failed;
    core_.pause(reason);
    for (auto connection : connections_)
      links_.send(connection,
                  {MessageKind::Pause, epoch_, core_.completed()},
                  {reinterpret_cast<const std::uint8_t*>(reason.data()), reason.size()});
    std::cerr << "GROUP_PAUSED component=coordinator seq=" << core_.completed()
              << " unresolved=" << core_.in_flight() << " reason=" << reason << std::endl;
    pausing_ = false;
  }

  void begin_join() {
    if (join_ == UINT64_MAX)
      throw std::overflow_error("Group join ID exhausted");
    ++join_;
    joining_ = true;
    router_approved_ = topology_sent_ = installing_ = false;
    recovered_mask_ = peers_ = 0;
    changed_ = 0;
    for (unsigned i = 0; i < 4; ++i)
      if (accepted_[i].instance != directory_[i].instance)
        changed_ |= 1U << i;
    depths_ = 15 & ~changed_;
    sources_ = core_.sources();
    progress_ = Clock::now();
    for (auto id : connections_)
      links_.send(id, {MessageKind::Recovered, epoch_, join_});
    links_.send(router_, {MessageKind::Recovered, epoch_, join_});
    std::cout << "GROUP_JOIN_BEGIN join=" << join_ << " changed=" << unsigned(changed_)
              << " completed=" << core_.completed() << std::endl;
  }

  void topology_ready() {
    if (topology_sent_ || recovered_mask_ != 15 || !router_approved_)
      return;
    for (unsigned i = 0; i < 4; ++i) {
      if (!same_recovery(recovered_[i], router_recovered_[i]))
        throw std::runtime_error("Router/control recovery identities or boundaries differ");
      if (accepted_[i].instance && recovered_[i].boundary.seq != core_.batches()[i])
        throw std::runtime_error("Recovery is behind a memory-complete local batch");
    }
    scratch_.clear();
    persistence::integer(scratch_, changed_, 1);
    for (const auto& endpoint : directory_)
      put_endpoint(scratch_, endpoint);
    topology_sent_ = true;
    for (auto id : connections_)
      links_.send(id, {MessageKind::Topology, epoch_, join_}, scratch_);
  }

  void initialize() {
    if (!topology_sent_ || peers_ != 15 || depths_ != 15 || installing_)
      return;
    std::array<std::uint64_t, 4> batches{};
    std::uint8_t targets = 0;
    for (unsigned i = 0; i < 4; ++i) {
      batches[i] = recovered_[i].boundary.seq;
      if (changed_ & (1U << i))
        targets |= member_targets(i);
    }
    if (accepted_[0].instance)
      for (unsigned i = 0; i < source_count; ++i)
        if (!sources_[i].same_levels(core_.sources()[i]))
          throw std::runtime_error("Recovered real window differs from confirmed group boundary");
    installing_ = true;
    core_.initialize(sources_, batches, targets);
  }

  void send(std::uint8_t destination, const Header& header, std::span<const std::uint8_t> body) {
    auto h = header;
    if (h.flags == group::initial_view)
      h.seq = join_;
    if (destination == router_destination && h.kind == MessageKind::Ready) {
      joining_ = false;
      accepted_ = recovered_;
      scratch_.clear();
      persistence::integer(scratch_, core_.completed(), 8);
      for (auto id : connections_)
        links_.send(id, {MessageKind::Ready, epoch_, join_}, scratch_);
      for (const auto& recovery : recovered_)
        put_recovery(scratch_, recovery);
      links_.send(router_, {MessageKind::Ready, epoch_, join_}, scratch_);
      std::cout << "GROUP_READY epoch=" << epoch_ << " join=" << join_
                << " completed=" << core_.completed() << std::endl;
      return;
    }
    if (!links_.send(
            destination == router_destination ? router_ : connections_.at(destination), h, body) &&
        h.kind != MessageKind::Pause)
      pause("control send failed", false);
  }

  void receive(ConnectionId id, const FrameView& message) {
    const auto& h = message.header;
    if (h.epoch != epoch_ || (h.round && h.kind == MessageKind::Recovered))
      throw std::runtime_error("Wrong coordinator epoch/join metadata");
    const bool join_message = h.kind == MessageKind::Recovered ||
                              h.kind == MessageKind::PeersReady ||
                              h.kind == MessageKind::InitialDepth || h.flags == group::initial_view;
    // An explicit maintenance fence can overtake replies on other connections.
    // Those replies belong to the retired join and cannot contribute to a new one.
    if (join_message && (h.seq < join_ || (h.seq == join_ && !joining_)))
      return;
    if (h.kind == MessageKind::Pause) {
      pause(
          std::string_view(reinterpret_cast<const char*>(message.body.data()), message.body.size()),
          false);
      return;
    }
    if (id == router_) {
      if (h.kind == MessageKind::Fence) {
        if (core_.in_flight() || h.seq != core_.completed() || h.round || h.flags ||
            !message.body.empty())
          throw std::runtime_error("Group fence before memory completion");
        maintenance_ = true;
        failed_ = false;
        pause("planned maintenance", false);
        links_.send(router_, {MessageKind::Fenced, epoch_, core_.completed()});
        return;
      }
      if (h.kind == MessageKind::Topology) {
        if (!maintenance_ || core_.in_flight() || h.seq != core_.completed() || h.round || h.flags)
          throw std::runtime_error("Endpoint replacement requires a drained group fence");
        persistence::Reader reader{message.body};
        const auto member = reader.number(1);
        const auto endpoint = read_endpoint(reader);
        reader.finish();
        if (member >= 4 || endpoint.instance)
          throw std::runtime_error("Invalid replacement member");
        maintenance_ = false;
        const auto old = connections_[member];
        connections_[member] = 0;
        io_.close(old);
        directory_[member] = endpoint;
        reconnect_[member] = {};
        return;
      }
      if (h.kind != MessageKind::Recovered || !joining_ || h.seq != join_ || h.round || h.flags ||
          router_approved_)
        throw std::runtime_error("Unexpected Router recovery approval");
      persistence::Reader reader{message.body};
      for (auto& recovery : router_recovered_)
        recovery = read_recovery(reader);
      reader.finish();
      router_approved_ = true;
      progress_ = Clock::now();
      topology_ready();
      return;
    }
    const auto member = topology_.member(links_.identity(id).shard);
    if (member >= 4 || connections_[member] != id)
      throw std::runtime_error("Stale worker control instance");
    if (h.kind == MessageKind::Recovered || h.kind == MessageKind::PeersReady ||
        h.kind == MessageKind::InitialDepth) {
      if (!joining_ || h.seq != join_ || h.round || h.flags)
        throw std::runtime_error("Stale join response");
      persistence::Reader reader{message.body};
      if (h.kind == MessageKind::Recovered) {
        if (recovered_mask_ & (1U << member))
          throw std::runtime_error("Duplicate recovered member");
        recovered_[member] = read_recovery(reader);
        if (recovered_[member].instance != directory_[member].instance)
          throw std::runtime_error("Recovery/control instance mismatch");
        recovered_mask_ |= 1U << member;
      } else if (h.kind == MessageKind::PeersReady) {
        if (!topology_sent_ || peers_ & (1U << member))
          throw std::runtime_error("Duplicate/early peer readiness");
        peers_ |= 1U << member;
      } else {
        if (!topology_sent_ || depths_ & (1U << member))
          throw std::runtime_error("Unexpected initial source snapshot");
        const auto mask = read_sources(reader, sources_, member_sources(member), true);
        if (mask != member_sources(member))
          throw std::runtime_error("Incomplete initial source snapshot");
        depths_ |= 1U << member;
      }
      reader.finish();
      progress_ = Clock::now();
      topology_ready();
      initialize();
      return;
    }
    auto normalized = h;
    if (h.flags == group::initial_view) {
      if (!joining_ || !installing_ || h.seq != join_)
        throw std::runtime_error("Stale initial view acknowledgment");
      normalized.seq = 0;
    }
    core_.receive(member, normalized, message.body);
  }
};
}  // namespace

void run_coordinator(const OrderConfig& config,
                     const std::atomic_bool& stop,
                     const std::string& stats) {
  Server server(config);
  server.run(stop);
  server.stats(stats);
}
}  // namespace marketlab::orders::spreads
