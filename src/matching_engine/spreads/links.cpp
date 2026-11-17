#include "matching_engine/spreads/links.hpp"
#include <iostream>

namespace marketlab::orders::spreads {
Links::Links(TcpReactor& io, const OrderConfig& config, ConnectionIdentity self)
    : io_(io),
      config_(config),
      self_(self) {
  links_.reserve(8);
}

void Links::incoming(ConnectionId id) {
  links_.emplace(id, State{});
}

void Links::outgoing(ConnectionId id,
                     group::Role role,
                     std::uint16_t shard,
                     std::uint64_t instance) {
  State state;
  state.outgoing = true; 
  state.expected = {role, shard, instance, self_.fingerprint};
  links_.emplace(id, state);
}

void Links::connected(ConnectionId id) {
  if (!links_.at(id).outgoing)
    throw std::logic_error("Connected callback for incoming group link");
  io_.send(id, frame({MessageKind::Hello}, identity_bytes(self_)));
}

bool Links::ready(ConnectionId id) const {
  const auto it = links_.find(id);
  return it != links_.end() && it->second.ready;
}

const ConnectionIdentity& Links::identity(ConnectionId id) const {
  if (!ready(id))
    throw std::runtime_error("Group link identity not established");
  return links_.at(id).remote;
}

void Links::lost(ConnectionId id) {
  if (links_.erase(id) && on_lost)
    on_lost(id);
}

bool Links::send(ConnectionId id, const Header& h, std::span<const std::uint8_t> body) {
  if (!ready(id))
    return false;
  if (config_.trace && h.kind != MessageKind::Ping && h.kind != MessageKind::Pong)
    std::cout << "GROUP_TX shard=" << self_.shard << " peer=" << identity(id).shard
              << " type=" << message_name(h.kind) << " epoch=" << h.epoch << " seq=" << h.seq
              << " round=" << h.round << " bytes=" << group::header_size + body.size() << '\n';
  return io_.send(id, frame(h, body));
}

void Links::receive(ConnectionId id, std::span<const std::uint8_t> bytes) {
  try {
    const auto message = parse_frame(bytes);
    const auto& h = message.header;
    auto& state = links_.at(id);
    state.received = Clock::now();
    if (!state.ready) {
      if (h.kind != (state.outgoing ? MessageKind::Welcome : MessageKind::Hello) || h.epoch ||
          h.seq || h.round || h.flags)
        throw std::runtime_error("Expected group role handshake");
      persistence::Reader reader{message.body};
      const auto remote = read_identity(reader);
      reader.finish();
      if (remote.fingerprint != self_.fingerprint ||
          (state.outgoing &&
           (remote.role != state.expected.role || remote.shard != state.expected.shard ||
            (state.expected.instance && remote.instance != state.expected.instance))) ||
          (allow && !allow(id, remote)))
        throw std::runtime_error("Group role/instance/configuration mismatch");
      state.remote = remote;
      state.ready = true;
      if (!state.outgoing && !io_.send(id, frame({MessageKind::Welcome}, identity_bytes(self_))))
        return;
      if (on_ready)
        on_ready(id, remote);
      return;
    }
    if (h.kind == MessageKind::Hello || h.kind == MessageKind::Welcome)
      throw std::runtime_error("Repeated group handshake");
    if (h.kind == MessageKind::Ping || h.kind == MessageKind::Pong) {
      if (!message.body.empty() || h.epoch || h.seq || h.round || h.flags)
        throw std::runtime_error("Invalid group heartbeat");
      if (h.kind == MessageKind::Ping)
        send(id, {MessageKind::Pong});
      return;
    }
    if (config_.trace)
      std::cout << "GROUP_RX shard=" << self_.shard << " peer=" << state.remote.shard
                << " type=" << message_name(h.kind) << " epoch=" << h.epoch << " seq=" << h.seq
                << " round=" << h.round << " bytes=" << bytes.size() << '\n';
    if (on_message)
      on_message(id, message);
  } catch (const std::exception& error) {
    std::cerr << "GROUP_LINK_REJECT shard=" << self_.shard << " connection=" << id
              << " reason=" << error.what() << std::endl;
    io_.close(id);
  }
}

void Links::tick() {
  const auto now = Clock::now();
  std::vector<ConnectionId> expired, ping;
  for (auto& [id, state] : links_) {
    if (now - state.received > std::chrono::milliseconds(config_.link_timeout_ms))
      expired.push_back(id);
    else if (state.ready && now - state.ping > std::chrono::milliseconds(config_.heartbeat_ms)) {
      state.ping = now;
      ping.push_back(id);
    }
  }
  for (const auto id : expired)
    io_.close(id);
  for (const auto id : ping)
    send(id, {MessageKind::Ping});
}
}  // namespace marketlab::orders::spreads
