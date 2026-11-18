#pragma once
#include "matching_engine/spreads/transport/protocol.hpp"
#include "shared_data/orders/tcp_reactor.hpp"

namespace marketlab::orders::spreads {
// Group control/peer links use their own endpoint and handshake. They never
// register with EngineLinks or compete for the unique Router business role.
class Links {
 public:
  Links(TcpReactor&, const OrderConfig&, ConnectionIdentity self);
  void incoming(ConnectionId);
  void outgoing(ConnectionId,
                group::Role expected,
                std::uint16_t shard,
                std::uint64_t instance = 0);
  void connected(ConnectionId);
  void lost(ConnectionId);
  void receive(ConnectionId, std::span<const std::uint8_t>);
  void tick();
  bool send(ConnectionId, const Header&, std::span<const std::uint8_t> body = {});

  bool contains(ConnectionId id) const {
    return links_.contains(id);
  }

  bool ready(ConnectionId) const;
  const ConnectionIdentity& identity(ConnectionId) const;

  std::function<bool(ConnectionId, const ConnectionIdentity&)> allow;
  std::function<void(ConnectionId, const ConnectionIdentity&)> on_ready;
  std::function<void(ConnectionId)> on_lost;
  std::function<void(ConnectionId, const FrameView&)> on_message;

 private:
  struct State {
    bool outgoing = false, ready = false;
    ConnectionIdentity expected{}, remote{};
    Clock::time_point received = Clock::now(), ping = Clock::now();
  };

  TcpReactor& io_;
  const OrderConfig& config_;
  ConnectionIdentity self_;
  std::unordered_map<ConnectionId, State> links_;
};
}  // namespace marketlab::orders::spreads
