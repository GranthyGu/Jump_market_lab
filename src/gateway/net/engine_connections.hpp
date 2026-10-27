#pragma once
#include "gateway/net/tcp_reactor.hpp"
#include <iostream>

namespace marketlab::orders {
// Authentication is at the client gateway. Engine links are trusted deployment
// endpoints; the version handshake is compatibility/liveness, not authentication.
class EngineLinks {
 public:
  EngineLinks(TcpReactor& io, const OrderConfig& config) : io_(io), config_(config) {}
  void incoming(ConnectionId id) { links_.emplace(id, State{}); }
  void outgoing(ConnectionId id) { State s; s.outgoing = true; links_.emplace(id, s); }
  bool contains(ConnectionId id) const { return links_.contains(id); }
  bool ready(ConnectionId id) const {
    const auto it = links_.find(id); return it != links_.end() && it->second.ready;
  }
  void connected(ConnectionId id) { io_.send(id, link_frame(LinkKind::Hello, 0, 0, 0, link_version)); }
  void lost(ConnectionId id) { links_.erase(id); }
  void receive(ConnectionId id, std::span<const std::uint8_t> bytes) {
    if (!valid_link_record(bytes)) { io_.close(id); return; }
    auto it = links_.find(id);
    if (it == links_.end()) { io_.close(id); return; }
    const auto h = decode_link_header(bytes);
    auto& s = it->second;
    s.received = Clock::now();
    if (!s.ready) {
      if (h.kind != (s.outgoing ? LinkKind::Ready : LinkKind::Hello) || (allow_ready && !allow_ready(id))) {
        io_.close(id); return;
      }
      s.ready = true;
      if (!s.outgoing) io_.send(id, link_frame(LinkKind::Ready, 0, 0, 0, link_version));
      if (on_ready) on_ready(id);
      return;
    }
    if (h.kind == LinkKind::Ping) { io_.send(id, link_frame(LinkKind::Pong)); return; }
    if (h.kind == LinkKind::Pong) return;
    if (h.kind == LinkKind::Hello || h.kind == LinkKind::Ready) { io_.close(id); return; }
    if (config_.trace) std::cout << "LINK RX connection=" << id << " kind=" << static_cast<unsigned>(h.kind)
        << " token=" << h.token << " user=" << h.user << " session=" << h.session << " bytes=" << bytes.size() << '\n';
    if (on_message) on_message(id, h, bytes.subspan(link_header_size));
  }
  bool send(ConnectionId id, LinkKind kind, const LinkHeader& h, std::span<const std::uint8_t> payload) {
    if (!ready(id)) return false;
    if (config_.trace) std::cout << "LINK TX connection=" << id << " kind=" << static_cast<unsigned>(kind)
        << " token=" << h.token << " user=" << h.user << " session=" << h.session << " bytes=" << link_header_size + payload.size() << '\n';
    return io_.send(id, link_frame(kind, h.token, h.user, h.session, payload));
  }
  void tick() {
    const auto now = Clock::now();
    std::vector<ConnectionId> expired, ping;
    for (auto& [id,s] : links_) {
      if (now - s.received > std::chrono::milliseconds(config_.link_timeout_ms)) expired.push_back(id);
      else if (s.ready && now - s.ping > std::chrono::milliseconds(config_.heartbeat_ms)) { s.ping = now; ping.push_back(id); }
    }
    for (const auto id : expired) io_.close(id);
    for (const auto id : ping) io_.send(id, link_frame(LinkKind::Ping));
  }
  std::function<bool(ConnectionId)> allow_ready;
  std::function<void(ConnectionId)> on_ready;
  std::function<void(ConnectionId, const LinkHeader&, std::span<const std::uint8_t>)> on_message;
 private:
  struct State { bool outgoing = false, ready = false; Clock::time_point received = Clock::now(), ping = Clock::now(); };
  TcpReactor& io_;
  const OrderConfig& config_;
  std::unordered_map<ConnectionId, State> links_;
};
}  // namespace marketlab::orders
