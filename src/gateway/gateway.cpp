#include "gateway/gateway.hpp"
#include "shared_data/orders/engine_connections.hpp"
#include <fstream>
#include <limits>
#include <unordered_set>

namespace marketlab::orders {
namespace {
class Gateway {
 public:
  explicit Gateway(const OrderConfig& config) : c(config), io(c.max_buffer_bytes, c.partial_timeout_ms), links(io,c), sequence(random_token_seed()) {
    usernames.reserve(c.accounts.size());
    for (std::size_t i = 0; i < c.accounts.size(); ++i) usernames.emplace(c.accounts[i].name, static_cast<std::uint32_t>(i + 1));
    online.resize(c.accounts.size() + 1, 0);
    if (c.direct) {
      for (std::size_t i = 0; i < c.shards.size(); ++i) {
        engines.push_back({c.shards[i].address,0,{}});
        shard_index.emplace(c.shards[i].id,i);
      }
      for (const auto& p : c.products) routes.emplace(p.symbol,shard_index.at(p.shard));
    } else engines.push_back({c.engine_endpoint,0,{}});
    pending.reserve(std::min<std::size_t>(c.max_pending, 65536));
    clients.reserve(c.max_connections);
    io.on_accept = [&](auto id) { clients.emplace(id, Client{}); };
    io.on_connect = [&](auto id) { links.connected(id); };
    io.on_close = [&](auto id) { closed(id); };
    io.on_frame = [&](auto id, auto bytes) {
      if (backend.contains(id)) links.receive(id,bytes); else request(id,bytes);
    };
    links.on_message = [&](auto id, const auto& h, auto bytes) { event(id, h, bytes); };
    io.listen(c.gateway_listen, Framing::Client, c.max_connections + engines.size());
  }
  void run(const std::atomic_bool& stop) {
    auto last_tick = Clock::now();
    while (!stop.load()) {
      const auto now = Clock::now();
      for (std::size_t i = 0; i < engines.size(); ++i) {
        auto& engine = engines[i];
        if (!engine.connection && now >= engine.retry) {
          engine.connection = io.connect(engine.address, Framing::Internal);
          if (engine.connection) { links.outgoing(engine.connection); backend.emplace(engine.connection,i); }
          engine.retry = now + std::chrono::milliseconds(c.reconnect_ms);
        }
      }
      io.step(10);
      if (now - last_tick >= std::chrono::milliseconds(50)) {
        links.tick(); io.expire();
        std::vector<ConnectionId> expired;
        for (const auto& [id,client] : clients) {
          if (!client.user && now - client.opened > std::chrono::milliseconds(c.request_timeout_ms)) expired.push_back(id);
        }
        for (const auto& [token,p] : pending) { (void)token; if (now > p.deadline) expired.push_back(p.session); }
        for (auto id : expired) io.close(id);
        last_tick = now;
      }
    }
  }
  void stats(const std::string& path) {
    if (path.empty()) return;
    std::ofstream out(path);
    out << "{\"client_connections\":" << io.accepted << ",\"engine_connect_attempts\":" << io.connects
        << ",\"commands\":" << commands << ",\"events\":" << events << ",\"logins\":" << logins
        << ",\"replaced_sessions\":" << replaced << ",\"peak_pending\":" << peak_pending
        << ",\"engine_frames\":" << engine_frames << ",\"bytes_in\":" << io.bytes_in << ",\"bytes_out\":" << io.bytes_out << "}\n";
  }
 private:
  struct Client { std::uint32_t user = 0; Clock::time_point opened = Clock::now(); std::unordered_set<std::uint64_t> tokens; };
  struct Pending {
    ConnectionId session; std::uint32_t user; RequestId request; LinkKind kind;
    Clock::time_point deadline; ConnectionId target; std::vector<bool> done;
  };
  struct Engine { Address address; ConnectionId connection; Clock::time_point retry; };
  const OrderConfig& c;
  TcpReactor io;
  EngineLinks links;
  std::vector<Engine> engines;
  std::unordered_map<ConnectionId,std::size_t> backend;
  std::unordered_map<std::uint16_t,std::size_t> shard_index;
  std::unordered_map<std::string,std::size_t> routes;
  std::uint64_t sequence, commands = 0, engine_frames = 0, events = 0, logins = 0, replaced = 0;
  std::size_t peak_pending = 0;
  std::unordered_map<std::string, std::uint32_t> usernames;
  std::vector<ConnectionId> online;
  std::unordered_map<ConnectionId, Client> clients;
  std::unordered_map<std::uint64_t, Pending> pending;

  bool ready() const {
    for (const auto& engine : engines) if (!links.ready(engine.connection)) return false;
    return true;
  }

  void closed(ConnectionId id) {
    const auto engine = backend.find(id);
    if (engine != backend.end()) {
      links.lost(id); engines[engine->second].connection = 0; backend.erase(engine);
      // Never replay commands with an unknown outcome. Re-login obtains truth
      // from engine state; closing sessions avoids reporting fabricated rejects.
      std::vector<ConnectionId> ids;
      for (const auto& [client,s] : clients) { (void)s; ids.push_back(client); }
      for (auto client : ids) io.close(client);
      return;
    }
    auto it = clients.find(id);
    if (it == clients.end()) return;
    if (it->second.user && online[it->second.user] == id) online[it->second.user] = 0;
    for (auto token : it->second.tokens) pending.erase(token);
    clients.erase(it);
  }
  void reply(ConnectionId id, const auto& value) { io.send(id, record_bytes(value)); }
  void request(ConnectionId id, std::span<const std::uint8_t> bytes) {
    if (!valid_client_record(bytes, true)) { io.close(id); return; }
    Header h{}; std::memcpy(&h,bytes.data(),sizeof(h));
    if (h.messageType == MessageType::LoginRequest) {
      const auto login = copy_record<LoginRequest>(bytes);
      const auto username = fixed_string(login.username), password = fixed_string(login.password);
      const auto found = usernames.find(username);
      if (found == usernames.end() || c.accounts[found->second - 1].password != password) {
        reply(id,rejected(h.requestId,RejectReason::InvalidCredentials)); return;
      }
      if (!ready()) { io.close(id); return; }
      // Reauthentication invalidates the old logical session, including pending
      // snapshot/reply tokens. A different TCP connection is physically closed.
      auto& session = clients.at(id);
      if (session.user) {
        if (online[session.user] == id) online[session.user] = 0;
        for (auto token : session.tokens) pending.erase(token);
        session.tokens.clear(); session.user = 0; ++replaced;
      }
      const auto user = found->second;
      if (online[user] && online[user] != id) { ++replaced; io.close(online[user]); }
      clients.at(id).user = user; online[user] = id; ++logins;
      reply(id,server_record<LoginAccepted>(MessageType::LoginAccepted,h.requestId));
      const Header snapshot{MessageType::ActiveOrderSnapshot,sizeof(Header),h.requestId};
      submit(id,LinkKind::Snapshot,record_bytes(snapshot),h.requestId);
      return;
    }
    const auto client = clients.find(id);
    if (client == clients.end() || !client->second.user || !ready()) { io.close(id); return; }
    if (h.messageType == MessageType::NewOrderRequest) {
      const auto order = copy_record<NewOrderRequest>(bytes);
      (void)fixed_string(order.symbol);
      submit(id,LinkKind::NewOrder,bytes,h.requestId);
    } else if (h.messageType == MessageType::CancelRequest) submit(id,LinkKind::Cancel,bytes,h.requestId);
    else io.close(id);
  }
  void submit(ConnectionId id, LinkKind kind, std::span<const std::uint8_t> bytes, RequestId request) {
    auto it = clients.find(id);
    if (it == clients.end()) return;
    if (pending.size() >= c.max_pending || it->second.tokens.size() >= c.max_pending_per_client ||
        sequence == std::numeric_limits<std::uint64_t>::max()) { io.close(id); return; }
    std::size_t target = 0;
    if (c.direct && kind == LinkKind::NewOrder) {
      const auto order = copy_record<NewOrderRequest>(bytes);
      const auto route = routes.find(fixed_string(order.symbol));
      if (route == routes.end()) { reply(id,rejected(request,RejectReason::InvalidOrder)); return; }
      target = route->second;
    } else if (c.direct && kind == LinkKind::Cancel) {
      const auto cancel = copy_record<CancelRequest>(bytes);
      const auto shard = shard_index.find(static_cast<std::uint16_t>(cancel.orderId >> 48));
      if (shard == shard_index.end()) { reply(id,rejected(request,RejectReason::CancelFailed)); return; }
      target = shard->second;
    }
    const auto token = sequence++;
    const auto user = it->second.user;
    Pending p{id,user,request,kind,Clock::now() + std::chrono::milliseconds(c.request_timeout_ms),engines[target].connection,{}};
    if (kind == LinkKind::Snapshot) p.done.resize(engines.size(),false);
    pending.emplace(token,std::move(p));
    it->second.tokens.insert(token);
    peak_pending = std::max(peak_pending,pending.size());
    auto send_to = [&](ConnectionId engine) {
      if (!links.send(engine,kind,{0,kind,token,user,id},bytes)) { io.close(engine); io.close(id); return false; }
      ++engine_frames; return true;
    };
    if (kind == LinkKind::Snapshot) {
      for (const auto& engine : engines) if (!send_to(engine.connection)) return;
    } else if (!send_to(engines[target].connection)) return;
    ++commands;
  }
  void event(ConnectionId engine, const LinkHeader& h, std::span<const std::uint8_t> bytes) {
    if ((h.kind != LinkKind::Event && h.kind != LinkKind::SnapshotEnd) || h.user >= online.size()) { io.close(engine); return; }
    Header wire{}; std::memcpy(&wire,bytes.data(),sizeof(wire));
    ConnectionId destination = online[h.user];
    bool completed = false;
    if (h.token) {
      const auto it = pending.find(h.token);
      if (it == pending.end()) return;
      auto& p = it->second;
      if (p.user != h.user || p.session != h.session || wire.requestId != p.request || destination != p.session) { io.close(engine); return; }
      if (p.kind == LinkKind::Snapshot) {
        if (wire.messageType != MessageType::ActiveOrderSnapshot) { io.close(engine); return; }
        const auto snapshot = copy_record<ActiveOrderSnapshot>(bytes);
        const OrderState zero{};
        if (snapshot.isEnd > 1 || (h.kind == LinkKind::SnapshotEnd) != (snapshot.isEnd == 1) ||
            (snapshot.isEnd && std::memcmp(&snapshot.order,&zero,sizeof(zero)) != 0)) { io.close(engine); return; }
        const auto index = backend.at(engine);
        if (p.done[index]) { io.close(engine); return; }
        if (snapshot.isEnd) {
          p.done[index] = true;
          for (bool done : p.done) if (!done) return;
          const auto end = server_record<ActiveOrderSnapshot>(MessageType::ActiveOrderSnapshot,p.request);
          auto marker = end; marker.isEnd = 1;
          clients.at(p.session).tokens.erase(h.token); pending.erase(it);
          ++events; io.send(destination,record_bytes(marker)); return;
        }
        if (snapshot.order.status != OrderStatus::Accepted && snapshot.order.status != OrderStatus::PartiallyFilled) { io.close(engine); return; }
      } else {
        if (engine != p.target) { io.close(engine); return; }
        if (h.kind != LinkKind::Event || (wire.messageType != MessageType::OrderUpdate && wire.messageType != MessageType::RequestRejected)) { io.close(engine); return; }
        if (wire.messageType == MessageType::OrderUpdate) {
          const auto update = copy_record<OrderUpdate>(bytes);
          if (update.order.status != (p.kind == LinkKind::NewOrder ? OrderStatus::Accepted : OrderStatus::Canceled)) { io.close(engine); return; }
        }
        completed = true;
      }
      if (completed) { clients.at(p.session).tokens.erase(h.token); pending.erase(it); }
    } else {
      if (wire.requestId || h.session || h.kind != LinkKind::Event ||
          (wire.messageType != MessageType::Trade && wire.messageType != MessageType::OrderUpdate)) { io.close(engine); return; }
    }
    ++events;
    if (destination) io.send(destination,bytes);
  }
};
}
void run_gateway(const OrderConfig& config, const std::atomic_bool& stop, const std::string& stats_path) {
  Gateway gateway(config);
  gateway.run(stop);
  gateway.stats(stats_path);
}
}  // namespace marketlab::orders
