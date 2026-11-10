#include "matching_engine/cluster/router.hpp"
#include "matching_engine/cluster/rpc.hpp"
#include "matching_engine/persistence/codec.hpp"
#include "shared_data/orders/engine_connections.hpp"
#include <deque>
#include <fstream>
#include <sstream>

namespace marketlab::orders {
namespace {
class Router {
 public:
  explicit Router(const OrderConfig& config,
                  std::uint16_t admin_port,
                  const std::string& market_routes)
      : c(config),
        io(c.max_buffer_bytes, c.partial_timeout_ms),
        links(io, c) {
    for (std::size_t i = 0; i < c.shards.size(); ++i) {
      shards.push_back({});
      shards.back().address = c.shards[i].address;
      shard_index.emplace(c.shards[i].id, i);
    }
    for (const auto& p : c.products)
      products.emplace(p.symbol, shard_index.at(p.shard));
    pending.reserve(c.max_pending);
    io.on_accept = [&](auto id) {
      links.incoming(id);
    };
    io.on_connect = [&](auto id) {
      links.connected(id);
    };
    io.on_close = [&](auto id) {
      closed(id);
    };
    io.on_frame = [&](auto id, auto bytes) {
      links.receive(id, bytes);
    };
    links.allow_ready = [&](auto id) {
      if (backend.contains(id))
        return true;
      for (const auto& shard : shards)
        if (!links.ready(shard.connection))
          return false;
      return true;
    };
    links.on_ready = [&](auto id) {
      if (backend.contains(id)) {
        auto& shard = shards[backend.at(id)];
        if (shard.stage == "connecting") {
          if (shard.market)
            shard.market->port.store(shard.next_market);
          shard.stage = "flushing";
        }
      } else {
        if (front && front != id)
          io.close(front);
        front = id;
        for (const auto& shard : shards)
          if (shard.stage != "active")
            links.send(front, LinkKind::MaintenanceBegin, {0, {}, shard.cut_token, 0, 0}, {});
      }
    };
    links.on_message = [&](auto id, const auto& h, auto bytes) {
      if (backend.contains(id))
        result(id, h, bytes);
      else if (id == front)
        command(h, bytes);
      else
        io.close(id);
    };
    io.listen(c.router_listen, Framing::Internal, 4 + c.shards.size());
    if (admin_port) {
      admin = std::make_unique<cluster::RpcServer>(admin_port);
      io.watch_wakeup(admin->fd(), [&] {
        service_admin();
      });
    }
    start_market(market_routes);
    std::cout << "ROUTER_LISTENING order=" << c.router_listen.port << " admin=" << admin_port
              << " planned_restart=enabled" << std::endl;
  }

  void run(const std::atomic_bool& stop) {
    auto tick = Clock::now();
    while (!stop.load()) {
      const auto now = Clock::now();
      for (std::size_t i = 0; i < shards.size(); ++i) {
        auto& shard = shards[i];
        if (!shard.connection && now >= shard.reconnect) {
          shard.connection = io.connect(shard.address, Framing::Internal);
          if (shard.connection) {
            backend.emplace(shard.connection, i);
            links.outgoing(shard.connection);
          }
          shard.reconnect = now + std::chrono::milliseconds(c.reconnect_ms);
        }
      }
      io.step(10);
      service_admin();
      for (auto& shard : shards)
        flush_queued(shard);
      if (now - tick >= std::chrono::milliseconds(50)) {
        links.tick();
        io.expire();
        bool expired = false;
        for (const auto& [token, p] : pending) {
          (void)token;
          const bool paused = std::any_of(shards.begin(), shards.end(), [](const auto& s) {
            return s.stage != "active";
          });
          if (!paused && now > p.deadline) {
            expired = true;
            break;
          }
        }
        if (expired && front)
          io.close(front);
        tick = now;
      }
    }
  }

  void stats(const std::string& path) {
    if (path.empty())
      return;
    std::ofstream out(path);
    out << "{\"commands\":" << commands << ",\"events\":" << events
        << ",\"backend_connect_attempts\":" << io.connects
        << ",\"front_connections\":" << io.accepted << ",\"snapshot_ends\":" << snapshot_ends
        << "}\n";
  }

 private:
  struct MarketRoute {
    std::atomic<std::uint16_t> port{0};
    const md::Epoch epoch = md::new_epoch();
    std::unique_ptr<md::StopThread> thread;
  };

  struct Queued {
    LinkHeader header;
    Bytes bytes;
  };

  struct Shard {
    ConnectionId connection = 0;
    Clock::time_point reconnect{};
    Address address;
    std::string stage = "active";
    std::uint64_t cut_token = 0;
    Bytes boundary;
    std::deque<Queued> queued;
    std::size_t queued_bytes = 0;
    std::size_t peak_queued = 0, peak_bytes = 0;
    std::shared_ptr<MarketRoute> market;
    std::uint16_t next_market = 0;
  };

  struct Pending {
    LinkHeader context;
    RequestId request;
    ConnectionId front;
    std::size_t target;
    std::vector<bool> completed;
    Clock::time_point deadline;
  };

  const OrderConfig& c;
  TcpReactor io;
  EngineLinks links;
  std::unique_ptr<cluster::RpcServer> admin;
  ConnectionId front = 0;
  std::vector<Shard> shards;
  std::unordered_map<ConnectionId, std::size_t> backend;
  std::unordered_map<std::uint16_t, std::size_t> shard_index;
  std::unordered_map<std::string, std::size_t> products;
  std::unordered_map<std::uint64_t, Pending> pending;
  std::uint64_t commands = 0, events = 0, snapshot_ends = 0;

  void start_market(const std::string& path) {
    if (path.empty())
      return;
    std::ifstream input(path);
    if (!input)
      throw std::runtime_error("Cannot read market routes");
    unsigned id, public_port, active_port;
    while (input >> id >> public_port >> active_port) {
      if (!public_port || public_port > 65535 || !active_port || active_port > 65535)
        throw std::runtime_error("Invalid market route port");
      auto route = std::make_shared<MarketRoute>();
      route->port.store(active_port);
      auto& shard = shards.at(shard_index.at(id));
      shard.market = route;
      md::ShardConfig source{"shard-" + std::to_string(id), "tcp", "127.0.0.1", 0, "", {}};
      for (std::size_t i = 0; i < c.products.size(); ++i)
        if (c.products[i].shard == id)
          source.symbols.push_back(static_cast<std::uint16_t>(i + 1));
      auto listener = md::listen_tcp("127.0.0.1", public_port);
      // The owning Shard outlives the joining thread; don't capture shared_ptr
      // here, which would form a cycle and leave the listener alive at shutdown.
      route->thread = std::make_unique<md::StopThread>(
          [state = route.get(), source, listener = std::move(listener)](const auto& stop) mutable {
            std::unique_ptr<md::ShardSource> connection;
            std::uint16_t connected = 0;
            try {
              md::serve_shard(
                  std::move(listener),
                  [&] {
                    for (unsigned retry = 0; retry < 2; ++retry) {
                      const auto port = state->port.load();
                      if (port != connected) {
                        source.port = port;
                        connection = md::make_source(source, 1000);
                        connected = port;
                      }
                      try {
                        auto view =
                            connection->read(md::Clock::now() + std::chrono::milliseconds(700));
                        if (state->port.load() == port) {
                          // The logical shard history continues through a planned
                          // cut. Publisher epochs change only on router restart.
                          view.epoch = state->epoch;
                          return view;
                        }
                      } catch (...) {
                        if (state->port.load() == port)
                          throw;
                      }
                    }
                    throw std::runtime_error("Market route changed during sample");
                  },
                  stop,
                  5000,
                  128);
            } catch (const std::exception& e) {
              std::cerr << "ROUTER_MARKET_FAILED " << e.what() << std::endl;
            }
          });
    }
  }

  void service_admin() {
    if (!admin)
      return;
    admin->service([&](auto bytes) -> std::optional<Bytes> {
      std::istringstream input(std::string(bytes.begin(), bytes.end()));
      std::string op;
      unsigned id = 0;
      input >> op;
      if (op == "status") {
        std::ostringstream out;
        for (std::size_t i = 0; i < shards.size(); ++i)
          out << c.shards[i].id << ' ' << shards[i].address.port << ' ' << shards[i].stage << ' '
              << shards[i].queued.size() << ' ' << links.ready(shards[i].connection) << '\n';
        return cluster::text_bytes(out.str());
      }
      if (!(input >> id) || !shard_index.contains(id))
        throw std::runtime_error("Unknown shard");
      auto& shard = shards[shard_index.at(id)];
      if (op == "metrics")
        return cluster::text_bytes(std::to_string(shard.peak_queued) + " " +
                                   std::to_string(shard.peak_bytes));
      if (op == "cut") {
        if (shard.stage != "active" || !links.ready(shard.connection))
          throw std::runtime_error("Shard is not ready for a cut");
        if (!(input >> shard.cut_token) || !shard.cut_token)
          throw std::runtime_error("Missing cut token");
        shard.stage = "draining";
        shard.peak_queued = shard.peak_bytes = 0;
        shard.boundary.clear();
        const LinkHeader context{0, {}, shard.cut_token, 0, 0};
        if (front)
          links.send(front, LinkKind::MaintenanceBegin, context, {});
        if (!links.send(shard.connection, LinkKind::Drain, context, {}))
          throw std::runtime_error("Cannot send drain marker");
        std::cout << "ROUTER_CUT shard=" << id << " token=" << shard.cut_token << std::endl;
        return cluster::text_bytes("STARTED");
      }
      if (op == "barrier") {
        if (shard.stage != "drained")
          return cluster::text_bytes("WAIT");
        persistence::Reader r{shard.boundary};
        std::ostringstream out;
        for (int i = 0; i < 5; ++i)
          out << (i ? " " : "") << r.number(8);
        return cluster::text_bytes(out.str());
      }
      if (op == "route") {
        unsigned port, market;
        std::string host;
        if (shard.stage != "drained" || !(input >> host >> port >> market) || host != "127.0.0.1" ||
            !port || port > 65535 || market > 65535)
          throw std::runtime_error("Route requires a drained shard and valid loopback ports");
        const auto old = shard.connection;
        backend.erase(old);  // Planned retirement must not close gateway sessions.
        io.close(old);
        shard.connection = 0;
        shard.address = {host, static_cast<std::uint16_t>(port)};
        shard.next_market = market;
        shard.stage = "connecting";
        shard.reconnect = {};
        return cluster::text_bytes("CONNECTING");
      }
      if (op == "resume") {
        if (shard.stage != "drained" || !links.ready(shard.connection))
          throw std::runtime_error("Resume requires a drained connected source");
        shard.stage = "flushing";
        return cluster::text_bytes("RESUMING");
      }
      throw std::runtime_error("Unknown router command");
    });
  }

  bool dispatch(Shard& shard, const LinkHeader& h, std::span<const std::uint8_t> bytes) {
    if (shard.stage == "active")
      return links.send(shard.connection, h.kind, h, bytes);
    if (shard.queued.size() >= c.max_pending ||
        shard.queued_bytes + bytes.size() > c.max_buffer_bytes)
      return false;
    shard.queued.push_back({h, Bytes(bytes.begin(), bytes.end())});
    shard.queued_bytes += bytes.size();
    shard.peak_queued = std::max(shard.peak_queued, shard.queued.size());
    shard.peak_bytes = std::max(shard.peak_bytes, shard.queued_bytes);
    return true;
  }

  void flush_queued(Shard& shard) {
    if (shard.stage != "flushing")
      return;
    for (unsigned i = 0; i < 64 && !shard.queued.empty(); ++i) {
      auto& queued = shard.queued.front();
      if (!links.send(shard.connection, queued.header.kind, queued.header, queued.bytes))
        return;
      shard.queued_bytes -= queued.bytes.size();
      shard.queued.pop_front();
    }
    if (!shard.queued.empty())
      return;
    shard.stage = "active";
    for (auto& [token, p] : pending) {
      (void)token;
      p.deadline = Clock::now() + std::chrono::milliseconds(c.request_timeout_ms);
    }
    if (front)
      links.send(front, LinkKind::MaintenanceEnd, {0, {}, shard.cut_token, 0, 0}, {});
    std::cout << "ROUTER_RESUMED port=" << shard.address.port << std::endl;
  }

  void closed(ConnectionId id) {
    links.lost(id);
    if (id == front) {
      front = 0;
      pending.clear();
      for (auto& shard : shards) {
        shard.queued.clear();
        shard.queued_bytes = 0;
      }
    }
    const auto it = backend.find(id);
    if (it != backend.end()) {
      shards[it->second].connection = 0;
      backend.erase(it);
      if (front)
        io.close(front);
    }
  }

  void reject(const LinkHeader& h, RequestId request, RejectReason reason) {
    links.send(front, LinkKind::Event, h, record_bytes(rejected(request, reason)));
  }

  void command(const LinkHeader& h, std::span<const std::uint8_t> bytes) {
    if (h.kind != LinkKind::NewOrder && h.kind != LinkKind::Cancel &&
        h.kind != LinkKind::Snapshot && h.kind != LinkKind::History) {
      io.close(front);
      return;
    }
    if (h.user > c.accounts.size() || pending.contains(h.token) ||
        pending.size() >= c.max_pending) {
      io.close(front);
      return;
    }
    Header wire{};
    std::memcpy(&wire, bytes.data(), sizeof(wire));
    ++commands;
    std::size_t target = 0;
    bool fanout = h.kind == LinkKind::Snapshot;
    if (h.kind == LinkKind::History) {
      const auto q = copy_record<TradeHistoryRequest>(bytes);
      fanout = q.shard == 0;
      if (!fanout) {
        const std::uint16_t shard_id = q.shard;
        const auto shard = shard_index.find(shard_id);
        if (shard == shard_index.end()) {
          reject(h, wire.requestId, RejectReason::InvalidHistoryQuery);
          return;
        }
        target = shard->second;
      }
    }
    if (h.kind == LinkKind::NewOrder) {
      const auto order = copy_record<NewOrderRequest>(bytes);
      const auto product = products.find(fixed_string(order.symbol));
      if (product == products.end()) {
        reject(h, wire.requestId, RejectReason::InvalidOrder);
        return;
      }
      target = product->second;
    } else if (h.kind == LinkKind::Cancel) {
      const auto order = copy_record<CancelRequest>(bytes);
      const auto shard = shard_index.find(static_cast<std::uint16_t>(order.orderId >> 48));
      if (shard == shard_index.end()) {
        reject(h, wire.requestId, RejectReason::CancelFailed);
        return;
      }
      target = shard->second;
    }
    Pending p{h,
              wire.requestId,
              front,
              target,
              {},
              Clock::now() + std::chrono::milliseconds(c.request_timeout_ms)};
    if (fanout)
      p.completed.resize(shards.size(), false);
    pending.emplace(h.token, std::move(p));
    if (fanout) {
      for (auto& shard : shards) {
        if (!dispatch(shard, h, bytes)) {
          io.close(front);
          return;
        }
      }
    } else if (!dispatch(shards[target], h, bytes))
      io.close(front);
  }

  void result(ConnectionId id, const LinkHeader& h, std::span<const std::uint8_t> bytes) {
    if (h.kind == LinkKind::Drained) {
      auto& shard = shards[backend.at(id)];
      if (shard.stage != "draining" || h.token != shard.cut_token) {
        io.close(id);
        return;
      }
      shard.boundary.assign(bytes.begin(), bytes.end());
      shard.stage = "drained";
      persistence::Reader reader{bytes};
      std::cout << "ROUTER_DRAINED shard=" << c.shards[backend.at(id)].id
                << " committed=" << reader.number(8) << std::endl;
      return;
    }
    if ((h.kind != LinkKind::Event && h.kind != LinkKind::SnapshotEnd &&
         h.kind != LinkKind::HistoryEnd) ||
        h.user > c.accounts.size()) {
      io.close(id);
      return;
    }
    Header wire{};
    std::memcpy(&wire, bytes.data(), sizeof(wire));
    if (!h.token) {
      if (h.kind != LinkKind::Event || h.session || wire.requestId ||
          (wire.messageType != MessageType::OrderUpdate &&
           wire.messageType != MessageType::Trade)) {
        io.close(id);
        return;
      }
      if (front) {
        links.send(front, h.kind, h, bytes);
        ++events;
      }
      return;
    }
    const auto it = pending.find(h.token);
    if (it == pending.end())
      return;
    auto& p = it->second;
    if (h.user != p.context.user || h.session != p.context.session || wire.requestId != p.request) {
      io.close(id);
      return;
    }
    if (p.context.kind == LinkKind::History) {
      history_result(id, h, wire, bytes, p);
      return;
    }
    if (p.context.kind == LinkKind::Snapshot) {
      const auto index = backend.at(id);
      if (wire.messageType != MessageType::ActiveOrderSnapshot || p.completed[index]) {
        io.close(id);
        return;
      }
      const auto snapshot = copy_record<ActiveOrderSnapshot>(bytes);
      if (h.kind == LinkKind::SnapshotEnd) {
        const OrderState zero{};
        if (snapshot.isEnd != 1 || std::memcmp(&snapshot.order, &zero, sizeof(zero)) != 0) {
          io.close(id);
          return;
        }
        p.completed[index] = true;
        for (const bool done : p.completed)
          if (!done)
            return;
        auto end = server_record<ActiveOrderSnapshot>(MessageType::ActiveOrderSnapshot, p.request);
        end.isEnd = 1;
        const auto destination = p.front;
        pending.erase(it);
        links.send(destination, LinkKind::SnapshotEnd, h, record_bytes(end));
        ++snapshot_ends;
        ++events;
      } else {
        if (snapshot.isEnd) {
          io.close(id);
          return;
        }
        links.send(p.front, h.kind, h, bytes);
        ++events;
      }
    } else {
      if (backend.at(id) != p.target || h.kind != LinkKind::Event ||
          (wire.messageType != MessageType::OrderUpdate &&
           wire.messageType != MessageType::RequestRejected)) {
        io.close(id);
        return;
      }
      const auto destination = p.front;
      pending.erase(it);
      links.send(destination, h.kind, h, bytes);
      ++events;
    }
  }

  void history_result(ConnectionId id,
                      const LinkHeader& h,
                      const Header& wire,
                      std::span<const std::uint8_t> bytes,
                      Pending& p) {
    const auto index = backend.at(id);
    const auto destination = p.front;
    const bool fanout = !p.completed.empty();
    if ((!fanout && index != p.target) || (fanout && p.completed[index])) {
      io.close(id);
      return;
    }
    if (wire.messageType == MessageType::RequestRejected && h.kind == LinkKind::Event) {
      pending.erase(h.token);
      links.send(destination, h.kind, h, bytes);
      ++events;
      return;
    }
    if (wire.messageType == MessageType::TradeHistoryRecord && h.kind == LinkKind::Event) {
      links.send(destination, h.kind, h, bytes);
      ++events;
      return;
    }
    if (wire.messageType != MessageType::TradeHistoryEnd || h.kind != LinkKind::HistoryEnd) {
      io.close(id);
      return;
    }
    const auto end = copy_record<TradeHistoryEnd>(bytes);
    if (end.shard != c.shards[index].id || (end.flags & ~history_more)) {
      io.close(id);
      return;
    }
    bool complete = !fanout;
    auto marker = server_record<TradeHistoryEnd>(MessageType::TradeHistoryEnd, p.request);
    marker.flags = history_all_shards;
    if (fanout) {
      p.completed[index] = true;
      complete = std::all_of(p.completed.begin(), p.completed.end(), [](bool done) {
        return done;
      });
    }
    if (complete)
      pending.erase(h.token);
    links.send(destination, h.kind, h, bytes);
    ++events;
    if (complete && fanout) {
      links.send(destination, LinkKind::HistoryEnd, h, record_bytes(marker));
      ++events;
    }
  }
};
}

void run_order_router(const OrderConfig& config,
                      const std::atomic_bool& stop,
                      const std::string& stats_path,
                      std::uint16_t admin_port,
                      const std::string& market_routes) {
  Router router(config, admin_port, market_routes);
  router.run(stop);
  router.stats(stats_path);
}
}  // namespace marketlab::orders
