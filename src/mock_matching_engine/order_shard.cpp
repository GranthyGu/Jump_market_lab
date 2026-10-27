#include "mock_matching_engine/order_router.hpp"
#include "mock_matching_engine/order_book.hpp"
#include "gateway/net/engine_connections.hpp"
#include "market_publisher/net/transport.hpp"
#include <fstream>

namespace marketlab::orders {
void serve_order_engine(EngineBackend& book,const OrderConfig& c,std::uint16_t shard,const std::atomic_bool& stop,
                         const std::string& stats_path,std::uint16_t market_port,const std::string& mmap_path) {
  const auto found = std::find_if(c.shards.begin(),c.shards.end(),[&](const auto& s) { return s.id == shard; });
  if (found == c.shards.end()) throw std::runtime_error("Unknown shard");
  TcpReactor io(c.max_buffer_bytes,c.partial_timeout_ms);
  EngineLinks links(io,c);
  ConnectionId router = 0;
  std::uint64_t commands = 0, events = 0;
  io.on_accept = [&](auto id) { links.incoming(id); };
  io.on_close = [&](auto id) { links.lost(id); if (router == id) router = 0; };
  io.on_frame = [&](auto id,auto bytes) { links.receive(id,bytes); };
  links.on_ready = [&](auto id) { if (router && router != id) io.close(router); router = id; };
  links.on_message = [&](auto id,const auto& h,auto bytes) {
    if (id != router || h.user > c.accounts.size() ||
        (h.kind != LinkKind::NewOrder && h.kind != LinkKind::Cancel && h.kind != LinkKind::Snapshot)) { io.close(id); return; }
    ++commands;
    book.command(h,bytes,[&](auto kind,const auto& context,const auto& payload) {
      links.send(id,kind,context,payload); ++events;
    });
  };
  io.listen(found->address,Framing::Internal,4);
  // Optional bridge reuses the existing market-data IPC exactly, allowing the
  // publisher to sample the same orders used by gateway integration tests.
  std::atomic_bool market_failed{false};
  std::unique_ptr<md::StopThread> market_tcp, market_shared;
  if (market_port) {
    auto listener = md::listen_tcp(found->address.host,market_port);
    market_tcp = std::make_unique<md::StopThread>([&, listener = std::move(listener)](const auto& cancelled) mutable {
      try { md::serve_shard(std::move(listener),[&] { return book.market_view(); },cancelled,c.request_timeout_ms,128); }
      catch (const std::exception& e) { std::cerr << "Market adapter failed: " << e.what() << '\n'; market_failed.store(true); }
    });
  }
  if (!mmap_path.empty()) {
    auto writer = std::make_unique<md::SharedViewWriter>(mmap_path,book.market_view().books.size());
    market_shared = std::make_unique<md::StopThread>([&, writer = std::move(writer)](const auto& cancelled) {
      try {
        while (!cancelled.load()) {
          writer->publish(book.market_view());
          md::sleep_until_or_stop(md::Clock::now() + std::chrono::milliseconds(100),cancelled,cancelled);
        }
      } catch (const std::exception& e) { std::cerr << "Shared market adapter failed: " << e.what() << '\n'; market_failed.store(true); }
    });
  }
  auto tick = Clock::now();
  while (!stop.load() && !market_failed.load()) {
    io.step(10);
    const auto now = Clock::now();
    if (now - tick >= std::chrono::milliseconds(50)) { links.tick(); io.expire(); tick = now; }
  }
  if (!stats_path.empty()) {
    std::ofstream out(stats_path);
    out << "{\"shard\":" << shard << ",\"commands\":" << commands << ",\"events\":" << events
        << ",\"router_connections\":" << io.accepted << "}\n";
  }
  if (market_failed.load()) throw std::runtime_error("Market adapter stopped");
}
void run_order_shard(const OrderConfig& c,std::uint16_t shard,const std::atomic_bool& stop,
                     const std::string& stats_path,std::uint16_t market_port,const std::string& mmap_path) {
  MockOrderBook book(c,shard);
  serve_order_engine(book,c,shard,stop,stats_path,market_port,mmap_path);
}
}  // namespace marketlab::orders
