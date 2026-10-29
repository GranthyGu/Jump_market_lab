#include "gateway/gateway.hpp"
#include "gateway/net/order_router.hpp"
#include "matching_engine/server.hpp"
#include <atomic>
#include <csignal>
#include <iostream>
#include <thread>

namespace {
volatile std::sig_atomic_t interrupted = 0;
void signal_handler(int) { interrupted = 1; }
std::uint16_t port_or_shard(const std::string& text) {
  if (text.empty() || text.find_first_not_of("0123456789") != std::string::npos) throw std::runtime_error("Invalid numeric argument");
  const auto value = std::stoul(text);
  if (!value || value > 65535) throw std::runtime_error("Port/shard outside 1..65535");
  return static_cast<std::uint16_t>(value);
}
}
int main(int argc,char** argv) {
  using namespace marketlab::orders;
  std::signal(SIGINT,signal_handler); std::signal(SIGTERM,signal_handler); std::signal(SIGPIPE,SIG_IGN);
  try {
    if (argc < 2 || std::string(argv[1]) == "--help") {
      std::cout << "market_gateway <gateway|router|shard> [--config PATH] [--shard ID]\n"
          << "  --config PATH           Default: src/gateway/scale_32x1000.conf\n"
          << "  --trace                 Log internal message metadata (no passwords)\n"
          << "  --stats PATH            Write counters on graceful shutdown\n"
          << "  --market-port PORT      Shard's optional existing market-data TCP IPC\n"
          << "  --market-shm PATH       Shard's optional existing shared market view\n";
      return argc < 2 ? 1 : 0;
    }
    const std::string role = argv[1];
    std::string path = "src/gateway/scale_32x1000.conf", stats, mmap_path;
    std::uint16_t shard = 0, market_port = 0;
    bool trace = false;
    for (int i = 2; i < argc; ++i) {
      const std::string flag = argv[i];
      if (flag == "--trace") { trace = true; continue; }
      if (++i == argc) throw std::runtime_error("Missing option value");
      const std::string value = argv[i];
      if (flag == "--config") path = value;
      else if (flag == "--stats") stats = value;
      else if (flag == "--shard") shard = port_or_shard(value);
      else if (flag == "--market-port") market_port = port_or_shard(value);
      else if (flag == "--market-shm") mmap_path = value;
      else throw std::runtime_error("Unknown option: " + flag);
    }
    auto config = load_order_config(path); config.trace = trace;
    std::atomic_bool stop{false}, done{false};
    std::thread signals([&] {
      while (!done.load() && !interrupted) std::this_thread::sleep_for(std::chrono::milliseconds(10));
      if (interrupted) stop.store(true);
    });
    try {
      if (role == "gateway") run_gateway(config,stop,stats);
      else if (role == "router") run_order_router(config,stop,stats);
      else if (role == "shard" && shard) run_order_shard(config,shard,stop,stats,market_port,mmap_path);
      else throw std::runtime_error("Unknown role or missing --shard");
    } catch (...) { done.store(true); signals.join(); throw; }
    done.store(true); signals.join();
    return 0;
  } catch (const std::exception& e) { std::cerr << "Error: " << e.what() << '\n'; return 1; }
}
