#include "shared_data/paths.hpp"
#include "matching_engine/server.hpp"
#include "shared_data/market_data/transport.hpp"
#include <csignal>
#include <iostream>

namespace {
volatile std::sig_atomic_t interrupted = 0;
void on_signal(int) { interrupted = 1; }
std::uint16_t number(const std::string& text) {
  if (text.empty() || text.find_first_not_of("0123456789") != std::string::npos)
    throw std::runtime_error("Expected a numeric shard/port");
  const auto value = std::stoul(text);
  if (!value || value > 65535) throw std::runtime_error("Shard/port outside 1..65535");
  return static_cast<std::uint16_t>(value);
}
}
int main(int argc,char** argv) {
  using namespace marketlab;
  using namespace marketlab::orders;
  std::signal(SIGINT,on_signal); std::signal(SIGTERM,on_signal); std::signal(SIGPIPE,SIG_IGN);
  try {
    if (argc == 1 || std::string(argv[1]) == "--help") {
      std::cout << "market_matching_engine --shard ID [options]\n"
        << "  --config PATH       Shared deployment config; default src/config/exchange.conf\n"
        << "  --market-port PORT  Enable publisher TCP source on this port\n"
        << "  --market-shm PATH   Enable local shared-memory market view\n"
        << "  --stats PATH        Write counters on graceful shutdown\n"
        << "  --trace             Log internal message metadata\n";
      return argc == 1 ? 1 : 0;
    }
    std::string path = marketlab::default_exchange_config,stats,mmap_path;
    std::uint16_t shard = 0,market_port = 0; bool trace = false;
    for (int i = 1; i < argc; ++i) {
      const std::string flag = argv[i];
      if (flag == "--trace") { trace = true; continue; }
      if (++i == argc) throw std::runtime_error("Missing option value");
      const std::string value = argv[i];
      if (flag == "--shard") shard = number(value);
      else if (flag == "--config") path = value;
      else if (flag == "--market-port") market_port = number(value);
      else if (flag == "--market-shm") mmap_path = value;
      else if (flag == "--stats") stats = value;
      else throw std::runtime_error("Unknown option: " + flag);
    }
    if (!shard) throw std::runtime_error("--shard ID is required");
    auto config = load_order_config(path); config.trace = trace;
    std::atomic_bool stop{false};
    md::StopThread signals([&](const std::atomic_bool& cancelled) {
      while (!cancelled.load() && !interrupted) std::this_thread::sleep_for(std::chrono::milliseconds(10));
      if (interrupted) stop.store(true);
    });
    run_order_shard(config,shard,stop,stats,market_port,mmap_path);
    return 0;
  } catch (const std::exception& e) { std::cerr << "Matching engine failed: " << e.what() << '\n'; return 1; }
}
