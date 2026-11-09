#include "gateway/gateway.hpp"
#include "gateway/net/order_router.hpp"
#include "shared_data/paths.hpp"
#include <atomic>
#include <cerrno>
#include <csignal>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <thread>
#include <unistd.h>

namespace {
volatile std::sig_atomic_t interrupted = 0;

void signal_handler(int) {
  interrupted = 1;
}

int run_legacy_shard(int argc, char** argv) {
  // Keep the old command as a process launcher, without linking engine business
  // code into the gateway. Standalone engine builds can live in another folder.
  const auto binary = std::filesystem::canonical(argv[0]).parent_path() / "market_matching_engine";
  std::string executable = binary.string();
  std::vector<char*> arguments{executable.data()};
  for (int i = 2; i < argc; ++i)
    arguments.push_back(argv[i]);
  arguments.push_back(nullptr);
  execv(executable.c_str(), arguments.data());
  std::cerr << "Cannot launch " << executable << ": " << std::strerror(errno)
            << ". Build market_matching_engine alongside market_gateway or run it directly.\n";
  return 1;
}
}

int main(int argc, char** argv) {
  using namespace marketlab::orders;
  std::signal(SIGINT, signal_handler);
  std::signal(SIGTERM, signal_handler);
  std::signal(SIGPIPE, SIG_IGN);
  try {
    if (argc < 2 || std::string(argv[1]) == "--help") {
      std::cout << "market_gateway <gateway|router|shard> [--config PATH] [--shard ID]\n"
                << "  --config PATH           Default: src/config/exchange.conf\n"
                << "  --trace                 Log internal message metadata (no passwords)\n"
                << "  --stats PATH            Write counters on graceful shutdown\n"
                << "  --market-port PORT      Shard's optional existing market-data TCP IPC\n"
                << "  --market-shm PATH       Shard's optional existing shared market view\n";
      return argc < 2 ? 1 : 0;
    }
    const std::string role = argv[1];
    if (role == "shard")
      return run_legacy_shard(argc, argv);
    if (role != "gateway" && role != "router")
      throw std::runtime_error("Unknown role");
    std::string path = marketlab::default_exchange_config, stats;
    bool trace = false;
    for (int i = 2; i < argc; ++i) {
      const std::string flag = argv[i];
      if (flag == "--trace") {
        trace = true;
        continue;
      }
      if (++i == argc)
        throw std::runtime_error("Missing option value");
      const std::string value = argv[i];
      if (flag == "--config")
        path = value;
      else if (flag == "--stats")
        stats = value;
      else
        throw std::runtime_error("Unknown option: " + flag);
    }
    auto config = load_order_config(path);
    config.trace = trace;
    std::atomic_bool stop{false}, done{false};
    std::thread signals([&] {
      while (!done.load() && !interrupted)
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      if (interrupted)
        stop.store(true);
    });
    try {
      if (role == "gateway")
        run_gateway(config, stop, stats);
      else if (role == "router")
        run_order_router(config, stop, stats);
    } catch (...) {
      done.store(true);
      signals.join();
      throw;
    }
    done.store(true);
    signals.join();
    return 0;
  } catch (const std::exception& e) {
    std::cerr << "Error: " << e.what() << '\n';
    return 1;
  }
}
