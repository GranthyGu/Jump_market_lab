#include "matching_engine/spreads/coordinator_server.hpp"
#include "shared_data/paths.hpp"
#include "shared_data/runtime/thread.hpp"
#include <csignal>
#include <iostream>

namespace {
volatile std::sig_atomic_t interrupted = 0;

void signal_handler(int) {
  interrupted = 1;
}
}

int main(int argc, char** argv) {
  using namespace marketlab;
  std::signal(SIGINT, signal_handler);
  std::signal(SIGTERM, signal_handler);
  std::signal(SIGPIPE, SIG_IGN);
  try {
    std::string path = default_exchange_config, stats;
    bool trace = false;
    for (int i = 1; i < argc; ++i) {
      const std::string flag = argv[i];
      if (flag == "--help") {
        std::cout << "market_group_coordinator [--config PATH] [--stats PATH] [--trace]\n";
        return 0;
      }
      if (flag == "--trace") {
        trace = true;
        continue;
      }
      if (++i == argc)
        throw std::runtime_error("Missing option value");
      if (flag == "--config")
        path = argv[i];
      else if (flag == "--stats")
        stats = argv[i];
      else
        throw std::runtime_error("Unknown Coordinator option: " + flag);
    }
    auto config = orders::load_order_config(path);
    config.trace = trace;
    std::atomic_bool stop{false};
    runtime::StopThread signals([&](const std::atomic_bool& cancelled) {
      while (!cancelled.load() && !interrupted)
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      if (interrupted)
        stop.store(true);
    });
    orders::spreads::run_coordinator(config, stop, stats);
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "Coordinator failed: " << error.what() << '\n';
    return 1;
  }
}
