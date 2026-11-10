#include "matching_engine/cluster/router.hpp"
#include "shared_data/market_data/transport.hpp"
#include "shared_data/paths.hpp"
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
    std::string config = default_exchange_config, stats, market_routes;
    std::uint16_t port = 0;
    bool trace = false;
    for (int i = 1; i < argc; ++i) {
      const std::string flag = argv[i];
      if (flag == "--help") {
        std::cout
            << "market_engine_router --config PATH [--admin-port PORT] [--market-routes PATH]\n"
               "Start the managed A/B group with src/scripts/run_matching_engine.sh.\n";
        return 0;
      }
      if (flag == "--trace") {
        trace = true;
        continue;
      }
      if (++i == argc)
        throw std::runtime_error("Missing option value");
      const std::string value = argv[i];
      if (flag == "--config")
        config = value;
      else if (flag == "--stats")
        stats = value;
      else if (flag == "--market-routes")
        market_routes = value;
      else if (flag == "--admin-port") {
        const auto n = std::stoul(value);
        if (!n || n > 65535)
          throw std::runtime_error("Invalid admin port");
        port = n;
      } else
        throw std::runtime_error("Unknown option: " + flag);
    }
    auto c = orders::load_order_config(config);
    c.trace = trace;
    std::atomic_bool stop{false};
    md::StopThread signals([&](const auto& cancelled) {
      while (!cancelled.load() && !interrupted)
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      if (interrupted)
        stop.store(true);
    });
    orders::run_order_router(c, stop, stats, port, market_routes);
    return 0;
  } catch (const std::exception& e) {
    std::cerr << "Router failed: " << e.what() << '\n';
    return 1;
  }
}
