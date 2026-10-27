#include "market_publisher/service.hpp"
#include "mock_matching_engine/mock_shard.hpp"
#include "client/client.hpp"

#include <csignal>
#include <iostream>
#include <stdexcept>
#include <thread>

namespace {
volatile std::sig_atomic_t interrupted = 0;
void on_signal(int) { interrupted = 1; }
void usage() {
  std::cout << "Usage: market_broadcast <publish|mock|observe|query> [options]\n"
      << "  --config PATH        Configuration file (default src/market_publisher/example.conf)\n"
      << "  --source tcp|shm     Override the input transport of every shard\n"
      << "  --shard NAME         Mock one shard (default: run all configured shards)\n"
      << "  --static             Mock unchanged books\n"
      << "  --count N            Observe N datagrams, then exit (default unlimited)\n"
      << "  --symbol ID          Query one symbol (default 1)\n";
}
}
int main(int argc, char** argv) {
  using namespace marketlab::md;
  std::signal(SIGINT, on_signal);
  std::signal(SIGTERM, on_signal);
  std::signal(SIGPIPE, SIG_IGN);
  if (argc < 2) { usage(); return 1; }
  const std::string command = argv[1];
  if (command == "--help" || command == "help") { usage(); return 0; }
  std::string path = "src/market_publisher/example.conf", source, shard_name;
  bool is_static = false;
  std::size_t count = 0;
  std::uint16_t symbol = 1;
  try {
    for (int i = 2; i < argc; ++i) {
      const std::string flag = argv[i];
      if (flag == "--static") { is_static = true; continue; }
      if (i + 1 >= argc) throw std::runtime_error("Missing option value: " + flag);
      const std::string value = argv[++i];
      if (flag == "--config") path = value;
      else if (flag == "--source") source = value;
      else if (flag == "--shard") shard_name = value;
      else if (flag == "--count") count = std::stoull(value);
      else if (flag == "--symbol") {
        const auto n = std::stoul(value);
        if (!n || n > 65535) throw std::runtime_error("Invalid symbol ID");
        symbol = static_cast<std::uint16_t>(n);
      } else throw std::runtime_error("Unknown option: " + flag);
    }
    const auto config = load_config(path, source);
    std::atomic_bool stop{false};
    StopThread signals([&](const std::atomic_bool& cancelled) {
      while (!cancelled.load() && !interrupted) std::this_thread::sleep_for(std::chrono::milliseconds(20));
      if (interrupted) stop.store(true);
    });
    if (command == "publish") {
      Publisher publisher(config);
      publisher.run(stop);
    } else if (command == "mock") {
      run_mock(config, shard_name, is_static, stop);
    } else if (command == "observe") observe_udp(config.receivers.front(), count, stop);
    else if (command == "query") print_message(query_snapshot(config.query, symbol, config.query_timeout_ms));
    else { usage(); throw std::runtime_error("Unknown command: " + command); }
    return 0;
  } catch (const std::exception& e) { std::cerr << "Error: " << e.what() << std::endl; return 1; }
}
