#include "shared_data/runtime/thread.hpp"
#include "publisher/app.hpp"
#include "shared_data/paths.hpp"
#include "publisher/publisher.hpp"
#include "publisher/demo_source.hpp"
#include "client/market_data.hpp"
#include "shared_data/console.hpp"

#include <csignal>
#include <iostream>
#include <stdexcept>
#include <thread>

namespace {
volatile std::sig_atomic_t interrupted = 0;

void on_signal(int) {
  interrupted = 1;
}

void usage() {
  std::cout
      << "Usage: market_broadcast <publish|mock|observe|query> [options]\n"
      << "  --config PATH        Configuration file (default src/config/publisher.conf)\n"
      << "  --source tcp|shm     Override the input transport of every shard\n"
      << "  --interface IPv4     Local multicast interface (publish/observe; default from config)\n"
      << "  --shard NAME         Mock one shard (default: run all configured shards)\n"
      << "  --static             Mock unchanged books\n"
      << "  --count N            Observe N datagrams, then exit (default unlimited)\n"
      << "  --raw                Observe raw packets without book reconstruction\n"
      << "  --symbol ID          Query one symbol (default 1)\n"
      << "  --host IPv4          Snapshot endpoint for query/observe (default from config)\n"
      << "  --port PORT          Snapshot TCP port for query/observe (default from config)\n";
}
}

int marketlab::md::run_publisher_cli(int argc, char** argv) {
  using namespace marketlab::md;
  std::signal(SIGINT, on_signal);
  std::signal(SIGTERM, on_signal);
  std::signal(SIGPIPE, SIG_IGN);
  if (argc < 2) {
    usage();
    return 1;
  }
  const std::string command = argv[1];
  if (command == "--help" || command == "help") {
    usage();
    return 0;
  }
  std::string path = marketlab::default_market_config, source, shard_name, multicast_interface,
              query_host;
  std::uint16_t query_port = 0;
  bool is_static = false, raw = false;
  std::size_t count = 0;
  std::uint16_t symbol = 1;
  try {
    for (int i = 2; i < argc; ++i) {
      const std::string flag = argv[i];
      if (flag == "--static") {
        is_static = true;
        continue;
      }
      if (flag == "--raw") {
        raw = true;
        continue;
      }
      if (i + 1 >= argc)
        throw std::runtime_error("Missing option value: " + flag);
      const std::string value = argv[++i];
      if (flag == "--config")
        path = value;
      else if (flag == "--source")
        source = value;
      else if (flag == "--interface")
        multicast_interface = value;
      else if (flag == "--host") {
        if (value.empty())
          throw std::runtime_error("Query host must not be empty");
        query_host = value;
      } else if (flag == "--port") {
        if (value.empty() || value.find_first_not_of("0123456789") != std::string::npos)
          throw std::runtime_error("Invalid query port");
        const auto n = std::stoul(value);
        if (!n || n > 65535)
          throw std::runtime_error("Invalid query port");
        query_port = static_cast<std::uint16_t>(n);
      } else if (flag == "--shard")
        shard_name = value;
      else if (flag == "--count")
        count = std::stoull(value);
      else if (flag == "--symbol") {
        const auto n = std::stoul(value);
        if (!n || n > 65535)
          throw std::runtime_error("Invalid symbol ID");
        symbol = static_cast<std::uint16_t>(n);
      } else
        throw std::runtime_error("Unknown option: " + flag);
    }
    auto config = load_config(path, source);
    if (!query_host.empty() || query_port) {
      if (command != "query" && command != "observe")
        throw std::runtime_error("--host and --port require query/observe");
      if (!query_host.empty())
        config.query.host = query_host;
      if (query_port)
        config.query.port = query_port;
    }
    if (!multicast_interface.empty()) {
      if (!config.multicast || (command != "publish" && command != "observe"))
        throw std::runtime_error(
            "--interface requires multicast configuration and publish/observe");
      config.multicast->interface = multicast_interface;
      validate_multicast(*config.multicast);
    }
    std::atomic_bool stop{false};
    runtime::StopThread signals([&](const std::atomic_bool& cancelled) {
      while (!cancelled.load() && !interrupted)
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
      if (interrupted)
        stop.store(true);
    });
    if (command == "publish") {
      Publisher publisher(config);
      publisher.run(stop);
    } else if (command == "mock") {
      run_demo_sources(config, shard_name, is_static, stop);
    } else if (command == "observe") {
      if (!raw)
        observe_market(config, count, stop);
      else if (config.multicast)
        observe_multicast(*config.multicast, count, stop);
      else
        observe_udp(config.receivers.front(), count, stop);
    } else if (command == "query")
      print_message(query_snapshot(config.query, symbol, config.query_timeout_ms));
    else {
      usage();
      throw std::runtime_error("Unknown command: " + command);
    }
    return 0;
  } catch (const std::exception& e) {
    std::cerr << console::red << "Error: " << e.what() << console::reset << std::endl;
    return 1;
  }
}
