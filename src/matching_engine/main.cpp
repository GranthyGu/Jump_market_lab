#include <csignal>
#include <filesystem>
#include <iostream>

#include "matching_engine/server.hpp"
#include "shared_data/market_data/transport.hpp"
#include "shared_data/paths.hpp"

namespace {
volatile std::sig_atomic_t interrupted = 0;

void on_signal(int) {
  interrupted = 1;
}

std::uint16_t number(const std::string& text) {
  if (text.empty() || text.find_first_not_of("0123456789") != std::string::npos)
    throw std::runtime_error("Expected a numeric shard/port");
  const auto value = std::stoul(text);
  if (!value || value > 65535)
    throw std::runtime_error("Shard/port outside 1..65535");
  return static_cast<std::uint16_t>(value);
}
}  // namespace

int main(int argc, char** argv) {
  using namespace marketlab;
  using namespace marketlab::orders;
  std::signal(SIGINT, on_signal);
  std::signal(SIGTERM, on_signal);
  std::signal(SIGPIPE, SIG_IGN);
  try {
    if (argc == 1 || std::string(argv[1]) == "--help") {
      std::cout
          << "market_matching_engine --shard ID [options]\n"
          << "  --config PATH       Shared deployment config; default src/config/exchange.conf\n"
          << "  --market-port PORT  Enable publisher TCP source on this port\n"
          << "  --market-shm PATH   Enable local shared-memory market view\n"
          << "  --control-port PORT  TCP replication/control endpoint (loopback)\n"
          << "  --standby           Accept replicated results until explicit promotion\n"
          << "  --stats PATH        Write counters on graceful shutdown\n"
          << "  --data-dir PATH     Persistent data root (each shard has its own subdirectory)\n"
          << "  --checkpoint-ms N   Checkpoint interval; default 600000\n"
          << "  --pending-mib N     In-flight result budget; default 64 MiB\n"
          << "  --history-cache-mib N       B+ tree page cache per shard; default 8 MiB\n"
          << "  --history-checkpoint-mib N  New trade-log bytes between index checkpoints; default 64 MiB\n"
          << "  --volatile          Disable persistence for isolated baseline tests\n"
          << "  --trace             Log internal message metadata\n";
      return argc == 1 ? 1 : 0;
    }
    std::string path = marketlab::default_exchange_config, stats, mmap_path, data_dir;
    orders::persistence::Options persistence_options;
    bool volatile_mode = false, standby = false;
    std::uint16_t shard = 0, market_port = 0, control_port = 0;
    bool trace = false;
    for (int i = 1; i < argc; ++i) {
      const std::string flag = argv[i];
      if (flag == "--standby") {
        standby = true;
        continue;
      }
      if (flag == "--volatile") {
        volatile_mode = true;
        continue;
      }
      if (flag == "--trace") {
        trace = true;
        continue;
      }
      if (++i == argc)
        throw std::runtime_error("Missing option value");
      const std::string value = argv[i];
      if (flag == "--shard")
        shard = number(value);
      else if (flag == "--config")
        path = value;
      else if (flag == "--market-port")
        market_port = number(value);
      else if (flag == "--control-port")
        control_port = number(value);
      else if (flag == "--market-shm")
        mmap_path = value;
      else if (flag == "--data-dir")
        data_dir = value;
      else if (flag == "--checkpoint-ms" || flag == "--pending-mib" ||
               flag == "--history-cache-mib" || flag == "--history-checkpoint-mib") {
        if (value.empty() || value.find_first_not_of("0123456789") != std::string::npos)
          throw std::runtime_error("Expected positive integer");
        const auto n = std::stoull(value);
        if (!n || n > 86400000)
          throw std::runtime_error("Persistence option outside allowed range");
        if (flag == "--checkpoint-ms")
          persistence_options.checkpoint_interval = std::chrono::milliseconds(n);
        else if (flag == "--history-cache-mib") {
          if (n > 4096)
            throw std::runtime_error("History cache limited to 4096 MiB");
          persistence_options.history_cache_pages = n * 128;
        } else if (flag == "--history-checkpoint-mib") {
          if (n > 4096)
            throw std::runtime_error("History checkpoint threshold limited to 4096 MiB");
          persistence_options.history_checkpoint_bytes = n * 1024 * 1024;
        } else {
          if (n > 4096)
            throw std::runtime_error("Pending buffer limited to 4096 MiB");
          persistence_options.pending_bytes = n * 1024 * 1024;
          persistence_options.command_bytes =
              std::min(persistence_options.command_bytes, persistence_options.pending_bytes);
        }
      } else if (flag == "--stats")
        stats = value;
      else
        throw std::runtime_error("Unknown option: " + flag);
    }
    if (!shard)
      throw std::runtime_error("--shard ID is required");
    auto config = load_order_config(path);
    config.trace = trace;
    std::atomic_bool stop{false};
    md::StopThread signals([&](const std::atomic_bool& cancelled) {
      while (!cancelled.load() && !interrupted)
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      if (interrupted)
        stop.store(true);
    });
    if (!volatile_mode) {
      const auto config_file = std::filesystem::absolute(path);
      const auto root = data_dir.empty()
                            ? config_file.parent_path() / ".marketlab-data" / config_file.stem()
                            : std::filesystem::absolute(data_dir);
      persistence_options.directory = root / ("shard-" + std::to_string(shard));
    }
    run_order_shard(config,
                    shard,
                    stop,
                    stats,
                    market_port,
                    mmap_path,
                    persistence_options,
                    control_port,
                    standby);
    return 0;
  } catch (const std::exception& e) {
    std::cerr << "Matching engine failed: " << e.what() << '\n';
    return 1;
  }
}
