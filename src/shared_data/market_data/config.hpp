#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <vector>

namespace marketlab::md {
struct ShardConfig {
  std::string name;
  std::string transport;
  std::string host;
  std::uint16_t port;
  std::string mmap_path;
  std::vector<std::uint16_t> symbols;
};
struct Endpoint { std::string host; std::uint16_t port; };
struct Config {
  std::map<std::uint16_t, std::string> symbols;
  std::vector<ShardConfig> shards;
  std::vector<Endpoint> receivers;
  Endpoint query{"127.0.0.1", 9100};
  std::uint64_t snapshot_every = 5;
  int source_timeout_ms = 800;
  int query_timeout_ms = 3000;
  int shm_max_age_ms = 1000;
  int mock_update_ms = 100;
  int mock_step_ms = 1000;
  std::size_t max_query_connections = 128;
};
Config load_config(const std::string& path, const std::string& source_override = "");
}  // namespace marketlab::md
