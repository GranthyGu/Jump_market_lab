#include "market_publisher/config.hpp"

#include <arpa/inet.h>
#include <fstream>
#include <set>
#include <sstream>
#include <stdexcept>
#include <utility>

namespace marketlab::md {
namespace {
sockaddr_in address(const std::string& host, std::uint16_t port) {
  sockaddr_in out{};
  out.sin_family = AF_INET;
  out.sin_port = htons(port);
  if (inet_pton(AF_INET, host.c_str(), &out.sin_addr) != 1)
    throw std::runtime_error("Expected numeric IPv4 address: " + host);
  return out;
}
std::vector<std::string> split(const std::string& text, char separator) {
  std::vector<std::string> parts;
  std::istringstream in(text);
  std::string part;
  while (std::getline(in, part, separator)) parts.push_back(part);
  return parts;
}
std::string trim(std::string text) {
  const auto first = text.find_first_not_of(" \t\r\n");
  if (first == std::string::npos) return {};
  return text.substr(first, text.find_last_not_of(" \t\r\n") - first + 1);
}
unsigned long long number(const std::string& text) {
  if (text.empty() || text.find_first_not_of("0123456789") != std::string::npos)
    throw std::runtime_error("Expected unsigned integer: " + text);
  return std::stoull(text);
}
std::uint16_t identifier(const std::string& text) {
  const auto value = number(text);
  if (!value || value > 65535) throw std::runtime_error("ID/port outside 1..65535");
  return static_cast<std::uint16_t>(value);
}

}  // namespace

Config load_config(const std::string& path, const std::string& source_override) {
  std::ifstream input(path);
  if (!input) throw std::runtime_error("Cannot open config: " + path);
  Config cfg;
  std::string line;
  std::set<std::string> singular;
  while (std::getline(input, line)) {
    line = trim(line.substr(0, line.find('#')));
    if (line.empty()) continue;
    const auto eq = line.find('=');
    if (eq == std::string::npos) throw std::runtime_error("Expected key=value");
    const auto key = trim(line.substr(0, eq));
    const auto value = trim(line.substr(eq + 1));
    auto fields = split(value, ',');
    for (auto& field : fields) field = trim(field);
    if (key != "shard" && key != "udp_receiver" && !singular.insert(key).second)
      throw std::runtime_error("Repeated setting: " + key);
    if (key == "symbols") {
      for (const auto& field : fields) {
        const auto pair = split(field, ':');
        if (pair.size() != 2 || pair[1].empty() || !cfg.symbols.emplace(identifier(pair[0]), pair[1]).second)
          throw std::runtime_error("Invalid or duplicate symbol");
      }
    } else if (key == "shard") {
      if (fields.size() < 6) throw std::runtime_error("Invalid shard config");
      ShardConfig shard{fields[0], source_override.empty() ? fields[1] : source_override,
                        fields[2], identifier(fields[3]), fields[4], {}};
      for (std::size_t i = 5; i < fields.size(); ++i) shard.symbols.push_back(identifier(fields[i]));
      cfg.shards.push_back(std::move(shard));
    } else if (key == "udp_receiver") {
      if (fields.size() != 2) throw std::runtime_error("Invalid UDP destination");
      cfg.receivers.push_back({fields[0], identifier(fields[1])});
    } else if (key == "query_host") cfg.query.host = value;
    else if (key == "query_port") cfg.query.port = identifier(value);
    else if (key == "snapshot_every") cfg.snapshot_every = number(value);
    else {
      const auto v = number(value);
      if (!v || v > 1'000'000) throw std::runtime_error("Setting outside safe range: " + key);
      if (key == "source_timeout_ms") cfg.source_timeout_ms = static_cast<int>(v);
      else if (key == "query_timeout_ms") cfg.query_timeout_ms = static_cast<int>(v);
      else if (key == "shm_max_age_ms") cfg.shm_max_age_ms = static_cast<int>(v);
      else if (key == "mock_update_ms") cfg.mock_update_ms = static_cast<int>(v);
      else if (key == "mock_step_ms") cfg.mock_step_ms = static_cast<int>(v);
      else if (key == "max_query_connections") cfg.max_query_connections = v;
      else throw std::runtime_error("Unknown setting: " + key);
    }
  }
  if (cfg.symbols.empty() || cfg.shards.empty() || cfg.receivers.empty() || cfg.snapshot_every < 5 || cfg.source_timeout_ms >= 1000)
    throw std::runtime_error("Missing config, snapshot period <5, or source timeout >=1000ms");
  std::set<std::uint16_t> owned;
  std::set<std::string> names, paths, symbol_names;
  for (const auto& [id, name] : cfg.symbols) {
    (void)id;
    if (!symbol_names.insert(name).second) throw std::runtime_error("Duplicate symbol name");
  }
  for (const auto& shard : cfg.shards) {
    (void)address(shard.host, shard.port);
    if (shard.name.empty() || shard.mmap_path.empty() || !names.insert(shard.name).second || !paths.insert(shard.mmap_path).second ||
        (shard.transport != "tcp" && shard.transport != "shm")) throw std::runtime_error("Invalid shard name, path or transport");
    for (auto id : shard.symbols)
      if (!cfg.symbols.contains(id) || !owned.insert(id).second) throw std::runtime_error("Invalid symbol ownership");
  }
  if (owned.size() != cfg.symbols.size()) throw std::runtime_error("Unassigned symbols");
  for (const auto& receiver : cfg.receivers) (void)address(receiver.host, receiver.port);
  (void)address(cfg.query.host, cfg.query.port);
  return cfg;
}
}  // namespace marketlab::md
