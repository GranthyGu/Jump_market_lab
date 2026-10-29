#include "shared_data/orders/config.hpp"
#include <arpa/inet.h>
#include <algorithm>
#include <fstream>
#include <limits>
#include <set>
#include <sstream>
#include <stdexcept>

namespace marketlab::orders {
namespace {
std::string trim(const std::string& s) {
  const auto first = s.find_first_not_of(" \r\n\t");
  return first == std::string::npos ? "" : s.substr(first, s.find_last_not_of(" \r\n\t") - first + 1);
}
std::vector<std::string> split(const std::string& s) {
  std::vector<std::string> out;
  std::istringstream stream(s);
  std::string part;
  while (std::getline(stream, part, ',')) out.push_back(trim(part));
  return out;
}
std::uint64_t number(const std::string& s, std::uint64_t max) {
  if (s.empty() || s.find_first_not_of("0123456789") != std::string::npos) throw std::runtime_error("Invalid number");
  const auto n = std::stoull(s);
  if (!n || n > max) throw std::runtime_error("Number outside configuration range");
  return n;
}
void check_address(const Address& a) {
  in_addr ip{};
  if (!a.port || inet_pton(AF_INET, a.host.c_str(), &ip) != 1) throw std::runtime_error("Expected IPv4 address and nonzero port");
}
Address address(const std::vector<std::string>& f, std::size_t offset = 0) {
  if (f.size() != offset + 2) throw std::runtime_error("Invalid address config");
  Address a{f[offset], static_cast<std::uint16_t>(number(f[offset + 1], 65535))};
  check_address(a);
  return a;
}
}
OrderConfig load_order_config(const std::string& path) {
  std::ifstream file(path);
  if (!file) throw std::runtime_error("Cannot open order config: " + path);
  OrderConfig c;
  std::set<std::string> singles, users, symbols;
  std::set<std::uint16_t> shard_ids;
  std::set<std::pair<std::string, std::uint16_t>> shard_addresses;
  std::string line;
  while (std::getline(file, line)) {
    line = trim(line.substr(0, line.find('#')));
    if (line.empty()) continue;
    const auto eq = line.find('=');
    if (eq == std::string::npos) throw std::runtime_error("Expected key=value");
    const auto key = trim(line.substr(0, eq)), value = trim(line.substr(eq + 1));
    const auto f = split(value);
    if (key != "user" && key != "symbol" && key != "shard" && !singles.insert(key).second)
      throw std::runtime_error("Repeated config key");
    if (key == "routing") {
      if (value != "direct" && value != "router") throw std::runtime_error("routing must be direct or router");
      c.direct = value == "direct";
    }
    else if (key == "gateway_listen") c.gateway_listen = address(f);
    else if (key == "router_listen") c.router_listen = address(f);
    else if (key == "engine_endpoint") c.engine_endpoint = address(f);
    else if (key == "user") {
      if (f.size() != 2 || f[0].empty() || f[1].empty() || f[0].size() > 31 || f[1].size() > 31 || !users.insert(f[0]).second)
        throw std::runtime_error("Invalid/duplicate account");
      c.accounts.push_back({f[0], f[1]});
    } else if (key == "shard") {
      if (f.size() != 3) throw std::runtime_error("Invalid shard");
      const auto id = static_cast<std::uint16_t>(number(f[0], 65535));
      const auto a = address(f, 1);
      if (!shard_ids.insert(id).second || !shard_addresses.insert({a.host,a.port}).second)
        throw std::runtime_error("Duplicate shard ID/address");
      c.shards.push_back({id, a});
    } else if (key == "symbol") {
      if (f.size() != 3 || f[0].empty() || f[0].size() > 31 || !symbols.insert(f[0]).second)
        throw std::runtime_error("Invalid/duplicate product");
      c.products.push_back({f[0], static_cast<std::int32_t>(number(f[1], std::numeric_limits<std::int32_t>::max())),
          static_cast<std::uint16_t>(number(f[2], 65535))});
    } else {
      const auto n = number(value, 64 * 1024 * 1024);
      if (key == "max_connections") c.max_connections = n;
      else if (key == "max_pending") c.max_pending = n;
      else if (key == "max_pending_per_client") c.max_pending_per_client = n;
      else if (key == "max_buffer_bytes") c.max_buffer_bytes = n;
      else if (key == "heartbeat_ms") c.heartbeat_ms = static_cast<int>(n);
      else if (key == "link_timeout_ms") c.link_timeout_ms = static_cast<int>(n);
      else if (key == "request_timeout_ms") c.request_timeout_ms = static_cast<int>(n);
      else if (key == "partial_timeout_ms") c.partial_timeout_ms = static_cast<int>(n);
      else if (key == "reconnect_ms") c.reconnect_ms = static_cast<int>(n);
      else throw std::runtime_error("Unknown configuration key: " + key);
    }
  }
  if (c.accounts.empty() || c.accounts.size() > 1000000 || c.shards.empty() || c.products.empty() || c.products.size() > 65535 || c.heartbeat_ms * 3 >= c.link_timeout_ms ||
      c.max_buffer_bytes < 4096 || c.max_connections > 1000000 || c.max_pending > 1000000 ||
      c.max_pending_per_client > c.max_pending) throw std::runtime_error("Invalid config limits");
  for (const auto& p : c.products) if (!shard_ids.contains(p.shard)) throw std::runtime_error("Unassigned product shard");
  return c;
}
}  // namespace marketlab::orders
