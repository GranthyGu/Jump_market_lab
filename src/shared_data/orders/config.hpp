#pragma once
#include <cstdint>
#include <array>
#include <string>
#include <vector>

namespace marketlab::orders {
inline constexpr std::uint16_t default_gateway_port = 7001;

struct Address {
  std::string host;
  std::uint16_t port;
};

struct Account {
  std::string name, password;
};

struct EngineShard {
  std::uint16_t id;
  Address address;
};

struct Product {
  std::string symbol;
  std::int32_t previous_close;
  std::uint16_t shard;
};

struct OrderConfig {
  bool direct = true;
  Address gateway_listen{"127.0.0.1", default_gateway_port};
  Address router_listen{"127.0.0.1", 7100};
  Address engine_endpoint{"127.0.0.1", 7100};
  std::vector<Account> accounts;
  std::vector<EngineShard> shards;
  std::vector<Product> products;
  bool spreads = false;
  Address coordinator{"127.0.0.1", 7120};
  std::array<std::string, 6> spread_symbols{"A", "B", "C", "SP_AB", "SP_AC", "BF_ABC"};
  std::vector<EngineShard> spread_peers;
  std::size_t max_connections = 4096;
  std::size_t max_pending = 65536;
  std::size_t max_pending_per_client = 1024;
  std::size_t max_buffer_bytes = 4 * 1024 * 1024;
  int heartbeat_ms = 500;
  int link_timeout_ms = 5000;
  int request_timeout_ms = 5000;
  int partial_timeout_ms = 2000;
  int reconnect_ms = 200;
  bool trace = false;
};

OrderConfig load_order_config(const std::string& path);

struct PriceLimits {
  std::int64_t lower, upper;
};

PriceLimits product_limits(const OrderConfig&, std::size_t product);
}  // namespace marketlab::orders
