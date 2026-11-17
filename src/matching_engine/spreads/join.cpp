#include "matching_engine/spreads/join.hpp"
#include <algorithm>

namespace marketlab::orders::spreads {
bool same_boundary(const persistence::Boundary& a, const persistence::Boundary& b) {
  return a.seq == b.seq && a.next_order == b.next_order && a.next_trade == b.next_trade &&
         a.trade_end == b.trade_end && a.trade_count == b.trade_count;
}

bool same_recovery(const Recovery& a, const Recovery& b) {
  return a.instance == b.instance && a.indexed_end == b.indexed_end &&
         same_boundary(a.boundary, b.boundary);
}

void put_recovery(Bytes& bytes, const Recovery& value) {
  persistence::integer(bytes, value.instance, 8);
  put_boundary(bytes, value.boundary);
  persistence::integer(bytes, value.indexed_end, 8);
}

Recovery read_recovery(persistence::Reader& reader) {
  Recovery value;
  value.instance = reader.number(8);
  value.boundary = read_boundary(reader);
  value.indexed_end = reader.number(8);
  if (!value.instance || value.indexed_end != value.boundary.trade_end)
    throw std::runtime_error("Recovered instance/index is not at the full durable boundary");
  return value;
}

void put_endpoint(Bytes& bytes, const Endpoint& value) {
  if (value.address.host.empty() || value.address.host.size() > 253 || !value.address.port)
    throw std::runtime_error("Invalid group endpoint");
  persistence::integer(bytes, value.instance, 8);
  persistence::integer(bytes, value.address.port, 2);
  persistence::integer(bytes, value.address.host.size(), 1);
  bytes.insert(bytes.end(), value.address.host.begin(), value.address.host.end());
}

Endpoint read_endpoint(persistence::Reader& reader) {
  Endpoint value;
  value.instance = reader.number(8);
  value.address.port = reader.number(2);
  const auto size = reader.number(1);
  if (!size || size > 253 || !value.address.port || size > reader.data.size() - reader.position)
    throw std::runtime_error("Invalid group endpoint length/port");
  const auto first = reader.data.begin() + reader.position;
  if (std::any_of(first, first + size, [](auto byte) {
        return byte <= 32 || byte > 126;
      }))
    throw std::runtime_error("Invalid group endpoint host");
  value.address.host.assign(first, first + size);
  reader.position += size;
  return value;
}

Directory directory(const OrderConfig& config, const Topology& topology) {
  Directory result;
  for (unsigned i = 0; i < 4; ++i) {
    const auto found = std::find_if(
        config.spread_peers.begin(), config.spread_peers.end(), [&](const auto& endpoint) {
          return endpoint.id == topology.shards[i];
        });
    if (found == config.spread_peers.end())
      throw std::runtime_error("Missing group peer endpoint");
    result[i].address = found->address;
    for (unsigned j = 0; j < i; ++j)
      if (result[i].address.host == result[j].address.host &&
          result[i].address.port == result[j].address.port)
        throw std::runtime_error("Duplicate group peer endpoint");
  }
  return result;
}
}  // namespace marketlab::orders::spreads
