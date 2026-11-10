#pragma once

#include "shared_data/orders/protocol.h"
#include <array>
#include <cstdint>
#include <cstring>
#include <span>
#include <stdexcept>
#include <string>
#include <vector>

namespace marketlab::orders {
using Bytes = std::vector<std::uint8_t>;
using ConnectionId = std::uint64_t;
// This envelope carries every gateway/engine command and event. Client records
// remain exactly those in protocol.h. Passwords never enter the engine link.
enum class LinkKind : std::uint16_t {
  Hello = 1,
  Ready = 2,
  Ping = 3,
  Pong = 4,
  NewOrder = 10,
  Cancel = 11,
  Snapshot = 12,
  History = 13,
  Event = 20,
  SnapshotEnd = 21,
  HistoryEnd = 22,
  Drain = 30,
  Drained = 31,
  MaintenanceBegin = 32,
  MaintenanceEnd = 33
};
#pragma pack(push, 1)

struct WireLinkHeader {
  std::uint32_t length;
  LinkKind kind;
  std::uint64_t token;
  std::uint32_t user;
  std::uint64_t session;
};

#pragma pack(pop)
static_assert(sizeof(WireLinkHeader) == 26);
inline constexpr std::size_t link_header_size = sizeof(WireLinkHeader);

// Native metadata is deliberately separate: packed fields must never be bound
// to references by unordered_map/find/emplace or other standard algorithms.
struct LinkHeader {
  std::uint32_t length;
  LinkKind kind;
  std::uint64_t token;
  std::uint32_t user;
  std::uint64_t session;
};

LinkHeader decode_link_header(std::span<const std::uint8_t> bytes);
inline constexpr std::array<std::uint8_t, 8> link_version{'M', 'L', 'G', 'W', 2, 0, 0, 0};
inline constexpr std::size_t max_client_frame = sizeof(TradeHistoryRecord);
inline constexpr std::size_t max_link_frame = link_header_size + max_client_frame;

std::uint64_t unix_ns();
std::uint64_t random_token_seed();
std::size_t message_size(MessageType type);
bool valid_client_record(std::span<const std::uint8_t> bytes, bool request);
bool valid_link_record(std::span<const std::uint8_t> bytes);
std::string fixed_string(std::span<const char> bytes);
void put_string(char (&destination)[32], const std::string& value);

template <typename T>
T copy_record(std::span<const std::uint8_t> bytes) {
  if (bytes.size() != sizeof(T))
    throw std::runtime_error("Wrong record size");
  T value{};
  std::memcpy(&value, bytes.data(), sizeof(T));
  return value;
}

template <typename T>
Bytes record_bytes(const T& value) {
  Bytes bytes(sizeof(T));
  std::memcpy(bytes.data(), &value, sizeof(T));
  return bytes;
}

template <typename T>
T server_record(MessageType type, RequestId request) {
  T value{};
  value.header = {type, sizeof(T), request};
  value.timestamp = unix_ns();
  return value;
}

Bytes link_frame(LinkKind kind,
                 std::uint64_t token = 0,
                 std::uint32_t user = 0,
                 std::uint64_t session = 0,
                 std::span<const std::uint8_t> payload = {});

template <typename T>
Bytes link_record(LinkKind kind, const LinkHeader& context, const T& payload) {
  const auto bytes = record_bytes(payload);
  return link_frame(kind, context.token, context.user, context.session, bytes);
}

RequestRejected rejected(RequestId request, RejectReason reason);
}  // namespace marketlab::orders
