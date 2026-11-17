#include "shared_data/orders/engine_link.hpp"
#include <algorithm>
#include <chrono>
#include <fstream>
#include <limits>

namespace marketlab::orders {
std::uint64_t unix_ns() {
  return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                        std::chrono::system_clock::now().time_since_epoch())
                                        .count());
}

std::uint64_t random_token_seed() {
  std::uint64_t value{};
  std::ifstream random("/dev/urandom", std::ios::binary);
  if (!random.read(reinterpret_cast<char*>(&value), sizeof(value)))
    throw std::runtime_error("Cannot read system random source");
  // Leave at least 2^32 monotonically allocated tokens before exhaustion.
  return (value & 0x7fffffff00000000ULL) | 1;
}

std::size_t message_size(MessageType type) {
  switch (type) {
    case MessageType::LoginRequest:
      return sizeof(LoginRequest);
    case MessageType::LoginAccepted:
      return sizeof(LoginAccepted);
    case MessageType::NewOrderRequest:
      return sizeof(NewOrderRequest);
    case MessageType::CancelRequest:
      return sizeof(CancelRequest);
    case MessageType::OrderUpdate:
      return sizeof(OrderUpdate);
    case MessageType::Trade:
      return sizeof(Trade);
    case MessageType::ActiveOrderSnapshot:
      return sizeof(ActiveOrderSnapshot);
    case MessageType::RequestRejected:
      return sizeof(RequestRejected);
    case MessageType::TradeHistoryRequest:
      return sizeof(TradeHistoryRequest);
    case MessageType::TradeHistoryRecord:
      return sizeof(TradeHistoryRecord);
    case MessageType::ImpliedLegHistory:
      return sizeof(ImpliedLegHistory);
    case MessageType::SpreadExecutionHistory:
      return sizeof(SpreadExecutionHistory);
    case MessageType::TradeHistoryEnd:
      return sizeof(TradeHistoryEnd);
  }
  return 0;
}

std::string fixed_string(std::span<const char> bytes) {
  const auto end = std::find(bytes.begin(), bytes.end(), '\0');
  if (end == bytes.end() || !std::all_of(end, bytes.end(), [](char c) {
        return c == 0;
      }))
    throw std::runtime_error("String must be terminated and zero padded");
  return {bytes.begin(), end};
}

void put_string(char (&destination)[32], const std::string& value) {
  if (value.size() > 31 || value.find('\0') != std::string::npos)
    throw std::runtime_error("String exceeds wire field");
  std::memset(destination, 0, 32);
  std::memcpy(destination, value.data(), value.size());
}

bool valid_client_record(std::span<const std::uint8_t> bytes, bool request) {
  if (bytes.size() < sizeof(Header))
    return false;
  Header h{};
  std::memcpy(&h, bytes.data(), sizeof(h));
  if (h.messageLength != bytes.size() || message_size(h.messageType) != bytes.size())
    return false;
  const bool is_request = h.messageType == MessageType::LoginRequest ||
                          h.messageType == MessageType::NewOrderRequest ||
                          h.messageType == MessageType::CancelRequest ||
                          h.messageType == MessageType::TradeHistoryRequest;
  return is_request == request && (!request || h.requestId != 0);
}

bool valid_link_record(std::span<const std::uint8_t> bytes) {
  if (bytes.size() < link_header_size || bytes.size() > max_link_frame)
    return false;
  const auto h = decode_link_header(bytes);
  if (h.length != bytes.size())
    return false;
  const auto payload = bytes.subspan(link_header_size);
  switch (h.kind) {
    case LinkKind::Hello:
    case LinkKind::Ready:
      return payload.size() == link_version.size() &&
             std::equal(payload.begin(), payload.end(), link_version.begin());
    case LinkKind::Ping:
    case LinkKind::Pong:
      return payload.empty();
    case LinkKind::Drain:
    case LinkKind::MaintenanceBegin:
    case LinkKind::MaintenanceEnd:
      return h.token && !h.user && !h.session && payload.empty();
    case LinkKind::Drained:
      return h.token && !h.user && !h.session && payload.size() == 40;
    case LinkKind::NewOrder:
      return h.user && h.token && h.session && payload.size() == sizeof(NewOrderRequest) &&
             valid_client_record(payload, true) &&
             copy_record<NewOrderRequest>(payload).header.messageType ==
                 MessageType::NewOrderRequest;
    case LinkKind::Cancel:
      return h.user && h.token && h.session && payload.size() == sizeof(CancelRequest) &&
             valid_client_record(payload, true) &&
             copy_record<CancelRequest>(payload).header.messageType == MessageType::CancelRequest;
    case LinkKind::Snapshot:
      return h.user && h.token && h.session && payload.size() == sizeof(Header) &&
             copy_record<Header>(payload).messageType == MessageType::ActiveOrderSnapshot &&
             copy_record<Header>(payload).messageLength == sizeof(Header) &&
             copy_record<Header>(payload).requestId;
    case LinkKind::History:
      return h.user && h.token && h.session && payload.size() == sizeof(TradeHistoryRequest) &&
             valid_client_record(payload, true) &&
             copy_record<TradeHistoryRequest>(payload).header.messageType ==
                 MessageType::TradeHistoryRequest;
    case LinkKind::Event:
    case LinkKind::SnapshotEnd:
    case LinkKind::HistoryEnd:
      return h.user && valid_client_record(payload, false);
  }
  return false;
}

Bytes link_frame(LinkKind kind,
                 std::uint64_t token,
                 std::uint32_t user,
                 std::uint64_t session,
                 std::span<const std::uint8_t> payload) {
  const WireLinkHeader h{
      static_cast<std::uint32_t>(link_header_size + payload.size()), kind, token, user, session};
  Bytes out(h.length);
  std::memcpy(out.data(), &h, sizeof(h));
  if (!payload.empty())
    std::memcpy(out.data() + sizeof(h), payload.data(), payload.size());
  return out;
}

LinkHeader decode_link_header(std::span<const std::uint8_t> bytes) {
  if (bytes.size() < link_header_size)
    throw std::runtime_error("Truncated engine header");
  WireLinkHeader wire{};
  std::memcpy(&wire, bytes.data(), sizeof(wire));
  return {wire.length, wire.kind, wire.token, wire.user, wire.session};
}

RequestRejected rejected(RequestId request, RejectReason reason) {
  auto result = server_record<RequestRejected>(MessageType::RequestRejected, request);
  result.reason = reason;
  return result;
}
}  // namespace marketlab::orders
