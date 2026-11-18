#pragma once

#include <cstdint>
#include <cstring>
#include <span>

namespace marketlab::orders::group {
inline constexpr std::uint16_t version = 2;
inline constexpr std::size_t header_size = 26, maximum_frame = 64 * 1024;
inline constexpr std::uint8_t final_part = 1, initial_view = 2;

enum class Kind : std::uint8_t {
  Hello = 1,
  Welcome,
  Ping,
  Pong,
  Recovered,
  Topology,
  PeersReady,
  InitialDepth,
  ImpliedReplace,
  ViewApplied,
  Ready,
  Pause,
  Run,
  RoundBegin,
  ExecutePrefix,
  SourceDone,
  ExecutionDone,
  ResultDetails,
  Allocations,
  RoundReady,
  CommandEnd,
  EndAck,
  LocalComplete,
  MemDone,
  DurableResult,
  Publish,
  Capture,
  PublicHistory,
  Fence,
  Fenced
};

enum class Role : std::uint8_t {
  Router = 1,
  Coordinator,
  Peer
};

#pragma pack(push, 1)

struct WireHeader {
  std::uint32_t length;
  Kind kind;
  std::uint8_t flags;
  std::uint64_t epoch, seq;
  std::uint32_t round;
};

#pragma pack(pop)
static_assert(sizeof(WireHeader) == header_size);

// This check is shared by framing and the business envelope. Role, instance,
// epoch and state-machine preconditions are validated by the owning endpoint.
inline bool valid_header(const WireHeader& h, std::size_t frame_size) {
  return frame_size >= header_size && frame_size <= maximum_frame && h.length == frame_size &&
         h.kind >= Kind::Hello && h.kind <= Kind::Fenced &&
         !(h.flags & ~(final_part | initial_view));
}

inline bool valid_frame(std::span<const std::uint8_t> bytes) {
  if (bytes.size() < header_size || bytes.size() > maximum_frame)
    return false;
  WireHeader h{};
  std::memcpy(&h, bytes.data(), sizeof(h));
  return valid_header(h, bytes.size());
}
}  // namespace marketlab::orders::group
