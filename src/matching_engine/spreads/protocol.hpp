#pragma once
#include "matching_engine/spreads/depth.hpp"
#include "matching_engine/persistence/codec.hpp"

namespace marketlab::orders::spreads {
using MessageKind = group::Kind;

struct Header {
  MessageKind kind{};
  std::uint64_t epoch = 0, seq = 0;
  std::uint32_t round = 0;
  std::uint8_t flags = 0;
};

struct FrameView {
  Header header;
  std::span<const std::uint8_t> body;
};

Bytes frame(const Header&, std::span<const std::uint8_t> body = {});
FrameView parse_frame(std::span<const std::uint8_t>);
const char* message_name(MessageKind);

struct ConnectionIdentity {
  group::Role role{};
  std::uint16_t shard = 0;
  std::uint64_t instance = 0, fingerprint = 0;
};

Bytes identity_bytes(const ConnectionIdentity&);
ConnectionIdentity read_identity(persistence::Reader&);
void put_boundary(Bytes&, const persistence::Boundary&);
persistence::Boundary read_boundary(persistence::Reader&);
void put_views(Bytes&, std::uint8_t targets, const Views&);
std::uint8_t read_views(persistence::Reader&, Views&, std::uint8_t allowed_targets);
void put_sources(
    Bytes&, std::uint16_t mask, const Sources& before, const Sources& after, bool initial);
std::uint16_t read_sources(persistence::Reader&,
                           Sources&,
                           std::uint16_t allowed_sources,
                           bool initial);
}  // namespace marketlab::orders::spreads
