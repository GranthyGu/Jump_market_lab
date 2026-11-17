#include "matching_engine/spreads/protocol.hpp"
#include <bit>

namespace marketlab::orders::spreads {
using persistence::integer;
using persistence::Reader;

Bytes frame(const Header& h, std::span<const std::uint8_t> body) {
  if (body.size() > group::maximum_frame - group::header_size)
    throw std::runtime_error("Group frame exceeds part limit");
  Bytes bytes(group::header_size + body.size());
  const group::WireHeader wire{
      static_cast<std::uint32_t>(bytes.size()), h.kind, h.flags, h.epoch, h.seq, h.round};
  std::memcpy(bytes.data(), &wire, sizeof(wire));
  std::copy(body.begin(), body.end(), bytes.begin() + group::header_size);
  if (!group::valid_frame(bytes))
    throw std::runtime_error("Invalid outgoing group frame");
  return bytes;
}

FrameView parse_frame(std::span<const std::uint8_t> bytes) {
  if (!group::valid_frame(bytes))
    throw std::runtime_error("Invalid group frame header");
  const auto h = copy_record<group::WireHeader>(bytes.first(group::header_size));
  return {{h.kind, h.epoch, h.seq, h.round, h.flags}, bytes.subspan(group::header_size)};
}

const char* message_name(MessageKind kind) {
  static constexpr const char* names[] = {"INVALID",        "HELLO",
                                          "WELCOME",        "PING",
                                          "PONG",           "RECOVERED",
                                          "TOPOLOGY",       "PEERS_READY",
                                          "INITIAL_DEPTH",  "IMPLIED_REPLACE",
                                          "VIEW_APPLIED",   "GROUP_READY",
                                          "GROUP_PAUSE",    "RUN",
                                          "ROUND_BEGIN",    "EXECUTE_PREFIX",
                                          "SOURCE_DONE",    "EXECUTION_DONE",
                                          "RESULT_DETAILS", "ALLOCATIONS",
                                          "ROUND_READY",    "COMMAND_END",
                                          "END_ACK",        "LOCAL_COMPLETE",
                                          "MEM_DONE",       "DURABLE_RESULT",
                                          "PUBLISH",        "CAPTURE",
                                          "PUBLIC_HISTORY", "FENCE",
                                          "FENCED"};
  const auto n = static_cast<unsigned>(kind);
  return n < std::size(names) ? names[n] : names[0];
}

Bytes identity_bytes(const ConnectionIdentity& value) {
  Bytes bytes;
  bytes.reserve(21);
  integer(bytes, group::version, 2);
  integer(bytes, static_cast<unsigned>(value.role), 1);
  integer(bytes, value.shard, 2);
  integer(bytes, value.instance, 8);
  integer(bytes, value.fingerprint, 8);
  return bytes;
}

ConnectionIdentity read_identity(Reader& r) {
  if (r.number(2) != group::version)
    throw std::runtime_error("Group protocol version mismatch");
  ConnectionIdentity identity{static_cast<group::Role>(r.number(1)),
                              static_cast<std::uint16_t>(r.number(2)),
                              r.number(8),
                              r.number(8)};
  if (identity.role < group::Role::Router || identity.role > group::Role::Peer ||
      !identity.instance || ((identity.role == group::Role::Peer) != bool(identity.shard)))
    throw std::runtime_error("Invalid group connection identity");
  return identity;
}

void put_boundary(Bytes& out, const persistence::Boundary& b) {
  for (const auto value : {b.seq, b.next_order, b.next_trade, b.trade_end, b.trade_count})
    integer(out, value, 8);
}

persistence::Boundary read_boundary(Reader& r) {
  persistence::Boundary b{r.number(8), r.number(8), r.number(8), r.number(8), r.number(8)};
  if (!b.next_order || !b.next_trade || b.next_order > (1ULL << 48) ||
      b.next_trade > (1ULL << 48) || b.trade_count != b.next_trade - 1 ||
      bool(b.trade_count) != bool(b.trade_end))
    throw std::runtime_error("Invalid recovered/durable boundary");
  return b;
}

void put_views(Bytes& out, std::uint8_t targets, const Views& views) {
  if (!targets || (targets & ~31U))
    throw std::runtime_error("Invalid implied target mask");
  integer(out, targets, 1);
  for (unsigned i = 0; i < routes.size(); ++i) {
    if (!(targets & (1U << routes[i].target)))
      continue;
    const auto& view = views[i];
    integer(out, i, 1);
    integer(out, view.count, 1);
    integer(out, view.more, 1);
    for (unsigned j = 0; j < view.count; ++j) {
      integer(out, static_cast<std::uint32_t>(view.price[j]), 4);
      integer(out, view.quantity[j], 8);
    }
  }
}

std::uint8_t read_views(Reader& r, Views& views, std::uint8_t allowed) {
  const auto targets = r.number(1);
  if (!targets || (targets & ~allowed))
    throw std::runtime_error("Implied replacement targets another owner");
  auto next = views;
  for (unsigned i = 0; i < routes.size(); ++i) {
    if (!(targets & (1U << routes[i].target)))
      continue;
    if (r.number(1) != i)
      throw std::runtime_error("Missing/duplicate implied route");
    Implied view;
    const auto count = r.number(1), more = r.number(1);
    if (count > route_depth || more > 1 || (!count && more))
      throw std::runtime_error("Invalid implied count/more");
    view.count = count;
    view.more = more;
    std::uint64_t total = 0;
    for (unsigned j = 0; j < count; ++j) {
      view.price[j] = std::bit_cast<std::int32_t>(static_cast<std::uint32_t>(r.number(4)));
      view.quantity[j] = r.number(8);
      if (!view.quantity[j] || view.quantity[j] > UINT64_MAX - total ||
          (j && (routes[i].side == Bid ? view.price[j] >= view.price[j - 1]
                                       : view.price[j] <= view.price[j - 1])))
        throw std::runtime_error("Invalid implied ordering/quantity");
      total += view.quantity[j];
    }
    next[i] = view;
  }
  views = next;
  return targets;
}

void put_sources(
    Bytes& out, std::uint16_t mask, const Sources& before, const Sources& after, bool initial) {
  if (mask & ~1023U)
    throw std::runtime_error("Invalid source mask");
  integer(out, std::popcount(mask), 1);
  for (std::uint8_t source = 0; source < 10; ++source) {
    if (!(mask & (1U << source)))
      continue;
    const auto length_at = out.size();
    integer(out, 0, 1);
    encode_depth(out, source, before[source], after[source], initial);
    out[length_at] = static_cast<std::uint8_t>(out.size() - length_at - 1);
  }
}

std::uint16_t read_sources(Reader& r, Sources& sources, std::uint16_t allowed, bool initial) {
  const auto count = r.number(1);
  if (count > 10)
    throw std::runtime_error("Invalid source-side count");
  auto next = sources;
  std::uint16_t seen = 0;
  for (unsigned i = 0; i < count; ++i) {
    const auto length = r.number(1);
    if (length > r.data.size() - r.position)
      throw std::runtime_error("Truncated source update");
    const auto source = apply_depth(r.data.subspan(r.position, length), next, initial);
    if (!(allowed & (1U << source)) || (seen & (1U << source)))
      throw std::runtime_error("Duplicate/foreign source update");
    seen |= 1U << source;
    r.position += length;
  }
  sources = next;
  return seen;
}
}  // namespace marketlab::orders::spreads
