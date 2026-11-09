#include "matching_engine/persistence/codec.hpp"

#include <algorithm>
#include <bit>

namespace marketlab::orders::persistence {
std::uint32_t crc32c(std::span<const std::uint8_t> bytes, std::uint32_t previous) {
  static const auto table = [] {
    std::array<std::uint32_t, 256> t{};
    for (unsigned i = 0; i < 256; ++i) {
      auto c = i;
      for (unsigned j = 0; j < 8; ++j)
        c = (c >> 1) ^ ((c & 1) ? 0x82f63b78U : 0);
      t[i] = c;
    }
    return t;
  }();
  auto crc = ~previous;
  for (auto b : bytes)
    crc = table[(crc ^ b) & 255] ^ (crc >> 8);
  return ~crc;
}

void integer(Bytes& out, std::uint64_t value, unsigned width) {
  for (unsigned i = 0; i < width; ++i)
    out.push_back(static_cast<std::uint8_t>(value >> (8 * i)));
}

std::uint64_t Reader::number(unsigned width) {
  if (width > 8 || width > data.size() - position)
    throw std::runtime_error("Truncated persistence field");
  std::uint64_t v = 0;
  for (unsigned i = 0; i < width; ++i)
    v |= std::uint64_t(data[position++]) << (8 * i);
  return v;
}

Name Reader::name() {
  if (data.size() - position < 32)
    throw std::runtime_error("Truncated persistence name");
  Name n{};
  std::copy_n(data.begin() + position, 32, n.begin());
  position += 32;
  auto z = std::find(n.begin(), n.end(), 0);
  if (z == n.end() || z == n.begin() || !std::all_of(z, n.end(), [](char c) {
        return c == 0;
      }))
    throw std::runtime_error("Invalid persisted name");
  return n;
}

void Reader::finish() const {
  if (position != data.size())
    throw std::runtime_error("Unexpected persistence payload bytes");
}

namespace {
void name(Bytes& out, const char* n) {
  out.insert(out.end(), n, n + 32);
}

std::int32_t signed32(Reader& r) {
  return std::bit_cast<std::int32_t>(static_cast<std::uint32_t>(r.number(4)));
}
}  // namespace

void encode_order(Bytes& out, const StoredOrder& o) {
  const auto& s = o.state;
  integer(out, s.orderId, 8);
  name(out, s.symbol);
  integer(out, static_cast<unsigned>(s.side), 1);
  integer(out, static_cast<std::uint32_t>(s.price), 4);
  integer(out, s.originalQuantity, 4);
  integer(out, s.filledQuantity, 4);
  integer(out, s.remainingQuantity, 4);
  integer(out, static_cast<unsigned>(s.status), 1);
  name(out, o.username.data());
  integer(out, o.arrival, 8);
}

StoredOrder decode_order(Reader& r) {
  StoredOrder o;
  auto& s = o.state;
  s.orderId = r.number(8);
  const auto symbol = r.name();
  std::copy(symbol.begin(), symbol.end(), s.symbol);
  s.side = static_cast<Side>(r.number(1));
  s.price = signed32(r);
  s.originalQuantity = signed32(r);
  s.filledQuantity = signed32(r);
  s.remainingQuantity = signed32(r);
  s.status = static_cast<OrderStatus>(r.number(1));
  o.username = r.name();
  o.arrival = r.number(8);
  if (!s.orderId || !o.arrival || s.price <= 0 || s.originalQuantity <= 0 || s.filledQuantity < 0 ||
      s.remainingQuantity <= 0 ||
      std::int64_t(s.filledQuantity) + s.remainingQuantity != s.originalQuantity ||
      (s.side != Side::Buy && s.side != Side::Sell) ||
      s.status != (s.filledQuantity ? OrderStatus::PartiallyFilled : OrderStatus::Accepted))
    throw std::runtime_error("Invalid persisted active order");
  return o;
}

void encode_trade(Bytes& out, const TradeRecord& t) {
  integer(out, t.id, 8);
  integer(out, t.timestamp, 8);
  name(out, t.symbol.data());
  integer(out, static_cast<std::uint32_t>(t.price), 4);
  integer(out, t.quantity, 4);
  integer(out, t.buy_order, 8);
  integer(out, t.sell_order, 8);
  name(out, t.buyer.data());
  name(out, t.seller.data());
}

TradeRecord decode_trade(Reader& r) {
  TradeRecord t;
  t.id = r.number(8);
  t.timestamp = r.number(8);
  t.symbol = r.name();
  t.price = signed32(r);
  t.quantity = signed32(r);
  t.buy_order = r.number(8);
  t.sell_order = r.number(8);
  t.buyer = r.name();
  t.seller = r.name();
  if (!t.id || !t.timestamp || t.price <= 0 || t.quantity <= 0 || !t.buy_order || !t.sell_order ||
      t.buy_order == t.sell_order)
    throw std::runtime_error("Invalid persisted trade");
  return t;
}

void start_frame(
    Bytes& out, Kind kind, const Boundary& b, std::uint32_t trades, std::uint32_t count) {
  out.clear();
  integer(out, frame_magic, 8);
  integer(out, static_cast<unsigned>(kind), 4);
  integer(out, 0, 4);
  integer(out, b.seq, 8);
  integer(out, b.next_order, 8);
  integer(out, b.next_trade, 8);
  integer(out, trades, 4);
  integer(out, count, 4);
}

void finish_frame(Bytes& out) {
  if (out.size() < header_bytes || out.size() - header_bytes > maximum_payload)
    throw std::runtime_error("Persistence frame too large");
  const auto n = out.size() - header_bytes;
  for (unsigned i = 0; i < 4; ++i)
    out[12 + i] = static_cast<std::uint8_t>(n >> (8 * i));
  integer(out, crc32c(out), 4);
  integer(out, frame_end, 8);
}

void encode_frame(Bytes& out,
                  Kind kind,
                  const Boundary& b,
                  std::uint32_t trades,
                  std::uint32_t count,
                  std::span<const std::uint8_t> payload) {
  start_frame(out, kind, b, trades, count);
  out.insert(out.end(), payload.begin(), payload.end());
  finish_frame(out);
}

Frame decode_frame(std::span<const std::uint8_t> bytes) {
  if (bytes.size() < header_bytes + trailer_bytes)
    throw std::runtime_error("Truncated persistence frame");
  Reader r{bytes};
  if (r.number(8) != frame_magic)
    throw std::runtime_error("Invalid persistence magic/version");
  Frame f;
  f.kind = static_cast<Kind>(r.number(4));
  const auto length = r.number(4);
  if (length > maximum_payload || length + header_bytes + trailer_bytes != bytes.size())
    throw std::runtime_error("Invalid persistence length");
  f.boundary.seq = r.number(8);
  f.boundary.next_order = r.number(8);
  f.boundary.next_trade = r.number(8);
  f.trades = r.number(4);
  f.count = r.number(4);
  f.payload.assign(bytes.begin() + header_bytes, bytes.end() - trailer_bytes);
  r.position = bytes.size() - trailer_bytes;
  f.checksum = r.number(4);
  if (f.checksum != crc32c(bytes.first(bytes.size() - trailer_bytes)) || r.number(8) != frame_end)
    throw std::runtime_error("Persistence checksum/footer mismatch");
  if (!f.boundary.next_order || !f.boundary.next_trade || f.boundary.next_order > (1ULL << 48) ||
      f.boundary.next_trade > (1ULL << 48))
    throw std::runtime_error("Invalid persistence ID counters");
  return f;
}
}  // namespace marketlab::orders::persistence
