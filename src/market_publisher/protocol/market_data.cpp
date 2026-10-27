#include "market_publisher/protocol/market_data.hpp"

#include <algorithm>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <sstream>
#include <stdexcept>

namespace marketlab::md {
namespace {
bool ordered(const Level& a, const Level& b) {
  if (a.side != b.side) return a.side < b.side;
  return a.side == Side::Buy ? a.price > b.price : a.price < b.price;
}
void append_levels(Bytes& out, const std::vector<Level>& levels) {
  for (const auto& x : levels) {
    append_uint(out, static_cast<std::uint8_t>(x.side), 1);
    append_uint(out, static_cast<std::uint32_t>(x.price), 4);
    append_uint(out, x.quantity, 8);
    append_uint(out, x.num_orders, 8);
  }
}
std::vector<Level> read_levels(std::span<const std::uint8_t> data, std::size_t& offset, std::size_t n) {
  if (offset > data.size() || n > (data.size() - offset) / level_size)
    throw std::runtime_error("Truncated price levels");
  std::vector<Level> out;
  out.reserve(n);
  for (std::size_t i = 0; i < n; ++i) {
    const auto side = static_cast<Side>(read_uint(data, offset, 1));
    const auto price = read_uint(data, offset, 4);
    if (price > static_cast<std::uint64_t>(std::numeric_limits<std::int32_t>::max()))
      throw std::runtime_error("Price exceeds int32");
    const auto quantity = read_uint(data, offset, 8);
    const auto orders = read_uint(data, offset, 8);
    out.push_back({side, static_cast<std::int32_t>(price), quantity, orders});
  }
  return out;
}
Epoch read_epoch(std::span<const std::uint8_t> data, std::size_t& offset) {
  if (offset + 16 > data.size()) throw std::runtime_error("Truncated epoch");
  Epoch epoch{};
  std::copy_n(data.begin() + static_cast<std::ptrdiff_t>(offset), 16, epoch.begin());
  offset += 16;
  return epoch;
}
}  // namespace

void append_uint(Bytes& out, std::uint64_t value, unsigned width) {
  for (unsigned i = 0; i < width; ++i) out.push_back(static_cast<std::uint8_t>(value >> (8 * i)));
}
std::uint64_t read_uint(std::span<const std::uint8_t> data, std::size_t& offset, unsigned width) {
  if (width > 8 || offset > data.size() || width > data.size() - offset)
    throw std::runtime_error("Truncated integer");
  std::uint64_t value = 0;
  for (unsigned i = 0; i < width; ++i) value |= std::uint64_t(data[offset++]) << (8 * i);
  return value;
}
void validate_levels(const std::vector<Level>& levels, bool delta) {
  if (levels.size() > (delta ? 4 : 2) * depth) throw std::runtime_error("Too many levels");
  std::size_t buys = 0, sells = 0;
  for (std::size_t i = 0; i < levels.size(); ++i) {
    const auto& x = levels[i];
    if ((x.side != Side::Buy && x.side != Side::Sell) || x.price <= 0)
      throw std::runtime_error("Invalid side or price");
    if (!((x.quantity > 0 && x.num_orders > 0) || (delta && x.quantity == 0 && x.num_orders == 0)))
      throw std::runtime_error("Invalid aggregate values");
    if (i != 0) {
      const auto& previous = levels[i - 1];
      if (previous.side == x.side && previous.price == x.price)
        throw std::runtime_error("Duplicate price level");
      if (ordered(x, previous)) throw std::runtime_error("Unsorted levels");
    }
    x.side == Side::Buy ? ++buys : ++sells;
  }
  if (!delta && (buys > depth || sells > depth)) throw std::runtime_error("Depth exceeds five");
}
void validate_book(const Book& book) {
  if (book.symbol_id == 0) throw std::runtime_error("Symbol zero is reserved");
  validate_levels(book.levels, false);
}
std::vector<Level> difference(const Book& before, const Book& after) {
  validate_book(before);
  validate_book(after);
  if (before.symbol_id != after.symbol_id) throw std::runtime_error("Different symbols");
  if (before.levels == after.levels) return {};

  std::vector<Level> changes;
  changes.reserve(before.levels.size() + after.levels.size());
  std::size_t old_index = 0, new_index = 0;
  // Merge validated, sorted keys so changes are emitted in wire order directly.
  while (old_index < before.levels.size() && new_index < after.levels.size()) {
    const auto& old = before.levels[old_index];
    const auto& now = after.levels[new_index];
    if (old.side == now.side && old.price == now.price) {
      if (old != now) changes.push_back(now);
      ++old_index;
      ++new_index;
    } else if (ordered(old, now)) {
      changes.push_back({old.side, old.price, 0, 0});
      ++old_index;
    } else {
      changes.push_back(now);
      ++new_index;
    }
  }
  for (; old_index < before.levels.size(); ++old_index) {
    const auto& old = before.levels[old_index];
    changes.push_back({old.side, old.price, 0, 0});
  }
  for (; new_index < after.levels.size(); ++new_index) changes.push_back(after.levels[new_index]);
  return changes;
}
Bytes encode(const Message& m) {
  if ((m.kind != Kind::Delta && m.kind != Kind::Snapshot) || !m.symbol_id || !m.timestamp ||
      (m.kind == Kind::Delta && m.index == 0)) throw std::runtime_error("Invalid message metadata");
  validate_levels(m.levels, m.kind == Kind::Delta);
  Bytes out;
  out.reserve(header_size + m.levels.size() * level_size);
  append_uint(out, static_cast<std::uint8_t>(m.kind), 1);
  out.insert(out.end(), m.epoch.begin(), m.epoch.end());
  append_uint(out, m.symbol_id, 2);
  append_uint(out, m.index, 8);
  append_uint(out, m.timestamp, 8);
  append_levels(out, m.levels);
  return out;
}
Message decode(std::span<const std::uint8_t> data) {
  if (data.size() < header_size || (data.size() - header_size) % level_size)
    throw std::runtime_error("Invalid message length");
  std::size_t offset = 0;
  Message m{};
  m.kind = static_cast<Kind>(read_uint(data, offset, 1));
  m.epoch = read_epoch(data, offset);
  m.symbol_id = static_cast<std::uint16_t>(read_uint(data, offset, 2));
  m.index = read_uint(data, offset, 8);
  m.timestamp = read_uint(data, offset, 8);
  m.levels = read_levels(data, offset, (data.size() - header_size) / level_size);
  // Reuse the encoder's validation to keep both directions identical.
  (void)encode(m);
  return m;
}
Bytes encode_shard(const ShardView& view) {
  if (view.books.size() > 65535) throw std::runtime_error("Too many symbols");
  Bytes out(view.epoch.begin(), view.epoch.end());
  append_uint(out, view.books.size(), 2);
  for (const auto& [id, book] : view.books) {
    validate_book(book);
    if (id != book.symbol_id) throw std::runtime_error("Book key mismatch");
    append_uint(out, id, 2);
    append_uint(out, book.levels.size(), 1);
    append_levels(out, book.levels);
  }
  return out;
}
ShardView decode_shard(std::span<const std::uint8_t> data, const std::vector<std::uint16_t>& expected) {
  std::size_t offset = 0;
  ShardView view{read_epoch(data, offset), {}};
  const auto count = read_uint(data, offset, 2);
  if (count != expected.size()) throw std::runtime_error("Missing shard symbols");
  for (std::size_t i = 0; i < count; ++i) {
    const auto id = static_cast<std::uint16_t>(read_uint(data, offset, 2));
    const auto n = read_uint(data, offset, 1);
    if (n > 2 * depth || std::find(expected.begin(), expected.end(), id) == expected.end())
      throw std::runtime_error("Unexpected symbol or depth");
    Book book{id, read_levels(data, offset, n)};
    validate_book(book);
    if (!view.books.emplace(id, std::move(book)).second) throw std::runtime_error("Duplicate symbol");
  }
  if (offset != data.size()) throw std::runtime_error("Trailing shard bytes");
  return view;
}
std::size_t max_shard_size(std::size_t symbols) { return 18 + symbols * (3 + 2 * depth * level_size); }
Epoch new_epoch() {
  Epoch epoch{};
  std::ifstream random("/dev/urandom", std::ios::binary);
  if (!random.read(reinterpret_cast<char*>(epoch.data()), epoch.size()))
    throw std::runtime_error("Cannot read system random source");
  return epoch;
}
std::string epoch_text(const Epoch& epoch) {
  std::ostringstream out;
  for (auto x : epoch) out << std::hex << std::setw(2) << std::setfill('0') << unsigned(x);
  return out.str();
}
std::uint64_t unix_ns() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
      std::chrono::system_clock::now().time_since_epoch()).count();
}
void print_message(const Message& m) {
  std::cout << (m.kind == Kind::Snapshot ? "SNAPSHOT" : "DELTA") << " symbol=" << m.symbol_id
            << " index=" << m.index << " timestamp=" << m.timestamp << " epoch=" << epoch_text(m.epoch)
            << " entries=" << m.levels.size() << '\n';
  for (const auto& x : m.levels)
    std::cout << "  " << (x.side == Side::Buy ? "BUY " : "SELL") << " price=" << x.price
              << " quantity=" << x.quantity << " numOfOrders=" << x.num_orders << '\n';
  std::cout << std::flush;
}
}  // namespace marketlab::md
