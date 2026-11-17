#pragma once

#include <array>
#include <cstdint>
#include <map>
#include <span>
#include <string>
#include <vector>

namespace marketlab::md {
using Bytes = std::vector<std::uint8_t>;
using Epoch = std::array<std::uint8_t, 16>;
inline constexpr std::size_t depth = 5;
inline constexpr std::size_t header_size = 35;
inline constexpr std::size_t level_size = 21;
inline constexpr std::size_t level_v2_size = 29;
inline constexpr std::size_t max_snapshot_size = header_size + 2 * depth * level_v2_size;
inline constexpr std::size_t max_delta_size = header_size + 4 * depth * level_v2_size;
inline constexpr std::uint8_t shard_query = 1;
enum class Kind : std::uint8_t {
  Delta = 1,
  Snapshot = 2,
  DeltaV2 = 3,
  SnapshotV2 = 4
};

constexpr bool is_delta(Kind kind) {
  return kind == Kind::Delta || kind == Kind::DeltaV2;
}

constexpr bool is_snapshot(Kind kind) {
  return kind == Kind::Snapshot || kind == Kind::SnapshotV2;
}

constexpr bool is_v2(Kind kind) {
  return kind == Kind::DeltaV2 || kind == Kind::SnapshotV2;
}
enum class Side : std::uint8_t {
  Buy = 1,
  Sell = 2
};

struct Level {
  Side side;
  std::int32_t price;
  std::uint64_t quantity;
  std::uint64_t num_orders;
  std::uint64_t implied_quantity = 0;
  bool operator==(const Level&) const = default;
};

struct Book {
  std::uint16_t symbol_id;
  std::vector<Level> levels;
  bool extended = false;
  bool operator==(const Book&) const = default;
};

struct Message {
  Kind kind;
  Epoch epoch;
  std::uint16_t symbol_id;
  std::uint64_t index;
  std::uint64_t timestamp;
  std::vector<Level> levels;
};

struct ShardView {
  Epoch epoch;
  std::map<std::uint16_t, Book> books;
};

// Serialization is explicit: native struct layout and host byte order are irrelevant.
void append_uint(Bytes& out, std::uint64_t value, unsigned width);
std::uint64_t read_uint(std::span<const std::uint8_t> data, std::size_t& offset, unsigned width);
void validate_book(const Book& book);
void validate_levels(const std::vector<Level>& levels, bool delta, bool extended = false);
std::vector<Level> difference(const Book& before, const Book& after);
Bytes encode(const Message& message);
Message decode(std::span<const std::uint8_t> data);
Bytes encode_shard(const ShardView& view);
ShardView decode_shard(std::span<const std::uint8_t> data,
                       const std::vector<std::uint16_t>& expected);
std::size_t max_shard_size(std::size_t symbols);
Epoch new_epoch();
std::string epoch_text(const Epoch& epoch);
std::uint64_t unix_ns();
void print_message(const Message& message);
void print_level(const Level&);
}  // namespace marketlab::md
