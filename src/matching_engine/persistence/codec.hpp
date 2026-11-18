#pragma once
#include "matching_engine/persistence/model.hpp"

namespace marketlab::orders::persistence {
inline constexpr std::uint64_t frame_magic = 0x32474f4c424c4dULL;
inline constexpr std::uint64_t frame_end = 0x32444e45424c4dULL;
inline constexpr std::size_t header_bytes = 48, trailer_bytes = 12,
                             maximum_payload = 64 * 1024 * 1024;
inline constexpr std::size_t stored_order_bytes = 98, stored_trade_bytes = 136;
enum class Kind : std::uint32_t {
  Trades = 1,
  Orders = 2,
  SnapshotMeta = 3,
  SnapshotPage = 4,
  SnapshotEnd = 5,
  TradesV2 = 6
};

struct Frame {
  Kind kind{};
  Boundary boundary{};
  std::uint32_t trades = 0, count = 0;
  Bytes payload;
  std::uint64_t start = 0, end = 0;
  std::uint32_t checksum = 0;
};

std::uint32_t crc32c(std::span<const std::uint8_t>, std::uint32_t previous = 0);
void integer(Bytes&, std::uint64_t, unsigned);

struct Reader {
  std::span<const std::uint8_t> data;
  std::size_t position = 0;
  std::uint64_t number(unsigned);
  Name name();
  void finish() const;
};

void encode_order(Bytes&, const StoredOrder&);
StoredOrder decode_order(Reader&, bool signed_price = false);
void encode_trade(Bytes&, const TradeRecord&);
TradeRecord decode_trade(Reader&, bool signed_price = false);
inline constexpr std::size_t execution_prefix_bytes = 3, maximum_execution_bytes = 285;
std::size_t execution_bytes(TradeType);
void encode_execution(Bytes&, const ExecutionRecord&);
ExecutionRecord decode_execution(Reader&);
ExecutionRecord decode_execution_body(Reader&, TradeType);

struct DecodedExecution {
  ExecutionRecord record;
  std::size_t offset, length;
  TradeType type;
};

std::vector<DecodedExecution> decode_trades(const Frame&);
void start_frame(Bytes&, Kind, const Boundary&, std::uint32_t trades, std::uint32_t count);
void finish_frame(Bytes&);
void encode_frame(Bytes&,
                  Kind,
                  const Boundary&,
                  std::uint32_t trades,
                  std::uint32_t count,
                  std::span<const std::uint8_t> payload);
Frame decode_frame(std::span<const std::uint8_t>);
}  // namespace marketlab::orders::persistence
