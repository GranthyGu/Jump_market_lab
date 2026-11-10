#include "matching_engine/cluster/worker.hpp"
#include <iostream>
#include <sstream>

namespace marketlab::orders::cluster {
using namespace persistence;

WorkerControl::WorkerControl(DurableEngine& engine, std::uint16_t port, bool standby)
    : engine_(engine),
      rpc_(port),
      role_(standby ? "standby" : "active") {
  engine_.freeze_checkpoints(standby);
  std::cout << "CONTROL_LISTENING port=" << port << " role=" << role_ << std::endl;
}

void WorkerControl::poll() {
  rpc_.service([this](auto bytes) {
    return handle(bytes);
  });
}

void WorkerControl::begin_drain() {
  if (!active())
    throw std::runtime_error("Drain requires the active instance");
  role_ = "draining";
}

bool WorkerControl::finish_drain() {
  if (role_ != "draining" || !engine_.idle())
    return false;
  role_ = "fenced";
  std::cout << "ENGINE_FENCED " << status() << std::endl;
  return true;
}

Bytes WorkerControl::boundary_bytes() const {
  const auto b = engine_.boundary();
  Bytes bytes;
  for (const auto value : {b.seq, b.next_order, b.next_trade, b.trade_end, b.trade_count})
    integer(bytes, value, 8);
  return bytes;
}

std::string WorkerControl::status() const {
  const auto b = engine_.boundary();
  std::ostringstream out;
  out << b.seq << ' ' << b.next_order << ' ' << b.next_trade << ' ' << b.trade_end << ' '
      << b.trade_count << ' ' << engine_.checkpoint_sequence() << ' ' << engine_.idle() << ' '
      << engine_.checkpoints_frozen() << ' ' << role_;
  return out.str();
}

std::optional<Bytes> WorkerControl::handle(std::span<const std::uint8_t> bytes) {
  const auto newline = std::find(bytes.begin(), bytes.end(), '\n');
  const std::string line(bytes.begin(), newline);
  std::istringstream in(line);
  std::string op;
  in >> op;
  if (op == "status")
    return text_bytes(status());
  if (op == "freeze") {
    if (!active() && role_ != "fenced")
      throw std::runtime_error("Only an authoritative instance can export");
    // Set the gate BEFORE polling a previous checkpoint to completion. No new
    // rotation, snapshot or pruning is allowed until this session is released.
    engine_.freeze_checkpoints(true);
    if (!engine_.checkpoints_frozen())
      return text_bytes("WAIT");
    if (!exporting_) {
      checkpoint_ = engine_.directory() / "checkpoints" /
                    ("book-" + std::to_string(engine_.checkpoint_sequence()) + ".bin");
      File file(checkpoint_, false);
      checkpoint_size_ = file.size();
      std::uint64_t offset = 0;
      const auto frame = next_frame(file, offset);
      if (!frame || frame->kind != Kind::SnapshotMeta)
        throw std::runtime_error("Invalid export checkpoint");
      baseline_ = frame->boundary;
      Reader meta{frame->payload};
      meta.number(8);  // Configuration fingerprint is verified by recovery.
      meta.number(2);
      baseline_.trade_end = meta.number(8);
      baseline_.trade_count = meta.number(8);
      meta.finish();
      deltas_ = numbered_files(engine_.directory() / "orders", "delta-", ".log");
      segment_ = 0;
      order_offset_ = 0;
      trade_offset_ = baseline_.trade_end;
      next_ = baseline_.seq + 1;
      exporting_ = true;
      std::cout << "SYNC_FROZEN baseline=" << baseline_.seq
                << " committed=" << engine_.boundary().seq << std::endl;
    }
    return text_bytes("READY " + std::to_string(baseline_.seq) + " " +
                      std::to_string(checkpoint_size_) + " " + std::to_string(baseline_.trade_end));
  }
  if (op == "book" || op == "trades") {
    std::uint64_t offset = 0, size = 0;
    if (!exporting_ || !(in >> offset >> size) || !size || size > 1024 * 1024)
      throw std::runtime_error("Invalid export read");
    const auto limit = op == "book" ? checkpoint_size_ : baseline_.trade_end;
    if (offset > limit || size > limit - offset)
      throw std::runtime_error("Read beyond fixed export prefix");
    File file(op == "book" ? checkpoint_ : engine_.directory() / "trades/trades.log", false);
    return file.read(offset, size);
  }
  if (op == "tail") {
    std::uint64_t next = 0;
    if (!(in >> next) || !exporting_)
      throw std::runtime_error("No frozen export session");
    return tail(next);
  }
  if (op == "apply") {
    if (role_ != "standby" || newline == bytes.end())
      throw std::runtime_error("Replication requires a standby");
    if (!applying_) {
      try {
        Reader input{bytes.subspan(static_cast<std::size_t>(newline - bytes.begin()) + 1)};
        auto expected_end = engine_.boundary().trade_end;
        std::uint64_t target = engine_.boundary().seq;
        while (input.position < input.data.size()) {
          const auto order_size = input.number(4), trade_size = input.number(4);
          if (order_size > input.data.size() - input.position)
            throw std::runtime_error("Truncated replicated order frame");
          const auto order = decode_frame(input.data.subspan(input.position, order_size));
          input.position += order_size;
          if (trade_size > input.data.size() - input.position)
            throw std::runtime_error("Truncated replicated trade frame");
          std::optional<Frame> trade;
          if (trade_size)
            trade = decode_frame(input.data.subspan(input.position, trade_size));
          input.position += trade_size;
          engine_.replicate(order, trade);
          expected_end += trade_size;
          target = order.boundary.seq;
        }
        applying_ = target;
        expected_trade_end_ = expected_end;
      } catch (...) {
        role_ = "failed";  // A partially applied corrupt stream must never promote.
        throw;
      }
    }
    if (engine_.boundary().seq < *applying_)
      return std::nullopt;
    applying_.reset();
    if (engine_.boundary().trade_end != expected_trade_end_) {
      role_ = "failed";
      throw std::runtime_error("Replication changed history cursor offsets");
    }
    return text_bytes(status());
  }
  if (op == "activate") {
    const auto b = engine_.boundary();
    Boundary expected;
    if (!(in >> expected.seq >> expected.next_order >> expected.next_trade >> expected.trade_end >>
          expected.trade_count) ||
        !engine_.idle() || (role_ != "standby" && role_ != "fenced") || b.seq != expected.seq ||
        b.next_order != expected.next_order || b.next_trade != expected.next_trade ||
        b.trade_end != expected.trade_end || b.trade_count != expected.trade_count)
      throw std::runtime_error("Promotion boundary/role mismatch");
    role_ = "active";
    engine_.freeze_checkpoints(false);
    exporting_ = false;
    std::cout << "ENGINE_ACTIVATED " << status() << std::endl;
    return text_bytes(status());
  }
  if (op == "release") {
    if (!active())
      throw std::runtime_error("Release requires an active source");
    exporting_ = false;
    engine_.freeze_checkpoints(false);
    return text_bytes(status());
  }
  throw std::runtime_error("Unknown worker control command");
}

Bytes WorkerControl::tail(std::uint64_t next) {
  if (next != next_)
    throw std::runtime_error("Export cursor mismatch; reseed the standby");
  Bytes output;
  File trades(engine_.directory() / "trades/trades.log", false);
  const auto committed = engine_.boundary().seq;
  for (unsigned count = 0; next_ <= committed && count < 256 && output.size() < 512 * 1024;) {
    if (segment_ >= deltas_.size())
      throw std::runtime_error("Missing retained order generation");
    File orders(deltas_[segment_].second, false);
    const auto start = order_offset_;
    auto order = next_frame(orders, order_offset_);
    if (!order) {
      ++segment_;
      order_offset_ = 0;
      continue;
    }
    if (order->boundary.seq < next_)
      continue;
    if (order->kind != Kind::Orders || order->boundary.seq != next_)
      throw std::runtime_error("Gap in retained order log");
    Bytes trade_bytes;
    if (order->trades) {
      const auto trade_start = trade_offset_;
      const auto trade = next_frame(trades, trade_offset_);
      if (!trade || trade->boundary.seq != next_ || trade->count != order->trades)
        throw std::runtime_error("Missing committed trade frame");
      trade_bytes = trades.read(trade_start, trade_offset_ - trade_start);
    }
    const auto order_bytes = orders.read(start, order_offset_ - start);
    integer(output, order_bytes.size(), 4);
    integer(output, trade_bytes.size(), 4);
    output.insert(output.end(), order_bytes.begin(), order_bytes.end());
    output.insert(output.end(), trade_bytes.begin(), trade_bytes.end());
    ++next_;
    ++count;
  }
  return output;
}
}  // namespace marketlab::orders::cluster
