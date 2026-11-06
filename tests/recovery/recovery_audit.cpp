#include <unistd.h>

#include <filesystem>
#include <functional>
#include <iostream>
#include <string>
#include <vector>

#include "matching_engine/persistence/durable_engine.hpp"

namespace p = marketlab::orders::persistence;
namespace o = marketlab::orders;
namespace m = marketlab;
namespace fs = std::filesystem;

namespace {

void require(bool condition, const char* message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

o::OrderConfig config() {
  o::OrderConfig result;
  result.accounts = {{"Alice", "demo"}, {"Bob", "demo"}};
  result.products = {{"ITEM0001", 100, 1}};
  result.shards = {{1, {"127.0.0.1", 1}}};
  return result;
}

p::Options options(const fs::path& path) {
  p::Options result;
  result.directory = path;
  result.checkpoint_interval = std::chrono::hours(1);
  return result;
}

void submit(p::DurableEngine& engine, m::Side side, std::uint64_t request,
            m::Quantity quantity, const o::EngineBackend::Emit& emit) {
  m::NewOrderRequest order{};
  order.header = {m::MessageType::NewOrderRequest, sizeof(order), request};
  o::put_string(order.symbol, "ITEM0001");
  order.side = side;
  order.price = 100;
  order.quantity = quantity;
  const std::uint32_t user = side == m::Side::Buy ? 1 : 2;
  engine.command({0, o::LinkKind::NewOrder, request, user, 1},
                 o::record_bytes(order), emit);
}

void prepare_confirmed_fixture(const fs::path& path, bool checkpoint) {
  p::DurableEngine engine(config(), 1, options(path));
  std::size_t notifications = 0;
  const o::EngineBackend::Emit emit = [&](auto, const auto&, auto) {
    ++notifications;
  };
  submit(engine, m::Side::Sell, 1, 10, emit);
  submit(engine, m::Side::Buy, 2, 4, emit);
  engine.drain();
  require(notifications == 6 && engine.boundary().seq == 2 &&
              engine.boundary().trade_count == 1,
          "Audit fixture did not commit and emit its business notifications");
  if (checkpoint) {
    engine.request_checkpoint();
    engine.wait_checkpoint();
  }
}

void flip_byte(const fs::path& path, std::uint64_t offset) {
  p::File file(path, true);
  auto value = file.read(offset, 1);
  require(value.size() == 1, "Cannot read byte for corruption injection");
  value[0] ^= 1;
  require(pwrite(file.fd(), value.data(), value.size(), static_cast<off_t>(offset)) == 1,
          "Cannot inject corruption");
  file.sync();
}

void mismatch_order_frame(const fs::path& root) {
  p::File file(root / "orders/delta-1.log", true);
  o::Bytes rewritten;
  std::uint64_t offset = 0;
  while (auto frame = p::next_frame(file, offset)) {
    if (frame->boundary.seq == 2) {
      // Keep the order frame internally consistent and recompute its CRC;
      // the expected trade count now disagrees with the independent trade log.
      ++frame->trades;
      ++frame->boundary.next_trade;
    }
    o::Bytes encoded;
    p::encode_frame(encoded, frame->kind, frame->boundary, frame->trades,
                    frame->count, frame->payload);
    rewritten.insert(rewritten.end(), encoded.begin(), encoded.end());
  }
  file.truncate(0);
  file.append(rewritten);
  file.sync();
}

struct Scenario {
  std::string name;
  bool checkpoint;
  bool should_reject;
  std::function<void(const fs::path&)> damage;
};

}  // namespace

int main(int argc, char** argv) {
  try {
    const auto parent = fs::absolute(argc > 1 ? argv[1] : "recovery-audit-results");
    fs::create_directories(parent);
    auto pattern = (parent / "audit-XXXXXX").string();
    require(mkdtemp(pattern.data()) != nullptr, "Cannot create isolated audit directory");
    const fs::path root(pattern);

    // These faults are injected AFTER a clean commit and notification delivery.
    // Unlike an interrupted uncommitted append, they must not silently roll back
    // acknowledged work. The audit intentionally exits nonzero for such gaps.
    const std::vector<Scenario> scenarios = {
        {"healthy_baseline", false, false, [](const auto&) {}},
        {"missing_trade_file", false, true, [](const auto& path) {
           require(fs::remove(path / "trades/trades.log"), "Cannot remove fixture trade log");
         }},
        {"missing_all_delta_files", false, true, [](const auto& path) {
           require(fs::remove(path / "orders/delta-1.log"), "Cannot remove fixture delta log");
         }},
        {"missing_checkpoint_root", false, true, [](const auto& path) {
           require(fs::remove(path / "checkpoints/book-0.bin"), "Cannot remove fixture checkpoint");
         }},
        {"truncated_previously_committed_trade_tail", false, true, [](const auto& path) {
           const auto file = path / "trades/trades.log";
           p::File log(file, true);
           log.truncate(log.size() - 1);
         }},
        {"corrupt_complete_delta_frame", false, true, [](const auto& path) {
           flip_byte(path / "orders/delta-1.log", p::header_bytes + 1);
         }},
        {"mismatched_parallel_frames_with_valid_crc", false, true, mismatch_order_frame},
        {"corrupt_trade_before_checkpoint_anchor", true, true, [](const auto& path) {
           flip_byte(path / "trades/trades.log", p::header_bytes + 8);
         }},
        {"incomplete_published_checkpoint", true, true, [](const auto& path) {
           p::File log(path / "checkpoints/book-2.bin", true);
           log.truncate(log.size() - 1);
         }},
    };

    std::size_t gaps = 0;
    for (const auto& scenario : scenarios) {
      const auto path = root / scenario.name;
      prepare_confirmed_fixture(path, scenario.checkpoint);
      scenario.damage(path);
      const auto trade_path = path / "trades/trades.log";
      const auto before = fs::exists(trade_path) ? fs::file_size(trade_path) : 0;
      bool rejected = false;
      std::string error;
      p::Boundary boundary;
      try {
        p::DurableEngine recovered(config(), 1, options(path));
        boundary = recovered.boundary();
      } catch (const std::exception& failure) {
        rejected = true;
        error = failure.what();
      }
      const auto after = fs::exists(trade_path) ? fs::file_size(trade_path) : 0;
      if (scenario.should_reject != rejected) {
        ++gaps;
        std::cout << "GAP " << scenario.name << " recovered_batch=" << boundary.seq
                  << " recovered_trades=" << boundary.trade_count
                  << " trade_bytes=" << before << "->" << after;
      } else if (rejected) {
        std::cout << "REJECTED_AS_REQUIRED " << scenario.name;
      } else {
        require(boundary.seq == 2 && boundary.trade_count == 1,
                "Healthy baseline recovered wrong boundary");
        std::cout << "PASS " << scenario.name;
      }
      if (!error.empty()) {
        std::cout << " error=" << error;
      }
      std::cout << '\n';
    }
    std::cout << "AUDIT scenarios=" << scenarios.size() << " gaps=" << gaps
              << " artifacts=" << root << '\n';
    return gaps == 0 ? 0 : 1;
  } catch (const std::exception& error) {
    std::cerr << "AUDIT_ERROR " << error.what() << '\n';
    return 2;
  }
}
