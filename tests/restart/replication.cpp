#include "matching_engine/persistence/durable_engine.hpp"

#include <future>
#include <iostream>
#include <unistd.h>

namespace o = marketlab::orders;
namespace p = marketlab::orders::persistence;
namespace m = marketlab;
using namespace std::chrono_literals;

namespace {
void require(bool value, const char* message) {
  if (!value)
    throw std::runtime_error(message);
}

template <class F>
void rejects(F fn, const char* message) {
  bool rejected = false;
  try {
    fn();
  } catch (const std::runtime_error&) {
    rejected = true;
  }
  require(rejected, message);
}

std::pair<p::Frame, std::optional<p::Frame>> frames(const p::Result& result,
                                                    p::Boundary boundary,
                                                    const p::OrderDictionary& dictionary) {
  o::Bytes orders, trades;
  p::start_frame(orders, p::Kind::Orders, boundary, result.trades.size(), result.changes.size());
  for (const auto& change : result.changes) {
    p::integer(orders, change.erase ? 0 : 1, 1);
    if (change.erase)
      p::integer(orders, change.order.id, 8);
    else
      p::encode_order(orders, dictionary.expand(change.order));
  }
  p::finish_frame(orders);
  std::optional<p::Frame> trade;
  if (!result.trades.empty()) {
    p::start_frame(trades, p::Kind::Trades, boundary, result.trades.size(), result.trades.size());
    for (const auto& record : result.trades)
      p::encode_trade(trades, record);
    p::finish_frame(trades);
    trade = p::decode_frame(trades);
  }
  return {p::decode_frame(orders), std::move(trade)};
}

void same_book(o::EngineBackend& left, o::EngineBackend& right) {
  require(left.market_view().books == right.market_view().books,
          "replicated book differs from authoritative book");
}
}  // namespace

int main(int argc, char** argv) {
  try {
    if (argc != 2)
      throw std::runtime_error("expected isolated output directory");
    o::OrderConfig config;
    config.accounts = {{"Alice", "demo"}, {"Bob", "demo"}};
    config.products = {{"ITEM1", 100, 1}};
    config.shards = {{1, {"127.0.0.1", 1}}};
    p::Options options;
    options.directory = std::filesystem::path(argv[1]) / ("replica-" + std::to_string(getpid()));
    options.checkpoint_interval = 1ms;
    std::atomic_bool block_snapshot{false}, snapshot_arrived{false}, snapshot_release{false};
    options.hook = [&](p::Stage stage, std::uint64_t) {
      if (stage == p::Stage::BeforeSnapshotPublish && block_snapshot.load()) {
        snapshot_arrived.store(true);
        while (!snapshot_release.load())
          std::this_thread::sleep_for(1ms);
      }
    };
    o::MatchingEngine source(config, 1);
    p::OrderDictionary dictionary(config);
    std::uint64_t sequence = 0;
    m::OrderId first = 0, second = 0;
    p::Boundary saved;
    {
      p::DurableEngine replica(config, 1, options);

      struct ReleaseSnapshot {
        std::atomic_bool& flag;

        ~ReleaseSnapshot() {
          flag.store(true);
        }
      } release_snapshot{snapshot_release};

      replica.freeze_checkpoints(true);
      std::uint64_t expected_checkpoint = 0;
      auto submit = [&](m::Side side, int quantity, unsigned user) {
        m::NewOrderRequest request{};
        request.header = {m::MessageType::NewOrderRequest, sizeof(request), sequence + 1};
        o::put_string(request.symbol, "ITEM1");
        request.side = side;
        request.price = 100;
        request.quantity = quantity;
        p::Result result;
        source.execute(
            {0, o::LinkKind::NewOrder, sequence + 1, user, 1}, o::record_bytes(request), result);
        const p::Boundary boundary{++sequence, source.next_order(), source.next_trade(), 0, 0};
        auto [order, trade] = frames(result, boundary, dictionary);
        replica.replicate(order, trade);
        replica.drain();
        same_book(source, replica);
        replica.request_checkpoint();
        replica.wait_checkpoint();
        require(replica.checkpoint_sequence() == expected_checkpoint,
                "frozen manual/automatic checkpoint advanced");
        require(replica.boundary().seq == sequence, "replica did not durably commit");
        auto bad = order;
        bad.boundary.seq += 2;
        rejects(
            [&] {
              replica.replicate(bad, trade);
            },
            "sequence gap accepted");
        rejects(
            [&] {
              replica.replicate(order, trade);
            },
            "duplicate batch accepted");
        return result;
      };
      auto one = submit(m::Side::Sell, 5, 2);
      first = one.changes.back().order.id;
      auto two = submit(m::Side::Sell, 5, 2);
      second = two.changes.back().order.id;
      auto partial = submit(m::Side::Buy, 2, 1);
      require(partial.trades.size() == 1 && partial.trades[0].sell_order == first,
              "partial maker mismatch");
      // Reject a correctly framed but unpaired batch before any mutation.
      auto [order, trade] =
          frames(partial, {sequence + 1, source.next_order(), source.next_trade() + 1}, dictionary);
      rejects(
          [&] {
            replica.replicate(order, std::nullopt);
          },
          "missing parallel trade frame accepted");
      trade->boundary.next_order++;
      rejects(
          [&] {
            replica.replicate(order, trade);
          },
          "mismatched counters accepted");
      auto fill = submit(m::Side::Buy, 4, 1);
      require(fill.trades.size() == 2 && fill.trades[0].sell_order == first &&
                  fill.trades[1].sell_order == second,
              "FIFO after replicated partial fill changed");
      block_snapshot.store(true);
      replica.freeze_checkpoints(false);
      replica.request_checkpoint();
      const auto deadline = std::chrono::steady_clock::now() + 5s;
      while (!snapshot_arrived.load()) {
        require(std::chrono::steady_clock::now() < deadline, "snapshot publication gate timed out");
        replica.poll();
        std::this_thread::sleep_for(1ms);
      }
      replica.freeze_checkpoints(true);
      require(!replica.checkpoints_frozen(),
              "export became ready while an old snapshot could still publish");
      snapshot_release.store(true);
      replica.wait_checkpoint();
      require(replica.checkpoint_sequence() == sequence, "checkpoints did not resume");
      require(replica.checkpoints_frozen(), "freeze did not wait for existing snapshot completion");
      expected_checkpoint = sequence;
      submit(m::Side::Buy, 1, 1);
      require(replica.checkpoint_sequence() == expected_checkpoint,
              "new snapshot started during frozen export");
      saved = replica.boundary();
    }
    {
      p::DurableEngine recovered(config, 1, options);
      require(recovered.boundary().seq == saved.seq &&
                  recovered.boundary().trade_end == saved.trade_end &&
                  recovered.boundary().trade_count == saved.trade_count,
              "replicated durable prefix failed recovery");
      same_book(source, recovered);
      m::NewOrderRequest request{};
      request.header = {m::MessageType::NewOrderRequest, sizeof(request), 90};
      o::put_string(request.symbol, "ITEM1");
      request.side = m::Side::Buy;
      request.price = 100;
      request.quantity = 1;
      std::vector<m::Trade> trades;
      recovered.command({0, o::LinkKind::NewOrder, 90, 1, 1},
                        o::record_bytes(request),
                        [&](auto, const auto&, auto bytes) {
                          m::Header h;
                          std::memcpy(&h, bytes.data(), sizeof(h));
                          if (h.messageType == m::MessageType::Trade)
                            trades.push_back(o::copy_record<m::Trade>(bytes));
                        });
      recovered.drain();
      require(trades.size() == 2 && (trades[0].orderId == second || trades[1].orderId == second),
              "promoted/recovered replica lost resting FIFO");
      require(recovered.boundary().next_trade == saved.next_trade + 1, "promotion reused trade ID");
    }
    std::cout
        << "PASS frozen_checkpoints contiguous_replication parallel_log_validation FIFO durable_promotion\n";
    return 0;
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
