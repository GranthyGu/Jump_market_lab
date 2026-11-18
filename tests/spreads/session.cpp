#include "session_fixture.hpp"
#include "matching_engine/spreads/session.hpp"
#include "matching_engine/spreads/coordinator/coordinator.hpp"
#include "fixtures.hpp"
#include <deque>
#include <random>
#include <unistd.h>

using namespace spread_test;
namespace s = o::spreads;

namespace {
using namespace spread_test::session_fixture;

void all_routes(const std::filesystem::path& root) {
  constexpr std::array<int, 5> price{120, 110, 105, 10, 15};
  for (unsigned route = 0; route < 12; ++route) {
    Fixture f(root / std::to_string(route));
    const auto& r = s::routes[route];
    for (const auto source : {r.first, r.second}) {
      const auto side = source % 2 == s::Bid ? m::Side::Buy : m::Side::Sell;
      f.seed(source / 2, side, price[source / 2], source == r.first ? 1 : 2, 2);
      f.seed(source / 2, side, price[source / 2], source == r.first ? 2 : 1, 3);
    }
    f.start();
    const auto before = f.coordinator->batches();
    f.run(r.target, r.side == s::Ask ? m::Side::Buy : m::Side::Sell, price[r.target], 3);
    f.settle();
    require(f.receipts.size() == 3, "One combination did not seal exactly three owned batches");
    for (const auto slot : {r.target, std::uint8_t(r.first / 2), std::uint8_t(r.second / 2)}) {
      const auto member = f.topology.owner(slot);
      require(f.coordinator->batches()[member] == before[member] + 1,
              "One command sealed multiple batches");
      const auto records = f.records(member);
      require(records.size() == 3, "Unequal FIFO boundaries were not split into three fills");
      for (unsigned i = 0; i < records.size(); ++i) {
        std::visit(
            [&](const auto& record) {
              using T = std::decay_t<decltype(record)>;
              if constexpr (std::is_same_v<T, p::TradeRecord>)
                require(false, "Implied fill became direct");
              else
                require(record.execution.fill == i + 1 && record.quantity == 1,
                        "FIFO split execution keys/quantities differ across "
                        "shards");
            },
            records[i]);
      }
      require(
          f.engines[member]
                  ->working()
                  .source_depth(f.topology.symbols[slot],
                                slot == r.target ? (r.side == s::Ask ? m::Side::Buy : m::Side::Sell)
                                : (slot == r.first / 2 ? r.first : r.second) % 2 == s::Bid
                                    ? m::Side::Buy
                                    : m::Side::Sell)
                  .count == 0,
          "Consumed real order remained in book");
    }
    const auto initiator = f.topology.owner(r.target);
    const auto receipt =
        std::find_if(f.receipts.begin(), f.receipts.end(), [&](const auto& receipt) {
          return receipt.member == initiator;
        });
    require(!receipt->events.empty(), "Initiator lost Accepted");
    const auto accepted = o::copy_record<m::OrderUpdate>(
        {receipt->events.front().payload.data(), receipt->events.front().size});
    require(accepted.order.status == m::OrderStatus::Accepted,
            "Accepted was reordered behind fills");
    for (const auto& receipt : f.receipts)
      for (const auto& event : receipt.events) {
        m::Header header{};
        std::memcpy(&header, event.payload.data(), sizeof(header));
        const auto order =
            header.messageType == m::MessageType::Trade
                ? o::copy_record<m::Trade>({event.payload.data(), event.size}).orderId
                : o::copy_record<m::OrderUpdate>({event.payload.data(), event.size}).order.orderId;
        require((order >> 48) == f.topology.shards[receipt.member],
                "A shard emitted a remote order event");
      }
  }
}

void overlap(const std::filesystem::path& root) {
  Fixture f(root);
  f.seed(s::A, m::Side::Sell, 120, 4);
  f.seed(s::B, m::Side::Buy, 110, 4);
  f.start();
  f.hold_details = true;
  f.run(s::AB, m::Side::Buy, 10, 4);
  require(f.coordinator->counters.rounds == 1 && f.coordinator->completed() == 0,
          "Source/view progress waited for FIFO identity transmission");
  require(std::none_of(f.delivered.begin(), f.delivered.end(), [](const auto& message) {
            return message.header.kind == s::MessageKind::RoundReady;
          }),
          "Terminal round sent an unnecessary round-ready message");
  for (unsigned member = 0; member < 3; ++member) {
    require(f.sessions[member]->active(), "RoundReady prematurely sealed a Batch");
    f.engines[member]->poll();
  }
  require(f.receipts.empty(), "Incomplete details entered durable publication");
  f.hold_details = false;
  f.settle();
  require(f.receipts.size() == 3, "Overlap did not finish all actual participants");
  for (unsigned member = 1; member < 3; ++member) {
    auto allocation = std::find_if(f.delivered.begin(), f.delivered.end(), [&](const auto& m) {
      return m.to == member && m.header.kind == s::MessageKind::Allocations &&
             (m.header.flags & o::group::final_part);
    });
    auto end = std::find_if(f.delivered.begin(), f.delivered.end(), [&](const auto& m) {
      return m.to == member && m.header.kind == s::MessageKind::CommandEnd;
    });
    require(allocation < end, "CommandEnd overtook final allocation on peer TCP");
  }
}

void terminal_overlap(const std::filesystem::path& root) {
  for (unsigned scenario = 0; scenario < 3; ++scenario)
    for (bool hold_ack : {false, true}) {
      Fixture f(root / (std::to_string(scenario) + (hold_ack ? "-ack" : "-view")));
      if (scenario < 2) {
        f.seed(s::A, m::Side::Sell, 120, 2);
        f.seed(s::B, m::Side::Buy, 110, 4);
        if (scenario == 1)
          f.seed(s::C, m::Side::Sell, 105, 2);
      } else {
        f.seed(s::B, m::Side::Buy, 110, 2);
      }
      f.start();
      f.hold_views = !hold_ack;
      f.hold_view_acks = hold_ack;
      if (scenario < 2)
        f.run(scenario == 0 ? s::AB : s::BF, m::Side::Buy, scenario == 0 ? 10 : 5, 2);
      else
        f.run(s::A, m::Side::Sell, 120, 2);
      require(f.coordinator->in_flight() && f.coordinator->completed() == 0,
              "Terminal closure bypassed a pending view or its acknowledgement");
      for (unsigned member = 0; member < 4; ++member) {
        require(!f.sessions[member]->active(), "Terminal result waited for implied installation");
        f.engines[member]->drain();
      }
      require(f.receipts.size() == (scenario == 0 ? 3U : scenario == 1 ? 4U : 1U),
              "Terminal closure did not produce exactly one batch per participant");
      f.hold_views = f.hold_view_acks = false;
      f.settle();
      for (const auto& message : f.delivered) {
        require(message.header.kind != s::MessageKind::RoundReady,
                "Terminal round retained a redundant round-ready message");
        if (message.header.kind == s::MessageKind::ImpliedReplace) {
          require(f.captures[message.to] == f.sequence,
                  "Late implied installation omitted the final market candidate");
          rejects([&] { f.sessions[message.to]->control(message.header, message.body); });
          auto invalid = message.header;
          ++invalid.round;
          if (message.to == (scenario < 2 ? 0U : 1U))
            rejects([&] { f.sessions[message.to]->control(invalid, message.body); });
        }
      }
    }
}

void early_local_source(const std::filesystem::path& root) {
  Fixture f(root);
  f.seed(s::A, m::Side::Sell, 120, 2);
  f.seed(s::B, m::Side::Buy, 110, 2);
  f.seed(s::AB, m::Side::Sell, 9, 1);
  f.start();
  f.hold_execution = true;
  f.run(s::AB, m::Side::Buy, 10, 3);
  require(f.coordinator->counters.rounds == 1 && f.coordinator->completed() == 0,
          "Source barrier waited for peer execution confirmations at the initiator");
  for (auto source : {s::A * 2 + s::Ask, s::B * 2 + s::Bid, s::AB * 2 + s::Ask})
    require(f.coordinator->sources()[source].count == 0,
            "Early source report omitted a direct or remote real mutation");
  for (unsigned member = 0; member < 3; ++member) {
    require(f.sessions[member]->active(), "Source completion sealed unpaired execution");
    f.engines[member]->poll();
  }
  require(f.receipts.empty(), "Unconfirmed peer execution produced a durable receipt");
  f.hold_execution = false;
  f.settle();
  require(f.records(0).size() == 2, "Mixed direct/implied pairing lost an execution");
}

void reordered_links(const std::filesystem::path& root) {
  Fixture f(root);
  f.reorder_links = true;
  f.seed(s::A, m::Side::Sell, 120, 2000);
  f.seed(s::B, m::Side::Buy, 110, 4000);
  f.seed(s::C, m::Side::Sell, 105, 2000);
  f.start();
  for (unsigned i = 0; i < 500; ++i) {
    f.run(i % 2 ? s::BF : s::AB, m::Side::Buy, i % 2 ? 5 : 10, 1 + (i % 7 == 0));
    require(f.coordinator->completed() == f.sequence,
            "Cross-link reordering left a terminal command incomplete");
    for (unsigned route = 0; route < s::routes.size(); ++route) {
      const auto member = f.topology.owner(s::routes[route].target);
      require(f.sessions[member]->views()[route] == f.coordinator->views()[route],
              "MEM_DONE overtook a target's final installed view");
    }
    if (i % 32 == 31)
      f.settle();
  }
  f.settle();
  require(f.receipts.size() == 1750 && f.coordinator->counters.terminal_rounds == 500,
          "Cross-link reordering lost or duplicated a participant batch");
}

void priority(const std::filesystem::path& root) {
  Fixture f(root);
  f.seed(s::A, m::Side::Sell, 120, 1);
  f.seed(s::A, m::Side::Sell, 120, 2);
  f.seed(s::AB, m::Side::Sell, 10, 8);
  f.seed(s::B, m::Side::Sell, 110, 20);
  f.seed(s::AC, m::Side::Sell, 14, 2);
  f.seed(s::AC, m::Side::Sell, 15, 5);
  f.seed(s::C, m::Side::Sell, 105, 20);
  f.start();
  f.run(s::A, m::Side::Buy, 121, 14);
  f.settle();
  const auto records = f.records(1);
  require(records.size() == 5, "Wrong merged fill count");
  require(std::holds_alternative<p::ImpliedLegTrade>(records[0]) &&
              std::holds_alternative<p::TradeRecord>(records[1]) &&
              std::holds_alternative<p::TradeRecord>(records[2]) &&
              std::holds_alternative<p::ImpliedLegTrade>(records[3]) &&
              std::holds_alternative<p::ImpliedLegTrade>(records[4]),
          "Direct FIFO/implied price priority changed");
  const auto first = std::get<p::ImpliedLegTrade>(records[0]);
  const auto ab = std::get<p::ImpliedLegTrade>(records[3]);
  const auto ac = std::get<p::ImpliedLegTrade>(records[4]);
  require(first.price == 119 && first.quantity == 2 && first.execution.fill == 1 &&
              ab.price == 120 && ab.quantity == 8 && ab.execution.fill == 4 && ac.price == 120 &&
              ac.quantity == 1 && ac.execution.fill == 5,
          "AB tie priority or command-wide fill numbers are wrong");
  require(f.receipts.size() == 4, "Two implied routes did not reuse one S batch");
}

void rolling(const std::filesystem::path& root) {
  Fixture f(root);
  for (unsigned i = 0; i < 5; ++i) {
    f.seed(s::A, m::Side::Sell, 118 + i, 2);
    f.seed(s::B, m::Side::Buy, 112 - i, 2);
  }
  // Real spread liquidity loses to the newly exposed fourth implied level.
  f.seed(s::AB, m::Side::Sell, 13, 1);
  f.start();
  const auto before = f.coordinator->batches();
  f.run(s::AB, m::Side::Buy, 14, 11);
  f.settle();
  require(f.coordinator->counters.rounds == 2, "hasMore did not cause exactly one source refresh");
  const auto records = f.records(0);
  require(records.size() == 6 && std::get<p::SpreadExecution>(records[3]).price == 12 &&
              std::get<p::TradeRecord>(records[4]).price == 13 &&
              std::get<p::SpreadExecution>(records[5]).price == 14,
          "Known-window exhaustion let a worse competitor overtake newly "
          "exposed liquidity");
  for (unsigned i = 0; i < 3; ++i)
    require(f.coordinator->batches()[i] == before[i] + 1, "Rolling round split a command Batch");
  require(std::get<p::SpreadExecution>(records.back()).execution.fill == 6,
          "Execution fill number restarted on second round");
}

void short_path(const std::filesystem::path& root) {
  Fixture f(root);
  std::array<m::OrderId, 5> ids{};
  for (unsigned i = 0; i < 5; ++i)
    ids[i] = f.seed(s::A, m::Side::Buy, 120 - i, 1);
  f.start();
  f.cancel(1, ids[4]);
  f.settle();
  require(f.coordinator->counters.local_complete == 1 && !f.coordinator->counters.rounds,
          "Deep cancellation with stable hasMore missed short path");
  f.cancel(1, ids[3]);
  f.settle();
  require(f.coordinator->counters.local_complete == 1 && f.coordinator->counters.rounds == 1,
          "hasMore change incorrectly used local completion");
  f.run(s::A, m::Side::Buy, 115, 1);
  f.settle();
  require(f.coordinator->counters.rounds == 2, "Fourth price insertion missed hasMore refresh");
  f.run(s::A, m::Side::Buy, 114, 1);
  f.settle();
  require(f.coordinator->counters.local_complete == 2, "Deep resting order missed short path");
}

void residual_and_view_only(const std::filesystem::path& root) {
  Fixture f(root);
  f.seed(s::A, m::Side::Sell, 120, 2);
  f.seed(s::AB, m::Side::Buy, 10, 2);
  f.seed(s::AC, m::Side::Buy, 15, 2);
  f.start();
  f.run(s::B, m::Side::Buy, 110, 3);
  f.settle();
  require(f.coordinator->counters.rounds == 2,
          "Residual resting order did not refresh its real source after pairing");
  const auto book = f.engines[2]->working().source_depth(2, m::Side::Buy);
  require(book.count == 1 && book.quantity[0] == 1, "Residual real order quantity is wrong");
  require(!f.sessions[3]->active() && f.engines[3]->boundary().seq == 0 && f.captures[3] == 1,
          "View-only C needed a Batch or lost its command-tagged market capture");
}

void multipart(const std::filesystem::path& root, demo::Suite& suite) {
  Fixture f(root);
  constexpr unsigned count = 5000;
  for (unsigned i = 0; i < count; ++i)
    f.seed(s::A, m::Side::Sell, 120, 1, 2 + i % 2);
  f.seed(s::B, m::Side::Buy, 110, count);
  f.start();
  const auto start = demo::Clock::now();
  f.run(s::AB, m::Side::Buy, 10, count);
  const auto memory = demo::us(start);
  f.settle();
  const auto elapsed = demo::us(start);
  for (unsigned member = 0; member < 3; ++member)
    require(f.records(member).size() == count, "Multipart FIFO pairing dropped a fill");
  const auto chunks = std::count_if(f.delivered.begin(), f.delivered.end(), [](const auto& m) {
    return m.header.kind == s::MessageKind::ResultDetails;
  });
  require(chunks > 2, "Stress did not cross a source-detail frame boundary");
  require(std::all_of(f.delivered.begin(),
                      f.delivered.end(),
                      [](const auto& m) {
                        return m.body.size() + o::group::header_size <= o::group::maximum_frame;
                      }),
          "Group payload exceeded bounded frame size");
  suite.metric("five-thousand-fifo-pieces-memory",
               {memory},
               memory / 1e6,
               count,
               "One 5000-fill command, four real engines and ordered "
               "in-process messages; no sockets/fsync wait");
  suite.metric("five-thousand-fifo-pieces-durable",
               {elapsed},
               elapsed / 1e6,
               count,
               "Same command through all actual shard fsyncs; includes overlap "
               "with memory execution");
}

void signed_prices(const std::filesystem::path& root) {
  for (const auto spread : {-1, 0}) {
    Fixture f(root / std::to_string(spread));
    f.seed(s::A, m::Side::Sell, 110 + spread, 1);
    f.seed(s::B, m::Side::Buy, 110, 1);
    f.start();
    f.run(s::AB, m::Side::Buy, spread, 1);
    f.settle();
    const auto records = f.records(0);
    require(records.size() == 1 && std::get<p::SpreadExecution>(records[0]).price == spread,
            "Zero/negative derived spread failed full group execution");
  }
}

void random_rolling(const std::filesystem::path& root) {
  std::mt19937 random(20261117);
  for (unsigned trial = 0; trial < 40; ++trial) {
    Fixture f(root / std::to_string(trial));
    const bool buy = trial % 2;
    std::vector<int> first, second, direct;
    for (unsigned level = 0; level < 10; ++level) {
      const int a = buy ? 110 + level : 130 - level;
      const int b = buy ? 118 - level : 100 + level;
      for (unsigned n = 1 + random() % 3; n; --n) {
        const auto quantity = 1 + random() % 4;
        f.seed(s::A, buy ? m::Side::Sell : m::Side::Buy, a, quantity);
        first.insert(first.end(), quantity, a);
      }
      for (unsigned n = 1 + random() % 3; n; --n) {
        const auto quantity = 1 + random() % 4;
        f.seed(s::B, buy ? m::Side::Buy : m::Side::Sell, b, quantity);
        second.insert(second.end(), quantity, b);
      }
      const int price = buy ? -5 + 2 * level : 27 - 2 * level;
      const auto quantity = 1 + random() % 4;
      f.seed(s::AB, buy ? m::Side::Sell : m::Side::Buy, price, quantity);
      direct.insert(direct.end(), quantity, price);
    }
    std::vector<std::pair<int, bool>> candidates;
    for (unsigned i = 0; i < std::min(first.size(), second.size()); ++i)
      candidates.emplace_back(first[i] - second[i], true);
    for (auto price : direct)
      candidates.emplace_back(price, false);
    std::stable_sort(candidates.begin(), candidates.end(), [&](const auto& a, const auto& b) {
      return a.first == b.first ? a.second < b.second : buy ? a.first < b.first : a.first > b.first;
    });
    const int limit = buy ? 3 + random() % 10 : 14 + random() % 10;
    const auto quantity = 10 + random() % 130;
    std::vector<std::pair<int, bool>> expected;
    for (auto candidate : candidates) {
      if (expected.size() == quantity || (buy ? candidate.first > limit : candidate.first < limit))
        break;
      expected.push_back(candidate);
    }
    f.start();
    f.run(s::AB, buy ? m::Side::Buy : m::Side::Sell, limit, quantity);
    f.settle();
    std::vector<std::pair<int, bool>> actual;
    for (const auto& record : f.records(0))
      std::visit(
          [&](const auto& r) {
            actual.insert(actual.end(),
                          r.quantity,
                          {r.price, !std::is_same_v<std::decay_t<decltype(r)>, p::TradeRecord>});
          },
          record);
    require(actual == expected,
            "Full rolling execution differs from "
            "independent per-unit sorted oracle");
  }
}
}  // namespace

int main(int argc, char** argv) {
  demo::Suite suite(argc > 1 ? argv[1] : "spread-session-results");
  const auto root =
      suite.output / ("run-" + std::to_string(::getpid()) + "-" + std::to_string(o::unix_ns()));
  suite.test("all-twelve-routes-fifo-splitting-owned-events-and-one-batch", [&] {
    all_routes(root / "routes");
  });
  suite.test("source-refresh-overlaps-delayed-details-and-ordered-end", [&] {
    overlap(root / "overlap");
  });
  suite.test("terminal-batches-overlap-views-without-early-memory-completion", [&] {
    terminal_overlap(root / "terminal");
  });
  suite.test("local-source-completes-before-peer-execution-confirmations", [&] {
    early_local_source(root / "early-source");
  });
  suite.test("five-hundred-terminal-commands-with-random-tcp-link-interleavings", [&] {
    reordered_links(root / "reordered");
  });
  suite.test("documented-direct-ab-ac-priority-and-command-wide-fill-numbers", [&] {
    priority(root / "priority");
  });
  suite.test("rolling-three-level-window-before-worse-direct-price", [&] {
    rolling(root / "rolling");
  });
  suite.test("deep-local-fast-path-and-has-more-transition", [&] {
    short_path(root / "short");
  });
  suite.test("residual-cleanup-round-and-view-only-participant", [&] {
    residual_and_view_only(root / "residual");
  });
  suite.test("five-thousand-source-pieces-multipart-durable-stress", [&] {
    multipart(root / "stress", suite);
  });
  suite.test("zero-and-negative-spread-execution", [&] {
    signed_prices(root / "signed");
  });
  suite.test("random-buy-sell-rolling-window-against-full-depth-unit-oracle", [&] {
    random_rolling(root / "random");
  });
  return suite.finish();
}
