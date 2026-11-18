#include "session_fixture.hpp"
#include "matching_engine/market_data/spread_market.hpp"

using namespace spread_test;
using namespace spread_test::session_fixture;
namespace s = o::spreads;

namespace {
std::vector<m::Trade> trades(const Fixture& f, unsigned member, m::OrderId order = 0) {
  std::vector<m::Trade> result;
  for (const auto& receipt : f.receipts)
    if (receipt.member == member)
      for (const auto& event : receipt.events) {
        m::Header h{};
        std::memcpy(&h, event.payload.data(), sizeof(h));
        if (h.messageType == m::MessageType::Trade) {
          const auto trade = o::copy_record<m::Trade>({event.payload.data(), event.size});
          if (!order || trade.orderId == order)
            result.push_back(trade);
        }
      }
  return result;
}

void all_routes(const std::filesystem::path& root) {
  constexpr std::array<int, 6> price{120, 110, 105, 10, 15, 5};
  for (unsigned id = 12; id < s::routes.size(); ++id) {
    Fixture f(root / std::to_string(id));
    const auto& r = s::routes[id];
    for (unsigned leg = 0; leg < r.legs(); ++leg) {
      const auto source = r.source(leg);
      const auto side = source % 2 == s::Bid ? m::Side::Buy : m::Side::Sell;
      f.seed(source / 2, side, price[source / 2], 1, 2);
      f.seed(source / 2, side, price[source / 2], 3 * r.weight(leg) - 1, 3);
    }
    f.start();
    const auto before = f.coordinator->batches();
    f.run(r.target,
          r.side == s::Ask ? m::Side::Buy : m::Side::Sell,
          price[r.target],
          3 * r.target_weight);
    f.settle();
    require(f.receipts.size() == 4, "Butterfly must close exactly four owner batches");
    for (unsigned member = 0; member < 4; ++member) {
      require(f.coordinator->batches()[member] == before[member] + 1,
              "Butterfly command split a local batch");
      const auto executions = f.records(member);
      std::uint64_t quantity = 0;
      for (const auto& execution : executions)
        std::visit(
            [&](const auto& record) {
              quantity += record.quantity;
            },
            execution);
      require(quantity == (member == 2 ? 6 : 3), "Per-leg butterfly quantity is not 1:2:1");
      if (!member)
        for (const auto& execution : executions) {
          const auto& bf = std::get<p::ButterflyExecution>(execution);
          require(bf.price == bf.a.price - bf.x.price - bf.b_second.price + bf.c.price,
                  "Persisted butterfly cash equation is not balanced");
        }
    }
    const auto boundary = f.coordinator->batches();
    for (auto& session : f.sessions)
      session.reset();
    f.coordinator.reset();
    for (auto& engine : f.engines)
      engine.reset();
    for (unsigned member = 0; member < 4; ++member) {
      p::DurableEngine recovered(
          f.config, f.topology.shards[member], options(f.root / std::to_string(member)));
      require(recovered.boundary().seq == boundary[member],
              "Butterfly recovery changed a completed batch");
    }
  }
}

void shared(const std::filesystem::path& root) {
  Fixture f(root);
  const auto b = f.seed(s::B, m::Side::Sell, 100, 10);
  f.seed(s::AB, m::Side::Sell, 10, 4);
  f.seed(s::BF, m::Side::Sell, 15, 5);
  f.seed(s::C, m::Side::Buy, 105, 5);
  f.start();
  o::MarketBookSnapshot real{};
  real.symbol_id = f.topology.symbols[s::A];
  const auto market = s::merged_market(real, s::A, f.sessions[1]->views());
  require(
      market.count == 1 && market.levels[0].price == 110 && market.levels[0].implied_quantity == 7,
      "Market double-counted shared B");
  f.run(s::A, m::Side::Buy, 110, 7);
  f.settle();
  const auto target = trades(f, 1);
  require(target.size() == 2 && target[0].quantity == 4 && target[1].quantity == 3,
          "Calendar then butterfly quantity allocation changed");
  const auto source = trades(f, 2, b);
  require(source.size() == 2 && source[0].quantity == 4 && source[1].quantity == 6,
          "Shared real order was not deducted 4 then 6");
  require(!f.sessions[2]->sources()[2 * s::B + s::Ask].count, "Shared B was not exhausted exactly");
}

void interleaved(const std::filesystem::path& root) {
  Fixture f(root);
  f.seed(s::B, m::Side::Sell, 100, 2);
  f.seed(s::B, m::Side::Sell, 101, 4);
  f.seed(s::AB, m::Side::Sell, 10, 3);
  f.seed(s::BF, m::Side::Sell, 14, 2);
  f.seed(s::C, m::Side::Buy, 105, 2);
  f.start();
  f.run(s::A, m::Side::Buy, 111, 4);
  f.settle();
  const auto target = trades(f, 1);
  require(target.size() == 2 && target[0].price == 109 && target[0].quantity == 1 &&
              target[1].price == 111 && target[1].quantity == 3,
          "Remote execution must follow price order, not numeric route order");
  const auto source = trades(f, 2);
  require(source.size() == 2 && source[0].price == 100 && source[0].quantity == 2 &&
              source[1].price == 101 && source[1].quantity == 3,
          "Wrong real B prices were assigned to the selected paths");
}

void half_ticks_and_remainder(const std::filesystem::path& root) {
  for (bool buy : {false, true}) {
    Fixture f(root / (buy ? "buy" : "sell"));
    const auto source_side = buy ? m::Side::Sell : m::Side::Buy;
    const auto spread_side = buy ? m::Side::Buy : m::Side::Sell;
    f.seed(s::A, source_side, 120, 5);
    f.seed(s::C, source_side, 105, 5);
    f.seed(s::BF, spread_side, 4, 5);
    f.seed(s::AB, spread_side, buy ? 8 : 12, 5);
    f.start();
    f.run(s::B, buy ? m::Side::Buy : m::Side::Sell, buy ? 112 : 108, 3);
    f.settle();
    const auto target = trades(f, 2);
    require(target.size() == 3 && target[0].quantity == 1 && target[1].quantity == 1 &&
                target[0].price == 110 && target[1].price == 111 &&
                target[2].price == (buy ? 112 : 108) && target[2].quantity == 1,
            "B must balance the half tick before its odd AB remainder");
    require(f.coordinator->counters.rounds == 2,
            "Odd remainder needs a fresh unreserved source view");
    const auto records = f.records(0);
    require(records.size() == 2 && std::holds_alternative<p::ButterflyExecution>(records[0]) &&
                std::holds_alternative<p::SpreadExecution>(records[1]),
            "Odd remainder used the wrong strategy");
  }
  Fixture f(root / "single-after-direct");
  f.seed(s::A, m::Side::Sell, 120, 5);
  f.seed(s::C, m::Side::Sell, 105, 5);
  f.seed(s::BF, m::Side::Buy, 4, 5);
  f.seed(s::AB, m::Side::Buy, 8, 5);
  f.seed(s::B, m::Side::Sell, 109, 1);
  f.start();
  f.run(s::B, m::Side::Buy, 112, 2);
  f.settle();
  const auto target = trades(f, 2, id(7, 2));
  require(target.size() == 2 && target[0].price == 109 && target[1].price == 112,
          "A direct fill leaving one B must activate the raw AB fallback");
}

void cross_level(const std::filesystem::path& root) {
  Fixture f(root);
  f.seed(s::A, m::Side::Sell, 120, 1);
  f.seed(s::C, m::Side::Sell, 105, 1);
  f.seed(s::B, m::Side::Buy, 110, 1, 2);
  f.seed(s::B, m::Side::Buy, 109, 1, 3);
  f.start();
  f.hold_details = true;
  f.run(s::BF, m::Side::Buy, 6, 1);
  require(f.coordinator->counters.rounds == 1 && f.coordinator->completed() == 0,
          "Weighted source refresh must still overlap FIFO detail pairing");
  f.hold_details = false;
  f.settle();
  const auto record = std::get<p::ButterflyExecution>(f.records(0).at(0));
  require(record.price == 6 && record.x.price == 110 && record.b_second.price == 109 &&
              record.x.order != record.b_second.order,
          "Two one-contract orders were not paired across B levels");
}

void window_pair(const std::filesystem::path& root) {
  Fixture f(root);
  f.seed(s::A, m::Side::Sell, 120, 3);
  f.seed(s::C, m::Side::Sell, 105, 3);
  f.seed(s::B, m::Side::Buy, 110, 1);
  f.seed(s::B, m::Side::Buy, 109, 1);
  const auto odd = f.seed(s::B, m::Side::Buy, 108, 1);
  f.seed(s::B, m::Side::Buy, 107, 3);
  f.start();
  f.run(s::BF, m::Side::Buy, 11, 3);
  f.settle();
  const auto target = trades(f, 0);
  require(target.size() == 3 && target[0].price == 6 && target[1].price == 10 &&
              target[2].price == 11 &&
              std::all_of(target.begin(),
                          target.end(),
                          [](const auto& trade) {
                            return trade.quantity == 1;
                          }),
          "Third-level odd B must pair with the fourth level after refresh");
  require(f.coordinator->counters.rounds == 2 && trades(f, 2, odd).size() == 1 &&
              !f.sessions[2]->sources()[2 * s::B + s::Bid].count,
          "Window refresh lost or reused a residual B contract");
}

void stress(const std::filesystem::path& root) {
  Fixture f(root);
  constexpr unsigned count = 10000;
  f.seed(s::B, m::Side::Sell, 110, 2 * count);
  f.seed(s::AB, m::Side::Sell, 10, count / 2);
  f.seed(s::BF, m::Side::Sell, 5, count / 2);
  f.seed(s::C, m::Side::Buy, 105, count / 2);
  f.start();
  for (unsigned i = 0; i < count; ++i) {
    f.run(s::A, m::Side::Buy, 120, 1);
    // Drain periodically to exercise the bounded persistence queue without
    // treating its deliberate backpressure as a malformed command.
    if (i % 32 == 31)
      f.settle();
  }
  f.settle();
  require(f.coordinator->completed() == count && trades(f, 1).size() == count,
          "Shared-source load lost or duplicated target orders");
  require(f.sessions[2]->sources()[2 * s::B + s::Ask].quantity[0] == count / 2,
          "Shared-source load did not conserve real B quantity");
}
}  // namespace

int main(int argc, char** argv) {
  demo::Suite suite(argc > 1 ? argv[1] : "butterfly-session-results");
  const auto data = suite.output / ("run-" + std::to_string(o::unix_ns()));
  suite.test("eight-butterfly-routes-owned-batches-records-and-recovery", [&] {
    all_routes(data / "routes");
  });
  suite.test("shared-real-order-and-market-quantity-conservation", [&] {
    shared(data / "shared");
  });
  suite.test("shared-execution-follows-price-order-across-routes", [&] {
    interleaved(data / "ordered");
  });
  suite.test("b-half-tick-pairs-odd-remainder-and-direct-prefix", [&] {
    half_ticks_and_remainder(data / "b");
  });
  suite.test("two-b-orders-across-levels-and-overlapped-source-refresh", [&] {
    cross_level(data / "levels");
  });
  suite.test("odd-b-crosses-third-and-fourth-levels-after-refresh", [&] {
    window_pair(data / "window");
  });
  suite.test("ten-thousand-shared-source-orders", [&] {
    stress(data / "stress");
  });
  return suite.finish();
}
