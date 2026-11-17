#include "matching_engine/spreads/execution.hpp"
#include "fixtures.hpp"
#include <random>

using namespace spread_test;
namespace s = o::spreads;

namespace {
void resting(o::MatchingEngine &core, unsigned request, m::Side side,
             m::Price price, int quantity) {
  const auto q = order(request, "A", side, price, quantity);
  core.command(context(request, 2), o::record_bytes(q),
               [](auto, const auto &, auto) {});
}

p::CompactOrder incoming(m::Side side, m::Price price, int quantity) {
  return {id(6, 1000), id(6, 1000), price, quantity, 0,
          quantity,    1,           1,     side,     m::OrderStatus::Accepted};
}

void documented() {
  o::MatchingEngine core(config(), 6);
  resting(core, 1, m::Side::Sell, 120, 2);
  resting(core, 2, m::Side::Sell, 120, 1);
  resting(core, 3, m::Side::Sell, 122, 4);
  s::Views views{};
  views[3] = {{8, 2}, {120, 121}, 2, false};
  views[9] = {{2, 5}, {119, 120}, 2, false};
  const auto plan =
      s::select(core, s::A, views, incoming(m::Side::Buy, 121, 14));
  require(plan.count == 4 && plan.remaining == 0 && !plan.refill,
          "Documented plan length");
  const std::array<std::uint8_t, 4> route{9, s::no_slot, 3, 9};
  const std::array<int, 4> quantity{2, 3, 8, 1};
  for (unsigned i = 0; i < 4; ++i)
    require(plan.slices[i].route == route[i] &&
                plan.slices[i].quantity == quantity[i],
            "AC better price, direct same price, AB tie priority broken");
  require(plan.prefixes[3] == 8 && plan.prefixes[9] == 3 && plan.direct == 3,
          "Prefix batching mismatch");
  p::Result result;
  std::vector<o::MatchingEngine::FillPiece> pieces;
  core.take_prefix(1, m::Side::Sell, plan.direct, pieces, result);
  require(pieces.size() == 2 && pieces[0].after.id == id(6, 1) &&
              pieces[1].after.id == id(6, 2),
          "Real-book reuse broke same-price FIFO");
  require(core.source_depth(1, m::Side::Sell).price[0] == 122,
          "Source refresh missed consumed prices");
}

void priority_oracle() {
  std::mt19937 random(20261117);
  for (unsigned trial = 0; trial < 3000; ++trial) {
    const bool buy = trial % 2;
    o::MatchingEngine core(config(), 6);
    struct Candidate {
      int price, quantity;
      std::uint8_t route;
      unsigned priority;
    };
    std::vector<Candidate> oracle;
    for (unsigned i = 0; i < 9; ++i) {
      const auto price = 116 + int(i);
      const auto quantity = 1 + random() % 7;
      resting(core, i + 1, buy ? m::Side::Sell : m::Side::Buy, price, quantity);
      oracle.push_back({price, int(quantity), s::no_slot, 0});
    }
    s::Views views{};
    std::array<unsigned, 12> unseen{};
    for (std::uint8_t route :
         {std::uint8_t(buy ? 3 : 2), std::uint8_t(buy ? 9 : 8)}) {
      auto &view = views[route];
      view.count = random() % 6;
      view.more = view.count && random() % 2;
      unseen[route] = view.count;
      const auto first =
          buy ? 114 + int(random() % 5) : 126 - int(random() % 5);
      for (unsigned i = 0; i < view.count; ++i) {
        view.price[i] = first + (buy ? int(i) : -int(i));
        view.quantity[i] = 1 + random() % 10;
        oracle.push_back({view.price[i], int(view.quantity[i]), route,
                          1U + s::routes[route].priority});
      }
    }
    std::sort(oracle.begin(), oracle.end(), [&](const auto &a, const auto &b) {
      return a.price == b.price ? a.priority < b.priority
             : buy              ? a.price < b.price
                                : a.price > b.price;
    });
    const auto request = incoming(buy ? m::Side::Buy : m::Side::Sell,
                                  116 + random() % 9, 1 + random() % 130);
    std::vector<std::uint8_t> expected, actual;
    int left = request.remaining;
    bool refill = false;
    for (const auto &candidate : oracle) {
      if (!left || (buy ? candidate.price > request.price
                        : candidate.price < request.price))
        break;
      const auto quantity = std::min(left, candidate.quantity);
      expected.insert(expected.end(), quantity, candidate.route);
      left -= quantity;
      if (candidate.route != s::no_slot && !--unseen[candidate.route] && left &&
          views[candidate.route].more) {
        refill = true;
        break;
      }
    }
    const auto plan = s::select(core, s::A, views, request);
    for (unsigned i = 0; i < plan.count; ++i)
      actual.insert(actual.end(), plan.slices[i].quantity,
                    plan.slices[i].route);
    require(actual == expected && plan.remaining == left &&
                plan.refill == refill,
            "Streaming selection differs from independent sorted oracle");
  }
}

void rolling_and_wide() {
  o::MatchingEngine core(config(), 6);
  resting(core, 1, m::Side::Sell, 125, 3);
  s::Views views{};
  views[3] = {{2, 2, 2}, {119, 120, 121}, 3, true};
  views[9] = {{8}, {123}, 1, false};
  auto plan = s::select(core, s::A, views, incoming(m::Side::Buy, 130, 20));
  require(plan.refill && plan.prefixes[3] == 6 && !plan.prefixes[9] &&
              !plan.direct,
          "Exhausted more route must refresh before competing liquidity");
  views[3] = {{5}, {122}, 1, false};
  plan = s::select(core, s::A, views, incoming(m::Side::Buy, 130, 14));
  require(plan.slices[0].route == 3 && plan.prefixes[3] == 5 &&
              plan.prefixes[9] == 8 && plan.direct == 1,
          "Refilled route lost its better price");
  views[3] = {{std::uint64_t(INT32_MAX) + 8}, {119}, 1, true};
  plan = s::select(core, s::A, views, incoming(m::Side::Buy, 120, INT32_MAX));
  require(plan.prefixes[3] == INT32_MAX && !plan.refill && !plan.remaining,
          "Wide price-level quantity was narrowed before selection");
}

void all_record_roles() {
  const auto c = config();
  const s::Topology topology(c);
  for (std::uint8_t route_id = 0; route_id < 12; ++route_id) {
    const auto &route = s::routes[route_id];
    const auto x = route.spread == s::AB ? s::B : s::C;
    auto price = [&](unsigned slot) {
      return slot == s::A    ? 120
             : slot == s::B  ? 110
             : slot == s::C  ? 105
             : slot == s::AB ? 10
                             : 15;
    };
    auto owner = [&](unsigned slot) {
      return topology.shards[topology.owner(slot)];
    };
    auto user = [&](unsigned slot) {
      return slot >= s::AB ? 1U : slot == s::A ? 2U : 3U;
    };
    p::CompactOrder aggressor{id(owner(route.target), 2),
                              2,
                              price(route.target),
                              3,
                              0,
                              3,
                              user(route.target),
                              topology.symbols[route.target],
                              route.side == s::Ask ? m::Side::Buy
                                                   : m::Side::Sell,
                              m::OrderStatus::Accepted};
    const auto first = route.first / 2, second = route.second / 2;
    const auto combination =
        s::pair(route_id, aggressor,
                {id(owner(first), 1), user(first), price(first), 3},
                {id(owner(second), 1), user(second), price(second), 3}, 3,
                {aggressor.id, 7}, 123);
    for (const auto slot : {s::A, x, static_cast<s::Slot>(route.spread)}) {
      o::MatchingEngine core(c, owner(slot));
      const auto &identity = slot == s::A ? combination.a
                             : slot == x  ? combination.x
                                          : combination.spread;
      const auto side = slot == route.spread ? combination.spread_side
                        : slot == x          ? combination.spread_side
                        : combination.spread_side == m::Side::Buy
                            ? m::Side::Sell
                            : m::Side::Buy;
      p::CompactOrder local{identity.order,
                            identity.order,
                            identity.price,
                            3,
                            3,
                            0,
                            identity.user,
                            topology.symbols[slot],
                            side,
                            m::OrderStatus::Filled};
      p::Result result;
      s::record(core, result, topology, combination, local);
      require(result.executions.size() == 1 && result.events.size() == 2 &&
                  result.changes.empty(),
              "Pairing emitted non-owned updates or changed a second order");
      const auto event = o::copy_record<m::Trade>(
          {result.events[0].payload.data(), result.events[0].size});
      require(event.orderId == identity.order &&
                  event.price == identity.price && event.quantity == 3,
              "Owned notification price/quantity mismatch");
      p::OrderDictionary(c).validate_execution(result.executions.front(),
                                               {1, 3, 2, 0, 0}, owner(slot));
    }
  }
}

void pairing_boundaries() {
  const std::array<s::SourcePiece, 2> a{
      {{id(6, 1), 2, 120, 2}, {id(6, 2), 2, 120, 4}}};
  const std::array<s::SourcePiece, 2> b{
      {{id(7, 1), 3, 110, 3}, {id(7, 2), 3, 110, 3}}};
  s::PieceCursor first(a), second(b);
  std::vector<int> actual;
  while (!first.empty() && !second.empty()) {
    const auto q = std::min(first.remaining(), second.remaining());
    actual.push_back(q);
    first.consume(q);
    second.consume(q);
  }
  require(actual == std::vector<int>{2, 1, 3} && first.empty() &&
              second.empty(),
          "Dual FIFO pairing split wrong boundaries");
  rejects([&] { first.consume(1); });
  o::MatchingEngine::FillPiece piece{{id(6, 1), 1, 120, 10, 6, 4, 2, 1,
                                      m::Side::Sell,
                                      m::OrderStatus::PartiallyFilled},
                                     6};
  const auto partial = s::allocated(piece, 2);
  require(partial.filled == 2 && partial.remaining == 8,
          "Split update exposed full source deduction too early");
  require(s::allocated(piece, 6).remaining == 4,
          "Last allocation lost source remainder");
  rejects([&] { s::allocated(piece, 7); });
}
} // namespace

int main(int argc, char **argv) {
  demo::Suite suite(argc > 1 ? argv[1] : "spread-execution-results");
  suite.test("documented-ac-direct-ab-selection-and-original-fifo", documented);
  suite.test("bid-ask-priority-vs-independent-sorted-oracle", priority_oracle);
  suite.test("more-refill-barrier-and-wide-level-quantity", rolling_and_wide);
  suite.test("all-twelve-routes-owned-events-and-economic-counterparties",
             all_record_roles);
  suite.test("dual-fifo-pairing-and-incremental-order-updates",
             pairing_boundaries);
  suite.test("million-fixed-plan-selections", [&] {
    o::MatchingEngine core(config(), 6);
    resting(core, 1, m::Side::Sell, 120, 3);
    s::Views views{};
    views[3] = {{8, 2}, {120, 121}, 2, false};
    views[9] = {{2, 5}, {119, 120}, 2, false};
    const auto request = incoming(m::Side::Buy, 121, 14);
    std::uint64_t checksum = 0;
    std::vector<double> samples;
    const auto start = demo::Clock::now();
    for (unsigned batch = 0; batch < 1000; ++batch) {
      const auto lap = demo::Clock::now();
      for (unsigned n = 0; n < 1000; ++n)
        checksum += s::select(core, s::A, views, request).prefixes[9];
      samples.push_back(demo::us(lap) / 1000);
    }
    require(checksum == 3000000, "Stress optimizer/checksum mismatch");
    suite.metric("fixed-plan-selection", std::move(samples),
                 demo::us(start) / 1e6, 1000000,
                 "Existing real price tree plus two implied streams; "
                 "batch-average samples, no network or storage");
  });
  return suite.finish();
}
