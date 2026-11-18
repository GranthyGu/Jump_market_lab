#include "matching_engine/spreads/depth.hpp"
#include "matching_engine/spreads/transport/protocol.hpp"
#include "fixtures.hpp"
#include <random>

using namespace spread_test;
namespace s = o::spreads;

namespace {
void level(s::Sources& sources, unsigned source, int price, std::uint64_t quantity) {
  sources[source] = {0, {quantity}, {price}, 1, false};
}

s::Sources overlapping() {
  s::Sources sources{};
  level(sources, 2 * s::B + s::Ask, 100, 10);
  level(sources, 2 * s::AB + s::Ask, 10, 4);
  level(sources, 2 * s::BF + s::Ask, 15, 5);
  level(sources, 2 * s::C + s::Bid, 105, 5);
  return sources;
}

void examples() {
  auto sources = overlapping();
  s::Views views{};
  s::recompute(sources, s::all_sources, views);
  require(views[3].price[0] == 110 && views[3].quantity[0] == 4 && views[15].price[0] == 110 &&
              views[15].quantity[0] == 3,
          "Same-price partial overlap must leave six B for three butterflies");
  sources[2 * s::AB + s::Ask].quantity[0] = 10;
  s::recompute(sources, 1U << (2 * s::AB + s::Ask), views);
  require(views[3].quantity[0] == 10 && !views[15].count,
          "Higher-priority path consumed all shared B");
  sources[2 * s::AB + s::Ask].quantity[0] = 4;
  const auto changed = s::recompute(sources, 1U << (2 * s::AB + s::Ask), views);
  require((changed & (1U << s::A)) && views[15].quantity[0] == 3,
          "SP_AB-only change must release B to a route not reading SP_AB");
  sources[2 * s::BF + s::Ask].price[0] = 14;
  s::recompute(sources, 1U << (2 * s::BF + s::Ask), views);
  require(!views[3].count && views[15].price[0] == 109 && views[15].quantity[0] == 5,
          "Better butterfly price takes precedence over calendar route priority");
  sources[2 * s::B + s::Ask].quantity[0] = 3;
  s::recompute(sources, 1U << (2 * s::B + s::Ask), views);
  require(views[15].quantity[0] == 1 && views[3].quantity[0] == 1,
          "Odd B remainder must survive for another path");
}

void weighted_levels() {
  s::Sources sources{};
  level(sources, 2 * s::A + s::Ask, 120, 10);
  level(sources, 2 * s::C + s::Ask, 105, 10);
  sources[2 * s::B + s::Bid] = {0, {1, 2, 2}, {110, 109, 108}, 3, false};
  const auto view = s::derive(s::routes[13], sources);
  require(view.count == 2 && view.price[0] == 6 && view.quantity[0] == 1 && view.price[1] == 8 &&
              view.quantity[1] == 1 && !view.more,
          "Pair adjacent B levels before flooring quantity; preserve odd remainder");
  sources[2 * s::A + s::Ask] = {0, {2, 5, 7}, {120, 121, 122}, 3, false};
  sources[2 * s::C + s::Ask] = {0, {3, 8, 3}, {105, 106, 107}, 3, false};
  sources[2 * s::B + s::Bid] = {0, {9, 10, 9}, {110, 109, 108}, 3, false};
  const auto nine = s::derive(s::routes[13], sources);
  require(nine.count == 9, "Three weighted sources require more than five segments");
  const std::array<int, 9> prices{5, 6, 7, 8, 9, 10, 11, 12, 13};
  const std::array<std::uint64_t, 9> amounts{2, 1, 1, 1, 2, 2, 1, 1, 3};
  require(nine.price == prices && nine.quantity == amounts, "Weighted nine-segment example");
}

void b_target() {
  s::Sources sources{};
  level(sources, 2 * s::A + s::Bid, 120, 5);
  level(sources, 2 * s::C + s::Bid, 105, 5);
  level(sources, 2 * s::BF + s::Ask, 4, 5);
  level(sources, 2 * s::AB + s::Ask, 11, 5);
  s::Views views{};
  s::recompute(sources, s::all_sources, views);
  require(views[18].price[0] == 110 && views[18].quantity[0] == 10 && !views[4].count,
          "B bid floors the half tick and contributes two B per unit");
  require(views.single_b[s::Bid].price[0] == 109 && views.single_b[s::Bid].quantity[0] == 5,
          "One-contract fallback must not reserve A for a hypothetical butterfly");
  s::Sources ask{};
  level(ask, 2 * s::A + s::Ask, 120, 5);
  level(ask, 2 * s::C + s::Ask, 105, 5);
  level(ask, 2 * s::BF + s::Bid, 4, 5);
  require(s::derive(s::routes[19], ask).price[0] == 111, "B ask must ceil half ticks");
  o::Bytes bytes;
  s::put_views(bytes, s::all_targets, views);
  s::Views restored{};
  p::Reader reader{bytes};
  s::read_views(reader, restored, s::all_targets);
  reader.finish();
  require(restored == views, "Weighted and fallback views must round-trip");
  views[18].quantity[0] = 9;
  bytes.clear();
  s::put_views(bytes, s::all_targets, views);
  rejects([&] {
    p::Reader bad{bytes};
    s::read_views(bad, restored, s::all_targets);
  });
}

// Deliberately slow oracle: expand real price levels to individual contracts,
// rebuild every candidate for every single combination, and record its route.
using Atom = std::pair<int, unsigned>;

std::vector<Atom> oracle(const s::Sources& sources, unsigned target, unsigned side) {
  std::array<std::vector<int>, s::source_count> real;
  std::array<unsigned, s::source_count> at{};
  for (unsigned i = 0; i < real.size(); ++i)
    for (unsigned j = 0; j < sources[i].count; ++j)
      real[i].insert(real[i].end(), sources[i].quantity[j], sources[i].price[j]);
  std::vector<Atom> out;
  for (;;) {
    unsigned best = s::no_slot;
    int best_price = 0;
    for (unsigned id = 0; id < s::routes.size(); ++id) {
      const auto& r = s::routes[id];
      if (r.target != target || r.side != side)
        continue;
      bool available = true;
      int numerator = 0;
      for (unsigned leg = 0; leg < r.legs(); ++leg) {
        const auto source = r.source(leg);
        if (real[source].size() - at[source] < r.weight(leg)) {
          available = false;
          break;
        }
        for (unsigned unit = 0; unit < r.weight(leg); ++unit)
          numerator += r.sign(leg) * real[source][at[source] + unit];
      }
      if (!available)
        continue;
      const double exact = double(numerator) / r.target_weight;
      const int price = side == s::Ask ? std::ceil(exact) : std::floor(exact);
      if (best == s::no_slot || (side == s::Ask ? price < best_price : price > best_price) ||
          (price == best_price && r.priority < s::routes[best].priority)) {
        best = id;
        best_price = price;
      }
    }
    if (best == s::no_slot)
      return out;
    const auto& route = s::routes[best];
    out.insert(out.end(), route.target_weight, {best_price, best});
    for (unsigned leg = 0; leg < route.legs(); ++leg)
      at[route.source(leg)] += route.weight(leg);
  }
}

void randomized() {
  std::mt19937 random(20261118);
  for (unsigned trial = 0; trial < 5000; ++trial) {
    s::Sources sources{};
    for (unsigned source = 0; source < sources.size(); ++source) {
      auto& depth = sources[source];
      depth.count = random() % 4;
      const int base = source / 2 < s::AB ? 105 + random() % 20 : int(random() % 25) - 10;
      for (unsigned i = 0; i < depth.count; ++i) {
        depth.price[i] = base + (source % 2 ? int(i) : -int(i));
        depth.quantity[i] = 1 + random() % 7;
      }
    }
    s::Views views{};
    s::recompute(sources, s::all_sources, views);
    for (unsigned target = 0; target < s::product_count; ++target)
      for (unsigned side = 0; side < 2; ++side) {
        std::vector<Atom> actual;
        for (unsigned id = 0; id < s::routes.size(); ++id)
          if (s::routes[id].target == target && s::routes[id].side == side)
            for (unsigned i = 0; i < views[id].count; ++i)
              actual.insert(actual.end(), views[id].quantity[i], {views[id].price[i], id});
        std::stable_sort(actual.begin(), actual.end(), [&](auto a, auto b) {
          if (a.first != b.first)
            return side == s::Ask ? a.first < b.first : a.first > b.first;
          return s::routes[a.second].priority < s::routes[b.second].priority;
        });
        require(actual == oracle(sources, target, side),
                "Shared allocation differs from unit oracle");
      }
    auto next = sources;
    const auto source = random() % s::source_count;
    if (next[source].count)
      ++next[source].quantity[0];
    s::recompute(next, 1U << source, views);
    s::Views full{};
    s::recompute(next, s::all_sources, full);
    require(views == full, "Incremental dependency expansion differs from full recompute");
  }
}
}  // namespace

int main(int argc, char** argv) {
  demo::Suite suite(argc > 1 ? argv[1] : "shared-depth-results");
  suite.test("shared-source-priority-partial-overlap-and-cancellation", examples);
  suite.test("weighted-adjacent-levels-odd-quantity-and-nine-segments", weighted_levels);
  suite.test("b-whole-pairs-rounded-quotes-and-single-contract-fallback", b_target);
  suite.test("sixty-thousand-target-sides-vs-contract-oracle", randomized);
  suite.test("million-shared-source-recomputations", [&] {
    auto sources = overlapping();
    s::Views views{};
    s::recompute(sources, s::all_sources, views);
    std::vector<double> samples;
    std::uint64_t checksum = 0;
    const auto start = demo::Clock::now();
    for (unsigned batch = 0; batch < 1000; ++batch) {
      const auto lap = demo::Clock::now();
      for (unsigned i = 0; i < 1000; ++i) {
        sources[2 * s::AB + s::Ask].quantity[0] = 1 + i % 10;
        checksum += s::recompute(sources, 1U << (2 * s::AB + s::Ask), views);
      }
      samples.push_back(demo::us(lap) / 1000);
    }
    require(checksum != 0, "Shared-depth stress did not change views");
    suite.metric("shared-source-recompute",
                 std::move(samples),
                 demo::us(start) / 1e6,
                 1000000,
                 "Coordinator computation only; competing calendar/butterfly source budgets");
  });
  return suite.finish();
}
