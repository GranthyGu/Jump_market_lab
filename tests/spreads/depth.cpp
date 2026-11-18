#include "matching_engine/spreads/depth.hpp"
#include "matching_engine/spreads/transport/protocol.hpp"
#include "harness.hpp"
#include <random>

using namespace marketlab::orders;
using namespace marketlab::orders::spreads;
using demo::require;

template <class F> void rejects(F fn) {
  bool failed = false;
  try {
    fn();
  } catch (const std::exception &) {
    failed = true;
  }
  require(failed, "Malformed source was accepted");
}

int main(int argc, char **argv) {
  demo::Suite suite(argc > 1 ? argv[1] : "depth-results");
  suite.test("derived-spread-price-limits-and-topology-identity", [] {
    OrderConfig c;
    c.direct = false;
    c.spreads = true;
    c.accounts = {{"ab", "p"}, {"c", "p"}};
    c.products = {{"A", 120, 6},
                  {"B", 110, 7},
                  {"C", 105, 8},
                  {"SP_AB", 10, 5},
                  {"SP_AC", 15, 5}};
    const Topology topology(c);
    require(topology.limits[AB].lower == -13 && topology.limits[AB].upper == 33,
            "SP_AB must derive its signed interval from both legs");
    require(topology.limits[AC].lower == -7 && topology.limits[AC].upper == 38,
            "SP_AC interval must preserve outright integer rounding");
    c.accounts = {{"a", "p"}, {"bc", "p"}};
    require(Topology(c).fingerprint != topology.fingerprint,
            "User dictionary boundaries must affect the topology fingerprint");
    c.products[4].shard = 8;
    rejects([&] { Topology invalid(c); });
  });
  suite.test("documented-five-segment-example", [] {
    Sources sources{};
    sources[2 * A + Ask] = {1, {20, 40, 60}, {100, 101, 102}, 3, false};
    sources[2 * AB + Bid] = {1, {30, 40, 30}, {6, 5, 4}, 3, false};
    auto view = derive(routes[5], sources);
    require(view.count == 5 && !view.more, "Expected five finite segments");
    require(view.price == std::array<std::int32_t, 5>{94, 95, 96, 97, 98},
            "Example prices");
    require(view.quantity == std::array<std::uint64_t, 5>{20, 10, 30, 10, 30},
            "Example quantities");
  });
  suite.test("all-twelve-routes-vs-unit-quantity-oracle", [] {
    std::mt19937 random(20261117);
    for (unsigned trial = 0; trial < 15000; ++trial) {
      Sources sources{};
      for (unsigned n = 0; n < sources.size(); ++n) {
        auto &w = sources[n];
        w.count = random() % 4;
        w.more = w.count == 3 && (random() % 2);
        for (unsigned i = 0; i < w.count; ++i) {
          w.price[i] = (n >= 6 ? -5 : 100) + (n % 2 ? int(i) : -int(i));
          w.quantity[i] = random() % 12 + 1;
        }
      }
      for (const auto &route : routes) {
        const auto &a = sources[route.first];
        const auto &b = sources[route.second];
        std::vector<std::int32_t> first, second, expected, actual;
        for (unsigned i = 0; i < a.count; ++i)
          first.insert(first.end(), a.quantity[i], a.price[i]);
        for (unsigned i = 0; i < b.count; ++i)
          second.insert(second.end(), b.quantity[i], b.price[i]);
        for (unsigned i = 0; i < std::min(first.size(), second.size()); ++i)
          expected.push_back(first[i] + (route.add ? second[i] : -second[i]));
        auto out = derive(route, sources);
        for (unsigned i = 0; i < out.count; ++i)
          actual.insert(actual.end(), out.quantity[i], out.price[i]);
        require(
            actual == expected,
            "Price/quantity allocation differs from independent unit oracle");
        require(out.count <= 5, "Implied window overflow");
        const auto q = std::min(first.size(), second.size());
        require(out.more == ((first.size() > q || a.more) &&
                             (second.size() > q || b.more)),
                "more mismatch");
      }
    }
  });
  suite.test("adaptive-depth-roundtrip-negative-zero-and-window-movement", [] {
    std::mt19937 random(91871);
    Sources receiver{};
    Bytes wire;
    for (unsigned n = 0; n < 100000; ++n) {
      const auto source = static_cast<std::uint8_t>(random() % 10);
      const auto old = receiver[source];
      DepthSide next;
      next.version = old.version + 1;
      next.count = random() % 4;
      next.more = next.count == 3 && (random() % 2);
      const auto base = std::int32_t(random() % 11) - 5;
      for (unsigned i = 0; i < next.count; ++i) {
        next.price[i] = base + (source % 2 ? int(i) : -int(i));
        next.quantity[i] = (std::uint64_t(random()) << 16) + 1;
      }
      wire.clear();
      encode_depth(wire, source, old, next);
      require(wire.size() <= 48,
              "Adaptive encoding exceeded full snapshot size");
      apply_depth(wire, receiver);
      require(receiver[source].same_levels(next) &&
                  receiver[source].version == next.version,
              "Depth roundtrip mismatch");
      rejects([&] { apply_depth(wire, receiver); });
    }
  });
  suite.test("truncated-versions-and-invalid-windows-do-not-mutate-cache", [] {
    Sources sources{};
    DepthSide next{1, {10, 20, 30}, {-1, 0, 1}, 3, true};
    Bytes wire;
    encode_depth(wire, 1, {}, next);
    for (unsigned n = 0; n < wire.size(); ++n) {
      rejects([&] { apply_depth(std::span(wire).first(n), sources); });
      require(sources[1].version == 0 && sources[1].count == 0,
              "Failed frame mutated cache");
    }
    auto wrong = next;
    wrong.count = 2;
    rejects([&] { validate(wrong, Ask); });
    wrong = next;
    wrong.quantity[0] = 0;
    rejects([&] { validate(wrong, Ask); });
    wrong = next;
    wrong.price[1] = wrong.price[0];
    rejects([&] { validate(wrong, Ask); });
    // Snapshot header is 12 bytes; each price/quantity pair is 12 bytes.
    std::copy_n(wire.begin() + 12, 4, wire.begin() + 24);
    rejects([&] { apply_depth(wire, sources); });
    require(sources[1].version == 0, "Duplicate price mutated source cache");
  });
  suite.test("quantity-over-uint32-and-price-overflow", [] {
    Sources sources{};
    const auto quantity = std::uint64_t(UINT32_MAX) + 100;
    sources[0] = {1, {quantity, 0, 0}, {100, 0, 0}, 1, false};
    sources[3] = {1, {quantity, 0, 0}, {100, 0, 0}, 1, false};
    require(derive(routes[0], sources).quantity[0] == quantity,
            "Truncated level total");
    sources[0].price[0] = INT32_MAX;
    sources[3].price[0] = -1;
    rejects([&] { derive(routes[0], sources); });
  });
  suite.test("fixed-cache-and-dirty-source-recompute-stress", [&] {
    require(sizeof(Sources) == 480 && sizeof(Views) == 768,
            "Fixed cache footprint changed");
    Sources sources{};
    Views views{};
    for (unsigned i = 0; i < 10; ++i) {
      sources[i] = {1,
                    {20, 40, 60},
                    i % 2 ? std::array<std::int32_t, 3>{100, 101, 102}
                          : std::array<std::int32_t, 3>{100, 99, 98},
                    3,
                    false};
      validate(sources[i], static_cast<BookSide>(i % 2));
    }
    std::vector<double> samples;
    samples.reserve(1000);
    std::uint64_t checksum = 0;
    const auto start = demo::Clock::now();
    for (unsigned batch = 0; batch < 1000; ++batch) {
      const auto lap = demo::Clock::now();
      for (unsigned n = 0; n < 1000; ++n) {
        sources[1].quantity[0] = 20 + (n % 7);
        checksum += recompute(sources, 1U << 1, views);
      }
      samples.push_back(demo::us(lap) / 1000);
    }
    require(checksum != 0, "Stress computation optimized away");
    suite.metric("dirty-source-recompute", std::move(samples),
                 demo::us(start) / 1e6, 1000000,
                 "One source change; dependent route recomputation and view "
                 "comparison, no I/O");
  });
  return suite.finish();
}
