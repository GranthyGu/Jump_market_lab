#include "matching_engine/market_data/spread_market.hpp"
#include "client/recovery.hpp"
#include "fixtures.hpp"

using namespace spread_test;
namespace s = o::spreads;
namespace md = m::md;

namespace {
md::Book book(const o::MarketBookSnapshot &snapshot) {
  return {snapshot.symbol_id,
          {snapshot.levels.begin(), snapshot.levels.begin() + snapshot.count},
          snapshot.extended};
}

void merge() {
  o::MatchingEngine core(config(), 6);
  for (unsigned i = 0; i < 7; ++i) {
    const auto q = order(i + 1, "A", m::Side::Sell, 120 + i, 3);
    core.command(context(i + 1), o::record_bytes(q),
                 [](auto, const auto &, auto) {});
  }
  s::Views views{};
  views[3] = {{8, 2, 9}, {120, 121, 125}, 3, false};
  views[9] = {{2, 5, 1}, {119, 120, 124}, 3, false};
  const auto snapshot = s::merged_market(core, 1, s::A, views);
  const auto merged = book(snapshot);
  md::validate_book(merged);
  require(merged.levels.size() == 5 && merged.levels[0].price == 119 &&
              merged.levels[4].price == 123,
          "Must merge all sources before truncating five prices");
  const auto &same = merged.levels[1];
  require(same.price == 120 && same.quantity == 3 &&
              same.implied_quantity == 13 && same.num_orders == 3,
          "Same-price direct/AB/AC quantities or route order count differ");
  require(merged.levels[0].quantity == 0 &&
              merged.levels[0].implied_quantity == 2 &&
              merged.levels[0].num_orders == 1,
          "Pure implied level must count each positive route once");
  views[9] = {};
  const auto updated = book(s::merged_market(core, 1, s::A, views));
  require(merged.levels[0].price == 119 && updated.levels[0].price == 120,
          "Captured candidate retained references into mutable view");
}

void wire() {
  const md::Book source{4,
                        {{md::Side::Buy, 0, 3, 2, 4},
                         {md::Side::Buy, -1, 0, 1, 7},
                         {md::Side::Sell, 1, 5, 2, 0}},
                        true};
  const auto epoch = md::new_epoch();
  const md::Message message{md::Kind::SnapshotV2, epoch, 4, 0, 123,
                            source.levels};
  const auto bytes = md::encode(message);
  require(bytes.size() == md::header_size + 3 * 29,
          "V2 must encode exactly 29 bytes per level");
  require(md::decode(bytes).levels == source.levels,
          "Signed D/I market roundtrip");
  auto legacy = message;
  legacy.kind = md::Kind::Snapshot;
  rejects([&] { md::encode(legacy); });
  md::ShardView shard{epoch, {{4, source}}};
  const auto encoded = md::encode_shard(shard);
  require(encoded.size() == 21 + 3 + 3 * 29,
          "Versioned shard encoding overhead mismatch");
  require(md::decode_shard(encoded, {4}).books.at(4) == source,
          "Shard V2 roundtrip mismatch");
  for (std::size_t i = 0; i < bytes.size(); ++i)
    if (i != md::header_size && i != md::header_size + 29 &&
        i != md::header_size + 58)
      rejects([&] { md::decode(std::span(bytes).first(i)); });
  auto invalid = source;
  invalid.levels[0].implied_quantity = UINT64_MAX;
  rejects([&] { md::validate_book(invalid); });
  invalid = source;
  invalid.levels[1].num_orders = 0;
  rejects([&] { md::validate_book(invalid); });
}

void client_delta() {
  const auto now = m::runtime::Clock::now();
  md::MarketRecovery recovery({{4, "SP_AB"}}, {}, now);
  const auto ticket = recovery.next_request(now);
  require(ticket.has_value(), "Initial snapshot query absent");
  const auto epoch = md::new_epoch();
  md::Book before{4, {{md::Side::Buy, 0, 2, 2, 3}}, true};
  recovery.complete(
      *ticket, {md::Kind::SnapshotV2, epoch, 4, 0, 123, before.levels}, now);
  md::Book after{4, {{md::Side::Buy, 0, 4, 2, 1}}, true};
  auto changes = md::difference(before, after);
  require(changes.size() == 1,
          "Unchanged total suppressed changed D/I quantities");
  recovery.udp({md::Kind::DeltaV2, epoch, 4, 1, 124, changes}, now);
  require(recovery.view(4).snapshot->levels == after.levels,
          "D/I delta not applied");
  md::Book pure{4, {{md::Side::Buy, 0, 0, 1, 9}}, true};
  // Receive index 3 before 2 to exercise buffered V2-kind preservation.
  recovery.udp({md::Kind::DeltaV2, epoch, 4, 3, 126,
                md::difference(pure, {4, {}, true})},
               now);
  recovery.udp(
      {md::Kind::DeltaV2, epoch, 4, 2, 125, md::difference(after, pure)}, now);
  require(recovery.view(4).snapshot->index == 3 &&
              recovery.view(4).snapshot->levels.empty(),
          "Buffered V2 deletion failed");
  recovery.udp({md::Kind::DeltaV2, epoch, 4, 4, 127, pure.levels}, now);
  require(recovery.view(4).snapshot->levels == pure.levels,
          "Zero direct quantity must not delete nonzero implied quantity");
}

void full_replacement() {
  md::Book before{4, {}, true}, after{4, {}, true};
  for (const auto side : {md::Side::Buy, md::Side::Sell})
    for (int i = 0; i < 5; ++i) {
      before.levels.push_back(
          {side, side == md::Side::Buy ? -1 - i : 1 + i, 0, 1, 1});
      after.levels.push_back(
          {side, side == md::Side::Buy ? -11 - i : 11 + i, 0, 1, 2});
    }
  const auto changes = md::difference(before, after);
  require(changes.size() == 20, "Rolling window omitted removed prices");
  const auto epoch = md::new_epoch();
  const auto delta = md::encode({md::Kind::DeltaV2, epoch, 4, 1, 123, changes});
  const auto snapshot =
      md::encode({md::Kind::SnapshotV2, epoch, 4, 1, 123, after.levels});
  require(delta.size() == md::max_delta_size &&
              snapshot.size() == md::max_snapshot_size &&
              snapshot.size() < delta.size(),
          "Maximum frame sizing or adaptive replacement failed");
}
} // namespace

int main(int argc, char **argv) {
  demo::Suite suite(argc > 1 ? argv[1] : "spread-market-results");
  suite.test("merge-direct-ab-ac-before-truncation-and-own-candidate", merge);
  suite.test("signed-v2-wire-shard-encoding-and-invalid-aggregate", wire);
  suite.test("client-delta-di-change-pure-implied-and-reordered-delete",
             client_delta);
  suite.test("full-window-replacement-and-maximum-frame-sizes",
             full_replacement);
  return suite.finish();
}
