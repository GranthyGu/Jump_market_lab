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
  o::MarketSnapshot real(1);
  core.copy_market(real);
  const auto snapshot = s::merged_market(real.books[0], s::A, views);
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
  const auto updated = book(s::merged_market(real.books[0], s::A, views));
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

void candidate_refresh() {
  o::MatchingEngine core(config(), 6);
  auto add = [&](unsigned request, int price, int quantity) {
    core.command(context(request), o::record_bytes(order(request, "A", m::Side::Sell, price, quantity)),
                 [](auto, const auto &, auto) {});
  };
  for (unsigned i = 0; i < 5; ++i)
    add(i + 1, 120 + i, 2);
  const auto source = core.source_depth(1, m::Side::Sell);
  s::Views views{};
  views[3] = {{4}, {119}, 1, false};
  s::MarketPublication publication(core, config(), 6);
  publication.capture(1, views);
  publication.capture(1, views);
  require(publication.pending() == 1, "Unchanged recapture consumed another slot");
  // Fourth-level quantity and first-level order count change without changing top3.
  add(6, 123, 7);
  m::CancelRequest cancel{{m::MessageType::CancelRequest, sizeof(cancel), 7}, id(6, 1)};
  auto cancel_context = context(7);
  cancel_context.kind = o::LinkKind::Cancel;
  core.command(cancel_context, o::record_bytes(cancel), [](auto, const auto &, auto) {});
  add(8, 120, 1);
  add(9, 120, 1);
  require(source.same_levels(core.source_depth(1, m::Side::Sell)),
          "Test must preserve source top3 and hasMore");
  publication.capture(1, views);
  views[3] = {};
  publication.capture(1, views);
  publication.capture(1, views);
  add(10, 124, 3);
  publication.capture(2, views);
  publication.permit(1);
  o::MarketSnapshot visible(1);
  publication.copy(visible);
  const auto &first = visible.books[0];
  require(first.count == 5 && first.levels[0].price == 120 &&
              first.levels[0].quantity == 2 && first.levels[0].num_orders == 2 &&
              first.levels[0].implied_quantity == 0 && first.levels[3].quantity == 9 &&
              first.levels[4].quantity == 2,
          "Recapture lost five-level/count changes, empty implied, or old command isolation");
  publication.permit(2);
  publication.copy(visible);
  require(visible.books[0].levels[4].quantity == 5, "New candidate retained previous raw levels");
  publication.reset();
  publication.capture(0, views);
  publication.permit(0);
  publication.copy(visible);
  require(visible.books[0].count == 5 && visible.books[0].levels[4].quantity == 5,
          "Reset incorrectly reused another candidate's cached inputs");
}

void capture_cost(demo::Suite &suite) {
  o::MatchingEngine core(config(), 6);
  for (unsigned i = 0; i < 5; ++i) {
    for (const auto side : {m::Side::Buy, m::Side::Sell}) {
      const auto price = side == m::Side::Buy ? 119 - i : 121 + i;
      const auto request = 2 * i + (side == m::Side::Buy ? 1 : 2);
      core.command(context(request), o::record_bytes(order(request, "A", side, price, 3)),
                   [](auto, const auto &, auto) {});
    }
  }
  s::Views views{};
  views[2] = {{8, 2}, {119, 118}, 2, false};
  views[3] = {{8, 2}, {121, 122}, 2, false};
  s::MarketPublication publication(core, config(), 6);
  for (unsigned i = 0; i < 1000; ++i)
    publication.capture(1, views);
  std::vector<double> samples;
  const auto start = demo::Clock::now();
  for (unsigned batch = 0; batch < 1000; ++batch) {
    const auto begin = demo::Clock::now();
    for (unsigned i = 0; i < 1000; ++i)
      publication.capture(1, views);
    samples.push_back(demo::us(begin) / 1000);
  }
  const auto seconds = demo::us(start) / 1e6;
  publication.permit(1);
  o::MarketSnapshot visible(1);
  publication.copy(visible);
  require(visible.books[0].count == 10 && visible.books[0].levels[0].quantity == 3 &&
              visible.books[0].levels[0].implied_quantity == 8,
          "Repeated capture changed the published five-level candidate");
  suite.metric("market-same-command-unchanged-capture", samples, seconds, 1000000,
               "Owner-thread repeated capture with unchanged real five levels and implied; "
               "1000 warmup calls, samples are batch averages over 1000 calls; no TCP/fsync");
}
} // namespace

int main(int argc, char **argv) {
  demo::Suite suite(argc > 1 ? argv[1] : "spread-market-results");
  if (argc > 2) {
    require(argc == 3 && std::string_view(argv[2]) == "--benchmark", "Unknown market test option");
    suite.test("warmed-repeated-unchanged-market-capture", [&] { capture_cost(suite); });
    return suite.finish();
  }
  suite.test("merge-direct-ab-ac-before-truncation-and-own-candidate", merge);
  suite.test("signed-v2-wire-shard-encoding-and-invalid-aggregate", wire);
  suite.test("client-delta-di-change-pure-implied-and-reordered-delete",
             client_delta);
  suite.test("full-window-replacement-and-maximum-frame-sizes",
             full_replacement);
  suite.test("same-command-five-level-order-count-empty-view-and-reset-refresh", candidate_refresh);
  return suite.finish();
}
