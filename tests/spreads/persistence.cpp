#include "fixtures.hpp"
#include <unistd.h>

using namespace spread_test;
using namespace std::chrono_literals;

namespace {
struct Page {
  std::vector<o::Bytes> records;
  m::TradeHistoryEnd end{};
};

Page query(p::DurableEngine &engine, unsigned user, std::uint64_t after = 0,
           std::uint64_t through = 0, unsigned limit = 256,
           std::optional<std::uint64_t> visible_end = {}) {
  Page page;
  bool done = false;
  m::TradeHistoryRequest q{
      {m::MessageType::TradeHistoryRequest, sizeof(q), 9000},
      0,
      static_cast<std::uint16_t>(limit),
      after,
      through};
  auto h = context(9000, user);
  h.kind = o::LinkKind::History;
  auto emit = [&](auto kind, const auto &, auto bytes) {
    if (kind == o::LinkKind::HistoryEnd) {
      page.end = o::copy_record<m::TradeHistoryEnd>(bytes);
      done = true;
    } else {
      m::Header header{};
      std::memcpy(&header, bytes.data(), sizeof(header));
      require(m::is_history_record(header.messageType), "History query failed");
      page.records.emplace_back(bytes.begin(), bytes.end());
    }
  };
  if (visible_end)
    engine.public_history(h, q, *visible_end, emit);
  else
    engine.command(h, o::record_bytes(q), emit);
  demo::eventually(
      [&] {
        engine.poll();
        return done;
      },
      "Typed history timed out");
  require(page.end.count == page.records.size(), "Wrong typed page count");
  return page;
}

void rest(p::DurableEngine &engine, unsigned user, unsigned request,
          const char *symbol, m::Side side, m::Price price,
          m::Quantity quantity) {
  const auto q = order(request, symbol, side, price, quantity);
  engine.command(context(request, user), o::record_bytes(q),
                 [](auto, const auto &, auto) {});
  engine.drain();
}

void codec() {
  p::TradeRecord direct{id(5, 1), 1,        name("SP_AB"), -2,         3,
                        id(5, 1), id(5, 2), name("Alice"), name("Bob")};
  p::ImpliedLegTrade leg;
  static_cast<p::TradeRecord &>(leg) = {
      id(6, 1),    2,        name("A"),     110,        3,
      id(5, 1234), id(6, 1), name("Alice"), name("Bob")};
  leg.execution = {id(7, 8765), 1};
  p::SpreadExecution spread{id(5, 2),
                            2,
                            name("SP_AB"),
                            0,
                            3,
                            {id(5, 1), 2},
                            id(5, 1),
                            name("Alice"),
                            m::Side::Buy,
                            {id(6, 1), name("Bob"), 110},
                            {id(7, 1), name("Carol"), 110}};
  for (const p::ExecutionRecord &record :
       {p::ExecutionRecord(direct), p::ExecutionRecord(leg),
        p::ExecutionRecord(spread)}) {
    o::Bytes bytes;
    p::encode_execution(bytes, record);
    p::Reader reader{bytes};
    require(p::decode_execution(reader) == record, "Typed record roundtrip");
    reader.finish();
    for (std::size_t n = 0; n < bytes.size(); ++n)
      rejects([&] {
        p::Reader partial{std::span(bytes).first(n)};
        p::decode_execution(partial);
      });
    bytes[1] ^= 1;
    rejects([&] {
      p::Reader invalid{bytes};
      p::decode_execution(invalid);
    });
  }
  p::OrderDictionary dictionary(config());
  dictionary.validate_execution(leg, {1, 2, 2, 0, 0}, 6);
  rejects([&] { dictionary.validate_execution(leg, {1, 2, 2, 0, 0}, 7); });
  dictionary.validate_execution(spread, {1, 3, 3, 0, 0}, 5);
  spread.execution.aggressor = id(8, 1);
  rejects([&] { dictionary.validate_execution(spread, {1, 3, 3, 0, 0}, 5); });
  spread.execution.aggressor = id(5, 1);
  spread.price = 1;
  o::Bytes bytes;
  p::encode_execution(bytes, spread);
  rejects([&] {
    p::Reader invalid{bytes};
    p::decode_execution(invalid);
  });
}

void mixed_batch(const std::filesystem::path &root) {
  const auto opt = options(root / "old");
  p::Boundary final;
  std::vector<o::Bytes> expected;
  {
    p::DurableEngine engine(config(), 5, opt);
    rest(engine, 2, 1, "SP_AB", m::Side::Sell, -2, 1);
    bool durable = false;
    auto *result = engine.begin_group([&](const auto &boundary, auto events) {
      require(boundary.seq == 2 && boundary.trade_count == 2,
              "One group must seal one batch");
      require(events.size() == 7, "Wrong owned-order event count");
      durable = true;
    });
    require(result, "Unexpected group capacity refusal");
    auto incoming = engine.working().begin_group(
        context(2), order(2, "SP_AB", m::Side::Buy, 0, 3), *result);
    require(incoming.has_value(), "Zero spread rejected");
    std::vector<o::MatchingEngine::FillPiece> pieces;
    engine.working().take_prefix(4, m::Side::Sell, 1, pieces, *result);
    require(pieces.size() == 1 && pieces[0].after.id == id(5, 1),
            "FIFO source mismatch");
    fill(*incoming, 1);
    const auto direct = engine.working().trade_record(
        4, -2, 1, 123, incoming->id, 1, id(5, 1), 2);
    result->add(result->executions, p::ExecutionRecord(direct));
    engine.working().fill_events(*incoming, direct.id, -2, 1, 123, *result);
    engine.working().fill_events(pieces[0].after, direct.id, -2, 1, 123,
                                 *result);
    engine.poll();
    require(!durable && engine.boundary().seq == 1 && engine.group_open(),
            "Unsealed group committed");
    require(std::filesystem::file_size(root / "old/trades/trades.log") == 0,
            "Partial group reached log writer");
    require(query(engine, 1).records.empty(),
            "Unsealed group leaked through history");

    fill(*incoming, 2);
    p::SpreadExecution spread{engine.working().allocate_trade(),
                              124,
                              name("SP_AB"),
                              0,
                              2,
                              {incoming->id, 2},
                              incoming->id,
                              name("Alice"),
                              m::Side::Buy,
                              {id(6, 9999), name("Bob"), 110},
                              {id(7, 9999), name("Carol"), 110}};
    result->add(result->executions, p::ExecutionRecord(spread));
    engine.working().fill_events(*incoming, spread.id, 0, 2, 124, *result);
    engine.working().finish_group(*incoming, *result);
    require(engine.seal_group() == 2, "Wrong local batch sequence");
    engine.drain();
    require(durable && !engine.group_open(), "Sealed group did not complete");
    const auto hidden = query(engine, 1, 0, 0, 256, 0);
    require(hidden.records.empty() && hidden.end.throughOffset == 0,
            "Explicit public boundary zero exposed locally durable trades");
    require(query(engine, 1, 0, 0, 256, engine.boundary().trade_end)
                    .records.size() == 2,
            "Public history did not advance at the exact permitted boundary");
    const auto first = query(engine, 1, 0, 0, 1);
    require(first.end.flags & m::history_more,
            "Mixed record pagination lost tail");
    const auto second =
        query(engine, 1, first.end.nextOffset, first.end.throughOffset, 1);
    require(first.end.nextOffset == p::header_bytes + p::execution_prefix_bytes,
            "Cursor must point at record body");
    require(o::copy_record<m::TradeHistoryRecord>(first.records.at(0)).price ==
                -2,
            "Negative direct trade changed");
    const auto record =
        o::copy_record<m::SpreadExecutionHistory>(second.records.at(0));
    require(record.price == 0 && record.a.orderId == id(6, 9999) &&
                record.fillNo == 2,
            "Combination identity changed in history");
    expected = query(engine, 1).records;
    require(query(engine, 2).records.size() == 1 &&
                query(engine, 3).records.empty(),
            "Combination history leaked to real-leg counterparties");
    final = engine.boundary();
  }
  {
    p::DurableEngine recovered(config(), 5, opt);
    require(recovered.boundary().seq == final.seq &&
                recovered.working().active_order_count(1) == 0,
            "Mixed-frame recovery lost final order state");
    auto actual = query(recovered, 1).records;
    require(actual == expected, "History changed across restart");
  }
  {
    p::DurableEngine replacement(config(), 5, options(root / "new"));
    p::File orders(root / "old/orders/delta-1.log", false),
        trades(root / "old/trades/trades.log", false);
    std::uint64_t oa = 0, ta = 0;
    while (auto batch = p::next_frame(orders, oa)) {
      auto trade = batch->trades ? p::next_frame(trades, ta) : std::nullopt;
      replacement.replicate(*batch, trade);
    }
    replacement.drain();
    require(replacement.boundary().trade_end == final.trade_end,
            "Replication moved history cursors");
    require(query(replacement, 1).records == expected,
            "Replicated history changed");
    p::File original(root / "old/trades/trades.log", false),
        copied(root / "new/trades/trades.log", false);
    require(original.read(0, original.size()) == copied.read(0, copied.size()),
            "Replication rewrote trade layout");
    rest(replacement, 1, 3, "SP_AB", m::Side::Buy, -3, 2);
    replacement.request_checkpoint();
    replacement.wait_checkpoint();
  }
  {
    p::DurableEngine reopened(config(), 5, options(root / "new"));
    const auto source = reopened.working().source_depth(4, m::Side::Buy);
    require(source.count == 1 && source.price[0] == -3 &&
                source.quantity[0] == 2,
            "Signed snapshot price failed recovery");
  }
}

void leg_history(const std::filesystem::path &root) {
  p::DurableEngine engine(config(), 6, options(root));
  rest(engine, 2, 1, "A", m::Side::Sell, 110, 3);
  auto *result = engine.begin_group([](const auto &, auto) {});
  std::vector<o::MatchingEngine::FillPiece> pieces;
  engine.working().take_prefix(1, m::Side::Sell, 3, pieces, *result);
  p::ImpliedLegTrade leg;
  static_cast<p::TradeRecord &>(leg) = engine.working().trade_record(
      1, 110, 3, 125, id(5, 1000), 1, id(6, 1), 2);
  leg.execution = {id(7, 4000), 1};
  result->add(result->executions, p::ExecutionRecord(leg));
  engine.working().fill_events(pieces[0].after, leg.id, 110, 3, 125, *result);
  engine.seal_group();
  engine.drain();
  for (unsigned user : {1, 2}) {
    const auto page = query(engine, user);
    const auto record =
        o::copy_record<m::ImpliedLegHistory>(page.records.at(0));
    require(record.aggressorOrderId == id(7, 4000) &&
                record.trade.buyOrderId == id(5, 1000),
            "Remote order was truncated/validated as local");
  }
  require(query(engine, 3).records.empty(),
          "Unrelated leg account saw history");
}
} // namespace

int main(int argc, char **argv) {
  const auto output =
      std::filesystem::path(argc > 1 ? argv[1] : "spread-persistence-results");
  demo::Suite suite(output);
  const auto root = output / std::to_string(getpid());
  suite.test("typed-record-roundtrip-truncation-and-ownership", codec);
  suite.test("unsealed-mixed-batch-history-recovery-and-byte-exact-replication",
             [&] { mixed_batch(root / "mixed"); });
  suite.test("leg-history-both-parties-and-remote-counters",
             [&] { leg_history(root / "leg"); });
  return suite.finish();
}
