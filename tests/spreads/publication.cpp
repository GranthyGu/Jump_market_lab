#include "matching_engine/spreads/publication.hpp"
#include "fixtures.hpp"

using namespace spread_test;
namespace s = o::spreads;
namespace {
p::Event event(unsigned member, unsigned ordinal, bool accepted = false) {
  p::Event event;
  event.kind = o::LinkKind::Event;
  event.context = {0, o::LinkKind::Event, accepted ? 99U : 0U, member + 1,
                   accepted ? 1U : 0U};
  auto payload = o::server_record<m::OrderUpdate>(m::MessageType::OrderUpdate,
                                                  accepted ? 99 : 0);
  payload.order.orderId = id(member + 5, ordinal);
  payload.order.status =
      accepted ? m::OrderStatus::Accepted : m::OrderStatus::Filled;
  event.size = sizeof(payload);
  std::memcpy(event.payload.data(), &payload, sizeof(payload));
  return event;
}
struct Part {
  s::Header header;
  o::Bytes bytes;
};
std::vector<Part> parts(std::uint64_t seq, std::uint64_t batch,
                        const std::vector<p::Event> &events) {
  std::vector<Part> result;
  o::Bytes scratch;
  s::send_result({s::MessageKind::DurableResult, 42, seq},
                 {batch, 1, batch + 1, batch * 200, batch}, events, scratch,
                 [&](const auto &h, auto bytes) {
                   result.push_back({h, {bytes.begin(), bytes.end()}});
                 });
  return result;
}
struct Fixture {
  std::vector<std::pair<unsigned, m::OrderUpdate>> events;
  std::vector<std::pair<std::uint64_t, unsigned>> permits;
  s::Publication publication{
      8 * 1024 * 1024,
      {[&](auto member, const auto &event) {
         events.emplace_back(member, o::copy_record<m::OrderUpdate>(
                                         {event.payload.data(), event.size}));
       },
       [&](auto seq, auto mask) { permits.emplace_back(seq, mask); }}};
  Fixture() { publication.initialize(42, 0, {}); }
  void memory(std::uint64_t seq, unsigned mask, unsigned market,
              std::array<std::uint64_t, 4> batches = {1, 1, 1, 1}) {
    o::Bytes bytes;
    p::integer(bytes, mask, 1);
    p::integer(bytes, market, 1);
    for (unsigned i = 0; i < 4; ++i)
      if (mask & (1U << i))
        p::integer(bytes, batches[i], 8);
    publication.memory({s::MessageKind::MemDone, 42, seq}, bytes);
  }
  void result(unsigned member, std::uint64_t seq, std::uint64_t batch,
              bool accepted = false) {
    for (const auto &part : parts(seq, batch, {event(member, batch, accepted)}))
      publication.durable(member, part.header, part.bytes);
  }
};

void reordered() {
  Fixture f;
  f.publication.start(1, 0);
  f.result(2, 1, 1);
  f.result(0, 1, 1, true);
  f.result(1, 1, 1);
  require(f.events.empty() && f.permits.empty(),
          "Results escaped before MEM_DONE");
  f.memory(1, 7, 15);
  require(f.events.size() == 3 && f.events[0].first == 0 &&
              f.events[0].second.order.status == m::OrderStatus::Accepted,
          "Remote fills overtook initiator Accepted");
  require(f.permits ==
                  std::vector<std::pair<std::uint64_t, unsigned>>{{1, 15}} &&
              f.publication.visible()[0].trade_end == 200 &&
              f.publication.visible()[3].trade_end == 0,
          "Market view-only targets or history prefix are wrong");
  require(f.publication.idle(), "Completed command retained result storage");
}

void independent_notifications() {
  Fixture f;
  f.publication.start(1, 0);
  f.memory(1, 7, 15);
  f.result(0, 1, 1, true);
  f.result(1, 1, 1);
  f.publication.start(2, 1);
  f.memory(2, 2, 2, {0, 2, 0, 0});
  f.result(1, 2, 2, true);
  require(f.events.size() == 1 && f.events[0].second.order.orderId == id(6, 2),
          "Later complete command waited on an unrelated durable result");
  require(f.publication.visible()[1].trade_end == 0 && f.permits.empty(),
          "Later notifications exposed a gap in the same shard history/market "
          "prefix");
  f.result(2, 1, 1);
  require(
      f.events.size() == 4 && f.publication.visible()[1].seq == 2 &&
          f.publication.published() == 2 && f.publication.idle(),
      "Closing the earlier gap did not advance the retained command cursors");
  require(f.permits.size() == 1 &&
              f.permits[0] == std::pair<std::uint64_t, unsigned>{2, 15},
          "Contiguous market permits were not coalesced");
}

void independent_history() {
  Fixture f;
  f.publication.start(1, 0);
  f.memory(1, 7, 15);
  f.publication.start(2, 3);
  f.memory(2, 8, 8);
  f.result(3, 2, 1, true);
  require(f.publication.visible()[3].seq == 1 &&
              f.publication.visible()[0].seq == 0 && !f.publication.published(),
          "Unrelated shard history cursor waited for a global prefix");
}

void multipart() {
  Fixture f;
  f.publication.start(1, 0);
  f.memory(1, 1, 1);
  std::vector<p::Event> events;
  for (unsigned i = 0; i < 5000; ++i)
    events.push_back(event(0, i + 1, i == 0));
  const auto messages = parts(1, 1, events);
  require(messages.size() > 1, "Result test did not cross a frame boundary");
  for (unsigned i = 0; i < messages.size(); ++i) {
    const auto &message = messages[i];
    require(message.bytes.size() + o::group::header_size <=
                o::group::maximum_frame,
            "Result chunk exceeded maximum frame");
    f.publication.durable(0, message.header, message.bytes);
    require(f.events.size() == (i + 1 == messages.size() ? events.size() : 0),
            "Multipart result released a partial command");
  }
  for (unsigned i = 0; i < f.events.size(); ++i)
    require(f.events[i].second.order.orderId == id(5, i + 1),
            "Compact event codec changed event order");
  Fixture bad;
  bad.publication.start(1, 0);
  rejects([&] {
    bad.publication.durable(0, messages[1].header, messages[1].bytes);
  });
  bad.publication.durable(0, messages[0].header, messages[0].bytes);
  rejects([&] {
    bad.publication.durable(0, messages[0].header, messages[0].bytes);
  });
  auto stale = messages[1].header;
  --stale.epoch;
  rejects([&] { bad.publication.durable(0, stale, messages[1].bytes); });
}

void admission_and_invalid() {
  Fixture f;
  f.publication.start(1, 0);
  rejects([&] { f.publication.start(2, 0); });
  f.memory(1, 1, 1);
  for (unsigned seq = 2; seq <= s::command_window; ++seq) {
    f.publication.start(seq, 3);
    f.memory(seq, 8, 8, {0, 0, 0, seq - 1});
    f.result(3, seq, seq - 1, true);
  }
  require(!f.publication.capacity() && !f.publication.published(),
          "Bounded command window did not backpressure an old durable gap");
  rejects([&] { f.publication.start(s::command_window + 1, 3); });
  f.result(0, 1, 1, true);
  require(f.publication.idle() && f.publication.capacity() &&
              f.publication.published() == s::command_window,
          "Result completion did not release bounded admission window");
  rejects([&] { f.memory(1, 1, 1); });
  Fixture foreign;
  foreign.publication.start(1, 0);
  foreign.memory(1, 1, 15);
  rejects([&] { foreign.result(3, 1, 1); });
  rejects([&] { foreign.result(0, 1, 2); });
  s::Publication tiny(8, {[](auto, const auto &) {}, [](auto, auto) {}});
  tiny.initialize(42, 0, {});
  tiny.start(1, 0);
  const auto part = parts(1, 1, {event(0, 1, true)})[0];
  rejects([&] { tiny.durable(0, part.header, part.bytes); });
}

void immutable_market() {
  o::MatchingEngine core(config(), 6);
  s::MarketPublication publication(core, config(), 6);
  s::Views views{};
  publication.capture(0, views);
  publication.permit(0);
  o::MarketSnapshot visible(1);
  core.command(context(1), o::record_bytes(order(1, "A", m::Side::Buy, 120, 3)),
               [](auto, const auto &, auto) {});
  views[2] = {{2}, {120}, 1, false};
  publication.capture(1, views);
  core.command(context(2), o::record_bytes(order(2, "A", m::Side::Buy, 121, 7)),
               [](auto, const auto &, auto) {});
  views[2] = {{4}, {122}, 1, false};
  publication.capture(2, views);
  publication.permit(1);
  publication.copy(visible);
  require(visible.books[0].count == 1 &&
              visible.books[0].levels[0].price == 120 &&
              visible.books[0].levels[0].quantity == 3 &&
              visible.books[0].levels[0].implied_quantity == 2 &&
              visible.books[0].levels[0].num_orders == 2,
          "Old permit sampled the newer working direct/implied book");
  publication.permit(2);
  publication.copy(visible);
  require(visible.books[0].count == 3 &&
              visible.books[0].levels[0].price == 122 &&
              visible.books[0].levels[0].implied_quantity == 4 &&
              publication.pending() == 0,
          "New permit did not install its own candidate");
  rejects([&] { publication.permit(1); });
  for (unsigned seq = 3; seq < s::command_window + 4; ++seq)
    publication.capture(seq, views);
  rejects([&] { publication.capture(s::command_window + 4, views); });
  // Replacing the current command's earlier round consumes no additional slot.
  publication.capture(s::command_window + 3, {});
  publication.permit(s::command_window + 3);
  publication.copy(visible);
  require(visible.books[0].count == 2 &&
              !visible.books[0].levels[0].implied_quantity,
          "Final empty view did not replace the earlier round candidate");
}
} // namespace
int main(int argc, char **argv) {
  demo::Suite suite(argc > 1 ? argv[1] : "spread-publication-results");
  suite.test("durable-before-memory-and-accepted-before-peer-fills", reordered);
  suite.test("independent-notifications-with-contiguous-history-and-market",
             independent_notifications);
  suite.test("unrelated-shard-history-has-no-global-barrier",
             independent_history);
  suite.test("multipart-results-no-partial-publication-and-duplicate-fences",
             multipart);
  suite.test("bounded-admission-masks-batch-order-and-byte-budgets",
             admission_and_invalid);
  suite.test("immutable-market-permits-and-bounded-round-coalescing",
             immutable_market);
  return suite.finish();
}
