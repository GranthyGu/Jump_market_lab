#include "matching_engine/spreads/coordinator.hpp"
#include <utility>
#include "fixtures.hpp"

using namespace spread_test;
namespace s = o::spreads;

namespace {
struct Message {
  std::uint8_t destination;
  s::Header header;
  o::Bytes body;
};

struct Fixture {
  std::vector<Message> sent;
  s::Coordinator coordinator{
      42, [&](auto member, const auto &header, auto body) {
        sent.push_back({member, header, {body.begin(), body.end()}});
      }};

  void ack_views() {
    const auto messages = std::exchange(sent, {});
    for (const auto &message : messages) {
      if (message.header.kind != s::MessageKind::ImpliedReplace) {
        sent.push_back(message);
        continue;
      }
      auto header = message.header;
      header.kind = s::MessageKind::ViewApplied;
      const std::uint8_t targets = message.body.at(0);
      coordinator.receive(message.destination, header, {&targets, 1});
    }
  }

  void start(const s::Sources &sources = {}) {
    coordinator.initialize(sources, {});
    require(!coordinator.ready() && sent.size() == 4,
            "Group became ready before initial view installs");
    ack_views();
    require(coordinator.ready() &&
                sent.back().header.kind == s::MessageKind::Ready,
            "Initial installation did not produce group readiness");
    sent.clear();
  }

  void begin(unsigned member, std::uint64_t seq, unsigned round,
             std::uint8_t participants) {
    coordinator.receive(member, {s::MessageKind::RoundBegin, 42, seq, round},
                        {&participants, 1});
  }

  void source(unsigned member, std::uint64_t seq, unsigned round,
              const s::Sources &after, std::uint16_t mask) {
    o::Bytes body;
    s::put_sources(body, mask, coordinator.sources(), after, false);
    coordinator.receive(member, {s::MessageKind::SourceDone, 42, seq, round},
                        body);
  }

  void source(unsigned member, std::uint64_t seq, unsigned round) {
    source(member, seq, round, coordinator.sources(), 0);
  }

  void end(unsigned member, std::uint64_t seq, unsigned round,
           std::uint64_t batch) {
    o::Bytes body;
    p::integer(body, batch, 8);
    coordinator.receive(member, {s::MessageKind::EndAck, 42, seq, round}, body);
  }

  void local(unsigned member, std::uint64_t seq, std::uint64_t batch) {
    o::Bytes body;
    p::integer(body, batch, 8);
    coordinator.receive(member, {s::MessageKind::LocalComplete, 42, seq}, body);
  }
};

void overlap() {
  s::Sources sources{};
  sources[2 * s::A + s::Ask] = {0, {5}, {120}, 1, false};
  sources[2 * s::B + s::Bid] = {0, {5}, {110}, 1, false};
  sources[2 * s::C + s::Bid] = {0, {5}, {105}, 1, false};
  sources[2 * s::AB + s::Bid] = {0, {5}, {10}, 1, false};
  sources[2 * s::AC + s::Bid] = {0, {5}, {15}, 1, false};
  Fixture f;
  f.start(sources);
  auto next = sources;
  next[1].quantity[0] = 3;
  next[1].version = 1;
  // An A -> Coordinator source packet can beat B -> Coordinator ROUND_BEGIN.
  f.source(1, 1, 1, next, 1U << 1);
  require(f.sent.empty(), "Early source completion released a round");
  f.begin(2, 1, 1, 7);
  next = f.coordinator.sources();
  next[6].quantity[0] = 3;
  next[6].version = 1;
  f.source(0, 1, 1, next, 1U << 6);
  f.source(2, 1, 1);
  require(!f.sent.empty() && f.coordinator.in_flight(),
          "Completed sources did not refresh views");
  require(std::all_of(f.sent.begin(), f.sent.end(),
                      [](const auto &m) {
                        return m.header.kind == s::MessageKind::ImpliedReplace;
                      }),
          "Round completed before view acknowledgements");
  const auto installs = f.sent;
  require(std::any_of(installs.begin(), installs.end(),
                      [](const auto &m) { return m.destination == 3; }),
          "C view-only participant was omitted");
  f.ack_views();
  require(f.sent.size() == 1 && f.sent[0].destination == 2 &&
              f.sent[0].header.kind == s::MessageKind::RoundReady,
          "Source refresh must proceed independently of trade detail pairing");
  require(f.coordinator.counters.memory_done == 0,
          "RoundReady sealed a command");
  f.sent.clear();
  f.end(0, 1, 1, 1);
  f.end(2, 1, 1, 1);
  require(f.sent.empty(), "Memory completion missed a real participant");
  f.end(1, 1, 1, 1);
  require(f.sent.size() == 1 &&
              f.sent[0].header.kind == s::MessageKind::MemDone,
          "All sealed batches did not produce memory completion");
  p::Reader body{f.sent[0].body};
  require(body.number(1) == 7 && body.number(1) == 15,
          "Real participants and view-only publication targets were conflated");
  for (unsigned i = 0; i < 3; ++i)
    require(body.number(8) == 1, "Local batch identity missing from MemDone");
  body.finish();
  require(f.coordinator.batches()[3] == 0,
          "View-only C was assigned a persistence batch");
}

void cumulative() {
  Fixture f;
  f.start();
  f.begin(1, 1, 1, 7);
  f.source(0, 1, 1);
  f.source(1, 1, 1);
  f.source(2, 1, 1);
  require(f.sent.size() == 1 &&
              f.sent[0].header.kind == s::MessageKind::RoundReady,
          "Unchanged sources should not install redundant views");
  rejects([&] { f.local(1, 1, 1); });
  f.sent.clear();
  f.source(3, 1, 2);
  f.begin(1, 1, 2, 11);
  f.source(0, 1, 2);
  f.source(1, 1, 2);
  require(f.coordinator.counters.rounds == 2 &&
              f.coordinator.counters.memory_done == 0,
          "A second round split the command into multiple memory completions");
  f.sent.clear();
  for (unsigned member : {0, 1, 3})
    f.end(member, 1, 2, 1);
  require(f.sent.empty(), "Earlier-round B participant was forgotten");
  f.end(2, 1, 2, 1);
  require(f.sent.size() == 1 && f.sent[0].body.at(0) == 15 &&
              f.coordinator.completed() == 1,
          "Cumulative participants were not completed once");
}

void local_and_fences() {
  Fixture f;
  f.start();
  f.local(3, 1, 1);
  require(f.sent.size() == 1 &&
              f.sent[0].header.kind == s::MessageKind::MemDone &&
              f.sent[0].header.round == 0 && f.coordinator.counters.rounds == 0,
          "Local completion took an ordinary round trip");
  rejects([&] { f.local(3, 1, 1); });
  rejects([&] { f.local(3, 3, 2); });
  const std::uint8_t mask = 2;
  rejects([&] {
    f.coordinator.receive(1, {s::MessageKind::RoundBegin, 41, 2, 1},
                          {&mask, 1});
  });
  f.sent.clear();
  f.begin(1, 2, 1, mask);
  rejects([&] { f.begin(1, 3, 1, mask); });
  rejects([&] { f.local(1, 2, 1); });
  f.coordinator.pause("peer lost during open command");
  require(!f.coordinator.ready() && f.coordinator.in_flight(),
          "Pause discarded unresolved real execution");
  rejects([&] { f.coordinator.initialize({}, {}); });
  Fixture clean;
  clean.start();
  clean.local(0, 1, 1);
  clean.coordinator.pause("clean reconnect");
  rejects([&] { clean.coordinator.initialize({}, {}); });
  clean.coordinator.initialize({}, {1, 0, 0, 0});
  clean.ack_views();
  require(clean.coordinator.ready() && clean.coordinator.completed() == 1,
          "Clean reconnect lost the memory-complete boundary");
}

void empty_replacement() {
  s::Sources sources{};
  sources[1] = {0, {1}, {120}, 1, false};
  sources[2] = {0, {1}, {110}, 1, false};
  Fixture f;
  f.start(sources);
  sources[1] = {1, {}, {}, 0, false};
  f.begin(1, 1, 1, 2);
  f.source(1, 1, 1, sources, 1U << 1);
  require(f.sent.size() == 1 && f.sent[0].destination == 0,
          "Empty target was not explicitly replaced");
  s::Views installed{};
  p::Reader reader{f.sent[0].body};
  s::read_views(reader, installed, s::member_targets(0));
  reader.finish();
  require(installed[1].count == 0 && !installed[1].more,
          "Empty implied replacement retained liquidity");
  f.ack_views();
  f.end(1, 1, 1, 1);
  require(f.coordinator.counters.memory_done == 1,
          "Empty replacement blocked command completion");
}
} // namespace

int main(int argc, char **argv) {
  demo::Suite suite(argc > 1 ? argv[1] : "spread-coordinator-results");
  suite.test("early-source-overlap-view-only-target-and-memory-completion",
             overlap);
  suite.test(
      "multi-round-cumulative-participants-and-no-redundant-view-install",
      cumulative);
  suite.test("local-complete-epoch-sequence-fences-and-unresolved-pause",
             local_and_fences);
  suite.test("explicit-empty-implied-replacement", empty_replacement);
  suite.test("million-local-completion-control-messages", [&] {
    std::uint64_t completions = 0;
    std::vector<std::pair<std::uint8_t, s::Header>> acks;
    s::Coordinator coordinator(
        42, [&](auto destination, const auto &header, auto) {
          if (header.kind == s::MessageKind::ImpliedReplace)
            acks.emplace_back(destination, header);
          if (header.kind == s::MessageKind::MemDone)
            ++completions;
        });
    coordinator.initialize({}, {});
    for (auto [member, header] : acks) {
      header.kind = s::MessageKind::ViewApplied;
      const auto targets = s::member_targets(member);
      coordinator.receive(member, header, {&targets, 1});
    }
    std::vector<double> samples;
    std::array<std::uint64_t, 4> batches{};
    o::Bytes body;
    body.reserve(8);
    const auto start = demo::Clock::now();
    for (unsigned batch = 0; batch < 1000; ++batch) {
      const auto lap = demo::Clock::now();
      for (unsigned i = 0; i < 1000; ++i) {
        const std::uint64_t seq = std::uint64_t(batch) * 1000 + i + 1;
        const auto member = std::uint8_t(seq % 4);
        body.clear();
        p::integer(body, ++batches[member], 8);
        coordinator.receive(member, {s::MessageKind::LocalComplete, 42, seq},
                            body);
      }
      samples.push_back(demo::us(lap) / 1000);
    }
    require(completions == 1000000 && coordinator.completed() == 1000000,
            "Short-path stress lost memory completions");
    suite.metric("coordinator-local-complete", std::move(samples),
                 demo::us(start) / 1e6, 1000000,
                 "Control state validation and MemDone serialization; batch "
                 "averages, no transport or fsync");
  });
  return suite.finish();
}
