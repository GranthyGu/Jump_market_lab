#pragma once
#include "matching_engine/spreads/session.hpp"
#include "matching_engine/spreads/coordinator/coordinator.hpp"
#include "fixtures.hpp"
#include <deque>

namespace spread_test::session_fixture {
namespace s = o::spreads;

struct Message {
  unsigned from, to;
  s::Header header;
  o::Bytes body;
};

struct Receipt {
  unsigned member;
  std::uint64_t sequence;
  p::Boundary boundary;
  std::vector<p::Event> events;
};

struct Fixture {
  o::OrderConfig config = spread_test::config();
  s::Topology topology{config};
  std::filesystem::path root;
  std::array<std::unique_ptr<p::DurableEngine>, 4> engines;
  std::array<std::unique_ptr<s::Session>, 4> sessions;
  std::unique_ptr<s::Coordinator> coordinator;
  std::deque<Message> messages;
  std::vector<Message> delivered;
  std::vector<Receipt> receipts;
  std::array<std::uint64_t, 4> captures{};
  unsigned request = 0;
  std::uint64_t sequence = 0;
  bool hold_details = false, hold_execution = false;
  bool hold_views = false, hold_view_acks = false;
  bool reorder_links = false;
  std::uint64_t delivery_state = 1;

  explicit Fixture(std::filesystem::path path)
      : root(std::move(path)) {
    for (unsigned i = 0; i < 4; ++i)
      engines[i] = std::make_unique<p::DurableEngine>(
          config, topology.shards[i], options(root / std::to_string(i)));
  }

  void send(unsigned from, unsigned to, const s::Header& h, std::span<const std::uint8_t> body) {
    messages.push_back({from, to, h, {body.begin(), body.end()}});
  }

  m::OrderId seed(unsigned slot, m::Side side, m::Price price, int quantity, unsigned user = 2) {
    const auto member = topology.owner(slot);
    auto& engine = *engines[member];
    const auto q = order(++request,
                         config.products[topology.symbols[slot] - 1].symbol.c_str(),
                         side,
                         price,
                         quantity);
    engine.command(context(request, user), o::record_bytes(q), [](auto, const auto&, auto) {
    });
    return id(topology.shards[member], engine.working().next_order() - 1);
  }

  void start() {
    s::Sources sources{};
    std::array<std::uint64_t, 4> batches{};
    for (unsigned i = 0; i < 4; ++i) {
      engines[i]->drain();
      batches[i] = engines[i]->boundary().seq;
      sessions[i] = std::make_unique<s::Session>(
          *engines[i],
          config,
          topology.shards[i],
          s::Session::Callbacks{[&, i](const auto& h, auto body) {
                                  send(i, 4, h, body);
                                },
                                [&, i](auto to, const auto& h, auto body) {
                                  send(i, to, h, body);
                                },
                                [&, i](auto seq, const auto& boundary, auto events) {
                                  receipts.push_back(
                                      {i, seq, boundary, {events.begin(), events.end()}});
                                },
                                [&, i](auto seq) {
                                  captures[i] = seq;
                                }});
      sessions[i]->epoch(42);
      for (unsigned source = 0; source < s::source_count; ++source)
        if (s::member_sources(i) & (1U << source))
          sources[source] = sessions[i]->sources()[source];
    }
    coordinator = std::make_unique<s::Coordinator>(42, [&](auto to, const auto& h, auto body) {
      send(4, to == s::router_destination ? 5 : to, h, body);
    });
    coordinator->initialize(sources, batches);
    pump();
    require(coordinator->ready(), "Initial views did not install");
    delivered.clear();
  }

  void pump() {
    unsigned steps = 0;
    while (!messages.empty()) {
      auto selected = messages.end();
      unsigned eligible = 0;
      for (auto it = messages.begin(); it != messages.end(); ++it) {
        if (hold_details && it->header.kind == s::MessageKind::ResultDetails)
          continue;
        if (hold_execution && it->header.kind == s::MessageKind::ExecutionDone)
          continue;
        if ((hold_views && it->header.kind == s::MessageKind::ImpliedReplace) ||
            (hold_view_acks && it->header.kind == s::MessageKind::ViewApplied))
          continue;
        // Each TCP direction stays ordered even when a different link overtakes
        // it.
        if (std::any_of(messages.begin(), it, [&](const auto& p) {
              return p.from == it->from && p.to == it->to;
            }))
          continue;
        if (!reorder_links) {
          selected = it;
          break;
        }
        delivery_state = delivery_state * 6364136223846793005ULL + 1442695040888963407ULL;
        if ((delivery_state >> 32) % ++eligible == 0)
          selected = it;
      }
      if (selected == messages.end())
        return;
      const auto message = std::move(*selected);
      messages.erase(selected);
      delivered.push_back(message);
      if (message.to == 4)
        coordinator->receive(message.from, message.header, message.body);
      else if (message.to < 4) {
        if (message.from == 4)
          sessions[message.to]->control(message.header, message.body);
        else
          require(sessions[message.to]->peer(message.from, message.header, message.body),
                  "Unexpected persistence backpressure in fixture");
      }
      if (++steps > 200000)
        throw std::runtime_error("Group state machine failed to make progress");
    }
  }

  void run(unsigned slot, m::Side side, m::Price price, int quantity, unsigned user = 1) {
    const auto q = order(++request,
                         config.products[topology.symbols[slot] - 1].symbol.c_str(),
                         side,
                         price,
                         quantity);
    const s::Header h{s::MessageKind::Run, 42, ++sequence};
    require(sessions[topology.owner(slot)]->run(h, context(request, user), o::record_bytes(q)),
            "Unexpected RUN backpressure");
    pump();
  }

  void cancel(unsigned member, m::OrderId order) {
    m::CancelRequest q{{m::MessageType::CancelRequest, sizeof(q), ++request}, order};
    auto h = context(request, 2);
    h.kind = o::LinkKind::Cancel;
    require(sessions[member]->run({s::MessageKind::Run, 42, ++sequence}, h, o::record_bytes(q)),
            "Unexpected cancel backpressure");
    pump();
  }

  void settle() {
    pump();
    require(coordinator->completed() == sequence && !coordinator->in_flight(),
            "Command did not reach memory completion");
    for (unsigned i = 0; i < 4; ++i) {
      require(!sessions[i]->active(), "Memory completion left an open Result");
      engines[i]->drain();
      require(engines[i]->boundary().seq == coordinator->batches()[i],
              "Durable/local batch order diverged from Coordinator");
    }
  }

  std::vector<p::ExecutionRecord> records(unsigned member) {
    std::vector<p::ExecutionRecord> result;
    p::File file(root / std::to_string(member) / "trades/trades.log", false);
    std::uint64_t offset = 0;
    while (auto frame = p::next_frame(file, offset))
      for (const auto& record : p::decode_trades(*frame))
        result.push_back(record.record);
    return result;
  }
};

}  // namespace spread_test::session_fixture
