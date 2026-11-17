#pragma once
#include "matching_engine/persistence/durable_engine.hpp"
#include "matching_engine/spreads/coordinator/coordinator.hpp"
#include "matching_engine/spreads/execution.hpp"

namespace marketlab::orders::spreads {
// One owner thread; callbacks copy/queue messages and never reenter the session.
// Actual mutations reuse MatchingEngine. One Result stays open across rounds.
class Session {
 public:
  struct Callbacks {
    std::function<void(const Header&, std::span<const std::uint8_t>)> coordinator;
    std::function<void(std::uint8_t, const Header&, std::span<const std::uint8_t>)> peer;
    std::function<void(
        std::uint64_t, const persistence::Boundary&, std::span<const persistence::Event>)>
        durable;
    std::function<void(std::uint64_t)> capture;
  };

  Session(persistence::DurableEngine&, const OrderConfig&, std::uint16_t shard, Callbacks);
  void epoch(std::uint64_t);
  void recover_sources();
  // False means persistence backpressure, before accepting/mutating the command.
  bool run(const Header&, const LinkHeader&, std::span<const std::uint8_t>);
  bool peer(std::uint8_t from, const Header&, std::span<const std::uint8_t>);
  void control(const Header&, std::span<const std::uint8_t>);

  bool active() const {
    return result_ != nullptr;
  }

  const Views& views() const {
    return views_;
  }

  const Sources& sources() const {
    return sources_;
  }

  std::uint8_t member() const {
    return member_;
  }

  std::uint64_t sequence() const {
    return current_.seq;
  }

 private:
  using FillPiece = MatchingEngine::FillPiece;

  struct Details {
    std::vector<SourcePiece> pieces;
    std::uint64_t quantity = 0;
    bool complete = false;
  };

  persistence::DurableEngine& durable_;
  MatchingEngine& core_;
  Topology topology_;
  Callbacks callbacks_;
  std::size_t users_, budget_;
  std::uint8_t member_, origin_ = no_slot, participants_ = 0, round_members_ = 0, executed_ = 0;
  std::uint64_t epoch_ = 0, closed_ = 0, view_sequence_ = 0;
  std::uint32_t view_round_ = 0;
  Header current_{};
  persistence::Result* result_ = nullptr;
  // incoming_ tracks confirmed real execution; pairing_ builds incremental
  // owned-order events while detail transfer runs behind the source refresh.
  persistence::CompactOrder incoming_{}, pairing_{};
  bool initiator_ = false, finished_ = false, round_ready_ = false, details_ready_ = false,
       allocations_complete_ = true;
  Quantity remote_quantity_ = 0;
  std::uint32_t fill_ = 0;
  Plan plan_{};
  Sources sources_{};
  Views views_{};
  std::vector<FillPiece> direct_;
  std::array<std::vector<FillPiece>, 12> local_;
  std::array<std::uint32_t, 12> allocation_at_{};
  std::array<std::array<Details, 2>, 12> details_;
  std::array<Bytes, 4> allocations_;
  Bytes scratch_;
  bool begin(const Header&, std::uint8_t origin);
  void reset_round();
  void select_round();
  void declare_round(std::uint8_t participants);
  Sources real_sources() const;
  bool sources_changed() const;
  void source_done();
  void complete_local();
  void close();
  void seal(MessageKind);
  void advance();
  void take(std::uint16_t symbol, Side, Quantity, std::vector<FillPiece>&);
  std::size_t buffer_bytes() const;
  void check_budget() const;
  void pair_details();
  void direct_fill(FillPiece&, Quantity);
  void fill_incoming(persistence::CompactOrder&, Quantity);
  std::uint32_t next_fill();
  void send_allocations(std::uint8_t member, bool final);
  void queue_allocation(std::uint8_t route,
                        unsigned source,
                        std::uint32_t index,
                        const Combination&,
                        const SourcePiece& other);
  bool execute(std::uint8_t, const Header&, std::span<const std::uint8_t>);
  void receive_details(std::uint8_t, const Header&, std::span<const std::uint8_t>);
  void receive_allocations(std::uint8_t, const Header&, std::span<const std::uint8_t>);
  void send_details(std::uint8_t route);
  void validate_identity(OrderId, std::uint32_t, std::uint8_t owner) const;
  Header message(MessageKind kind, std::uint8_t flags = 0) const;
};
}  // namespace marketlab::orders::spreads
