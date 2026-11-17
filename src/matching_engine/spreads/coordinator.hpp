#pragma once
#include "matching_engine/spreads/protocol.hpp"

namespace marketlab::orders::spreads {
inline constexpr std::uint8_t router_destination = 4;

constexpr std::uint8_t member_targets(std::uint8_t member) {
  return member == 0 ? (1U << AB) | (1U << AC) : member <= 3 ? 1U << (member - 1) : 0;
}

constexpr std::uint16_t member_sources(std::uint8_t member) {
  std::uint16_t mask = 0;
  const auto targets = member_targets(member);
  for (unsigned slot = 0; slot < 5; ++slot)
    if (targets & (1U << slot))
      mask |= 3U << (2 * slot);
  return mask;
}

// Single-owner control state. No command FIFO, remote orders, trade identities,
// result payloads or persistence threads live here.
class Coordinator {
 public:
  using Send =
      std::function<void(std::uint8_t destination, const Header&, std::span<const std::uint8_t>)>;
  Coordinator(std::uint64_t epoch, Send);
  void initialize(const Sources&, const std::array<std::uint64_t, 4>& local_batches);
  void receive(std::uint8_t member, const Header&, std::span<const std::uint8_t>);
  void pause(std::string_view reason);

  bool ready() const {
    return ready_;
  }

  bool in_flight() const {
    return active_seq_ != 0;
  }

  std::uint64_t completed() const {
    return completed_;
  }

  std::uint64_t epoch() const {
    return epoch_;
  }

  const Sources& sources() const {
    return sources_;
  }

  const Views& views() const {
    return views_;
  }

  const std::array<std::uint64_t, 4>& batches() const {
    return batches_;
  }

  struct Counters {
    std::uint64_t rounds = 0, local_complete = 0, memory_done = 0, route_recomputations = 0,
                  view_installs = 0;
  } counters;

 private:
  std::uint64_t epoch_, completed_ = 0, active_seq_ = 0;
  std::uint32_t round_ = 0;
  std::uint8_t initiator_ = no_slot, expected_ = 0, done_ = 0, participants_ = 0, ended_ = 0,
               market_members_ = 0, awaiting_views_ = 0;
  std::uint16_t dirty_ = 0;
  bool ready_ = false, initializing_ = false, declared_ = false, round_ready_ = false;
  Sources sources_{};
  Views views_{};
  std::array<std::uint64_t, 4> batches_{};
  Send send_;
  Bytes scratch_;
  void prepare_round(const Header&);
  void install(std::uint8_t targets, bool initial);
  void finish_sources();
  void finish_views();
  void memory_done();
  void reset_round();
};
}  // namespace marketlab::orders::spreads
