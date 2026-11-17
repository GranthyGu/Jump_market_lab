#pragma once
#include <deque>
#include "matching_engine/spreads/market.hpp"
#include "matching_engine/spreads/protocol.hpp"

namespace marketlab::orders::spreads {
inline constexpr std::size_t command_window = 128;

// Multipart results keep existing client event bytes. Unsolicited events omit
// token/session (both zero), and boundaries occur only in the first part.
using SendPart = std::function<void(const Header&, std::span<const std::uint8_t>)>;
void send_result(const Header&,
                 const persistence::Boundary&,
                 std::span<const persistence::Event>,
                 Bytes& scratch,
                 const SendPart&);

class Publication {
 public:
  struct Callbacks {
    std::function<void(std::uint8_t, const persistence::Event&)> event;
    std::function<void(std::uint64_t, std::uint8_t)> market;
  };

  Publication(std::size_t bytes, Callbacks);
  void initialize(std::uint64_t epoch,
                  std::uint64_t sequence,
                  const std::array<persistence::Boundary, 4>&);

  bool capacity() const {
    return rows_.size() < command_window && bytes_ < budget_;
  }

  bool idle() const {
    return rows_.empty();
  }

  void start(std::uint64_t sequence, std::uint8_t initiator);
  void memory(const Header&, std::span<const std::uint8_t>);
  void durable(std::uint8_t member, const Header&, std::span<const std::uint8_t>);

  std::uint64_t completed() const {
    return memory_;
  }

  std::uint64_t published() const {
    return published_;
  }

  std::uint64_t dispatched() const {
    return dispatched_;
  }

  const auto& visible() const {
    return visible_;
  }

 private:
  struct Result {
    persistence::Boundary boundary;
    Bytes events;
    std::uint32_t next_part = 0;
    bool complete = false;
  };

  struct Row {
    std::uint64_t seq;
    std::uint8_t initiator, participants = 0, market = 0;
    std::array<std::uint64_t, 4> batches{};
    std::array<Result, 4> results;
    bool ready = false;
  };

  std::size_t budget_, bytes_ = 0;
  Callbacks callbacks_;
  std::uint64_t epoch_ = 0, memory_ = 0, published_ = 0, dispatched_ = 0;
  // Four cursors into the same command rows, not four publication queues.
  std::array<std::uint64_t, 4> cursor_{}, received_{}, assembling_{};
  std::array<persistence::Boundary, 4> visible_{};
  std::deque<Row> rows_;
  Row& row(std::uint64_t);
  void finish(Row&);
  void advance();
};

// Owner-thread immutable market candidates. All storage is allocated at startup;
// a later permit can never sample the newer working order book.
class MarketPublication {
 public:
  MarketPublication(MatchingEngine&, const OrderConfig&, std::uint16_t shard);
  void reset();
  void capture(std::uint64_t sequence, const Views&);
  void permit(std::uint64_t sequence);
  void copy(MarketSnapshot&) const noexcept;

  std::size_t pending() const {
    return size_;
  }

 private:
  struct Candidate {
    std::uint64_t seq = 0;
    MarketSnapshot snapshot;

    explicit Candidate(std::size_t symbols)
        : snapshot(symbols) {
    }
  };

  MatchingEngine& core_;
  Topology topology_;
  MarketSnapshot visible_;
  std::vector<Candidate> slots_;
  std::size_t head_ = 0, size_ = 0;
  std::uint64_t permitted_ = 0;
};
}  // namespace marketlab::orders::spreads
