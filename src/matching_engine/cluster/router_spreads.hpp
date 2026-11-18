#pragma once
#include "matching_engine/spreads/join.hpp"
#include "matching_engine/spreads/links.hpp"
#include "matching_engine/cluster/router_spreads_publication.hpp"

namespace marketlab::orders::spreads {
// Spread-group scheduling and publication inside the main Router process.
// Uses the Router's reactor and existing shard business connections.
class RouterGroup {
 public:
  struct Callbacks {
    std::function<bool(std::uint8_t, const LinkHeader&, std::span<const std::uint8_t>)> send;
    std::function<void(std::uint8_t, const persistence::Event&)> event;
    std::function<void(std::uint64_t token, std::uint64_t sequence)> capture;
  };

  RouterGroup(const OrderConfig&, TcpReactor&, Callbacks);

  bool contains(ConnectionId id) const {
    return links_.contains(id);
  }

  void connected(ConnectionId id) {
    links_.connected(id);
  }

  void lost(ConnectionId id) {
    links_.lost(id);
  }

  void receive(ConnectionId id, std::span<const std::uint8_t> bytes) {
    links_.receive(id, bytes);
  }

  bool enqueue(std::uint8_t member, const LinkHeader&, std::span<const std::uint8_t>);
  void result(std::uint8_t member, const LinkHeader&, std::span<const std::uint8_t>);
  void worker_lost();
  void discard_queued();
  void poll();

  void tick() {
    links_.tick();
  }

  bool ready() const {
    return ready_;
  }

  bool prepared() const {
    return prepared_;
  }

  bool paused() const {
    return !ready_ || maintenance_;
  }

  bool settled() const {
    return publication_.idle() && !capture_mask_;
  }

  std::uint64_t published() const {
    return publication_.published();
  }

  std::size_t queued() const {
    return queued_.size();
  }

  std::pair<std::size_t, std::size_t> queue_peaks() const {
    return {peak_queued_, peak_bytes_};
  }

  void maintenance();
  void fence();

  bool fenced() const {
    return fenced_;
  }

  void replace(std::uint8_t, const Address&);
  void resume();
  void restart_prepare();
  std::string status() const;
  void pause(std::string_view reason);

 private:
  struct Queued {
    std::uint8_t members;
    LinkHeader context;
    Bytes bytes;
  };

  const OrderConfig& config_;
  Topology topology_;
  TcpReactor& io_;
  Callbacks callbacks_;
  Links links_;
  Publication publication_;
  ConnectionId coordinator_ = 0;
  Clock::time_point reconnect_{}, progress_ = Clock::now();
  std::uint64_t epoch_ = 0, join_ = 0;
  bool ready_ = false, prepared_ = false, initialized_ = false, failed_ = false,
       maintenance_ = false, fence_sent_ = false, fenced_ = false, approval_sent_ = false;
  std::uint8_t recovering_ = 0, requested_ = 0, capture_mask_ = 0;
  std::array<Recovery, 4> recovered_{};
  std::uint64_t capture_token_ = 0, capture_sequence_ = 0;
  std::deque<Queued> queued_;
  std::size_t queued_bytes_ = 0;
  std::size_t peak_queued_ = 0, peak_bytes_ = 0;
  Bytes scratch_;
  void control(const FrameView&);
  bool send(std::uint8_t, const Header&, const LinkHeader&, std::span<const std::uint8_t>);
  void recovery_progress();
  void initial_publication();
};
}  // namespace marketlab::orders::spreads
