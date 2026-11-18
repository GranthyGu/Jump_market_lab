#pragma once
#include "shared_data/orders/config.hpp"
#include "shared_data/orders/engine_link.hpp"
#include <atomic>
#include <chrono>
#include <functional>
#include <initializer_list>
#include <memory>
#include <poll.h>
#include <unordered_map>

namespace marketlab::orders {
using Clock = std::chrono::steady_clock;
enum class Framing {
  Client,
  Internal,
  Group
};

// Single-owner nonblocking reactor. No socket operation waits for a peer.
// Linux uses epoll; the portable poll backend keeps macOS tests equivalent.
class TcpReactor {
 public:
  struct Peer {
    ConnectionId id;
    int fd;
    Framing framing;
    bool connecting = false;
    bool read_enabled = true;
    Bytes input, output;
    std::size_t consumed = 0, sent = 0;
    Clock::time_point last_receive = Clock::now(), last_progress = Clock::now();
    Clock::time_point partial_since{};
  };

  using Handler = std::function<void(ConnectionId)>;
  using FrameHandler = std::function<void(ConnectionId, std::span<const std::uint8_t>)>;
  explicit TcpReactor(std::size_t max_buffer = 4 * 1024 * 1024, int partial_timeout_ms = 2000);
  ~TcpReactor();
  TcpReactor(const TcpReactor&) = delete;
  TcpReactor& operator=(const TcpReactor&) = delete;
  void listen(const Address& address, Framing framing, std::size_t limit);
  ConnectionId connect(const Address& address, Framing framing);
  bool send(ConnectionId id, std::span<const std::uint8_t> bytes);
  // Copy all parts before returning. Inputs can be reused immediately and
  // must not alias this connection's output buffer.
  bool send_parts(ConnectionId id, std::initializer_list<std::span<const std::uint8_t>> parts);
  void close(ConnectionId id);
  void enable_reads(ConnectionId id, bool enabled);

  bool contains(ConnectionId id) const {
    return peers_.contains(id);
  }

  void step(int timeout_ms = 10);
  // Borrow a nonblocking wakeup descriptor; callback drains it on this owner.
  // The descriptor and callback captures must outlive all calls to step().
  void watch_wakeup(int fd, std::function<void()> handler);
  void expire();
  std::shared_ptr<Peer> peer(ConnectionId id) const;

  std::size_t size() const {
    return peers_.size();
  }

  std::uint64_t accepted = 0, connects = 0, disconnected = 0, bytes_in = 0, bytes_out = 0;
  Handler on_accept, on_connect, on_close;
  FrameHandler on_frame;

 private:
  void watch(const Peer& p, bool add);
  void flush(const std::shared_ptr<Peer>& p);
  void receive(const std::shared_ptr<Peer>& p);
  std::size_t max_buffer_;
  int partial_timeout_ms_;
  int poller_ = -1;

  struct Listener {
    int fd;
    Framing framing;
  };

  std::vector<Listener> listeners_;
  std::vector<std::pair<int, std::function<void()>>> wakeups_;
  std::size_t connection_limit_ = 0;
  ConnectionId next_id_ = 1;
  std::unordered_map<ConnectionId, std::shared_ptr<Peer>> peers_;
  std::vector<std::pair<ConnectionId, short>> ready_events_;
  std::vector<pollfd> poll_fds_;
  std::vector<ConnectionId> poll_ids_;
};
}  // namespace marketlab::orders
