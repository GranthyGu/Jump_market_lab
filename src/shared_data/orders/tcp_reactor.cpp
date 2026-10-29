#include "shared_data/orders/tcp_reactor.hpp"
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>
#ifdef __linux__
#include <sys/epoll.h>
#endif
#include <algorithm>
#include <cerrno>
#include <limits>

namespace marketlab::orders {
namespace {
sockaddr_in socket_address(const Address& address) {
  sockaddr_in a{};
  a.sin_family = AF_INET;
  a.sin_port = htons(address.port);
  if (inet_pton(AF_INET, address.host.c_str(), &a.sin_addr) != 1) throw std::runtime_error("Expected numeric IPv4");
  return a;
}
void setup(int fd) {
  const int flags = fcntl(fd, F_GETFL, 0);
  if (flags < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0 || fcntl(fd, F_SETFD, FD_CLOEXEC) < 0)
    throw std::runtime_error("Cannot set nonblocking/CLOEXEC");
  int yes = 1;
  if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes)) < 0 ||
      setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &yes, sizeof(yes)) < 0) throw std::runtime_error("Cannot set TCP options");
#ifdef SO_NOSIGPIPE
  if (setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &yes, sizeof(yes)) < 0) throw std::runtime_error("SO_NOSIGPIPE");
#endif
}
constexpr std::size_t io_budget = 256 * 1024;
}
TcpReactor::TcpReactor(std::size_t max_buffer, int partial_timeout_ms)
    : max_buffer_(max_buffer), partial_timeout_ms_(partial_timeout_ms) {
  peers_.reserve(4096);
#ifdef __linux__
  poller_ = epoll_create1(EPOLL_CLOEXEC);
  if (poller_ < 0) throw std::runtime_error("epoll_create1");
#endif
}
TcpReactor::~TcpReactor() {
  for (auto& [id,p] : peers_) { (void)id; if (p->fd >= 0) ::close(p->fd); }
  if (listener_ >= 0) ::close(listener_);
  if (poller_ >= 0) ::close(poller_);
}
void TcpReactor::listen(const Address& address, Framing framing, std::size_t limit) {
  if (listener_ >= 0) throw std::runtime_error("Listener already configured");
  listener_ = socket(AF_INET, SOCK_STREAM, 0);
  if (listener_ < 0) throw std::runtime_error("socket listener");
  setup(listener_);
  int yes = 1;
  setsockopt(listener_, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
  const auto a = socket_address(address);
  if (bind(listener_, reinterpret_cast<const sockaddr*>(&a), sizeof(a)) < 0 || ::listen(listener_, 1024) < 0)
    throw std::runtime_error("Cannot bind/listen " + address.host + ':' + std::to_string(address.port));
  listen_framing_ = framing;
  connection_limit_ = limit;
#ifdef __linux__
  epoll_event event{};
  event.events = EPOLLIN;
  event.data.u64 = 0;
  if (epoll_ctl(poller_, EPOLL_CTL_ADD, listener_, &event) < 0) throw std::runtime_error("epoll listener");
#endif
}
void TcpReactor::watch(const Peer& p, bool add) {
#ifdef __linux__
  epoll_event event{};
  event.events = EPOLLIN | (p.connecting || p.sent < p.output.size() ? EPOLLOUT : 0);
  event.data.u64 = p.id;
  if (epoll_ctl(poller_, add ? EPOLL_CTL_ADD : EPOLL_CTL_MOD, p.fd, &event) < 0)
    throw std::runtime_error("epoll peer");
#else
  (void)p; (void)add;
#endif
}
std::shared_ptr<TcpReactor::Peer> TcpReactor::peer(ConnectionId id) const {
  const auto it = peers_.find(id);
  return it == peers_.end() ? nullptr : it->second;
}
ConnectionId TcpReactor::connect(const Address& address, Framing framing) {
  const auto a = socket_address(address);
  const int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) throw std::runtime_error("socket connector");
  try { setup(fd); } catch (...) { ::close(fd); throw; }
  const int result = ::connect(fd, reinterpret_cast<const sockaddr*>(&a), sizeof(a));
  if (result < 0 && errno != EINPROGRESS) { ::close(fd); return 0; }
  const auto id = next_id_++;
  auto p = std::make_shared<Peer>();
  p->id = id; p->fd = fd; p->framing = framing; p->connecting = true;
  peers_.emplace(id, p);
  watch(*p, true);
  ++connects;
  return id;
}
void TcpReactor::close(ConnectionId id) {
  auto p = peer(id);
  if (!p) return;
#ifdef __linux__
  epoll_ctl(poller_, EPOLL_CTL_DEL, p->fd, nullptr);
#endif
  ::close(p->fd); p->fd = -1;
  peers_.erase(id);
  ++disconnected;
  if (on_close) on_close(id);
}
bool TcpReactor::send(ConnectionId id, std::span<const std::uint8_t> bytes) {
  auto p = peer(id);
  if (!p) return false;
  const auto remaining = p->output.size() - p->sent;
  if (bytes.size() > max_buffer_ || remaining > max_buffer_ - bytes.size()) { close(id); return false; }
  if (p->sent && (p->sent >= p->output.size() / 2 || p->output.size() + bytes.size() > max_buffer_)) {
    p->output.erase(p->output.begin(), p->output.begin() + static_cast<std::ptrdiff_t>(p->sent));
    p->sent = 0;
  }
  if (remaining == 0) p->last_progress = Clock::now();
  p->output.insert(p->output.end(), bytes.begin(), bytes.end());
  if (remaining == 0) watch(*p, false);
  return true;
}
void TcpReactor::flush(const std::shared_ptr<Peer>& p) {
  std::size_t total = 0;
  while (p->fd >= 0 && p->sent < p->output.size() && total < io_budget) {
#ifdef MSG_NOSIGNAL
    constexpr int flags = MSG_NOSIGNAL;
#else
    constexpr int flags = 0;
#endif
    const auto n = ::send(p->fd, p->output.data() + p->sent,
        std::min(io_budget - total, p->output.size() - p->sent), flags);
    if (n < 0 && errno == EINTR) continue;
    if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) break;
    if (n <= 0) { close(p->id); return; }
    p->sent += static_cast<std::size_t>(n); total += static_cast<std::size_t>(n); bytes_out += n;
    p->last_progress = Clock::now();
  }
  if (p->sent == p->output.size()) {
    p->output.clear(); p->sent = 0;
    if (p->fd >= 0) watch(*p, false);
  }
}
void TcpReactor::receive(const std::shared_ptr<Peer>& p) {
  std::array<std::uint8_t, 16384> buffer{};
  std::size_t total = 0;
  while (p->fd >= 0 && total < io_budget) {
    const auto n = recv(p->fd, buffer.data(), buffer.size(), 0);
    if (n < 0 && errno == EINTR) continue;
    if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) break;
    if (n <= 0) { close(p->id); return; }
    total += static_cast<std::size_t>(n); bytes_in += n;
    p->last_receive = Clock::now();
    if (p->input.size() + n > max_buffer_) { close(p->id); return; }
    p->input.insert(p->input.end(), buffer.begin(), buffer.begin() + n);
    const auto prefix_size = p->framing == Framing::Client ? sizeof(Header) : link_header_size;
    const auto max_frame = p->framing == Framing::Client ? max_client_frame : max_link_frame;
    while (p->input.size() - p->consumed >= prefix_size) {
      std::uint32_t length{};
      const auto offset = p->framing == Framing::Client ? 2U : 0U;
      std::memcpy(&length, p->input.data() + p->consumed + offset, sizeof(length));
      if (length < prefix_size || length > max_frame) { close(p->id); return; }
      if (p->input.size() - p->consumed < length) break;
      const auto frame = std::span<const std::uint8_t>(p->input).subspan(p->consumed, length);
      try { if (on_frame) on_frame(p->id, frame); }
      catch (const std::exception&) { close(p->id); return; }
      if (p->fd < 0) return;
      p->consumed += length;
      p->partial_since = {};
    }
    if (p->consumed) {
      p->input.erase(p->input.begin(), p->input.begin() + static_cast<std::ptrdiff_t>(p->consumed));
      p->consumed = 0;
    }
    if (!p->input.empty() && p->partial_since == Clock::time_point{}) p->partial_since = Clock::now();
  }
}
void TcpReactor::step(int timeout_ms) {
  auto& events = ready_events_; events.clear();
#ifdef __linux__
  std::array<epoll_event, 256> ready{};
  const int count = epoll_wait(poller_, ready.data(), ready.size(), timeout_ms);
  if (count < 0 && errno != EINTR) throw std::runtime_error("epoll_wait");
  for (int i = 0; i < count; ++i) {
    short flags = 0;
    if (ready[i].events & EPOLLIN) flags |= POLLIN;
    if (ready[i].events & EPOLLOUT) flags |= POLLOUT;
    if (ready[i].events & (EPOLLHUP | EPOLLERR)) flags |= POLLHUP;
    events.emplace_back(ready[i].data.u64, flags);
  }
#else
  auto& fds = poll_fds_; fds.clear();
  auto& ids = poll_ids_; ids.clear();
  fds.reserve(peers_.size() + 1); ids.reserve(peers_.size() + 1);
  if (listener_ >= 0) { fds.push_back({listener_, POLLIN, 0}); ids.push_back(0); }
  for (const auto& [id,p] : peers_) {
    fds.push_back({p->fd, static_cast<short>(POLLIN | (p->connecting || p->sent < p->output.size() ? POLLOUT : 0)), 0});
    ids.push_back(id);
  }
  if (poll(fds.data(), fds.size(), timeout_ms) < 0 && errno != EINTR) throw std::runtime_error("poll");
  for (std::size_t i = 0; i < fds.size(); ++i) if (fds[i].revents) events.emplace_back(ids[i], fds[i].revents);
#endif
  for (const auto& [id,flags] : events) {
    if (!id) {
      for (unsigned i = 0; i < 64; ++i) {
        const int fd = accept(listener_, nullptr, nullptr);
        if (fd < 0) break;
        if (peers_.size() >= connection_limit_) { ::close(fd); continue; }
        try { setup(fd); } catch (...) { ::close(fd); continue; }
        auto p = std::make_shared<Peer>();
        p->id = next_id_++; p->fd = fd; p->framing = listen_framing_;
        peers_.emplace(p->id, p); watch(*p, true); ++accepted;
        if (on_accept) on_accept(p->id);
      }
      continue;
    }
    auto p = peer(id);
    if (!p) continue;
    if (p->connecting) {
      int error = 0; socklen_t size = sizeof(error);
      if (getsockopt(p->fd, SOL_SOCKET, SO_ERROR, &error, &size) < 0 || error) { close(id); continue; }
      p->connecting = false; watch(*p, false);
      if (on_connect) on_connect(id);
    }
    if (p->fd >= 0 && (flags & POLLIN)) receive(p);
    if (p->fd >= 0 && (flags & POLLOUT)) flush(p);
    if (p->fd >= 0 && (flags & (POLLHUP | POLLERR | POLLNVAL))) close(id);
  }
}
void TcpReactor::expire() {
  const auto now = Clock::now();
  std::vector<ConnectionId> expired;
  for (const auto& [id,p] : peers_) {
    if ((p->connecting && now - p->last_receive > std::chrono::seconds(1)) ||
        (p->partial_since != Clock::time_point{} && now - p->partial_since > std::chrono::milliseconds(partial_timeout_ms_)) ||
        (p->sent < p->output.size() && now - p->last_progress > std::chrono::milliseconds(partial_timeout_ms_)))
      expired.push_back(id);
  }
  for (const auto id : expired) close(id);
}
}  // namespace marketlab::orders
