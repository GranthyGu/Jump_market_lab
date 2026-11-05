#include "matching_engine/snapshot_mailbox.hpp"
#include <algorithm>
#include <cerrno>
#include <climits>
#include <fcntl.h>
#include <poll.h>
#include <unistd.h>

namespace marketlab::orders {
SnapshotMailbox::Signal::Signal() {
  int descriptors[2];
  if (pipe(descriptors) < 0) throw std::runtime_error("Cannot create snapshot wakeup pipe");
  read_.reset(descriptors[0]); write_.reset(descriptors[1]);
  for (int fd : descriptors) {
    const int flags = fcntl(fd,F_GETFL,0);
    if (flags < 0 || fcntl(fd,F_SETFL,flags | O_NONBLOCK) < 0 || fcntl(fd,F_SETFD,FD_CLOEXEC) < 0)
      throw std::runtime_error("Cannot configure snapshot wakeup pipe");
  }
}
void SnapshotMailbox::Signal::notify() noexcept {
  const unsigned char byte = 1;
  while (write(write_.get(),&byte,1) < 0 && errno == EINTR) {}
  // EAGAIN means an unread notification already exists. State is authoritative.
}
void SnapshotMailbox::Signal::drain() noexcept {
  std::array<unsigned char,256> bytes;
  // Bound drain work even if callers repeatedly submit and cancel requests.
  for (unsigned attempt = 0; attempt < 16; ++attempt) {
    const auto n = ::read(read_.get(),bytes.data(),bytes.size());
    if (n > 0 || (n < 0 && errno == EINTR)) continue;
    return;
  }
}
SnapshotMailbox::SnapshotMailbox(std::size_t symbols) {
  for (auto& slot : slots_) slot = std::make_unique<Slot>(symbols);
}
SnapshotMailbox::~SnapshotMailbox() { close(); }
int SnapshotMailbox::request_fd() const noexcept { return requested_.fd(); }
void SnapshotMailbox::drain_requests() noexcept { requested_.drain(); }
void SnapshotMailbox::close() noexcept {
  closed_.store(1,std::memory_order_release);
  for (auto& slot : slots_) slot->completed.notify();
}
void SnapshotMailbox::abandon(Slot& slot) noexcept {
  auto state = slot.state.load(std::memory_order_acquire);
  for (;;) {
    // Never make a writing payload reusable. Its writer releases it after copy.
    if (state == Abandoned || state == Free) return;
    const auto next = state == Writing ? Abandoned : Free;
    if (slot.state.compare_exchange_weak(state,next,std::memory_order_acq_rel)) return;
  }
}
md::ShardView SnapshotMailbox::read(md::Deadline deadline) {
  if (closed_.load(std::memory_order_acquire)) throw std::runtime_error("Snapshot mailbox stopped");
  Slot* selected = nullptr; unsigned index = 0;
  const auto first = cursor_.fetch_add(1,std::memory_order_relaxed);
  for (unsigned n = 0; n < capacity; ++n) {
    index = (first + n) % capacity;
    std::uint32_t expected = Free;
    if (slots_[index]->state.compare_exchange_strong(expected,Reserved,std::memory_order_acq_rel)) {
      selected = slots_[index].get(); break;
    }
  }
  if (!selected) throw std::runtime_error("Snapshot mailbox full");
  auto& slot = *selected;
  try {
    slot.completed.drain();
    slot.state.store(Requested,std::memory_order_release);
    pending_.fetch_or(1U << index,std::memory_order_release);
    requested_.notify();
    for (;;) {
      if (closed_.load(std::memory_order_acquire)) throw std::runtime_error("Snapshot mailbox stopped");
      const auto now = md::Clock::now();
      if (now >= deadline) throw std::runtime_error("Snapshot mailbox timeout");
      if (slot.state.load(std::memory_order_acquire) == Ready) {
        auto view = slot.snapshot.materialize();
        slot.state.store(Free,std::memory_order_release);
        return view;
      }
      const auto remaining = std::chrono::ceil<std::chrono::milliseconds>(deadline - now).count();
      pollfd p{slot.completed.fd(),POLLIN,0};
      const int result = poll(&p,1,static_cast<int>(std::min<std::int64_t>(remaining,INT_MAX)));
      if (result < 0 && errno == EINTR) continue;
      if (result < 0 || (p.revents & (POLLERR | POLLHUP | POLLNVAL)))
        throw std::runtime_error("Snapshot notification failed");
      if (result > 0) slot.completed.drain();
    }
  } catch (...) { abandon(slot); throw; }
}
void SnapshotMailbox::service(EngineBackend& engine) noexcept {
  if (!pending_.load(std::memory_order_relaxed)) return;
  const auto mask = pending_.exchange(0,std::memory_order_acquire);
  for (unsigned index = 0; index < capacity; ++index) {
    if (!(mask & (1U << index))) continue;
    auto& slot = *slots_[index];
    std::uint32_t expected = Requested;
    if (!slot.state.compare_exchange_strong(expected,Writing,std::memory_order_acq_rel)) continue;
    engine.copy_market(slot.snapshot);
    expected = Writing;
    if (slot.state.compare_exchange_strong(expected,Ready,std::memory_order_release,std::memory_order_relaxed))
      slot.completed.notify();
    else slot.state.store(Free,std::memory_order_release); // Reader abandoned it.
  }
}
}  // namespace marketlab::orders
