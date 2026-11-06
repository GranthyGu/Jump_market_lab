#pragma once
#include <condition_variable>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <vector>

namespace marketlab::orders::persistence {
// Bounded handle queue. Blocking wait is intentional: no spinning, no dropping.
// Payload ownership stays with the producer until a completion is observed.
template <class T>
class Queue {
 public:
  explicit Queue(std::size_t capacity) : slots_(capacity) {
    if (!capacity) throw std::invalid_argument("Zero persistence queue capacity");
  }
  bool push(T value) {
    std::unique_lock lock(mutex_);
    room_.wait(lock, [&] { return closed_ || size_ < slots_.size(); });
    if (closed_) return false;
    slots_[(head_ + size_) % slots_.size()] = std::move(value);
    ++size_;
    lock.unlock();
    ready_.notify_one();
    return true;
  }
  std::optional<T> pop() {
    std::unique_lock lock(mutex_);
    ready_.wait(lock, [&] { return closed_ || size_; });
    if (!size_) return {};
    return take(lock);
  }
  std::optional<T> try_pop() {
    std::unique_lock lock(mutex_);
    if (!size_) return {};
    return take(lock);
  }
  void close() {
    {
      std::lock_guard lock(mutex_);
      closed_ = true;
    }
    ready_.notify_all();
    room_.notify_all();
  }

 private:
  std::optional<T> take(std::unique_lock<std::mutex>& lock) {
    auto value = std::move(slots_[head_]);
    head_ = (head_ + 1) % slots_.size();
    --size_;
    lock.unlock();
    room_.notify_one();
    return value;
  }
  std::vector<T> slots_;
  std::size_t head_ = 0, size_ = 0;
  bool closed_ = false;
  std::mutex mutex_;
  std::condition_variable ready_, room_;
};
}  // namespace marketlab::orders::persistence
