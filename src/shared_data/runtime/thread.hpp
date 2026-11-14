#pragma once

#include "shared_data/runtime/io.hpp"

#include <atomic>
#include <memory>
#include <thread>
#include <utility>

namespace marketlab::runtime {
void sleep_until_or_stop(Deadline deadline,
                         const std::atomic_bool& stop,
                         const std::atomic_bool& cancelled);

// A small joining thread keeps this C++20 program compatible with older libc++.
class StopThread {
 public:
  template <typename F>
  explicit StopThread(F fn)
      : stop_(std::make_shared<std::atomic_bool>(false)),
        thread_([flag = stop_, fn = std::move(fn)]() mutable {
          fn(*flag);
        }) {
  }

  ~StopThread() {
    stop_->store(true);
    if (thread_.joinable())
      thread_.join();
  }

  StopThread(const StopThread&) = delete;
  StopThread& operator=(const StopThread&) = delete;

 private:
  std::shared_ptr<std::atomic_bool> stop_;
  std::thread thread_;
};

}  // namespace marketlab::runtime
