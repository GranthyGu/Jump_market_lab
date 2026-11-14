#include "shared_data/runtime/thread.hpp"

namespace marketlab::runtime {
void sleep_until_or_stop(Deadline deadline,
                         const std::atomic_bool& stop,
                         const std::atomic_bool& cancelled) {
  while (!stop.load() && !cancelled.load() && Clock::now() < deadline)
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
}

}  // namespace marketlab::runtime
