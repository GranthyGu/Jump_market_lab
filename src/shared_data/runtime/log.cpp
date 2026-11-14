#include "shared_data/runtime/log.hpp"

#include <mutex>

namespace marketlab::runtime {
Log::~Log() {
  static std::mutex mutex;
  std::lock_guard lock(mutex);
  output_ << buffer_.str() << std::flush;
}

}  // namespace marketlab::runtime
