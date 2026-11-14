#pragma once
#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <stdexcept>

#include "shared_data/runtime/io.hpp"
#include <array>

namespace marketlab::runtime {
class Wakeup {
 public:
  Wakeup() {
    int p[2];
    if (pipe(p) < 0)
      throw std::runtime_error("Persistence wakeup pipe");
    read_.reset(p[0]);
    write_.reset(p[1]);
    for (int fd : p) {
      const int flags = fcntl(fd, F_GETFL, 0);
      if (flags < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0 ||
          fcntl(fd, F_SETFD, FD_CLOEXEC) < 0)
        throw std::runtime_error("Persistence wakeup setup");
    }
  }

  int fd() const {
    return read_.get();
  }

  void notify() noexcept {
    const char b = 1;
    while (::write(write_.get(), &b, 1) < 0 && errno == EINTR) {
    }
  }

  void drain() noexcept {
    std::array<char, 256> b{};
    for (unsigned i = 0; i < 16; ++i) {
      const auto n = ::read(read_.get(), b.data(), b.size());
      if (n > 0 || (n < 0 && errno == EINTR))
        continue;
      break;
    }
  }

 private:
  Fd read_, write_;
};
}  // namespace marketlab::runtime
