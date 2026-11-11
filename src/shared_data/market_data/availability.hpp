#pragma once

#include "shared_data/console.hpp"
#include <iostream>
#include <mutex>
#include <string>
#include <string_view>
#include <utility>

namespace marketlab::md {
// One conspicuous warning per outage, followed by one recovery notice. ANSI
// color is retained in redirected service logs because launchers tail them.
class AvailabilityNotice {
 public:
  explicit AvailabilityNotice(std::string component)
      : component_(std::move(component)) {
  }

  void unavailable(std::string_view detail) {
    if (active_)
      return;
    active_ = true;
    std::ostringstream line;
    line << console::red << "!!! " << component_ << "_UNAVAILABLE: " << detail
         << " — retrying in background !!!" << console::reset << '\n';
    write(std::cerr, line.str());
  }

  void recovered() {
    if (!active_)
      return;
    active_ = false;
    std::ostringstream line;
    line << console::green << component_ << "_RESTORED: service is available again"
         << console::reset << '\n';
    write(std::cout, line.str());
  }

  bool active() const {
    return active_;
  }

 private:
  static void write(std::ostream& out, const std::string& line) {
    static std::mutex mutex;
    const std::lock_guard lock(mutex);
    out << line << std::flush;
  }

  std::string component_;
  bool active_ = false;
};
}  // namespace marketlab::md
