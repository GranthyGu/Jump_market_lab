#pragma once

#include <ostream>
#include <sstream>

namespace marketlab::runtime {
class Log {
 public:
  explicit Log(std::ostream& output)
      : output_(output) {
  }

  ~Log();

  template <typename T>
  Log& operator<<(const T& value) {
    buffer_ << value;
    return *this;
  }

 private:
  std::ostream& output_;
  std::ostringstream buffer_;
};

}  // namespace marketlab::runtime
