#pragma once

#include <ctime>
#include <cstdint>
#include <iomanip>
#include <ostream>
#include <sstream>
#include <string>
#include <string_view>

namespace marketlab::console {
// Retain ANSI in redirected logs: service launchers display them through tail.
inline constexpr std::string_view reset = "\033[0m";
inline constexpr std::string_view red = "\033[1;31m";
inline constexpr std::string_view green = "\033[1;32m";
inline constexpr std::string_view yellow = "\033[1;33m";
inline constexpr std::string_view magenta = "\033[1;35m";
inline constexpr std::string_view cyan = "\033[1;36m";
inline constexpr std::string_view dim = "\033[2m";

struct Label {
  std::string_view text, color;
};

inline std::ostream& operator<<(std::ostream& out, Label label) {
  return out << label.color << label.text << reset;
}

inline std::string time(std::uint64_t nanoseconds, bool date = false) {
  const auto seconds = static_cast<std::time_t>(nanoseconds / 1'000'000'000);
  std::tm local{};
  if (!localtime_r(&seconds, &local))
    return "unknown time";
  std::ostringstream out;
  out << std::put_time(&local, date ? "%Y-%m-%d %H:%M:%S" : "%H:%M:%S") << '.' << std::setfill('0')
      << std::setw(3) << (nanoseconds / 1'000'000) % 1000;
  return out.str();
}
}  // namespace marketlab::console
