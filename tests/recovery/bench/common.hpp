#pragma once

#include <sys/resource.h>

#include <chrono>
#include <stdexcept>

namespace marketlab::bench {

using Clock = std::chrono::steady_clock;

inline void require(bool condition, const char *message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

struct Usage {
    double cpu_seconds;
    long context_switches;
};

inline Usage usage() {
    rusage result{};
    require(getrusage(RUSAGE_SELF, &result) == 0, "getrusage failed");
    const auto seconds = [](const timeval &time) {
        return static_cast<double>(time.tv_sec) + time.tv_usec / 1e6;
    };
    return {seconds(result.ru_utime) + seconds(result.ru_stime),
            result.ru_nvcsw + result.ru_nivcsw};
}

} // namespace marketlab::bench
