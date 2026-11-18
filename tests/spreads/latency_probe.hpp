#pragma once
// Test-build-only, single-owner recording. No I/O or allocation after warmup.
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <memory>
#include <stdexcept>
#include <string>
#include <time.h>
#include <unistd.h>

namespace marketlab::testing::latency {
inline std::uint64_t now_ns() {
  timespec value{};
  if (clock_gettime(CLOCK_MONOTONIC, &value))
    throw std::runtime_error("Latency clock failed");
  return std::uint64_t(value.tv_sec) * 1'000'000'000ULL + value.tv_nsec;
}

enum class Kind : std::uint8_t { OrdinaryStart, OrdinaryMemory, GroupStart, GroupMemory };
struct Record {
  std::uint64_t timestamp, sequence, request;
  Kind kind;
  std::uint8_t shard, participants;
};
static_assert(sizeof(Record) == 32);

class Recorder {
  static constexpr std::size_t capacity = 131072;
  std::unique_ptr<Record[]> records_;
  std::string path_;
  std::size_t size_ = 0;

 public:
  Recorder() {
    if (const auto* directory = std::getenv("MARKETLAB_LATENCY_PROBE_DIR")) {
      path_ = std::string(directory) + "/probe-" + std::to_string(getpid()) + ".csv";
      records_ = std::make_unique<Record[]>(capacity);
    }
  }
  ~Recorder() {
    if (!records_)
      return;
    std::ofstream out(path_);
    out << "timestamp_ns,sequence,request,kind,shard,participants\n";
    for (std::size_t i = 0; i < size_; ++i) {
      const auto& r = records_[i];
      out << r.timestamp << ',' << r.sequence << ',' << r.request << ','
          << unsigned(r.kind) << ',' << unsigned(r.shard) << ','
          << unsigned(r.participants) << '\n';
    }
    out.close();
    if (!out)
      std::fprintf(stderr, "Latency probe write failed: %s\n", path_.c_str());
  }
  void append(Kind kind, std::uint64_t sequence, std::uint64_t request,
              std::uint8_t shard, std::uint8_t participants) {
    if (!records_)
      return;
    if (size_ == capacity)
      throw std::runtime_error("Latency probe capacity exceeded");
    records_[size_++] = {now_ns(), sequence, request, kind, shard, participants};
  }
};

inline void record(Kind kind, std::uint64_t sequence, std::uint64_t request = 0,
                   std::uint8_t shard = 0, std::uint8_t participants = 0) {
  static Recorder recorder;
  recorder.append(kind, sequence, request, shard, participants);
}
} // namespace marketlab::testing::latency
