#pragma once
#include <optional>

#include "matching_engine/persistence/codec.hpp"

namespace marketlab::orders::persistence {
class File {
 public:
  File() = default;
  File(const std::filesystem::path&, bool writable, bool create = false);
  ~File();
  File(File&&) noexcept;
  File& operator=(File&&) noexcept;
  File(const File&) = delete;
  File& operator=(const File&) = delete;
  int fd() const { return fd_; }
  std::uint64_t size() const;
  std::uint64_t append_position() const { return append_position_; }
  void append(std::span<const std::uint8_t>);
  void sync();
  void truncate(std::uint64_t);
  Bytes read(std::uint64_t, std::size_t) const;

 private:
  int fd_ = -1;
  std::uint64_t append_position_ = 0;
};
void sync_directory(const std::filesystem::path&);
void make_directory(const std::filesystem::path&);
// Incomplete final frames return nullopt with position left at their start.
// Complete corrupt frames throw: they must never be silently discarded.
std::optional<Frame> next_frame(const File&, std::uint64_t& position);
std::vector<std::pair<std::uint64_t, std::filesystem::path>> numbered_files(const std::filesystem::path&,
                                                                            const std::string&,
                                                                            const std::string&);
class DirectoryLock {
 public:
  explicit DirectoryLock(const std::filesystem::path&);

 private:
  File file_;
};
}  // namespace marketlab::orders::persistence
