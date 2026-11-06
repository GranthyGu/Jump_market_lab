#include "matching_engine/persistence/files.hpp"

#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <system_error>

namespace marketlab::orders::persistence {
namespace {
[[noreturn]] void error(const char* s) { throw std::system_error(errno, std::generic_category(), s); }
}  // namespace
File::File(const std::filesystem::path& p, bool writable, bool create) {
  fd_ = ::open(p.c_str(), (writable ? O_RDWR : O_RDONLY) | O_CLOEXEC | (create ? O_CREAT : 0), 0600);
  if (fd_ < 0) error("open persistence file");
  if (writable) {
    const auto end = lseek(fd_, 0, SEEK_END);
    if (end < 0) {
      const auto saved = errno;
      ::close(fd_);
      fd_ = -1;
      errno = saved;
      error("seek persistence file");
    }
    append_position_ = static_cast<std::uint64_t>(end);
  }
}
File::~File() {
  if (fd_ >= 0) ::close(fd_);
}
File::File(File&& o) noexcept : fd_(o.fd_), append_position_(o.append_position_) { o.fd_ = -1; }
File& File::operator=(File&& o) noexcept {
  if (this != &o) {
    if (fd_ >= 0) ::close(fd_);
    fd_ = o.fd_;
    append_position_ = o.append_position_;
    o.fd_ = -1;
  }
  return *this;
}
std::uint64_t File::size() const {
  struct stat s{};
  if (fstat(fd_, &s) < 0) error("stat persistence file");
  return s.st_size;
}
void File::append(std::span<const std::uint8_t> bytes) {
  // One owning writer per file; pread never changes this descriptor's offset.
  while (!bytes.empty()) {
    const auto n = ::write(fd_, bytes.data(), bytes.size());
    if (n < 0 && errno == EINTR) continue;
    if (n <= 0) error("write persistence file");
    append_position_ += static_cast<std::uint64_t>(n);
    bytes = bytes.subspan(static_cast<std::size_t>(n));
  }
}
void File::sync() {
  int rc;
  do {
    rc = ::fsync(fd_);
  } while (rc < 0 && errno == EINTR);
  if (rc < 0) error("sync persistence file");
}
void File::truncate(std::uint64_t n) {
  if (::ftruncate(fd_, static_cast<off_t>(n)) < 0) error("truncate uncommitted tail");
  if (lseek(fd_, static_cast<off_t>(n), SEEK_SET) < 0) error("seek truncated persistence file");
  append_position_ = n;
  sync();
}
Bytes File::read(std::uint64_t at, std::size_t count) const {
  Bytes out(count);
  std::size_t done = 0;
  while (done < count) {
    const auto n = pread(fd_, out.data() + done, count - done, static_cast<off_t>(at + done));
    if (n < 0 && errno == EINTR) continue;
    if (n < 0) error("read persistence file");
    if (!n) break;
    done += n;
  }
  out.resize(done);
  return out;
}
void sync_directory(const std::filesystem::path& p) {
  File dir(p, false);
  dir.sync();
}
void make_directory(const std::filesystem::path& p) {
  if (p.empty() || std::filesystem::exists(p)) return;
  const auto parent = p.parent_path();
  if (!parent.empty() && parent != p) make_directory(parent);
  if (std::filesystem::create_directory(p)) {
    std::filesystem::permissions(p, std::filesystem::perms::owner_all);
    sync_directory(parent.empty() ? std::filesystem::path(".") : parent);
  }
}
std::optional<Frame> next_frame(const File& f, std::uint64_t& at) {
  const auto length = f.size();
  if (at > length) throw std::runtime_error("Missing committed file prefix");
  if (length - at < header_bytes) return {};
  auto header = f.read(at, header_bytes);
  Reader r{header};
  if (r.number(8) != frame_magic) throw std::runtime_error("Bad persistence frame magic");
  r.number(4);
  const auto payload = r.number(4);
  if (payload > maximum_payload) throw std::runtime_error("Persistence length exceeds bound");
  const auto size = header_bytes + payload + trailer_bytes;
  if (length - at < size) return {};
  auto frame = decode_frame(f.read(at, size));
  frame.start = at;
  at += size;
  frame.end = at;
  return frame;
}
std::vector<std::pair<std::uint64_t, std::filesystem::path>> numbered_files(const std::filesystem::path& dir,
                                                                            const std::string& pre,
                                                                            const std::string& post) {
  std::vector<std::pair<std::uint64_t, std::filesystem::path>> out;
  for (const auto& entry : std::filesystem::directory_iterator(dir)) {
    const auto name = entry.path().filename().string();
    if (!name.starts_with(pre) || !name.ends_with(post)) continue;
    const auto digits = name.substr(pre.size(), name.size() - pre.size() - post.size());
    if (digits.empty() ||
        !std::all_of(digits.begin(), digits.end(), [](char c) { return c >= '0' && c <= '9'; }))
      throw std::runtime_error("Invalid persistence filename");
    out.emplace_back(std::stoull(digits), entry.path());
  }
  std::sort(out.begin(), out.end());
  return out;
}
DirectoryLock::DirectoryLock(const std::filesystem::path& p)
    : file_((make_directory(p), p / "LOCK"), true, true) {
  if (flock(file_.fd(), LOCK_EX | LOCK_NB) < 0) error("Persistence directory already in use");
}
}  // namespace marketlab::orders::persistence
