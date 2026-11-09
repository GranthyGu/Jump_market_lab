#include "page_file.hpp"

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <stdexcept>
#include <system_error>
#include <sys/stat.h>
#include <unistd.h>

namespace marketlab::orders::bpt {
namespace {
[[noreturn]] void io_error(const char* action) { throw std::system_error(errno, std::generic_category(), action); }
std::uint64_t offset(bustub::page_id_t id) {
  if (id < 0) throw std::invalid_argument("negative B+ tree page id");
  return static_cast<std::uint64_t>(id) * bustub::BUSTUB_PAGE_SIZE;
}
}
PageFile::PageFile(const std::filesystem::path& path, bool reset) {
  fd_ = ::open(path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC | (reset ? O_TRUNC : 0), 0600);
  if (fd_ < 0) io_error("open B+ tree page file");
}
PageFile::~PageFile() { if (fd_ >= 0) ::close(fd_); }
std::uint64_t PageFile::size() const {
  struct stat s{};
  if (::fstat(fd_, &s)) io_error("stat B+ tree page file");
  return static_cast<std::uint64_t>(s.st_size);
}
void PageFile::WritePage(bustub::page_id_t id, const char* data) {
  auto pos = offset(id);
  std::size_t done = 0;
  while (done < bustub::BUSTUB_PAGE_SIZE) {
    auto n = ::pwrite(fd_, data + done, bustub::BUSTUB_PAGE_SIZE - done, pos + done);
    if (n < 0 && errno == EINTR) continue;
    if (n < 0) io_error("write B+ tree page");
    if (n == 0) throw std::runtime_error("zero length B+ tree write");
    done += static_cast<std::size_t>(n);
  }
  ++writes_;
}
void PageFile::ReadPage(bustub::page_id_t id, char* data) {
  auto pos = offset(id);
  std::size_t done = 0;
  while (done < bustub::BUSTUB_PAGE_SIZE) {
    auto n = ::pread(fd_, data + done, bustub::BUSTUB_PAGE_SIZE - done, pos + done);
    if (n < 0 && errno == EINTR) continue;
    if (n < 0) io_error("read B+ tree page");
    if (n == 0) {
      if (done != 0) throw std::runtime_error("truncated B+ tree page");
      std::memset(data, 0, bustub::BUSTUB_PAGE_SIZE); break;
    }
    done += static_cast<std::size_t>(n);
  }
  ++reads_;
}
void PageFile::DeletePage(bustub::page_id_t) {
  // IDs are never reused. Trade indexes are append-only; the unique-key test
  // mode may unlink pages but does not need a persisted free-list.
}
void PageFile::sync() {
  int result;
  do { result = ::fsync(fd_); } while (result < 0 && errno == EINTR);
  if (result < 0) io_error("sync B+ tree page file");
}
}  // namespace marketlab::orders::bpt
