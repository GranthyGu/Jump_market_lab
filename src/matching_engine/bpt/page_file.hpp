#pragma once

#include <atomic>
#include <filesystem>
#include "storage/disk/disk_manager.h"

namespace marketlab::orders::bpt {

// Fixed page offsets survive reopening. BusTub's teaching DiskManager keeps its
// page-to-offset map only in RAM, so it cannot serve as our persistent page file.
class PageFile final : public bustub::DiskManager {
 public:
  explicit PageFile(const std::filesystem::path& path, bool reset = false);
  ~PageFile() override;
  void WritePage(bustub::page_id_t, const char*) override;
  void ReadPage(bustub::page_id_t, char*) override;
  void DeletePage(bustub::page_id_t) override;
  void sync();
  std::uint64_t size() const;
  struct Metrics { std::uint64_t reads, writes; };
  Metrics metrics() const { return {reads_.load(), writes_.load()}; }
 private:
  int fd_ = -1;
  std::atomic<std::uint64_t> reads_{0}, writes_{0};
};
}  // namespace marketlab::orders::bpt
