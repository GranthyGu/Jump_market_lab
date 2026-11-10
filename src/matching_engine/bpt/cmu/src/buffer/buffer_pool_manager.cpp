//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include <algorithm>
#include <limits>
#include <stdexcept>

namespace bustub {
namespace {
void IO(const std::shared_ptr<DiskScheduler>& scheduler, bool write, page_id_t id, char* data) {
  std::vector<DiskRequest> requests;
  requests.push_back({write, data, id, {}});
  auto done = requests[0].callback_.get_future();
  scheduler->Schedule(requests);
  done.get();
}
}

FrameHeader::FrameHeader(frame_id_t id)
    : frame_id_(id),
      data_(BUSTUB_PAGE_SIZE, 0) {
  Reset();
}

auto FrameHeader::GetData() const -> const char* {
  return data_.data();
}

auto FrameHeader::GetDataMut() -> char* {
  return data_.data();
}

void FrameHeader::Reset() {
  std::fill(data_.begin(), data_.end(), 0);
  pin_count_ = 0;
  is_dirty_ = false;
  page_id_ = INVALID_PAGE_ID;
}

BufferPoolManager::BufferPoolManager(size_t n, DiskManager* disk, LogManager* log)
    : num_frames_(n),
      next_page_id_(0),
      bpm_latch_(std::make_shared<std::mutex>()),
      replacer_(std::make_shared<ArcReplacer>(n)),
      disk_scheduler_(std::make_shared<DiskScheduler>(disk)),
      log_manager_(log) {
  if (n == 0) {
    throw std::invalid_argument("empty buffer pool");
  }
  frames_.reserve(n);
  page_table_.reserve(n);
  for (size_t i = 0; i < n; ++i) {
    frames_.push_back(std::make_shared<FrameHeader>(i));
    free_frames_.push_back(i);
  }
}

BufferPoolManager::~BufferPoolManager() = default;

auto BufferPoolManager::Size() const -> size_t {
  return num_frames_;
}

auto BufferPoolManager::NewPage() -> page_id_t {
  auto id = next_page_id_.fetch_add(1);
  if (id < 0 || id == std::numeric_limits<page_id_t>::max()) {
    throw std::overflow_error("page ids exhausted");
  }
  return id;
}

// Pinning and mapping changes are serialized, but no page latch is acquired while holding bpm_latch_.
// This avoids a cycle with guard destruction. A miss currently serializes disk I/O; hits need no I/O.
auto BufferPoolManager::Pin(page_id_t id, AccessType type) -> std::shared_ptr<FrameHeader> {
  std::lock_guard lock(*bpm_latch_);
  if (id < 0) {
    throw std::invalid_argument("negative page id");
  }
  std::shared_ptr<FrameHeader> f;
  auto found = page_table_.find(id);
  if (found != page_table_.end()) {
    f = frames_[found->second];
  } else {
    frame_id_t fid;
    if (!free_frames_.empty()) {
      fid = free_frames_.front();
      free_frames_.pop_front();
    } else {
      auto victim = replacer_->Evict();
      if (!victim) {
        return nullptr;
      }
      fid = *victim;
    }
    f = frames_[fid];
    try {
      if (f->is_dirty_) {
        IO(disk_scheduler_, true, f->page_id_, f->GetDataMut());
      }
      if (f->page_id_ != INVALID_PAGE_ID) {
        page_table_.erase(f->page_id_);
      }
      f->Reset();
      IO(disk_scheduler_, false, id, f->GetDataMut());
      f->page_id_ = id;
      page_table_[id] = fid;
    } catch (...) {
      if (f->page_id_ != INVALID_PAGE_ID) {
        page_table_.erase(f->page_id_);
      }
      f->Reset();
      free_frames_.push_front(fid);
      throw;
    }
  }
  ++f->pin_count_;
  replacer_->RecordAccess(f->frame_id_, id, type);
  replacer_->SetEvictable(f->frame_id_, false);
  return f;
}

auto BufferPoolManager::CheckedReadPage(page_id_t id, AccessType type)
    -> std::optional<ReadPageGuard> {
  auto f = Pin(id, type);
  if (!f) {
    return std::nullopt;
  }
  return ReadPageGuard(id, std::move(f), replacer_, bpm_latch_, disk_scheduler_);
}

auto BufferPoolManager::TryReadPage(page_id_t id) -> std::optional<ReadPageGuard> {
  auto f = Pin(id, AccessType::Scan);
  if (!f) {
    return std::nullopt;
  }
  if (!f->rwlatch_.try_lock_shared()) {
    std::lock_guard lock(*bpm_latch_);
    if (--f->pin_count_ == 0) {
      replacer_->SetEvictable(f->frame_id_, true);
    }
    return std::nullopt;
  }
  ReadPageGuard guard;
  guard.page_id_ = id;
  guard.frame_ = std::move(f);
  guard.replacer_ = replacer_;
  guard.bpm_latch_ = bpm_latch_;
  guard.disk_scheduler_ = disk_scheduler_;
  guard.is_valid_ = true;
  return guard;
}

auto BufferPoolManager::CheckedWritePage(page_id_t id, AccessType type)
    -> std::optional<WritePageGuard> {
  auto f = Pin(id, type);
  if (!f) {
    return std::nullopt;
  }
  return WritePageGuard(id, std::move(f), replacer_, bpm_latch_, disk_scheduler_);
}

auto BufferPoolManager::ReadPage(page_id_t id, AccessType type) -> ReadPageGuard {
  auto guard = CheckedReadPage(id, type);
  if (!guard) {
    throw std::runtime_error("buffer pool exhausted by pinned pages");
  }
  return std::move(*guard);
}

auto BufferPoolManager::WritePage(page_id_t id, AccessType type) -> WritePageGuard {
  auto guard = CheckedWritePage(id, type);
  if (!guard) {
    throw std::runtime_error("buffer pool exhausted by pinned pages");
  }
  return std::move(*guard);
}

auto BufferPoolManager::EvictUnpinnedPages() -> size_t {
  std::lock_guard lock(*bpm_latch_);
  size_t evicted = 0;
  for (auto it = page_table_.begin(); it != page_table_.end();) {
    auto f = frames_[it->second];
    if (f->pin_count_ != 0) {
      ++it;
      continue;
    }
    // With zero pins and the pool latch held, no guard can access this frame.
    // Keep the mapping intact if a write fails so the caller can retry safely.
    if (f->is_dirty_) {
      IO(disk_scheduler_, true, it->first, f->GetDataMut());
    }
    replacer_->Remove(f->frame_id_);
    it = page_table_.erase(it);
    f->Reset();
    free_frames_.push_back(f->frame_id_);
    ++evicted;
  }
  return evicted;
}

auto BufferPoolManager::DeletePage(page_id_t id) -> bool {
  std::lock_guard lock(*bpm_latch_);
  auto it = page_table_.find(id);
  if (it != page_table_.end()) {
    auto f = frames_[it->second];
    if (f->pin_count_ != 0) {
      return false;
    }
    replacer_->Remove(f->frame_id_);
    page_table_.erase(it);
    f->Reset();
    free_frames_.push_back(f->frame_id_);
  }
  disk_scheduler_->DeallocatePage(id);
  return true;
}

auto BufferPoolManager::FlushPageUnsafe(page_id_t id) -> bool {
  std::lock_guard lock(*bpm_latch_);
  auto it = page_table_.find(id);
  if (it == page_table_.end()) {
    return false;
  }
  auto f = frames_[it->second];
  IO(disk_scheduler_, true, id, f->GetDataMut());
  f->is_dirty_ = false;
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t id) -> bool {
  // Pin the existing mapping before releasing the pool latch. Do not read an absent page just to flush it.
  std::shared_ptr<FrameHeader> f;
  {
    std::lock_guard lock(*bpm_latch_);
    auto it = page_table_.find(id);
    if (it == page_table_.end()) {
      return false;
    }
    f = frames_[it->second];
    ++f->pin_count_;
    replacer_->SetEvictable(f->frame_id_, false);
  }
  ReadPageGuard guard(id, std::move(f), replacer_, bpm_latch_, disk_scheduler_);
  guard.Flush();
  return true;
}

void BufferPoolManager::FlushAllPagesUnsafe() {
  std::lock_guard lock(*bpm_latch_);
  for (auto& [id, fid] : page_table_) {
    auto f = frames_[fid];
    IO(disk_scheduler_, true, id, f->GetDataMut());
    f->is_dirty_ = false;
  }
}

void BufferPoolManager::FlushAllPages() {
  std::vector<page_id_t> ids;
  {
    std::lock_guard lock(*bpm_latch_);
    for (auto& [id, fid] : page_table_) {
      ids.push_back(id);
    }
  }
  for (auto id : ids) {
    FlushPage(id);
  }
}

auto BufferPoolManager::GetPinCount(page_id_t id) -> std::optional<size_t> {
  std::lock_guard lock(*bpm_latch_);
  auto it = page_table_.find(id);
  if (it == page_table_.end()) {
    return std::nullopt;
  }
  return frames_[it->second]->pin_count_.load();
}
}  // namespace bustub
