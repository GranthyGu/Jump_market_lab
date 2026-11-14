//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// page_guard.cpp
//
// Identification: src/storage/page/page_guard.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/page_guard.h"
#include <utility>

namespace bustub {
ReadPageGuard::ReadPageGuard(page_id_t id,
                             std::shared_ptr<FrameHeader> frame,
                             std::shared_ptr<ArcReplacer> replacer,
                             std::shared_ptr<std::mutex> latch,
                             std::shared_ptr<DiskScheduler> scheduler)
    : page_id_(id),
      frame_(std::move(frame)),
      replacer_(std::move(replacer)),
      bpm_latch_(std::move(latch)),
      disk_scheduler_(std::move(scheduler)),
      is_valid_(true) {
  frame_->rwlatch_.lock_shared();
}

ReadPageGuard::ReadPageGuard(ReadPageGuard&& other) noexcept {
  *this = std::move(other);
}

auto ReadPageGuard::operator=(ReadPageGuard&& other) noexcept -> ReadPageGuard& {
  if (this == &other) {
    return *this;
  }
  Drop();
  page_id_ = other.page_id_;
  frame_ = std::move(other.frame_);
  replacer_ = std::move(other.replacer_);
  bpm_latch_ = std::move(other.bpm_latch_);
  disk_scheduler_ = std::move(other.disk_scheduler_);
  is_valid_ = std::exchange(other.is_valid_, false);
  return *this;
}

auto ReadPageGuard::GetPageId() const -> page_id_t {
  BUSTUB_ASSERT(is_valid_, "invalid guard");
  return page_id_;
}

auto ReadPageGuard::GetData() const -> const char* {
  BUSTUB_ASSERT(is_valid_, "invalid guard");
  return frame_->GetData();
}

auto ReadPageGuard::IsDirty() const -> bool {
  BUSTUB_ASSERT(is_valid_, "invalid guard");
  return frame_->is_dirty_.load();
}

void ReadPageGuard::Flush() {
  BUSTUB_ASSERT(is_valid_, "invalid guard");
  std::vector<DiskRequest> requests;
  requests.push_back({true, frame_->GetDataMut(), page_id_, {}});
  auto done = requests[0].callback_.get_future();
  disk_scheduler_->Schedule(requests);
  done.get();
  frame_->is_dirty_ = false;
}

void ReadPageGuard::Drop() {
  if (!is_valid_) {
    return;
  }
  frame_->rwlatch_.unlock_shared();
  std::lock_guard lock(*bpm_latch_);
  if (--frame_->pin_count_ == 0) {
    replacer_->SetEvictable(frame_->frame_id_, true);
  }
  is_valid_ = false;
}

ReadPageGuard::~ReadPageGuard() {
  Drop();
}

WritePageGuard::WritePageGuard(page_id_t id,
                               std::shared_ptr<FrameHeader> frame,
                               std::shared_ptr<ArcReplacer> replacer,
                               std::shared_ptr<std::mutex> latch,
                               std::shared_ptr<DiskScheduler> scheduler)
    : page_id_(id),
      frame_(std::move(frame)),
      replacer_(std::move(replacer)),
      bpm_latch_(std::move(latch)),
      disk_scheduler_(std::move(scheduler)),
      is_valid_(true) {
  frame_->rwlatch_.lock();
}

WritePageGuard::WritePageGuard(WritePageGuard&& other) noexcept {
  *this = std::move(other);
}

auto WritePageGuard::operator=(WritePageGuard&& other) noexcept -> WritePageGuard& {
  if (this == &other) {
    return *this;
  }
  Drop();
  page_id_ = other.page_id_;
  frame_ = std::move(other.frame_);
  replacer_ = std::move(other.replacer_);
  bpm_latch_ = std::move(other.bpm_latch_);
  disk_scheduler_ = std::move(other.disk_scheduler_);
  is_valid_ = std::exchange(other.is_valid_, false);
  return *this;
}

auto WritePageGuard::GetPageId() const -> page_id_t {
  BUSTUB_ASSERT(is_valid_, "invalid guard");
  return page_id_;
}

auto WritePageGuard::GetData() const -> const char* {
  BUSTUB_ASSERT(is_valid_, "invalid guard");
  return frame_->GetData();
}

auto WritePageGuard::IsDirty() const -> bool {
  BUSTUB_ASSERT(is_valid_, "invalid guard");
  return frame_->is_dirty_.load();
}

void WritePageGuard::Flush() {
  BUSTUB_ASSERT(is_valid_, "invalid guard");
  std::vector<DiskRequest> requests;
  requests.push_back({true, frame_->GetDataMut(), page_id_, {}});
  auto done = requests[0].callback_.get_future();
  disk_scheduler_->Schedule(requests);
  done.get();
  frame_->is_dirty_ = false;
}

void WritePageGuard::Drop() {
  if (!is_valid_) {
    return;
  }
  frame_->rwlatch_.unlock();
  std::lock_guard lock(*bpm_latch_);
  if (--frame_->pin_count_ == 0) {
    replacer_->SetEvictable(frame_->frame_id_, true);
  }
  is_valid_ = false;
}

WritePageGuard::~WritePageGuard() {
  Drop();
}

auto WritePageGuard::GetDataMut() -> char* {
  BUSTUB_ASSERT(is_valid_, "invalid guard");
  frame_->is_dirty_ = true;
  return frame_->GetDataMut();
}
}  // namespace bustub
