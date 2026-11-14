// :bustub-keep-private:
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// arc_replacer.cpp
//
// Identification: src/buffer/arc_replacer.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/arc_replacer.h"
#include <algorithm>
#include <stdexcept>

namespace bustub {
ArcReplacer::ArcReplacer(size_t n)
    : replacer_size_(n) {
}

auto ArcReplacer::Evict() -> std::optional<frame_id_t> {
  std::lock_guard lock(latch_);
  auto victim = [&](auto& live, auto& ghost, ArcStatus status) -> std::optional<frame_id_t> {
    for (auto it = live.rbegin(); it != live.rend(); ++it) {
      auto f = alive_map_.at(*it);
      if (!f->evictable_) {
        continue;
      }
      auto id = f->frame_id_;
      live.erase(std::next(it).base());
      ghost.push_front(f->page_id_);
      f->position_ = ghost.begin();
      f->arc_status_ = status;
      ghost_map_[f->page_id_] = f;
      alive_map_.erase(id);
      --curr_size_;
      return id;
    }
    return std::nullopt;
  };
  if (mru_.size() >= mru_target_size_) {
    auto id = victim(mru_, mru_ghost_, ArcStatus::MRU_GHOST);
    if (id) {
      return id;
    }
    return victim(mfu_, mfu_ghost_, ArcStatus::MFU_GHOST);
  }
  auto id = victim(mfu_, mfu_ghost_, ArcStatus::MFU_GHOST);
  if (id) {
    return id;
  }
  return victim(mru_, mru_ghost_, ArcStatus::MRU_GHOST);
}

void ArcReplacer::RecordAccess(frame_id_t fid, page_id_t pid, AccessType) {
  std::lock_guard lock(latch_);
  if (fid < 0 || static_cast<size_t>(fid) > replacer_size_) {
    throw std::out_of_range("frame id");
  }
  auto live = alive_map_.find(fid);
  if (live != alive_map_.end()) {
    auto f = live->second;
    auto& list = f->arc_status_ == ArcStatus::MRU ? mru_ : mfu_;
    mfu_.splice(mfu_.begin(), list, f->position_);
    f->position_ = mfu_.begin();
    f->arc_status_ = ArcStatus::MFU;
    return;
  }
  auto old = ghost_map_.find(pid);
  bool hit = old != ghost_map_.end();
  if (hit) {
    auto f = old->second;
    if (f->arc_status_ == ArcStatus::MRU_GHOST) {
      mru_target_size_ =
          std::min(replacer_size_,
                   mru_target_size_ + std::max(size_t{1}, mfu_ghost_.size() / mru_ghost_.size()));
      mru_ghost_.erase(f->position_);
    } else {
      auto delta = std::max(size_t{1}, mru_ghost_.size() / mfu_ghost_.size());
      mru_target_size_ -= std::min(mru_target_size_, delta);
      mfu_ghost_.erase(f->position_);
    }
    ghost_map_.erase(old);
  } else {
    auto trim = [&](auto& list) {
      if (!list.empty()) {
        ghost_map_.erase(list.back());
        list.pop_back();
      }
    };
    if (mru_.size() + mru_ghost_.size() >= replacer_size_) {
      trim(mru_ghost_);
    } else if (alive_map_.size() + ghost_map_.size() >= 2 * replacer_size_) {
      trim(mfu_ghost_);
    }
  }
  auto f = std::make_shared<FrameStatus>(pid, fid, false, hit ? ArcStatus::MFU : ArcStatus::MRU);
  auto& list = hit ? mfu_ : mru_;
  list.push_front(fid);
  f->position_ = list.begin();
  alive_map_[fid] = std::move(f);
}

void ArcReplacer::SetEvictable(frame_id_t fid, bool ev) {
  std::lock_guard lock(latch_);
  if (fid < 0 || static_cast<size_t>(fid) > replacer_size_) {
    throw std::out_of_range("frame id");
  }
  auto it = alive_map_.find(fid);
  if (it == alive_map_.end() || it->second->evictable_ == ev) {
    return;
  }
  it->second->evictable_ = ev;
  if (ev) {
    ++curr_size_;
  } else {
    --curr_size_;
  }
}

void ArcReplacer::Remove(frame_id_t fid) {
  std::lock_guard lock(latch_);
  auto it = alive_map_.find(fid);
  if (it == alive_map_.end()) {
    return;
  }
  auto f = it->second;
  if (!f->evictable_) {
    throw std::logic_error("remove pinned frame");
  }
  auto& list = f->arc_status_ == ArcStatus::MRU ? mru_ : mfu_;
  list.erase(f->position_);
  alive_map_.erase(it);
  --curr_size_;
}

auto ArcReplacer::Size() -> size_t {
  std::lock_guard lock(latch_);
  return curr_size_;
}
}  // namespace bustub
