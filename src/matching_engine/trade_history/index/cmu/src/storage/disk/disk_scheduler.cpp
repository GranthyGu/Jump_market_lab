//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.cpp
//
// Identification: src/storage/disk/disk_scheduler.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/disk/disk_scheduler.h"

namespace bustub {
DiskScheduler::DiskScheduler(DiskManager* dm)
    : disk_manager_(dm) {
  background_thread_.emplace([this] {
    StartWorkerThread();
  });
}

DiskScheduler::~DiskScheduler() {
  request_queue_.Put(std::nullopt);
  if (background_thread_) {
    background_thread_->join();
  }
}

void DiskScheduler::Schedule(std::vector<DiskRequest>& requests) {
  for (auto& request : requests) {
    request_queue_.Put(std::move(request));
  }
}

void DiskScheduler::StartWorkerThread() {
  for (;;) {
    auto request = request_queue_.Get();
    if (!request) {
      return;
    }
    try {
      if (request->is_write_) {
        disk_manager_->WritePage(request->page_id_, request->data_);
      } else {
        disk_manager_->ReadPage(request->page_id_, request->data_);
      }
      request->callback_.set_value(true);
    } catch (...) {
      request->callback_.set_exception(std::current_exception());
    }
  }
}
}  // namespace bustub
