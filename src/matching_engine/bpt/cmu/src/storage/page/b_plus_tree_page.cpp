//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_page.cpp
//
// Identification: src/storage/page/b_plus_tree_page.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/b_plus_tree_page.h"

namespace bustub {
auto BPlusTreePage::IsLeafPage() const -> bool {
  return page_type_ == IndexPageType::LEAF_PAGE;
}

void BPlusTreePage::SetPageType(IndexPageType t) {
  page_type_ = t;
}

auto BPlusTreePage::GetSize() const -> int {
  return size_;
}

void BPlusTreePage::SetSize(int n) {
  size_ = n;
}

void BPlusTreePage::ChangeSizeBy(int n) {
  size_ += n;
}

auto BPlusTreePage::GetMaxSize() const -> int {
  return max_size_;
}

void BPlusTreePage::SetMaxSize(int n) {
  max_size_ = n;
}

auto BPlusTreePage::GetMinSize() const -> int {
  return (max_size_ + 1) / 2;
}
}  // namespace bustub
