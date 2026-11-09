//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_iterator.h
//
// Identification: src/include/storage/index/index_iterator.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once
#include <functional>
#include <optional>
#include <utility>
#include "buffer/traced_buffer_pool_manager.h"
#include "storage/page/b_plus_tree_leaf_page.h"
namespace bustub {
#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator, NumTombs>
#define SHORT_INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>
FULL_INDEX_TEMPLATE_ARGUMENTS_DEFN
class IndexIterator {
  using Self = IndexIterator<KeyType, ValueType, KeyComparator, NumTombs>;
  using Leaf = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator, NumTombs>;
 public:
  IndexIterator() = default;
  IndexIterator(ReadPageGuard guard, int pos, std::function<Self(const KeyType &, const ValueType &)> seek)
    : guard_(std::move(guard)), pos_(pos), seek_(std::move(seek)) {}
  IndexIterator(IndexIterator &&) noexcept = default;
  auto operator=(IndexIterator &&) noexcept -> IndexIterator & = default;
  ~IndexIterator() = default;
  auto IsEnd() const -> bool { return !guard_; }
  auto operator*() -> std::pair<const KeyType &, const ValueType &> {
    auto leaf = guard_->template As<Leaf>(); return {leaf->KeyRef(pos_), leaf->ValueRef(pos_)};
  }
  auto operator++() -> Self & {
    auto leaf = guard_->template As<Leaf>(); auto key = leaf->KeyAt(pos_); auto value = leaf->ValueAt(pos_);
    do { ++pos_; } while (pos_ < leaf->GetSize() && leaf->IsTomb(pos_));
    if (pos_ == leaf->GetSize()) {
      bool last = leaf->GetNextPageId() == INVALID_PAGE_ID;
      guard_.reset();
      if (!last) { auto seek = seek_; *this = seek(key, value); }
    }
    return *this;
  }
  auto operator==(const Self &other) const -> bool {
    if (IsEnd() || other.IsEnd()) { return IsEnd() && other.IsEnd(); }
    return guard_->GetPageId() == other.guard_->GetPageId() && pos_ == other.pos_;
  }
  auto operator!=(const Self &other) const -> bool { return !(*this == other); }
 private:
  std::optional<ReadPageGuard> guard_;
  int pos_{0};
  std::function<Self(const KeyType &, const ValueType &)> seek_;
};
}  // namespace bustub
