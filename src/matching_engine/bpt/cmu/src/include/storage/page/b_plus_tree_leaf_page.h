//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_leaf_page.h
//
// Identification: src/include/storage/page/b_plus_tree_leaf_page.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <algorithm>
#include <cstring>
#include <utility>
#include <vector>

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

#define B_PLUS_TREE_LEAF_PAGE_TYPE BPlusTreeLeafPage<KeyType, ValueType, KeyComparator, NumTombs>
#define LEAF_PAGE_HEADER_SIZE 32
#define LEAF_PAGE_DEFAULT_TOMB_CNT 0
#define LEAF_PAGE_TOMB_CNT ((NumTombs < 0) ? LEAF_PAGE_DEFAULT_TOMB_CNT : NumTombs)
#define LEAF_PAGE_SLOT_CNT                                                                               \
  ((BUSTUB_PAGE_SIZE - LEAF_PAGE_HEADER_SIZE - sizeof(size_t) - (LEAF_PAGE_TOMB_CNT * sizeof(size_t))) / \
   (sizeof(KeyType) + sizeof(ValueType)))  // NOLINT

/**
 * Store indexed key and record id(record id = page id combined with slot id,
 * see include/common/rid.h for detailed implementation) together within leaf
 * page. Duplicate mode sorts equal keys by their value location.
 *
 * Leaf pages also contain a fixed buffer of "tombstone" indexes for entries
 * that have been deleted.
 *
 * Leaf page format (keys are stored in order, tomb order is up to you):
 *  --------------------
 * | HEADER | TOMB_SIZE | (where TOMB_SIZE is num_tombstones_)
 *  --------------------
 *  -----------------------------------
 * | TOMB(0) | TOMB(1) | ... | TOMB(k) |
 *  -----------------------------------
 *  ---------------------------------
 * | KEY(1) | KEY(2) | ... | KEY(n) |
 *  ---------------------------------
 *  ---------------------------------
 * | RID(1) | RID(2) | ... | RID(n) |
 *  ---------------------------------
 *
 *  Header format (size in byte, 16 bytes in total):
 *  -----------------------------------------------
 * | PageType (4) | CurrentSize (4) | MaxSize (4) |
 *  -----------------------------------------------
 *  -----------------
 * | NextPageId (4) |
 *  -----------------
 */
FULL_INDEX_TEMPLATE_ARGUMENTS_DEFN
class BPlusTreeLeafPage : public BPlusTreePage {
 public:
  // Delete all constructor / destructor to ensure memory safety
  BPlusTreeLeafPage() = delete;
  BPlusTreeLeafPage(const BPlusTreeLeafPage &other) = delete;

  void Init(int max_size = LEAF_PAGE_SLOT_CNT);

  auto GetTombstones() const -> std::vector<KeyType>;

  auto KeyRef(int i) const -> const KeyType & { return key_array_[i]; }
  auto ValueRef(int i) const -> const ValueType & { return rid_array_[i]; }
  auto ValueAt(int i) const -> ValueType { return rid_array_[i]; }
  void SetValueAt(int i, const ValueType &value) { rid_array_[i] = value; }
  auto IsTomb(int i) const -> bool {
    return std::find(tombstones_, tombstones_ + num_tombstones_, static_cast<size_t>(i)) != tombstones_ + num_tombstones_;
  }
  void Untomb(int i) {
    auto end = std::remove(tombstones_, tombstones_ + num_tombstones_, static_cast<size_t>(i));
    num_tombstones_ = end - tombstones_;
  }
  void InsertAt(int i, const KeyType &key, const ValueType &value) {
    for (int j = GetSize(); j > i; --j) { key_array_[j] = key_array_[j-1]; rid_array_[j] = rid_array_[j-1]; }
    for (size_t j = 0; j < num_tombstones_; ++j) { if (tombstones_[j] >= static_cast<size_t>(i)) { ++tombstones_[j]; } }
    key_array_[i] = key; rid_array_[i] = value; ChangeSizeBy(1);
  }
  void EraseAt(int i) {
    Untomb(i);
    for (int j = i + 1; j < GetSize(); ++j) { key_array_[j-1] = key_array_[j]; rid_array_[j-1] = rid_array_[j]; }
    for (size_t j = 0; j < num_tombstones_; ++j) { if (tombstones_[j] > static_cast<size_t>(i)) { --tombstones_[j]; } }
    ChangeSizeBy(-1);
  }
  void Tomb(int i) {
    if constexpr (LEAF_PAGE_TOMB_CNT == 0) { EraseAt(i); }
    else {
      if (num_tombstones_ == LEAF_PAGE_TOMB_CNT) { auto old = tombstones_[0]; EraseAt(old); if (old < static_cast<size_t>(i)) { --i; } }
      tombstones_[num_tombstones_++] = i;
    }
  }
  // Split without flushing pending deletions; preserve each page's tombstone FIFO.
  void SplitTo(BPlusTreeLeafPage *right, int cut) {
    int n = GetSize();
    for (int i = cut; i < n; ++i) { right->InsertAt(right->GetSize(), key_array_[i], rid_array_[i]); }
    size_t left_count = 0;
    for (size_t j = 0; j < num_tombstones_; ++j) {
      auto i = tombstones_[j];
      if (i >= static_cast<size_t>(cut)) { right->tombstones_[right->num_tombstones_++] = i - cut; }
      else { tombstones_[left_count++] = i; }
    }
    num_tombstones_ = left_count; SetSize(cut);
  }
  // Used during borrowing; incoming deletions are newer than the recipient's pending deletions.
  void MoveOneTo(BPlusTreeLeafPage *to, int from, int at) {
    auto key = KeyAt(from); auto value = ValueAt(from); bool tomb = IsTomb(from);
    EraseAt(from); to->InsertAt(at, key, value); if (tomb) { to->Tomb(at); }
  }
  void MergeFrom(BPlusTreeLeafPage *right) {
    for (int i = 0; i < right->GetSize(); ++i) { InsertAt(GetSize(), right->KeyAt(i), right->ValueAt(i)); }
    // Apply source tombstones in FIFO order after those already in the destination.
    auto tombs = right->GetTombstones();
    for (const auto &key : tombs) {
      for (int i = 0; i < GetSize(); ++i) {
        if (std::memcmp(&key_array_[i], &key, sizeof(KeyType)) == 0) { Tomb(i); break; }
      }
    }
  }
  // Helper methods
  auto GetNextPageId() const -> page_id_t;
  void SetNextPageId(page_id_t next_page_id);
  auto KeyAt(int index) const -> KeyType;

  /**
   * @brief for test only return a string representing all keys in
   * this leaf page formatted as "(tombkey1, tombkey2, ...|key1,key2,key3,...)"
   *
   * @return std::string
   */
  auto ToString() const -> std::string {
    std::string kstr = "(";
    bool first = true;

    auto tombs = GetTombstones();
    for (size_t i = 0; i < tombs.size(); i++) {
      kstr.append(std::to_string(tombs[i].ToString()));
      if ((i + 1) < tombs.size()) {
        kstr.append(",");
      }
    }

    kstr.append("|");

    for (int i = 0; i < GetSize(); i++) {
      KeyType key = KeyAt(i);
      if (first) {
        first = false;
      } else {
        kstr.append(",");
      }

      kstr.append(std::to_string(key.ToString()));
    }
    kstr.append(")");

    return kstr;
  }

 private:
  page_id_t next_page_id_;
  size_t num_tombstones_;
  // Fixed-size tombstone buffer (indexes into key_array_ / rid_array_).
  size_t tombstones_[(LEAF_PAGE_TOMB_CNT > 0) ? LEAF_PAGE_TOMB_CNT : 1];
  // Array members for page data.
  KeyType key_array_[LEAF_PAGE_SLOT_CNT];
  ValueType rid_array_[LEAF_PAGE_SLOT_CNT];
  // Feel free to add more fields and helper functions below if needed
};

}  // namespace bustub

#include "storage/page/b_plus_tree_leaf_page_impl.h"
