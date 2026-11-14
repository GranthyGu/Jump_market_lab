//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_internal_page.h
//
// Identification: src/include/storage/page/b_plus_tree_internal_page.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <queue>
#include <string>

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

#define B_PLUS_TREE_INTERNAL_PAGE_TYPE BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>
#define INTERNAL_PAGE_HEADER_SIZE 32
#define INTERNAL_PAGE_SLOT_CNT                      \
  ((BUSTUB_PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / \
   ((int)(sizeof(KeyType) + sizeof(ValueType) + sizeof(uint64_t))))  // NOLINT

/**
 * Store `n` child pointers (page_id) and `n - 1` indexed keys within internal page.
 * Pointer PAGE_ID(i) points to a subtree in which all keys K satisfy:
 * K(i) <= K < K(i+1).
 * NOTE: Since the number of keys does not equal to number of child pointers,
 * the first key in key_array_ always remains invalid. That is to say, any search / lookup
 * should ignore the first key.
 *
 * Internal page format (keys are stored in increasing order):
 *  ---------
 * | HEADER |
 *  ---------
 *  ------------------------------------------
 * | KEY(1)(INVALID) | KEY(2) | ... | KEY(n) |
 *  ------------------------------------------
 *  ---------------------------------------------
 * | PAGE_ID(1) | PAGE_ID(2) | ... | PAGE_ID(n) |
 *  ---------------------------------------------
 */
INDEX_TEMPLATE_ARGUMENTS

class BPlusTreeInternalPage : public BPlusTreePage {
 public:
  static constexpr auto Capacity() -> int {
    return INTERNAL_PAGE_SLOT_CNT;
  }

  // Tie breakers live only in internal routing entries. Leaf keys remain exactly KeyType.
  auto TieAt(int i) const -> uint64_t {
    return tie_array_[i];
  }

  void SetTieAt(int i, uint64_t value) {
    tie_array_[i] = value;
  }

  void SetValueAt(int i, ValueType value) {
    page_id_array_[i] = value;
  }

  void InsertAt(int at, const KeyType& key, uint64_t tie, ValueType value) {
    for (int i = GetSize(); i > at; --i) {
      key_array_[i] = key_array_[i - 1];
      tie_array_[i] = tie_array_[i - 1];
      page_id_array_[i] = page_id_array_[i - 1];
    }
    key_array_[at] = key;
    tie_array_[at] = tie;
    page_id_array_[at] = value;
    ChangeSizeBy(1);
  }

  void EraseAt(int at) {
    for (int i = at + 1; i < GetSize(); ++i) {
      key_array_[i - 1] = key_array_[i];
      tie_array_[i - 1] = tie_array_[i];
      page_id_array_[i - 1] = page_id_array_[i];
    }
    ChangeSizeBy(-1);
  }

  // Delete all constructor / destructor to ensure memory safety
  BPlusTreeInternalPage() = delete;
  BPlusTreeInternalPage(const BPlusTreeInternalPage& other) = delete;

  void Init(int max_size = INTERNAL_PAGE_SLOT_CNT);

  auto KeyAt(int index) const -> KeyType;

  void SetKeyAt(int index, const KeyType& key);

  /**
   * @param value The value to search for
   * @return The index that corresponds to the specified value
   */
  auto ValueIndex(const ValueType& value) const -> int;

  auto ValueAt(int index) const -> ValueType;

  /**
   * @brief For test only, return a string representing all keys in
   * this internal page, formatted as "(key1,key2,key3,...)"
   *
   * @return The string representation of all keys in the current internal page
   */
  auto ToString() const -> std::string {
    std::string kstr = "(";
    bool first = true;

    // First key of internal page is always invalid
    for (int i = 1; i < GetSize(); i++) {
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
  // Array members for page data.
  KeyType key_array_[INTERNAL_PAGE_SLOT_CNT];
  ValueType page_id_array_[INTERNAL_PAGE_SLOT_CNT];
  uint64_t tie_array_[INTERNAL_PAGE_SLOT_CNT];
  // Feel free to add more fields and helper functions below if needed
};

}  // namespace bustub

#include "storage/page/b_plus_tree_internal_page_impl.h"
