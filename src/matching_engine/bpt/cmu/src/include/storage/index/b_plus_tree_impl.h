//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree.cpp
//
// Identification: src/storage/index/b_plus_tree.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once
#include <stdexcept>
#include <thread>
namespace bustub {
FULL_INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header, BufferPoolManager *bpm, const KeyComparator &comparator,
                         int leaf_size, int internal_size, bool duplicates, bool reopen)
 : bpm_(std::make_shared<TracedBufferPoolManager>(bpm)), duplicates_(duplicates), index_name_(std::move(name)),
   comparator_(comparator), leaf_max_size_(leaf_size), internal_max_size_(internal_size), header_page_id_(header) {
  static_assert(sizeof(LeafPage) <= BUSTUB_PAGE_SIZE);
  static_assert(sizeof(InternalPage) <= BUSTUB_PAGE_SIZE);
  if (leaf_size < 2 || leaf_size > static_cast<int>(LEAF_PAGE_SLOT_CNT) || internal_size < 3 || internal_size > InternalPage::Capacity()) {
    throw std::invalid_argument("invalid B+ tree page capacity");
  }
  if (duplicates && LEAF_PAGE_TOMB_CNT != 0) { throw std::invalid_argument("duplicate mode is append-only"); }
  if (!reopen) { auto guard = bpm_->WritePage(header); guard.template AsMut<BPlusTreeHeaderPage>()->root_page_id_ = INVALID_PAGE_ID; }
}
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  return bpm_->ReadPage(header_page_id_).template As<BPlusTreeHeaderPage>()->root_page_id_;
}
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  return bpm_->ReadPage(header_page_id_).template As<BPlusTreeHeaderPage>()->root_page_id_ == INVALID_PAGE_ID;
}
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Child(const InternalPage *p, const KeyType &key, uint64_t tie) const -> int {
  int lo = 1, hi = p->GetSize();
  while (lo < hi) { int mid = (lo + hi) / 2;
    if (Compare(p->KeyAt(mid), p->TieAt(mid), key, tie) <= 0) { lo = mid + 1; } else { hi = mid; }
  } return lo - 1;
}
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Position(const LeafPage *p, const KeyType &key, uint64_t tie, bool strict) const -> int {
  int lo = 0, hi = p->GetSize();
  while (lo < hi) { int mid = (lo + hi) / 2; auto c = Compare(p->KeyAt(mid), Tie(p->ValueAt(mid)), key, tie);
    if (c < 0 || (strict && c == 0)) { lo = mid + 1; } else { hi = mid; }
  } return lo;
}
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::OptimisticLeaf(const KeyType &key, uint64_t tie, bool inserting) -> std::optional<WritePageGuard> {
  auto parent = bpm_->ReadPage(header_page_id_);
  auto root = parent.template As<BPlusTreeHeaderPage>()->root_page_id_;
  if (root == INVALID_PAGE_ID) { return std::nullopt; }
  auto guard = bpm_->ReadPage(root);
  while (!guard.template As<BPlusTreePage>()->IsLeafPage()) {
    auto p = guard.template As<InternalPage>(); auto child = p->ValueAt(Child(p, key, tie));
    parent = std::move(guard); guard = bpm_->ReadPage(child);
  }
  auto id = guard.GetPageId(); guard.Drop();
  // The immediate parent remains read-latched across the upgrade: no split/merge can move this leaf.
  auto writer = bpm_->WritePage(id); auto p = writer.template As<LeafPage>();
  bool safe = inserting ? p->GetSize() < p->GetMaxSize() : p->GetSize() > p->GetMinSize();
  if (!safe) { return std::nullopt; } return writer;
}
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertLeaf(LeafPage *p, const KeyType &key, const ValueType &value) -> bool {
  int at = Position(p, key, Tie(value));
  if (at < p->GetSize() && Compare(p->KeyAt(at), Tie(p->ValueAt(at)), key, Tie(value)) == 0) {
    if (!p->IsTomb(at)) { return false; } p->Untomb(at); p->SetValueAt(at, value); return true;
  }
  p->InsertAt(at, key, value); return true;
}
FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SplitLeaf(LeafPage *left, LeafPage *right, const KeyType &key, const ValueType &value) {
  std::vector<std::pair<KeyType, ValueType>> entries;
  entries.reserve(left->GetSize() + 1); int at = Position(left, key, Tie(value));
  for (int i = 0; i <= left->GetSize(); ++i) {
    if (i == at) { entries.emplace_back(key, value); }
    if (i != left->GetSize()) { entries.emplace_back(left->KeyAt(i), left->ValueAt(i)); }
  }
  auto tombs = left->GetTombstones(); auto next = left->GetNextPageId();
  left->Init(leaf_max_size_); right->Init(leaf_max_size_); right->SetNextPageId(next);
  const int cut = entries.size() / 2;
  for (size_t i = 0; i < entries.size(); ++i) {
    auto dst = static_cast<int>(i) < cut ? left : right; dst->InsertAt(dst->GetSize(), entries[i].first, entries[i].second);
  }
  for (const auto &tomb : tombs) {
    auto dst = comparator_(tomb, right->KeyAt(0)) < 0 ? left : right;
    dst->Tomb(Position(dst, tomb, 0));
  }
}
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value) -> bool {
  if (auto guard = OptimisticLeaf(key, Tie(value), true)) { return InsertLeaf(guard->template AsMut<LeafPage>(), key, value); }
  Context ctx; ctx.header_page_ = bpm_->WritePage(header_page_id_);
  auto header = ctx.header_page_->template AsMut<BPlusTreeHeaderPage>();
  ctx.root_page_id_ = header->root_page_id_;
  if (ctx.root_page_id_ == INVALID_PAGE_ID) {
    auto id = bpm_->NewPage(); auto guard = bpm_->WritePage(id); auto leaf = guard.template AsMut<LeafPage>();
    leaf->Init(leaf_max_size_); leaf->InsertAt(0, key, value); header->root_page_id_ = id; return true;
  }
  ctx.write_set_.push_back(bpm_->WritePage(ctx.root_page_id_));
  while (!ctx.write_set_.back().template As<BPlusTreePage>()->IsLeafPage()) {
    auto p = ctx.write_set_.back().template As<InternalPage>(); auto child = bpm_->WritePage(p->ValueAt(Child(p, key, Tie(value))));
    auto c = child.template As<BPlusTreePage>();
    if (c->GetSize() < c->GetMaxSize()) { ctx.write_set_.clear(); ctx.header_page_.reset(); }
    ctx.write_set_.push_back(std::move(child));
  }
  auto leaf = ctx.write_set_.back().template AsMut<LeafPage>(); int at = Position(leaf, key, Tie(value));
  if (leaf->GetSize() < leaf->GetMaxSize() || (at < leaf->GetSize() && Compare(leaf->KeyAt(at), Tie(leaf->ValueAt(at)), key, Tie(value)) == 0)) {
    return InsertLeaf(leaf, key, value);
  }
  auto right_id = bpm_->NewPage(); auto right_guard = bpm_->WritePage(right_id);
  auto right = right_guard.template AsMut<LeafPage>();
  SplitLeaf(leaf, right, key, value); leaf->SetNextPageId(right_id);
  KeyType separator = right->KeyAt(0); uint64_t sep_tie = Tie(right->ValueAt(0));
  auto left_id = ctx.write_set_.back().GetPageId(); right_guard.Drop(); ctx.write_set_.pop_back();
  for (;;) {
    if (ctx.write_set_.empty()) {
      BUSTUB_ASSERT(ctx.header_page_.has_value(), "root split requires header latch");
      auto id = bpm_->NewPage(); auto guard = bpm_->WritePage(id); auto root = guard.template AsMut<InternalPage>();
      root->Init(internal_max_size_); root->InsertAt(0, {}, 0, left_id); root->InsertAt(1, separator, sep_tie, right_id);
      ctx.header_page_->template AsMut<BPlusTreeHeaderPage>()->root_page_id_ = id; return true;
    }
    auto &parent_guard = ctx.write_set_.back(); auto parent = parent_guard.template AsMut<InternalPage>();
    int slot = parent->ValueIndex(left_id) + 1;
    BUSTUB_ASSERT(slot > 0, "missing split child");
    if (parent->GetSize() < parent->GetMaxSize()) { parent->InsertAt(slot, separator, sep_tie, right_id); return true; }
    struct Entry { KeyType key; uint64_t tie; page_id_t id; };
    std::vector<Entry> entries; entries.reserve(parent->GetSize() + 1);
    for (int i = 0; i <= parent->GetSize(); ++i) {
      if (i == slot) { entries.push_back({separator, sep_tie, right_id}); }
      if (i < parent->GetSize()) { entries.push_back({parent->KeyAt(i), parent->TieAt(i), parent->ValueAt(i)}); }
    }
    right_id = bpm_->NewPage(); auto guard = bpm_->WritePage(right_id); auto sibling = guard.template AsMut<InternalPage>();
    sibling->Init(internal_max_size_); parent->SetSize(0); const int cut = entries.size() / 2;
    separator = entries[cut].key; sep_tie = entries[cut].tie;
    for (size_t i = 0; i < entries.size(); ++i) {
      auto dst = static_cast<int>(i) < cut ? parent : sibling; const auto &e = entries[i]; dst->InsertAt(dst->GetSize(), e.key, e.tie, e.id);
    }
    left_id = parent_guard.GetPageId(); ctx.write_set_.pop_back();
  }
}
FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key) {
  if (duplicates_) { throw std::logic_error("duplicate trade index is append-only"); }
  if (auto guard = OptimisticLeaf(key, 0, false)) {
    auto leaf = guard->template AsMut<LeafPage>(); int at = Position(leaf, key, 0);
    if (at < leaf->GetSize() && comparator_(leaf->KeyAt(at), key) == 0 && !leaf->IsTomb(at)) { leaf->Tomb(at); }
    return;
  }
  Context ctx; ctx.header_page_ = bpm_->WritePage(header_page_id_);
  ctx.root_page_id_ = ctx.header_page_->template As<BPlusTreeHeaderPage>()->root_page_id_;
  if (ctx.root_page_id_ == INVALID_PAGE_ID) { return; }
  ctx.write_set_.push_back(bpm_->WritePage(ctx.root_page_id_));
  while (!ctx.write_set_.back().template As<BPlusTreePage>()->IsLeafPage()) {
    auto p = ctx.write_set_.back().template As<InternalPage>(); auto child = bpm_->WritePage(p->ValueAt(Child(p, key, 0)));
    auto c = child.template As<BPlusTreePage>();
    if (c->GetSize() > c->GetMinSize()) { ctx.write_set_.clear(); ctx.header_page_.reset(); }
    ctx.write_set_.push_back(std::move(child));
  }
  auto leaf = ctx.write_set_.back().template AsMut<LeafPage>(); int at = Position(leaf, key, 0);
  if (at == leaf->GetSize() || comparator_(leaf->KeyAt(at), key) != 0 || leaf->IsTomb(at)) { return; }
  leaf->Tomb(at);
  for (;;) {
    auto &guard = ctx.write_set_.back(); auto page = guard.template As<BPlusTreePage>(); auto id = guard.GetPageId();
    if (id == ctx.root_page_id_) {
      if (page->IsLeafPage() && page->GetSize() == 0) {
        ctx.header_page_->template AsMut<BPlusTreeHeaderPage>()->root_page_id_ = INVALID_PAGE_ID;
        guard.Drop(); bpm_->DeletePage(id);
      } else if (!page->IsLeafPage() && page->GetSize() == 1) {
        auto child = guard.template As<InternalPage>()->ValueAt(0);
        ctx.header_page_->template AsMut<BPlusTreeHeaderPage>()->root_page_id_ = child;
        guard.Drop(); bpm_->DeletePage(id);
      } return;
    }
    if (page->GetSize() >= page->GetMinSize()) { return; }
    BUSTUB_ASSERT(ctx.write_set_.size() >= 2, "underflow requires parent latch");
    auto &parent_guard = ctx.write_set_[ctx.write_set_.size()-2]; auto parent = parent_guard.template AsMut<InternalPage>();
    int slot = parent->ValueIndex(id); int sibling_slot = slot > 0 ? slot - 1 : slot + 1;
    auto sibling_guard = bpm_->WritePage(parent->ValueAt(sibling_slot)); auto sibling_base = sibling_guard.template As<BPlusTreePage>();
    const bool sibling_left = sibling_slot < slot;
    if (page->IsLeafPage()) {
      auto current = guard.template AsMut<LeafPage>(); auto sibling = sibling_guard.template AsMut<LeafPage>();
      while (current->GetSize() < current->GetMinSize() && sibling->GetSize() > sibling->GetMinSize()) {
        sibling->MoveOneTo(current, sibling_left ? sibling->GetSize()-1 : 0, sibling_left ? 0 : current->GetSize());
      }
      if (current->GetSize() >= current->GetMinSize()) {
        auto right = sibling_left ? current : sibling; int sep = sibling_left ? slot : sibling_slot;
        parent->SetKeyAt(sep, right->KeyAt(0)); parent->SetTieAt(sep, Tie(right->ValueAt(0))); return;
      }
      auto left = sibling_left ? sibling : current; auto right = sibling_left ? current : sibling;
      left->MergeFrom(right); left->SetNextPageId(right->GetNextPageId());
    } else {
      auto current = guard.template AsMut<InternalPage>(); auto sibling = sibling_guard.template AsMut<InternalPage>();
      if (sibling_base->GetSize() > sibling_base->GetMinSize()) {
        if (sibling_left) {
          current->SetKeyAt(0, parent->KeyAt(slot)); current->SetTieAt(0, parent->TieAt(slot));
          int last = sibling->GetSize()-1;
          current->InsertAt(0, {}, 0, sibling->ValueAt(last));
          parent->SetKeyAt(slot, sibling->KeyAt(last)); parent->SetTieAt(slot, sibling->TieAt(last)); sibling->EraseAt(last);
        } else {
          current->InsertAt(current->GetSize(), parent->KeyAt(sibling_slot), parent->TieAt(sibling_slot), sibling->ValueAt(0));
          parent->SetKeyAt(sibling_slot, sibling->KeyAt(1)); parent->SetTieAt(sibling_slot, sibling->TieAt(1)); sibling->EraseAt(0);
        } return;
      }
      auto left = sibling_left ? sibling : current; auto right = sibling_left ? current : sibling; int sep = sibling_left ? slot : sibling_slot;
      left->InsertAt(left->GetSize(), parent->KeyAt(sep), parent->TieAt(sep), right->ValueAt(0));
      for (int i = 1; i < right->GetSize(); ++i) { left->InsertAt(left->GetSize(), right->KeyAt(i), right->TieAt(i), right->ValueAt(i)); }
    }
    auto removed = sibling_left ? id : sibling_guard.GetPageId(); parent->EraseAt(sibling_left ? slot : sibling_slot);
    guard.Drop(); sibling_guard.Drop(); bpm_->DeletePage(removed); ctx.write_set_.pop_back();
  }
}
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Seek(const KeyType *key, uint64_t tie, bool strict) -> INDEXITERATOR_TYPE {
  for (;;) {
    auto header = bpm_->ReadPage(header_page_id_); auto root = header.template As<BPlusTreeHeaderPage>()->root_page_id_;
    if (root == INVALID_PAGE_ID) { return {}; }
    auto guard = bpm_->ReadPage(root); header.Drop();
    while (!guard.template As<BPlusTreePage>()->IsLeafPage()) {
      auto page = guard.template As<InternalPage>(); auto child = page->ValueAt(key ? Child(page, *key, tie) : 0);
      auto next = bpm_->ReadPage(child); guard = std::move(next);
    }
    for (;;) {
      auto leaf = guard.template As<LeafPage>(); int pos = key ? Position(leaf, *key, tie, strict) : 0;
      while (pos < leaf->GetSize() && leaf->IsTomb(pos)) { ++pos; }
      if (pos < leaf->GetSize()) {
        return INDEXITERATOR_TYPE(std::move(guard), pos, [this](const KeyType &k, const ValueType &v) { return Seek(&k, Tie(v), true); });
      }
      auto next_id = leaf->GetNextPageId(); if (next_id == INVALID_PAGE_ID) { return {}; }
      // Never wait for a sibling while holding a leaf: deletion may latch the same pair in reverse order.
      auto next = bpm_->TryReadPage(next_id);
      if (!next) { guard.Drop(); std::this_thread::yield(); break; }
      guard = std::move(*next);
    }
  }
}
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { return Seek(nullptr, 0, false); }
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { return Seek(&key, 0, false); }
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::BeginAfter(const KeyType &key, const ValueType &value) -> INDEXITERATOR_TYPE { return Seek(&key, Tie(value), true); }
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return {}; }
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result) -> bool {
  bool found = false;
  for (auto it = Begin(key); !it.IsEnd() && comparator_((*it).first, key) == 0; ++it) {
    result->push_back((*it).second); found = true; if (!duplicates_) { break; }
  } return found;
}
}  // namespace bustub
