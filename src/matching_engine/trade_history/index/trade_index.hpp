#pragma once

#include <cstdint>
#include <vector>
#include "page_file.hpp"
#include "storage/index/b_plus_tree.h"

namespace marketlab::orders::bpt {
struct UserComparator {
  int operator()(std::uint32_t a, std::uint32_t b) const {
    return a < b ? -1 : (a > b ? 1 : 0);
  }
};

// Logical leaf key: 4-byte user ID. One entry per participant per trade.
// Frame + ordinal locate the canonical record. A derived per-record CRC lets
// sparse queries verify 136 bytes instead of reading an entire matching batch.
struct TradePosition {
  std::uint64_t frame = 0;
  std::uint32_t ordinal = 0;
  std::uint32_t checksum = 0;

  std::uint64_t Get() const {
    return frame + 48 + static_cast<std::uint64_t>(ordinal) * 136;
  }

  bool operator==(const TradePosition&) const = default;
};

static_assert(sizeof(TradePosition) == 16);
using TradeTree = bustub::BPlusTree<std::uint32_t, TradePosition, UserComparator>;

class TradeIndex {
 public:
  TradeIndex(const std::filesystem::path& path,
             std::size_t cache_pages,
             bool reopen = false,
             int leaf_size = 400,
             int internal_size = 500)
      : file_(path, !reopen),
        pool_(cache_pages, &file_) {
    if (reopen) {
      auto bytes = file_.size();
      if (bytes == 0 || bytes % bustub::BUSTUB_PAGE_SIZE != 0 ||
          bytes / bustub::BUSTUB_PAGE_SIZE >= INT32_MAX)
        throw std::runtime_error("invalid B+ tree page file size");
      pool_.SetNextPageId(static_cast<bustub::page_id_t>(bytes / bustub::BUSTUB_PAGE_SIZE));
    } else if (pool_.NewPage() != 0) {
      throw std::logic_error("B+ tree header must be page zero");
    }
    tree_ = std::make_unique<TradeTree>(
        "user_trades", 0, &pool_, UserComparator{}, leaf_size, internal_size, true, reopen);
  }

  bool insert(std::uint32_t user, TradePosition position) {
    return tree_->Insert(user, position);
  }

  // A bounded range scan. after=0 starts at the first entry; through is the
  // captured committed log boundary. No later trade is visible in this query.
  std::vector<TradePosition> query(std::uint32_t user,
                                   std::uint64_t after,
                                   std::uint64_t through,
                                   std::size_t limit) {
    std::vector<TradePosition> result;
    if (limit == 0 || through == 0)
      return result;
    result.reserve(limit);
    // Construct a synthetic value with exactly the desired byte offset.
    auto it =
        after >= 48 ? tree_->BeginAfter(user, TradePosition{after - 48, 0, 0}) : tree_->Begin(user);
    while (!it.IsEnd() && (*it).first == user && result.size() < limit) {
      const auto position = (*it).second;
      if (position.Get() >= through)
        break;
      result.push_back(position);
      ++it;
    }
    return result;
  }

  void flush() {
    pool_.FlushAllPages();
    file_.sync();
  }

  // Explicit cache control for experiments; normal indexing never calls this.
  // Call between operations to measure a tree with no cross-operation page reuse.
  std::size_t evict_unpinned_pages() {
    return pool_.EvictUnpinnedPages();
  }

  PageFile::Metrics metrics() const {
    return file_.metrics();
  }

  std::size_t cache_pages() const {
    return pool_.Size();
  }

  std::uint64_t page_count() const {
    return pool_.NextPageId();
  }

 private:
  PageFile file_;
  bustub::BufferPoolManager pool_;
  std::unique_ptr<TradeTree> tree_;
};
}  // namespace marketlab::orders::bpt
