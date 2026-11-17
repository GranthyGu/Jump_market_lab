#pragma once
#include <map>
#include <memory>
#include <optional>
#include <unordered_map>

#include "matching_engine/core/backend.hpp"
#include "matching_engine/persistence/model.hpp"
#include "shared_data/orders/config.hpp"

namespace marketlab::orders::persistence {
struct Page {
  static constexpr std::size_t slots = 128;
  std::array<std::optional<CompactOrder>, slots> orders;
};

using FrozenPages = std::vector<std::shared_ptr<const Page>>;

class CommittedState {
 public:
  CommittedState(const OrderConfig&, std::uint16_t);
  void apply(const Change&);
  FrozenPages freeze() const;
  bool needs_copy(const Change&) const;

  std::size_t copied_bytes() const {
    return copied_bytes_;
  }

  void reset_copy_count() {
    copied_bytes_ = 0;
  }

  void market(MarketSnapshot&) const noexcept;

  std::size_t symbol_count() const {
    return books_.size();
  }

 private:
  struct Level {
    std::uint64_t quantity = 0, count = 0;
  };

  struct Book {
    std::uint16_t id = 0;
    std::map<Price, Level> bids, asks;
    bool extended = false;
  };

  std::vector<std::shared_ptr<Page>> pages_;
  std::vector<std::size_t> free_;
  std::unordered_map<OrderId, std::size_t> positions_;
  std::vector<std::size_t> symbol_books_;
  std::vector<Book> books_;
  md::Epoch epoch_;
  std::size_t copied_bytes_ = 0;
  Page& writable(std::size_t);
  std::map<Price, Level>& levels(const CompactOrder&);
  void aggregate(const CompactOrder&, bool add);
  void replace(const CompactOrder& previous, const CompactOrder& next);
};
}  // namespace marketlab::orders::persistence
