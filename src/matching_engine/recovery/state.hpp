#pragma once
#include <map>
#include <memory>
#include <optional>
#include <unordered_map>

#include "matching_engine/backend.hpp"
#include "matching_engine/persistence/model.hpp"
#include "shared_data/orders/config.hpp"

namespace marketlab::orders::persistence {
struct Page {
  static constexpr std::size_t slots = 128;
  std::array<std::optional<StoredOrder>, slots> orders;
};
using FrozenPages = std::vector<std::shared_ptr<const Page>>;
class CommittedState {
 public:
  CommittedState(const OrderConfig&, std::uint16_t);
  void apply(const Change&);
  FrozenPages freeze() const;
  bool needs_copy(const Change&) const;
  std::size_t copied_bytes() const { return copied_bytes_; }
  void reset_copy_count() { copied_bytes_ = 0; }
  std::vector<StoredOrder> orders() const;
  void snapshot(const LinkHeader&, const Header&, const EngineBackend::Emit&) const;
  void market(MarketSnapshot&) const noexcept;
  std::size_t symbol_count() const { return books_.size(); }

 private:
  struct Level {
    std::uint64_t quantity = 0, count = 0;
  };
  struct Book {
    std::uint16_t id = 0;
    std::map<Price, Level> bids, asks;
  };
  std::vector<std::shared_ptr<Page>> pages_;
  std::vector<std::size_t> free_;
  std::unordered_map<OrderId, std::size_t> positions_;
  std::unordered_map<std::string, std::size_t> symbols_;
  std::vector<Name> users_;
  std::vector<Book> books_;
  md::Epoch epoch_;
  std::size_t copied_bytes_ = 0;
  Page& writable(std::size_t);
  void aggregate(const StoredOrder&, bool add);
};
}  // namespace marketlab::orders::persistence
