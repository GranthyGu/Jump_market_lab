#pragma once
#include <map>
#include <memory>
#include <memory_resource>
#include <string_view>
#include <unordered_map>

#include "matching_engine/core/active_index.hpp"
#include "matching_engine/core/backend.hpp"
#include "matching_engine/persistence/model.hpp"
#include "matching_engine/persistence/order_dictionary.hpp"
#include "shared_data/orders/config.hpp"

namespace marketlab::orders {
class MatchingEngine final : public EngineBackend {
 public:
  MatchingEngine(const OrderConfig&, std::uint16_t shard);
  void execute(const LinkHeader&, std::span<const std::uint8_t>, persistence::Result&);
  void restore(std::span<const persistence::StoredOrder>,
               std::uint64_t next_order,
               std::uint64_t next_trade);
  // Apply authoritative final state without matching again or emitting events.
  void replicate(std::span<const persistence::Change>,
                 const persistence::OrderDictionary&,
                 const persistence::Boundary&);

  std::uint64_t next_order() const {
    return next_order_;
  }

  std::uint64_t next_trade() const {
    return next_trade_;
  }

  std::size_t active_order_count(std::uint32_t user) const;
  void copy_active_orders(std::uint32_t user, std::span<OrderState> destination) const;
  void command(const LinkHeader&, std::span<const std::uint8_t>, const Emit&) override;

  std::size_t symbol_count() const noexcept override {
    return books_.size();
  }

  void copy_market(MarketSnapshot&) noexcept override;

 private:
  struct Order;

  struct Level {
    Order* head = nullptr;
    Order* tail = nullptr;
    std::uint64_t quantity = 0, count = 0;
  };

  using Levels = std::pmr::map<Price, Level>;

  struct Book {
    explicit Book(std::pmr::memory_resource* resource)
        : bids(resource),
          asks(resource) {
    }

    SymbolName symbol{};
    std::uint16_t symbol_id = 0;
    std::int64_t lower = 0, upper = 0;
    Levels bids, asks;
  };

  struct Order {
    OrderState state{};
    std::uint32_t user = 0;
    Book* book = nullptr;
    Levels::iterator position{};
    Order* prev = nullptr;
    Order* next = nullptr;
    Order* user_prev = nullptr;
    Order* user_next = nullptr;
  };

  // Stable pointers, lazy 1024-node blocks, and an intrusive free list.
  class Pool {
   public:
    Order* acquire();
    void release(Order* order) noexcept;

   private:
    static constexpr std::size_t block_size = 1024;
    std::vector<std::unique_ptr<Order[]>> blocks_;
    Order* free_ = nullptr;
  };

  using Event = persistence::Event;
  persistence::Result* capture_ = nullptr;
  std::vector<persistence::Name> usernames_;
  void change(const Order&, bool erase);
  std::uint16_t shard_;
  std::uint64_t next_order_ = 1, next_trade_ = 1;
  md::Epoch epoch_;
  std::pmr::unsynchronized_pool_resource price_memory_;
  std::vector<Book> books_;
  std::unordered_map<std::string_view, std::size_t> symbols_;
  std::vector<Order*> users_;
  std::vector<std::size_t> user_order_counts_;
  ActiveIndex<Order> active_;
  Pool pool_;
  std::vector<Event> outgoing_;  // Reused by the single command thread.
  template <typename T>
  void queue(LinkKind, const LinkHeader&, const T&);
  void update(const Order&, RequestId, const LinkHeader&, Timestamp = 0);
  void trade(const Order&, TradeId, Price, Quantity, Timestamp);
  void new_order(const LinkHeader&, const NewOrderRequest&);
  void cancel(const LinkHeader&, const CancelRequest&);
  void snapshot(const LinkHeader&, const Header&);
  void match(Order&, Levels&);
  void rest(const Order&);
  void unlink(Order&);
};
}  // namespace marketlab::orders
