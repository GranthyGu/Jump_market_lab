#pragma once
#include <map>
#include <memory>
#include <memory_resource>
#include <optional>
#include <string_view>
#include <unordered_map>

#include "matching_engine/core/active_index.hpp"
#include "matching_engine/core/backend.hpp"
#include "matching_engine/persistence/model.hpp"
#include "matching_engine/persistence/order_dictionary.hpp"
#include "matching_engine/spreads/depth.hpp"
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

  // Group execution uses the same real books, FIFO nodes and active index.
  // These owner-thread primitives never send messages or wait on another shard.
  struct FillPiece {
    persistence::CompactOrder after;
    Quantity quantity = 0;
  };

  std::optional<persistence::CompactOrder> begin_group(const LinkHeader&,
                                                       const NewOrderRequest&,
                                                       persistence::Result&);
  void finish_group(const persistence::CompactOrder&, persistence::Result&);
  void take_prefix(std::uint16_t symbol,
                   Side resting_side,
                   Quantity,
                   std::vector<FillPiece>&,
                   persistence::Result&);
  void fill_events(
      const persistence::CompactOrder&, TradeId, Price, Quantity, Timestamp, persistence::Result&);
  TradeId allocate_trade();
  persistence::Name symbol_name(std::uint16_t symbol) const;
  persistence::TradeRecord trade_record(std::uint16_t symbol,
                                        Price,
                                        Quantity,
                                        Timestamp,
                                        OrderId buy,
                                        std::uint32_t buyer,
                                        OrderId sell,
                                        std::uint32_t seller);

  const persistence::Name& username(std::uint32_t user) const {
    return usernames_.at(user);
  }

  spreads::DepthSide source_depth(std::uint16_t symbol, Side) const;

  template <class F>
  void visit_levels(std::uint16_t symbol, Side side, F&& visit) const {
    const auto& book = find_book(symbol);
    auto scan = [&](auto begin, auto end) {
      for (; begin != end; ++begin)
        if (!visit(begin->first, begin->second.quantity, begin->second.count))
          break;
    };
    if (side == Side::Buy)
      scan(book.bids.rbegin(), book.bids.rend());
    else
      scan(book.asks.begin(), book.asks.end());
  }

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
    bool signed_price = false;
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

  struct Capture {
    MatchingEngine& engine;
    Capture(MatchingEngine&, persistence::Result&);
    ~Capture();
  };

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
  std::optional<Order> accept(const LinkHeader&, const NewOrderRequest&);
  Book& find_book(std::uint16_t);
  const Book& find_book(std::uint16_t) const;
  Order expand_order(const persistence::CompactOrder&);
  void finish_resting_fill(Order*, Quantity);
  void cancel(const LinkHeader&, const CancelRequest&);
  void snapshot(const LinkHeader&, const Header&);
  void match(Order&, Levels&);
  void rest(const Order&);
  void unlink(Order&);
};
}  // namespace marketlab::orders
