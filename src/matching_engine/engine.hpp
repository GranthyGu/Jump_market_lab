#pragma once
#include "matching_engine/active_index.hpp"
#include "matching_engine/backend.hpp"
#include "gateway/config.hpp"
#include <map>
#include <memory>
#include <memory_resource>
#include <mutex>
#include <string_view>
#include <unordered_map>

namespace marketlab::orders {
class MatchingEngine final : public EngineBackend {
 public:
  MatchingEngine(const OrderConfig&,std::uint16_t shard);
  void command(const LinkHeader&,std::span<const std::uint8_t>,const Emit&) override;
  md::ShardView market_view() override;
 private:
  struct Order;
  struct Level {
    Order* head = nullptr;
    Order* tail = nullptr;
    std::uint64_t quantity = 0, count = 0;
  };
  using Levels = std::pmr::map<Price,Level>;
  struct Book {
    explicit Book(std::pmr::memory_resource* resource) : bids(resource),asks(resource) {}
    SymbolName symbol{};
    std::uint16_t symbol_id = 0;
    std::int64_t lower = 0, upper = 0;
    Levels bids,asks;
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
  struct Event {
    LinkKind kind;
    LinkHeader context;
    std::array<std::uint8_t,max_client_frame> payload;
    std::size_t size;
  };
  std::uint16_t shard_;
  std::uint64_t next_order_ = 1, next_trade_ = 1;
  md::Epoch epoch_;
  std::mutex state_mutex_;
  std::pmr::unsynchronized_pool_resource price_memory_;
  std::vector<Book> books_;
  std::unordered_map<std::string_view,std::size_t> symbols_;
  std::vector<Order*> users_;
  ActiveIndex<Order> active_;
  Pool pool_;
  std::vector<Event> outgoing_; // Reused by the single command thread.
  template <typename T> void queue(LinkKind,const LinkHeader&,const T&);
  void update(const Order&,RequestId,const LinkHeader&,Timestamp = 0);
  void trade(const Order&,TradeId,Price,Quantity,Timestamp);
  void new_order(const LinkHeader&,const NewOrderRequest&);
  void cancel(const LinkHeader&,const CancelRequest&);
  void snapshot(const LinkHeader&,const Header&);
  void match(Order&,Levels&);
  void rest(const Order&);
  void unlink(Order&);
};
}  // namespace marketlab::orders
