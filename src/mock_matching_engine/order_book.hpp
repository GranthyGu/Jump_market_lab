#pragma once
#include "gateway/protocol/engine_link.hpp"
#include "gateway/protocol/engine_backend.hpp"
#include "gateway/config.hpp"
#include "market_publisher/protocol/market_data.hpp"
#include <functional>
#include <list>
#include <map>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

namespace marketlab::orders {
// Order-path simulator and executable adapter contract, not gateway matching.
// Active order/index memory is released after fill/cancel; history is unnecessary.
class MockOrderBook final : public EngineBackend {
 public:
  MockOrderBook(const OrderConfig&,std::uint16_t shard);
  void command(const LinkHeader&,std::span<const std::uint8_t>,const Emit&) override;
  md::ShardView market_view() override;
 private:
  struct Level { std::list<OrderId> fifo; std::uint64_t quantity = 0; };
  struct Book {
    Product product;
    std::uint16_t symbol_id;
    std::map<Price,Level,std::greater<Price>> bids;
    std::map<Price,Level> asks;
  };
  struct Order {
    OrderState state;
    std::uint32_t user;
    std::list<OrderId>::iterator position;
  };
  std::uint16_t shard_;
  std::uint64_t next_order_ = 1, next_trade_ = 1;
  md::Epoch epoch_;
  std::mutex mutex_;
  std::unordered_map<std::string,Book> books_;
  std::unordered_map<OrderId,Order> active_;
  std::unordered_map<std::uint32_t,std::unordered_set<OrderId>> by_user_;
  void new_order(const LinkHeader&,const NewOrderRequest&,const Emit&);
  void cancel(const LinkHeader&,const CancelRequest&,const Emit&);
  void snapshot(const LinkHeader&,const Header&,const Emit&);
  template <typename Levels> void match(Order&,Levels&,const Emit&);
  void order_update(const Order&,RequestId,const LinkHeader&,const Emit&);
  void trade(const Order&,TradeId,Price,Quantity,Timestamp,const Emit&);
};
}  // namespace marketlab::orders
