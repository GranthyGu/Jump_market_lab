#pragma once
#include "shared_data/orders/engine_link.hpp"
#include "shared_data/market_data/market_data.hpp"
#include <functional>

namespace marketlab::orders {
// One command owner (the shard reactor). market_view() may run concurrently.
// Emit is synchronous: the receiver must copy the span before returning.
// Neither the callback nor its span may be retained or reenter command().
class EngineBackend {
 public:
  using Emit = std::function<void(LinkKind,const LinkHeader&,std::span<const std::uint8_t>)>;
  virtual ~EngineBackend() = default;
  virtual void command(const LinkHeader&,std::span<const std::uint8_t>,const Emit&) = 0;
  virtual md::ShardView market_view() = 0;
};
}  // namespace marketlab::orders
