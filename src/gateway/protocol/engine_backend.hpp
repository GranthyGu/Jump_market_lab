#pragma once
#include "gateway/protocol/engine_link.hpp"
#include "market_publisher/protocol/market_data.hpp"
#include <functional>

namespace marketlab::orders {
// Implement this interface to replace the simulator while retaining TCP framing,
// heartbeats, routing and client sessions. Emit before returning; do not retain
// the callback or payload span. The adapter performs network writes afterwards.
class EngineBackend {
 public:
  using Emit = std::function<void(LinkKind,const LinkHeader&,const Bytes&)>;
  virtual ~EngineBackend() = default;
  virtual void command(const LinkHeader&,std::span<const std::uint8_t>,const Emit&) = 0;
  virtual md::ShardView market_view() = 0;
};
}  // namespace marketlab::orders
