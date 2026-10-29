#pragma once

namespace marketlab {
// Explicit --config paths override these repository-local defaults.
inline constexpr auto default_exchange_config = MARKETLAB_SOURCE_DIR "/config/exchange.conf";
inline constexpr auto default_market_config = MARKETLAB_SOURCE_DIR "/config/publisher.conf";
}  // namespace marketlab
