#pragma once
#include "shared_data/orders/config.hpp"
#include <atomic>

namespace marketlab::orders {
void run_order_router(const OrderConfig&,
                      const std::atomic_bool&,
                      const std::string& stats_path = "",
                      std::uint16_t admin_port = 0,
                      const std::string& market_routes = "");
}  // namespace marketlab::orders
