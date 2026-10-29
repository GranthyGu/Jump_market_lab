#pragma once
#include "shared_data/orders/config.hpp"
#include <atomic>

namespace marketlab::orders {
void run_order_router(const OrderConfig&,const std::atomic_bool&,const std::string& stats_path = "");
}  // namespace marketlab::orders
