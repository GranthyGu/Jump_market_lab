#pragma once
#include "shared_data/orders/config.hpp"
#include <atomic>
#include <string>

namespace marketlab::orders {
void run_gateway(const OrderConfig& config, const std::atomic_bool& stop, const std::string& stats_path = "");
}  // namespace marketlab::orders
