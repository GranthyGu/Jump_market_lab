#pragma once
#include "shared_data/orders/config.hpp"
#include <atomic>

namespace marketlab::orders::spreads {
void run_coordinator(const OrderConfig&, const std::atomic_bool&, const std::string& stats = "");
}  // namespace marketlab::orders::spreads
