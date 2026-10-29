#pragma once
#include "matching_engine/backend.hpp"
#include "gateway/config.hpp"
#include <atomic>

namespace marketlab::orders {
void serve_order_engine(EngineBackend&,const OrderConfig&,std::uint16_t,const std::atomic_bool&,
                        const std::string& stats = "",std::uint16_t market_port = 0,const std::string& mmap_path = "");
void run_order_shard(const OrderConfig&,std::uint16_t,const std::atomic_bool&,
                     const std::string& stats = "",std::uint16_t market_port = 0,const std::string& mmap_path = "");
}  // namespace marketlab::orders
