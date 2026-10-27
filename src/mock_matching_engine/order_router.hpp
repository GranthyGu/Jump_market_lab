#pragma once
#include "gateway/config.hpp"
#include <atomic>
#include <string>

namespace marketlab::orders {
class EngineBackend;
void serve_order_engine(EngineBackend&,const OrderConfig&,std::uint16_t shard,const std::atomic_bool&,
                        const std::string& stats_path = "",std::uint16_t market_port = 0,const std::string& mmap_path = "");
void run_order_router(const OrderConfig&, const std::atomic_bool&, const std::string& stats_path = "");
void run_order_shard(const OrderConfig&, std::uint16_t shard, const std::atomic_bool&, const std::string& stats_path = "",
                     std::uint16_t market_port = 0, const std::string& mmap_path = "");
}  // namespace marketlab::orders
