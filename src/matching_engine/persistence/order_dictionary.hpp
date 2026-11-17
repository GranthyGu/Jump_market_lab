#pragma once

#include <unordered_map>
#include <optional>

#include "matching_engine/persistence/model.hpp"
#include "shared_data/orders/config.hpp"
#include "matching_engine/spreads/depth.hpp"

namespace marketlab::orders::persistence {

// Constructed before the workers start; all subsequent access is read-only.
class OrderDictionary {
 public:
  explicit OrderDictionary(const OrderConfig&);
  CompactOrder compact(const StoredOrder&) const;
  StoredOrder expand(const CompactOrder&) const;

  const Name& user(std::uint32_t id) const {
    return users_.at(id);
  }

  const Name& symbol(std::uint16_t id) const {
    return symbols_.at(id);
  }

  void validate_execution(const ExecutionRecord&, const Boundary&, std::uint16_t owner) const;

 private:
  std::vector<Name> users_;
  std::vector<Name> symbols_;
  std::unordered_map<std::string, std::uint32_t> user_ids_;
  std::unordered_map<std::string, std::uint16_t> symbol_ids_;

  struct Policy {
    std::uint16_t shard;
    PriceLimits limits;
    bool signed_price;
  };

  std::vector<Policy> policies_;
  std::optional<spreads::Topology> topology_;
};

}  // namespace marketlab::orders::persistence
