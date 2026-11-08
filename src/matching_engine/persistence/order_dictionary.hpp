#pragma once

#include <unordered_map>

#include "matching_engine/persistence/model.hpp"
#include "shared_data/orders/config.hpp"

namespace marketlab::orders::persistence {

// Constructed before the workers start; all subsequent access is read-only.
class OrderDictionary {
 public:
  explicit OrderDictionary(const OrderConfig&);
  CompactOrder compact(const StoredOrder&) const;
  StoredOrder expand(const CompactOrder&) const;

 private:
  std::vector<Name> users_;
  std::vector<Name> symbols_;
  std::unordered_map<std::string, std::uint32_t> user_ids_;
  std::unordered_map<std::string, std::uint16_t> symbol_ids_;
};

}  // namespace marketlab::orders::persistence
