#pragma once
#include "matching_engine/persistence/files.hpp"
#include "matching_engine/recovery/state.hpp"

namespace marketlab::orders::persistence {
struct Recovered {
  Boundary boundary;
  std::vector<StoredOrder> orders;
  std::uint64_t checkpoint = 0;
};
class Storage {
 public:
  Storage(const Options&, const OrderConfig&, std::uint16_t);
  Recovered recover();
  void checkpoint(const Boundary&, const FrozenPages&, bool prune = true);
  std::filesystem::path trade_path() const { return root_ / "trades" / "trades.log"; }
  std::filesystem::path delta_path(std::uint64_t first) const {
    return root_ / "orders" / ("delta-" + std::to_string(first) + ".log");
  }
  const std::filesystem::path& root() const { return root_; }

 private:
  std::filesystem::path root_;
  DirectoryLock lock_;
  Options options_;
  std::uint16_t shard_;
  std::uint64_t fingerprint_;
  Recovered read_snapshot(const std::filesystem::path&) const;
};
}  // namespace marketlab::orders::persistence
