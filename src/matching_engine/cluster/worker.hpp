#pragma once
#include "matching_engine/cluster/rpc.hpp"
#include "matching_engine/persistence/durable_engine.hpp"

namespace marketlab::orders::cluster {
class WorkerControl {
 public:
  WorkerControl(persistence::DurableEngine&, std::uint16_t port, bool standby);

  int fd() const {
    return rpc_.fd();
  }

  void poll();

  bool active() const {
    return role_ == "active";
  }

  void begin_drain();
  bool finish_drain();
  Bytes boundary_bytes() const;

 private:
  persistence::DurableEngine& engine_;
  RpcServer rpc_;
  std::string role_;
  bool exporting_ = false;
  persistence::Boundary baseline_;
  std::filesystem::path checkpoint_;
  std::uint64_t checkpoint_size_ = 0;
  std::vector<std::pair<std::uint64_t, std::filesystem::path>> deltas_;
  std::size_t segment_ = 0;
  std::uint64_t order_offset_ = 0, trade_offset_ = 0, next_ = 0;
  std::optional<std::uint64_t> applying_;
  std::uint64_t expected_trade_end_ = 0;
  std::optional<Bytes> handle(std::span<const std::uint8_t>);
  Bytes tail(std::uint64_t next);
  std::string status() const;
};
}  // namespace marketlab::orders::cluster
