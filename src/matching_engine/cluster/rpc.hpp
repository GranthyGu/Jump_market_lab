#pragma once
#include <future>
#include <optional>

#include "matching_engine/persistence/wakeup.hpp"
#include "shared_data/market_data/transport.hpp"
#include "shared_data/orders/engine_link.hpp"

namespace marketlab::orders::cluster {
// Management/replication channel: little-endian uint32 length, then bytes.
// A response starts with 0 (success) or 1 (UTF-8 error). Trusted loopback only.
// Socket I/O runs on a separate thread. Handlers always run on the engine owner.
class RpcServer {
 public:
  using Handler = std::function<std::optional<Bytes>(std::span<const std::uint8_t>)>;
  explicit RpcServer(std::uint16_t port);
  ~RpcServer();

  int fd() const {
    return wakeup_.fd();
  }

  void service(const Handler&);

 private:
  struct Request {
    Bytes bytes;
    std::promise<Bytes> reply;
  };

  persistence::Wakeup wakeup_;
  std::mutex mutex_;
  std::shared_ptr<Request> request_;
  bool closed_ = false;
  std::unique_ptr<md::StopThread> thread_;
};

Bytes text_bytes(const std::string&);
}  // namespace marketlab::orders::cluster
