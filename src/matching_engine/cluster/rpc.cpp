#include "shared_data/runtime/thread.hpp"
#include "shared_data/runtime/io.hpp"
#include "matching_engine/cluster/rpc.hpp"

#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <fcntl.h>
#include <iostream>

#include "matching_engine/persistence/codec.hpp"

namespace marketlab::orders::cluster {
Bytes text_bytes(const std::string& text) {
  return {text.begin(), text.end()};
}

RpcServer::RpcServer(std::uint16_t port) {
  auto listener = runtime::listen_tcp("127.0.0.1", port);
  thread_ =
      std::make_unique<runtime::StopThread>([this, listener = std::move(listener)](const auto& stop) {
        while (!stop.load()) {
          pollfd p{listener.get(), POLLIN, 0};
          if (::poll(&p, 1, 50) <= 0)
            continue;
          runtime::Fd peer(::accept(listener.get(), nullptr, nullptr));
          if (!peer)
            continue;
          const int one = 1;
          fcntl(peer.get(), F_SETFL, fcntl(peer.get(), F_GETFL, 0) | O_NONBLOCK);
          setsockopt(peer.get(), IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
          try {
            while (!stop.load()) {
              pollfd readable{peer.get(), POLLIN, 0};
              if (::poll(&readable, 1, 50) <= 0)
                continue;
              const auto deadline = runtime::Clock::now() + std::chrono::seconds(30);
              auto length = runtime::receive_exact(peer.get(), 4, deadline);
              persistence::Reader r{length};
              const auto size = r.number(4);
              if (!size || size > 128 * 1024 * 1024)
                throw std::runtime_error("Invalid management frame size");
              auto request = std::make_shared<Request>();
              request->bytes = runtime::receive_exact(peer.get(), size, deadline);
              auto reply = request->reply.get_future();
              {
                std::lock_guard lock(mutex_);
                if (closed_)
                  return;
                request_ = request;
              }
              wakeup_.notify();
              while (reply.wait_for(std::chrono::milliseconds(50)) != std::future_status::ready) {
                if (stop.load())
                  return;
              }
              auto bytes = reply.get();
              Bytes prefix;
              persistence::integer(prefix, bytes.size(), 4);
              runtime::send_all(peer.get(), prefix, deadline);
              runtime::send_all(peer.get(), bytes, deadline);
            }
          } catch (const std::exception&) {
            // Disconnects affect this management session, never the order stream.
          }
        }
      });
}

RpcServer::~RpcServer() {
  {
    std::lock_guard lock(mutex_);
    closed_ = true;
  }
  thread_.reset();
}

void RpcServer::service(const Handler& handler) {
  wakeup_.drain();
  std::shared_ptr<Request> request;
  {
    std::lock_guard lock(mutex_);
    request = request_;
  }
  if (!request)
    return;
  Bytes response;
  try {
    auto result = handler(request->bytes);
    if (!result)
      return;
    response.push_back(0);
    response.insert(response.end(), result->begin(), result->end());
  } catch (const std::exception& e) {
    response.push_back(1);
    const auto message = text_bytes(e.what());
    response.insert(response.end(), message.begin(), message.end());
  }
  {
    std::lock_guard lock(mutex_);
    request_.reset();
  }
  request->reply.set_value(std::move(response));
}
}  // namespace marketlab::orders::cluster
