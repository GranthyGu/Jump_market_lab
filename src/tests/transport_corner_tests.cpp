#include "tests/stress_support.hpp"

#include <arpa/inet.h>
#include <fcntl.h>
#include <sys/file.h>
#include <sys/socket.h>
#include <unistd.h>

#include <csignal>
#include <filesystem>
#include <iostream>

using namespace marketlab::md;
using namespace marketlab::md::test;
namespace {
std::uint16_t port_of(int fd) {
  sockaddr_in addr{};
  socklen_t size = sizeof(addr);
  require(getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &size) == 0, "getsockname failed");
  return ntohs(addr.sin_port);
}

void malformed_shard_tests() {
  auto listener = listen_tcp("127.0.0.1", 0);
  ShardConfig config{"fault-test", "tcp", "127.0.0.1", port_of(listener.get()), "/tmp/unused", {1, 2}};
  const auto epoch = new_epoch();
  const auto valid = encode_shard({epoch, {{1, full_book(1)}, {2, full_book(2)}}});
  std::atomic_int mode{9};
  StopThread server([fd = std::move(listener), &mode, &valid, epoch](const auto& stop) mutable {
    serve_requests(std::move(fd), 1, 1024, 500, 16, [&](const auto&) {
      auto payload = valid;
      std::uint64_t declared = payload.size();
      switch (mode.load()) {
        case 0: declared = 0; payload.clear(); break;
        case 1: declared = 17; payload.clear(); break;
        case 2: declared = max_shard_size(2) + 1; payload.clear(); break;
        case 3: payload[16] = 1; break;
        case 4: payload = encode_shard({epoch, {{1, full_book(1)}, {3, full_book(3)}}}); break;
        case 5: payload[18 + 213] = 1; payload[18 + 213 + 1] = 0; break;
        case 6: payload[21] = 0; break;
        case 7: payload.pop_back(); break;
        case 8: return Bytes{0, 0};
        default: break;
      }
      Bytes response;
      append_uint(response, declared, 4);
      response.insert(response.end(), payload.begin(), payload.end());
      return response;
    }, stop);
  });
  auto source = make_source(config, 1000);
  for (int i = 0; i < 9; ++i) {
    mode.store(i);
    require(!accepts([&] { (void)source->read(Clock::now() + std::chrono::milliseconds(100)); }),
            "Malformed shard response accepted");
    mode.store(9);
    require(source->read(Clock::now() + std::chrono::seconds(1)).books.size() == 2,
            "Source did not reconnect after malformed response");
    source->disconnect();
  }
  std::cout << "malformed_shard_cases=9 reconnects=9 passed\n";
}

void slow_client_tests() {
  auto listener = listen_tcp("127.0.0.1", 0);
  const auto port = port_of(listener.get());
  std::atomic_uint calls{0};
  StopThread server([fd = std::move(listener), &calls](const auto& stop) mutable {
    serve_requests(std::move(fd), 2, 3, 150, 4,
      [&](const auto&) { ++calls; return Bytes{1, 2, 3}; }, stop);
  });
  std::vector<Fd> stalled;
  for (unsigned i = 0; i < 4; ++i) {
    stalled.push_back(connect_tcp("127.0.0.1", port, Clock::now() + std::chrono::seconds(1)));
    const std::array<std::uint8_t, 1> partial{1};
    send_all(stalled.back().get(), partial, Clock::now() + std::chrono::seconds(1));
  }
  auto excess = connect_tcp("127.0.0.1", port, Clock::now() + std::chrono::seconds(1));
  const std::array<std::uint8_t, 2> request{1, 0};
  require(!accepts([&] {
    const auto deadline = Clock::now() + std::chrono::seconds(1);
    send_all(excess.get(), request, deadline);
    (void)receive_exact(excess.get(), 3, deadline);
  }), "Connection above configured limit was served");
  for (auto& fd : stalled)
    require(!accepts([&] { (void)receive_exact(fd.get(), 1, Clock::now() + std::chrono::seconds(1)); }),
            "Partial request did not time out");
  require(calls.load() == 0, "Partial request reached the handler");
  auto healthy = connect_tcp("127.0.0.1", port, Clock::now() + std::chrono::seconds(1));
  send_all(healthy.get(), request, Clock::now() + std::chrono::seconds(1));
  require(receive_exact(healthy.get(), 3, Clock::now() + std::chrono::seconds(1)) == Bytes{1, 2, 3},
          "Server did not recover capacity after stalled clients");
  std::cout << "stalled_clients=4 connection_limit=passed partial_timeout=passed capacity_recovery=passed\n";
}

void shared_concurrency_tests() {
  const auto path = "/tmp/marketlab-concurrency-" + epoch_text(new_epoch());
  struct Cleanup {
    std::string path;
    ~Cleanup() { std::filesystem::remove(path); std::filesystem::remove(path + ".owner"); }
  } cleanup{path};
  const auto epoch = new_epoch();
  ShardConfig cfg{"shared-test", "shm", "127.0.0.1", 1, path, {1}};
  SharedViewWriter writer(path, 1);
  writer.publish({epoch, {{1, full_book(1)}}});
  std::atomic_bool failed{false};
  std::atomic_uint reads{0}, writes{0};
  {
    StopThread producer([&](const auto&) {
      try {
        for (unsigned i = 1; i <= 200; ++i) {
          writer.publish({epoch, {{1, full_book(1, 0, i)}}});
          ++writes;
          std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
      } catch (const std::exception&) { failed.store(true); }
    });
    std::vector<std::unique_ptr<StopThread>> readers;
    for (unsigned i = 0; i < 4; ++i) readers.push_back(std::make_unique<StopThread>([&](const auto&) {
      try {
        auto source = make_source(cfg, 5000);
        for (unsigned j = 0; j < 1000; ++j) {
          const auto view = source->read(Clock::now() + std::chrono::seconds(2));
          const auto& book = view.books.at(1);
          require(view.epoch == epoch && book.levels.size() == 10, "Shared view identity/depth mismatch");
          for (const auto& level : book.levels)
            require(level.quantity == book.levels[0].quantity && level.num_orders == book.levels[0].num_orders,
                    "Torn shared view observed");
          ++reads;
        }
      } catch (const std::exception&) { failed.store(true); }
    }));
    readers.clear();
  }
  require(!failed.load() && reads.load() == 4000 && writes.load() == 200, "Concurrent shared view test failed");
  auto source = make_source(cfg, 5000);
  Fd file(open(path.c_str(), O_RDWR));
  require(file && flock(file.get(), LOCK_EX) == 0, "Cannot acquire fault injection lock");
  const bool accepted_locked = accepts([&] { (void)source->read(Clock::now() + std::chrono::milliseconds(20)); });
  require(flock(file.get(), LOCK_UN) == 0 && !accepted_locked, "Shared read lock deadline failed");
  require(source->read(Clock::now() + std::chrono::seconds(1)).books.size() == 1, "Shared lock recovery failed");
  const auto capacity = static_cast<off_t>(16 + max_shard_size(1));
  require(ftruncate(file.get(), capacity + 1) == 0, "Cannot alter shared capacity");
  require(!accepts([&] { (void)source->read(Clock::now() + std::chrono::seconds(1)); }),
          "Invalid shared capacity accepted");
  require(ftruncate(file.get(), capacity) == 0, "Cannot restore shared capacity");
  require(source->read(Clock::now() + std::chrono::seconds(1)).books.size() == 1, "Shared capacity recovery failed");
  std::cout << "shared_readers=4 shared_reads=4000 shared_writes=200 torn_views=0 lock_deadline=passed capacity=passed\n";
}
}  // namespace

int main() {
  std::signal(SIGPIPE, SIG_IGN);
  try {
    malformed_shard_tests();
    slow_client_tests();
    shared_concurrency_tests();
  } catch (const std::exception& e) { std::cerr << "TRANSPORT CORNER FAILURE: " << e.what() << '\n'; return 1; }
}
