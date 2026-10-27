#include "market_publisher/service.hpp"
#include "client/client.hpp"
#include "common/top_five.hpp"
#include "tests/reference_difference.hpp"

#include <arpa/inet.h>
#include <fcntl.h>
#include <poll.h>
#include <sys/file.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <csignal>
#include <filesystem>
#include <iostream>
#include <random>
#include <stdexcept>
#include <thread>

using namespace marketlab::md;
namespace {
void check(bool condition, const char* message) { if (!condition) throw std::runtime_error(message); }
template <typename F> void rejects(F fn) {
  bool rejected = false;
  try { fn(); } catch (const std::exception&) { rejected = true; }
  check(rejected, "Expected rejection");
}
Book example(std::uint64_t quantity = 100, std::uint64_t orders = 2) {
  return {1, {{Side::Buy, 100, quantity, orders}, {Side::Buy, 99, 80, 3}, {Side::Sell, 110, 200, 4}}};
}
Book apply_delta(const Book& old, const std::vector<Level>& changes) {
  auto out = old;
  for (const auto& x : changes) {
    std::erase_if(out.levels, [&](const Level& y) { return x.side == y.side && x.price == y.price; });
    if (x.quantity) out.levels.push_back(x);
  }
  std::sort(out.levels.begin(), out.levels.end(), [](const Level& a, const Level& b) {
    return a.side != b.side ? a.side < b.side : (a.side == Side::Buy ? a.price > b.price : a.price < b.price);
  });
  validate_book(out);
  return out;
}
std::uint16_t bound_port(int fd) {
  sockaddr_in addr{};
  socklen_t size = sizeof(addr);
  check(getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &size) == 0, "getsockname");
  return ntohs(addr.sin_port);
}
struct RunningShard {
  std::atomic_bool stop{false};
  StopThread thread;
  RunningShard(Fd listener, SnapshotProvider provider)
      : thread([this, fd = std::move(listener), provider = std::move(provider)](const std::atomic_bool&) mutable {
      serve_shard(std::move(fd), provider, stop);
    }) {}
  ~RunningShard() { stop.store(true); }
};

void protocol_tests() {
  const auto epoch = new_epoch();
  const auto old = example();
  const auto modified = example(100, 5);
  auto changes = difference(old, modified);
  check(changes.size() == 1 && changes[0].num_orders == 5, "Order-count-only change lost");
  check(apply_delta(old, changes) == modified, "Diff failed");
  check(difference(old, old).empty(), "Unchanged view not empty");
  Book empty{1, {}};
  check(apply_delta(old, difference(old, empty)) == empty, "Clear failed");
  check(apply_delta(empty, difference(empty, old)) == old, "Populate failed");
  Book before{1, {}}, after{1, {}};
  for (int i = 0; i < 5; ++i) {
    before.levels.push_back({Side::Buy, 100 - i, 20, 1});
    after.levels.push_back({Side::Buy, 99 - i, 30, 2});
  }
  check(apply_delta(before, difference(before, after)) == after, "Fifth-level replacement failed");
  const auto large = example(1ULL << 40);
  const auto encoded = encode({Kind::Snapshot, epoch, 1, 9, 123, large.levels});
  check(encoded.size() == header_size + 3 * level_size, "Wire size mismatch");
  check(encoded[0] == 2 && encoded[17] == 1 && encoded[18] == 0 && encoded[19] == 9, "Wire offset mismatch");
  const auto decoded = decode(encoded);
  check(decoded.levels == large.levels && decoded.index == 9 && decoded.epoch == epoch, "Round trip failed");
  check(encode({Kind::Delta, epoch, 1, 1, 123, {}}).size() == 35, "Empty delta size");
  rejects([&] { encode({Kind::Delta, epoch, 1, 0, 123, {}}); });
  rejects([&] { decode(std::span(encoded).first(encoded.size() - 1)); });
  auto duplicate = old;
  duplicate.levels.push_back(old.levels.back());
  rejects([&] { validate_book(duplicate); });
  const auto frame = encode_shard({epoch, {{1, old}, {3, {3, {}}}}});
  check(decode_shard(frame, {1, 3}).books.at(3).levels.empty(), "Empty symbol missing");
  rejects([&] { decode_shard(frame, {1, 2}); });
  rejects([&] { auto bad = frame; bad.push_back(0); decode_shard(bad, {1, 3}); });
  std::map<int, PriceLevelTotals, std::greater<int>> bids;
  std::map<int, PriceLevelTotals> asks;
  for (int i = 0; i < 100; ++i) { bids.emplace(1000 - i, PriceLevelTotals{10, 1}); asks.emplace(1100 + i, PriceLevelTotals{20, 2}); }
  const auto top = copy_top_five(1, bids, asks);
  check(top.levels.size() == 10 && top.levels[4].price == 996 && top.levels[9].price == 1104, "Top-five adapter failed");
}

void difference_tests() {
  Book full{1, {}}, replacement{1, {}};
  for (const auto side : {Side::Buy, Side::Sell}) {
    for (int i = 0; i < 5; ++i) {
      const int price = side == Side::Buy ? 100 - i : 110 + i;
      full.levels.push_back({side, price, 10, 2});
      replacement.levels.push_back({side, price + 100, 20, 3});
    }
  }
  const auto changes = difference(full, replacement);
  check(changes.size() == 20, "Full replacement must include ten deletes and ten sets");
  check(changes == test::reference_difference(full, replacement), "Full replacement order differs");
  check(apply_delta(full, changes) == replacement, "Full replacement reconstruction failed");

  // The same price on opposite sides is two different keys.
  const Book buy_only{1, {{Side::Buy, 100, 10, 2}}};
  const Book sell_only{1, {{Side::Sell, 100, 10, 2}}};
  check(difference(buy_only, sell_only) == std::vector<Level>{
      {Side::Buy, 100, 0, 0}, {Side::Sell, 100, 10, 2}}, "Side transition lost a key");

  auto invalid = full;
  std::swap(invalid.levels[0], invalid.levels[1]);
  rejects([&] { difference(invalid, invalid); });
  invalid = full;
  invalid.levels[1] = invalid.levels[0];
  rejects([&] { difference(invalid, invalid); });
  invalid = full;
  invalid.levels[2] = invalid.levels[0];
  rejects([&] { difference(full, invalid); });
  invalid = full;
  invalid.levels[0].num_orders = 0;
  rejects([&] { difference(invalid, full); });
  invalid = full;
  invalid.symbol_id = 0;
  rejects([&] { difference(invalid, invalid); });
  invalid = full;
  invalid.symbol_id = 2;
  rejects([&] { difference(full, invalid); });
  invalid = {1, {}};
  for (int i = 0; i < 6; ++i) invalid.levels.push_back({Side::Buy, 100 - i, 10, 2});
  rejects([&] { difference(invalid, invalid); });
  rejects([&] { validate_levels({{Side::Buy, 100, 0, 1}}, true); });
  rejects([&] { validate_levels({{Side::Sell, 101, 1, 1}, {Side::Buy, 100, 1, 1}}, true); });

  std::mt19937_64 random(20261026);
  auto make_book = [&] {
    Book book{1, {}};
    for (const auto side : {Side::Buy, Side::Sell}) {
      std::array<std::int32_t, 12> prices{};
      for (std::size_t i = 0; i < prices.size(); ++i) prices[i] = 100 + static_cast<std::int32_t>(i);
      std::shuffle(prices.begin(), prices.end(), random);
      const auto count = random() % 6;
      for (std::size_t i = 0; i < count; ++i)
        book.levels.push_back({side, prices[i], 1 + random() % (1ULL << 40), 1 + random() % 100});
    }
    std::sort(book.levels.begin(), book.levels.end(), test::wire_order);
    return book;
  };
  const auto epoch = new_epoch();
  for (unsigned i = 0; i < 2000; ++i) {
    const auto before = make_book();
    auto after = make_book();
    if (i % 4 == 0) after = before;
    if (i % 4 == 1 && !before.levels.empty()) {
      after = before;
      ++after.levels[random() % after.levels.size()].quantity;
    }
    if (i % 4 == 2 && !before.levels.empty()) {
      after = before;
      ++after.levels[random() % after.levels.size()].num_orders;
    }
    const auto delta = difference(before, after);
    check(delta == test::reference_difference(before, after), "Merge differs from map reference");
    check(apply_delta(before, delta) == after, "Random delta reconstruction failed");
    const auto wire = encode({Kind::Delta, epoch, 1, 1, 123, delta});
    check(decode(wire).levels == delta, "Random delta wire round trip failed");
  }
}

void shared_memory_tests() {
  const std::string path = "/tmp/marketlab-test-" + epoch_text(new_epoch()) + ".mmap";
  struct Cleanup { std::string path; ~Cleanup() { std::filesystem::remove(path); std::filesystem::remove(path + ".owner"); } } cleanup{path};
  ShardConfig config{"test", "shm", "127.0.0.1", 1, path, {1}};
  const auto epoch = new_epoch();
  {
    SharedViewWriter writer(path, 1);
    writer.publish({epoch, {{1, example()}}});
    auto source = make_source(config, 1000);
    check(source->read(Clock::now() + std::chrono::seconds(1)).books.at(1) == example(), "Shared read failed");
    rejects([&] { SharedViewWriter duplicate(path, 1); });
    for (std::uint64_t i = 0; i < 20; ++i) {
      writer.publish({epoch, {{1, example(100 + i)}}});
      check(source->read(Clock::now() + std::chrono::seconds(1)).books.at(1) == example(100 + i), "Shared update failed");
    }
    {
      // Simulate a writer dying after invalidation, before the commit marker.
      Fd interrupted(open(path.c_str(), O_RDWR));
      check(interrupted && flock(interrupted.get(), LOCK_EX) == 0, "Shared fault lock");
      const std::array<std::uint8_t, 4> zeros{};
      check(pwrite(interrupted.get(), zeros.data(), zeros.size(), 0) == 4, "Shared fault write");
      check(flock(interrupted.get(), LOCK_UN) == 0, "Shared fault unlock");
    }
    rejects([&] { source->read(Clock::now() + std::chrono::seconds(1)); });
    writer.publish({epoch, {{1, example()}}});
    auto stale = make_source(config, 1);
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    rejects([&] { stale->read(Clock::now() + std::chrono::seconds(1)); });
  }
  const auto restarted = new_epoch();
  SharedViewWriter replacement(path, 1);
  replacement.publish({restarted, {{1, example()}}});
  auto source = make_source(config, 1000);
  check(source->read(Clock::now() + std::chrono::seconds(1)).epoch == restarted, "Restart epoch lost");
}

void fragmented_io_test() {
  int pair[2];
  check(socketpair(AF_UNIX, SOCK_STREAM, 0, pair) == 0, "socketpair");
  Fd reader(pair[0]);
  StopThread writer([fd = Fd(pair[1])](const std::atomic_bool&) mutable {
    for (std::uint8_t i = 0; i < 16; ++i) {
      send_all(fd.get(), std::span(&i, 1), Clock::now() + std::chrono::seconds(1));
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  });
  const auto bytes = receive_exact(reader.get(), 16, Clock::now() + std::chrono::seconds(1));
  for (unsigned i = 0; i < 16; ++i) check(bytes[i] == i, "Fragmented read failed");
}

void tcp_and_publication_tests() {
  const auto source_epoch = new_epoch();
  std::atomic_uint64_t calls{0};
  auto engine_listener = listen_tcp("127.0.0.1", 0);
  const auto engine_port = bound_port(engine_listener.get());
  auto engine = std::make_unique<RunningShard>(std::move(engine_listener), [&] {
    return ShardView{source_epoch, {{1, example(100 + calls.fetch_add(1))}, {3, {3, {}}}}};
  });
  ShardConfig shard{"test", "tcp", "127.0.0.1", engine_port, "/tmp/unused", {1, 3}};
  auto direct = make_source(shard, 1000);
  const auto first = direct->read(Clock::now() + std::chrono::seconds(1));
  const auto second = direct->read(Clock::now() + std::chrono::seconds(1));
  check(second.books.at(1).levels[0].quantity == first.books.at(1).levels[0].quantity + 1, "Repeated shard query failed");
  direct->disconnect();

  Fd udp(socket(AF_INET, SOCK_DGRAM, 0));
  sockaddr_in destination{};
  destination.sin_family = AF_INET;
  destination.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  check(bind(udp.get(), reinterpret_cast<sockaddr*>(&destination), sizeof(destination)) == 0, "UDP bind");
  auto reserved = listen_tcp("127.0.0.1", 0);
  const auto query_port = bound_port(reserved.get());
  reserved.reset();
  Config cfg;
  cfg.symbols = {{1, "TEST"}, {3, "EMPTY"}};
  cfg.shards = {shard};
  cfg.receivers = {{"127.0.0.1", bound_port(udp.get())}};
  cfg.query.port = query_port;
  Publisher publisher(cfg);
  std::atomic_bool stop{false};
  StopThread worker([&](const std::atomic_bool&) { publisher.run(stop); });
  struct Stopper { std::atomic_bool& stop; ~Stopper() { stop.store(true); } } stopper{stop};
  const auto deadline = Clock::now() + std::chrono::seconds(9);
  std::map<std::uint16_t, Book> reconstructed;
  std::map<std::pair<std::uint64_t, std::uint16_t>, Message> deltas;
  bool fifth_delta = false, fifth_snapshot = false;
  Epoch publisher_epoch{};
  while (Clock::now() < deadline && (!fifth_delta || !fifth_snapshot)) {
    pollfd p{udp.get(), POLLIN, 0};
    if (poll(&p, 1, 100) <= 0) continue;
    std::array<std::uint8_t, 1024> buffer{};
    const auto n = recv(udp.get(), buffer.data(), buffer.size(), 0);
    check(n > 0, "UDP receive");
    const auto m = decode(std::span(buffer.data(), static_cast<std::size_t>(n)));
    if (m.kind == Kind::Delta) {
      check(reconstructed.contains(m.symbol_id), "Missing initial snapshot");
      reconstructed[m.symbol_id] = apply_delta(reconstructed.at(m.symbol_id), m.levels);
      deltas.emplace(std::pair{m.index, m.symbol_id}, m);
      if (m.index == 5 && m.symbol_id == 1) fifth_delta = true;
      if (m.symbol_id == 3) check(m.levels.empty(), "Empty symbol did not receive empty delta");
    } else {
      check(m.index == 0 || m.index == 5, "Unexpected full snapshot");
      Book book{m.symbol_id, m.levels};
      if (m.index == 5) {
        check(reconstructed.at(m.symbol_id) == book, "Delta reconstruction differs from snapshot");
        check(deltas.at({5, m.symbol_id}).timestamp == m.timestamp, "Same-batch timestamps differ");
        if (m.symbol_id == 1) fifth_snapshot = true;
      }
      reconstructed[m.symbol_id] = book;
      publisher_epoch = m.epoch;
    }
  }
  check(fifth_delta && fifth_snapshot, "Missing fifth-batch dual messages");
  const auto snapshot = query_snapshot(cfg.query, 1, 1000);
  check(snapshot.index >= 5 && snapshot.epoch == publisher_epoch, "TCP returned stale generation");
  check(query_snapshot(cfg.query, 3, 1000).levels.empty(), "Empty TCP snapshot failed");
  rejects([&] { query_snapshot(cfg.query, 65535, 1000); });

  // Fragmented two-byte requests and repeated queries share one TCP connection.
  auto query = connect_tcp(cfg.query.host, cfg.query.port, Clock::now() + std::chrono::seconds(1));
  for (unsigned repeat = 0; repeat < 2; ++repeat) {
    const std::array<std::uint8_t, 2> request{1, 0};
    for (auto byte : request) send_all(query.get(), std::span(&byte, 1), Clock::now() + std::chrono::seconds(1));
    const auto prefix = receive_exact(query.get(), 2, Clock::now() + std::chrono::seconds(1));
    std::size_t offset = 0;
    const auto length = read_uint(prefix, offset, 2);
    check(decode(receive_exact(query.get(), length, Clock::now() + std::chrono::seconds(1))).symbol_id == 1, "Serial query mismatch");
  }
  engine.reset();
  auto until = Clock::now() + std::chrono::seconds(3);
  while (publisher.latest() && Clock::now() < until) std::this_thread::sleep_for(std::chrono::milliseconds(10));
  check(!publisher.latest(), "Source failure left stale query cache available");
  rejects([&] { query_snapshot(cfg.query, 1, 1000); });
  const auto new_source_epoch = new_epoch();
  engine = std::make_unique<RunningShard>(listen_tcp("127.0.0.1", engine_port), [&] {
    return ShardView{new_source_epoch, {{1, example()}, {3, {3, {}}}}};
  });
  until = Clock::now() + std::chrono::seconds(3);
  while (!publisher.latest() && Clock::now() < until) std::this_thread::sleep_for(std::chrono::milliseconds(10));
  const auto recovered = publisher.latest();
  check(recovered && recovered->index == 0 && recovered->epoch != publisher_epoch, "Recovery did not reset generation");
}
}  // namespace

int main() {
  std::signal(SIGPIPE, SIG_IGN);
  try {
    protocol_tests();
    difference_tests();
    shared_memory_tests();
    fragmented_io_test();
    tcp_and_publication_tests();
    std::cout << "All broadcast tests passed\n";
    return 0;
  } catch (const std::exception& e) { std::cerr << "TEST FAILURE: " << e.what() << '\n'; return 1; }
}
