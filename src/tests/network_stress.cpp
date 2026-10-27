#include "market_publisher/publisher.hpp"
#include "tests/stress_support.hpp"

#include <arpa/inet.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/resource.h>

#include <csignal>
#include <filesystem>
#include <iomanip>
#include <iostream>

#ifndef STRESS_IMPLEMENTATION
#define STRESS_IMPLEMENTATION "optimized"
#endif

using namespace marketlab::md;
using namespace marketlab::md::test;
namespace {
class Workers {
 public:
  std::atomic_bool stop{false}, failed{false};
  ~Workers() { join(); }
  template <typename F> void start(F fn) {
    threads_.push_back(std::make_unique<StopThread>([this, fn = std::move(fn)](const std::atomic_bool&) mutable {
      try { fn(stop); }
      catch (const std::exception& e) {
        std::lock_guard lock(mutex_);
        error_ = e.what(); failed.store(true); stop.store(true);
      }
    }));
  }
  void join() { stop.store(true); threads_.clear(); }
  void check() { std::lock_guard lock(mutex_); if (failed.load()) throw std::runtime_error(error_); }
 private:
  std::vector<std::unique_ptr<StopThread>> threads_;
  std::mutex mutex_;
  std::string error_;
};

struct Capture {
  std::ostringstream output;
  std::ostringstream errors;
  std::streambuf* previous = std::cout.rdbuf(output.rdbuf());
  std::streambuf* previous_errors = std::cerr.rdbuf(errors.rdbuf());
  ~Capture() {
    restore();
    if (std::uncaught_exceptions()) std::cerr << output.str() << errors.str();
  }
  void restore() {
    if (previous) { std::cout.rdbuf(previous); previous = nullptr; }
    if (previous_errors) { std::cerr.rdbuf(previous_errors); previous_errors = nullptr; }
  }
};
struct Files {
  std::vector<std::string> paths;
  ~Files() { for (const auto& p : paths) { std::filesystem::remove(p); std::filesystem::remove(p + ".owner"); } }
};

std::uint16_t port_of(int fd) {
  sockaddr_in address{};
  socklen_t size = sizeof(address);
  require(getsockname(fd, reinterpret_cast<sockaddr*>(&address), &size) == 0, "getsockname failed");
  return ntohs(address.sin_port);
}
ShardView view_for(const ShardConfig& cfg, const Epoch& epoch, std::uint64_t step) {
  ShardView view{epoch, {}};
  for (const auto id : cfg.symbols)
    view.books.emplace(id, id % 3 == 0 ? Book{id, {}} : full_book(id, 0, id % 3 == 1 ? 0 : step));
  return view;
}
struct Receiver {
  Fd fd;
  std::uint64_t received = 0, unique = 0, duplicates = 0, invalid = 0;
  int buffer_size = 0;
  std::vector<double> age_us;
  std::map<std::uint64_t, std::vector<std::uint8_t>> seen;
  std::map<std::uint64_t, std::uint64_t> timestamps;
  Epoch epoch{};
};

void collect(Receiver& r, std::size_t products, const std::atomic_bool& stop) {
  while (!stop.load()) {
    pollfd p{r.fd.get(), POLLIN, 0};
    if (poll(&p, 1, 20) <= 0) continue;
    std::array<std::uint8_t, 1024> bytes{};
    const auto count = recv(r.fd.get(), bytes.data(), bytes.size(), 0);
    if (count <= 0) continue;
    ++r.received;
    try {
      const auto m = decode(std::span(bytes.data(), static_cast<std::size_t>(count)));
      require(m.symbol_id > 0 && m.symbol_id <= products, "Unknown UDP product");
      if (r.unique == 0) r.epoch = m.epoch;
      require(m.epoch == r.epoch, "Unexpected publisher epoch reset");
      auto [timestamp, inserted] = r.timestamps.emplace(m.index, m.timestamp);
      require(inserted || timestamp->second == m.timestamp, "Same-batch timestamps differ");
      if (m.kind == Kind::Snapshot)
        require(m.levels.size() == (m.symbol_id % 3 == 0 ? 0 : 10), "Snapshot depth mismatch");
      else if (m.symbol_id % 3 != 2) require(m.levels.empty(), "Static/empty product changed");
      auto [batch, first] = r.seen.try_emplace(m.index);
      if (first) batch->second.resize(products);
      auto& mask = batch->second[m.symbol_id - 1];
      const auto bit = m.kind == Kind::Delta ? 1U : 2U;
      if (mask & bit) ++r.duplicates;
      else { mask |= bit; ++r.unique; }
      const auto now = unix_ns();
      if (now >= m.timestamp) r.age_us.push_back(static_cast<double>(now - m.timestamp) / 1000);
    } catch (const std::exception&) { ++r.invalid; }
  }
}

void query_load(const Config& config, unsigned worker, const std::atomic_bool& stop,
                std::vector<double>& results, std::mutex& mutex, std::atomic_uint64_t& failures) {
  std::vector<double> local;
  try {
    auto fd = connect_tcp(config.query.host, config.query.port, Clock::now() + std::chrono::seconds(2));
    std::uint64_t sequence = worker;
    while (!stop.load()) {
      const auto id = static_cast<std::uint16_t>(1 + (sequence++ * 17) % config.symbols.size());
      const auto started = Clock::now();
      const auto deadline = started + std::chrono::seconds(2);
      Bytes request;
      append_uint(request, id, 2);
      send_all(fd.get(), request, deadline);
      const auto prefix = receive_exact(fd.get(), 2, deadline);
      std::size_t offset = 0;
      const auto length = read_uint(prefix, offset, 2);
      require(length >= header_size && length <= max_snapshot_size, "Invalid TCP response length");
      const auto m = decode(receive_exact(fd.get(), length, deadline));
      require(m.kind == Kind::Snapshot && m.symbol_id == id, "Invalid TCP response identity");
      require(m.levels.size() == (id % 3 == 0 ? 0 : 10), "Invalid TCP response depth");
      local.push_back(std::chrono::duration<double, std::micro>(Clock::now() - started).count());
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  } catch (const std::exception&) { if (!stop.load()) ++failures; }
  std::lock_guard lock(mutex);
  results.insert(results.end(), local.begin(), local.end());
}

void run(const std::string& transport, unsigned products, unsigned shard_count, unsigned users) {
  require(products > 0 && products <= 65535 && shard_count > 0 && shard_count <= products, "Invalid stress dimensions");
  Config config;
  config.shm_max_age_ms = 2000;
  Files files;
  const auto run_id = epoch_text(new_epoch());
  std::vector<Fd> listeners;
  for (unsigned i = 0; i < shard_count; ++i) {
    listeners.push_back(listen_tcp("127.0.0.1", 0));
    const auto path = "/tmp/marketlab-stress-" + run_id + "-" + std::to_string(i);
    files.paths.push_back(path);
    config.shards.push_back({"s" + std::to_string(i), transport, "127.0.0.1", port_of(listeners.back().get()), path, {}});
  }
  for (unsigned i = 1; i <= products; ++i) {
    const auto id = static_cast<std::uint16_t>(i);
    config.symbols.emplace(id, std::to_string(i));
    config.shards[(i - 1) % shard_count].symbols.push_back(id);
  }
  auto reserved = listen_tcp("127.0.0.1", 0);
  config.query.port = port_of(reserved.get());
  reserved.reset();

  std::vector<std::shared_ptr<Receiver>> receivers;
  for (unsigned i = 0; i < users; ++i) {
    auto r = std::make_shared<Receiver>();
    r->fd = bind_udp("127.0.0.1", 0);
    const int requested = 4 * 1024 * 1024;
    (void)setsockopt(r->fd.get(), SOL_SOCKET, SO_RCVBUF, &requested, sizeof(requested));
    socklen_t size = sizeof(r->buffer_size);
    require(getsockopt(r->fd.get(), SOL_SOCKET, SO_RCVBUF, &r->buffer_size, &size) == 0, "SO_RCVBUF failed");
    config.receivers.push_back({"127.0.0.1", port_of(r->fd.get())});
    receivers.push_back(std::move(r));
  }

  std::vector<double> query_times;
  std::mutex query_mutex;
  std::atomic_uint64_t query_failures{0};
  Capture capture;
  Workers receive_workers, services;
  for (const auto& r : receivers) receive_workers.start([r, products](const auto& stop) { collect(*r, products, stop); });
  for (unsigned i = 0; i < shard_count; ++i) {
    const auto cfg = config.shards[i];
    const auto epoch = new_epoch();
    if (transport == "tcp") {
      services.start([cfg, epoch, fd = std::move(listeners[i])](const auto& stop) mutable {
        std::uint64_t step = 0;
        serve_shard(std::move(fd), [&] { return view_for(cfg, epoch, step++); }, stop);
      });
    } else {
      auto writer = std::make_shared<SharedViewWriter>(cfg.mmap_path, cfg.symbols.size());
      writer->publish(view_for(cfg, epoch, 0));
      services.start([cfg, epoch, writer](const auto& stop) {
        std::uint64_t step = 1;
        while (!stop.load()) {
          writer->publish(view_for(cfg, epoch, step++));
          std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
      });
    }
  }
  auto publisher = std::make_shared<Publisher>(config);
  const auto started = Clock::now();
  services.start([publisher](const auto& stop) { publisher->run(stop); });
  const auto deadline = started + std::chrono::seconds(18);
  while (!publisher->latest() && Clock::now() < deadline && !services.failed.load())
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  services.check();
  require(static_cast<bool>(publisher->latest()), "Publisher did not initialize");
  constexpr unsigned query_clients = 16;
  for (unsigned i = 0; i < query_clients; ++i)
    services.start([&, i](const auto& stop) { query_load(config, i, stop, query_times, query_mutex, query_failures); });
  while (Clock::now() < deadline && !services.failed.load()) {
    const auto latest = publisher->latest();
    if (latest && latest->index >= 5) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  const auto latest = publisher->latest();
  require(latest && latest->index >= 5, "Publisher did not reach the fifth round");
  services.join();
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  receive_workers.join();
  services.check(); receive_workers.check();
  capture.restore();

  std::vector<double> sample_us;
  std::uint64_t packets = 0;
  unsigned missed_deadlines = 0, send_errors = 0, source_errors = 0;
  std::istringstream lines(capture.output.str());
  for (std::string line; std::getline(lines, line);) {
    if (line.find("Published index=") == 0) {
      packets += std::stoull(line.substr(line.find("packets=") + 8));
      sample_us.push_back(std::stod(line.substr(line.find("sample_us=") + 10)));
    }
  }
  std::istringstream error_lines(capture.errors.str());
  for (std::string line; std::getline(error_lines, line);) {
    if (line.find("deadline missed") != std::string::npos) ++missed_deadlines;
    if (line.find("UDP send failed") != std::string::npos) ++send_errors;
    if (line.find("Source unavailable") != std::string::npos) ++source_errors;
  }
  std::uint64_t received = 0, unique = 0, invalid = 0, duplicates = 0;
  int min_buffer = std::numeric_limits<int>::max();
  for (const auto& r : receivers) {
    received += r->received; unique += r->unique; invalid += r->invalid; duplicates += r->duplicates;
    min_buffer = std::min(min_buffer, r->buffer_size);
  }
  const auto expected = packets * users;
  require(sample_us.size() >= 6, "Missing publication rounds");
  require(unique <= expected && unique > 0, "Invalid UDP receive accounting");
  require(invalid == 0 && query_failures.load() == 0 && !query_times.empty(), "Protocol/query stress failure");
  std::cout << std::fixed << std::setprecision(2)
      << "METRIC implementation=" << STRESS_IMPLEMENTATION << " transport=" << transport
      << " products=" << products << " shards=" << shard_count << " users=" << users
      << " rounds=" << sample_us.size() << " sample_p50_us=" << percentile(sample_us, .5)
      << " sample_max_us=" << *std::max_element(sample_us.begin(), sample_us.end())
      << " expected_udp=" << expected << " received_udp=" << received << " unique_udp=" << unique
      << " missing_udp=" << expected - unique << " invalid_udp=" << invalid << " duplicate_udp=" << duplicates
      << " queries=" << query_times.size() << " query_failures=" << query_failures.load()
      << " query_p50_us=" << percentile(query_times, .5) << " query_p95_us=" << percentile(query_times, .95)
      << " query_p99_us=" << percentile(query_times, .99) << " rcvbuf=" << min_buffer
      << " missed_deadlines=" << missed_deadlines << " send_errors=" << send_errors
      << " source_errors=" << source_errors << '\n';
}
}  // namespace

int main(int argc, char** argv) {
  std::signal(SIGPIPE, SIG_IGN);
  try {
    require(argc == 5, "Expected: tcp|shm products shards users");
    struct rlimit limit{};
    require(getrlimit(RLIMIT_NOFILE, &limit) == 0, "Cannot inspect descriptor limit");
    if (limit.rlim_cur < 4096) {
      limit.rlim_cur = std::min<rlim_t>(limit.rlim_max, 4096);
      require(setrlimit(RLIMIT_NOFILE, &limit) == 0, "Cannot raise process descriptor soft limit");
    }
    run(argv[1], static_cast<unsigned>(std::stoul(argv[2])),
        static_cast<unsigned>(std::stoul(argv[3])), static_cast<unsigned>(std::stoul(argv[4])));
  } catch (const std::exception& e) { std::cerr << "NETWORK STRESS FAILURE: " << e.what() << '\n'; return 1; }
}
