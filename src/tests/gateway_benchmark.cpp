#include "tests/gateway_support.hpp"
#include <barrier>
#include <mutex>
#include <unordered_map>

using namespace marketlab;
using namespace marketlab::orders;
using namespace marketlab::orders::test;
namespace {
struct Timing { std::vector<double> us; double wall_ms = 0; std::uint64_t operations = 0; };
void report(std::ostream& out,const std::string& topology,unsigned repeat,const std::string& scenario,const Timing& t) {
  out << topology << ',' << repeat << ',' << scenario << ',' << t.operations << ',' << t.wall_ms << ','
      << t.operations * 1000.0 / t.wall_ms << ',' << percentile(t.us,.50) << ',' << percentile(t.us,.95) << ','
      << percentile(t.us,.99) << ',' << *std::max_element(t.us.begin(),t.us.end()) << '\n';
}
Timing serial(Fixture& f) {
  Client client(f.config.gateway_listen); client.login("alice");
  // Warmup uses the same TCP connections and is excluded from measurements.
  for (unsigned i = 0; i < 100; ++i) { const auto a = client.order(i * 2 + 10,"AAPL",Side::Buy,90,1); client.cancel(i * 2 + 11,a.order.orderId); }
  Timing t; t.us.reserve(4000); const auto begin = md::Clock::now();
  for (unsigned i = 0; i < 2000; ++i) {
    const auto symbol = i % 2 ? "AAPL" : "MSFT";
    const auto price = i % 2 ? 90 : 180;
    auto start = md::Clock::now(); const auto a = client.order(i * 2 + 1000,symbol,Side::Buy,price,1);
    t.us.push_back(elapsed_ms(start) * 1000);
    start = md::Clock::now(); client.cancel(i * 2 + 1001,a.order.orderId); t.us.push_back(elapsed_ms(start) * 1000);
  }
  t.wall_ms = elapsed_ms(begin); t.operations = 4000; return t;
}
Timing pipelined(Fixture& f) {
  constexpr unsigned workers = 16, batches = 16, window = 64;
  std::vector<std::unique_ptr<Client>> idle;
  for (unsigned i = workers; i < 80; ++i) { auto p = std::make_unique<Client>(f.config.gateway_listen); p->login("load" + std::to_string(i)); idle.push_back(std::move(p)); }
  std::barrier start_line(static_cast<std::ptrdiff_t>(workers + 1));
  std::vector<std::thread> threads;
  std::vector<Timing> per_worker(workers);
  std::mutex errors_mutex; std::exception_ptr error;
  for (unsigned worker = 0; worker < workers; ++worker) threads.emplace_back([&,worker] {
    bool passed_start = false;
    try {
      Client client(f.config.gateway_listen); client.login("load" + std::to_string(worker));
      auto& t = per_worker[worker]; t.us.reserve(batches * window * 2);
      start_line.arrive_and_wait(); passed_start = true;
      start_line.arrive_and_wait();
      for (unsigned batch = 0; batch < batches; ++batch) {
        std::array<md::Clock::time_point,window> starts{};
        std::array<OrderId,window> ids{};
        const auto base = static_cast<RequestId>(batch) * 2 * window + 100;
        for (unsigned j = 0; j < window; ++j) {
          starts[j] = md::Clock::now();
          client.send(Client::new_request(base + j,j % 2 ? "AAPL" : "MSFT",Side::Buy,j % 2 ? 90 : 180,1));
        }
        for (unsigned j = 0; j < window; ++j) {
          const auto update = client.receive_as<OrderUpdate>(MessageType::OrderUpdate);
          check(update.header.requestId >= base && update.header.requestId < base + window && update.order.status == OrderStatus::Accepted,"Pipelined acceptance mismatch");
          const auto index = static_cast<std::size_t>(update.header.requestId - base);
          check(ids[index] == 0,"Duplicate acceptance"); ids[index] = update.order.orderId;
          check((ids[index] >> 48) == (index % 2 ? 1 : 2),"Pipeline wrong shard");
          t.us.push_back(elapsed_ms(starts[index]) * 1000);
        }
        for (unsigned j = 0; j < window; ++j) { starts[j] = md::Clock::now(); client.send_cancel(base + window + j,ids[j]); }
        std::array<bool,window> canceled{};
        for (unsigned j = 0; j < window; ++j) {
          const auto update = client.receive_as<OrderUpdate>(MessageType::OrderUpdate);
          check(update.header.requestId >= base + window && update.header.requestId < base + 2 * window && update.order.status == OrderStatus::Canceled,"Pipeline cancellation mismatch");
          const auto index = static_cast<std::size_t>(update.header.requestId - base - window);
          check(!canceled[index] && update.order.orderId == ids[index],"Duplicate/crossed cancel"); canceled[index] = true;
          t.us.push_back(elapsed_ms(starts[index]) * 1000);
        }
      }
      t.operations = batches * window * 2;
    } catch (...) {
      if (!passed_start) start_line.arrive_and_drop();
      std::lock_guard lock(errors_mutex); if (!error) error = std::current_exception();
    }
  });
  start_line.arrive_and_wait(); const auto begin = md::Clock::now(); start_line.arrive_and_wait();
  for (auto& thread : threads) thread.join();
  Timing result; result.wall_ms = elapsed_ms(begin);
  if (error) std::rethrow_exception(error);
  for (auto& t : per_worker) { result.operations += t.operations; result.us.insert(result.us.end(),t.us.begin(),t.us.end()); }
  check(result.operations == workers * batches * window * 2,"Pipeline operation count");
  return result;
}
}
int main(int argc,char** argv) {
  try {
    check(argc == 3,"Expected binary and artifact directory");
    std::filesystem::create_directories(argv[2]);
    std::ofstream csv(std::filesystem::path(argv[2]) / "latency.csv");
    csv << "topology,repeat,scenario,requests,wall_ms,requests_per_second,p50_us,p95_us,p99_us,max_us\n";
    for (unsigned repeat = 0; repeat < 3; ++repeat) {
      for (unsigned position = 0; position < 2; ++position) {
        const bool direct = (repeat + position) % 2 == 0;
        const std::string topology = direct ? "direct" : "router";
        Fixture fixture(argv[1],std::filesystem::path(argv[2]) / (topology + '_' + std::to_string(repeat)),direct);
        const auto one = serial(fixture); report(csv,topology,repeat,"serial_new_cancel",one); report(std::cout,topology,repeat,"serial_new_cancel",one);
        const auto many = pipelined(fixture); report(csv,topology,repeat,"16_clients_window64_plus64_idle",many); report(std::cout,topology,repeat,"16_clients_window64_plus64_idle",many);
        csv.flush(); fixture.gateway.stop(); fixture.router.stop(); for (auto& shard : fixture.shards) shard.stop();
        check(metric(fixture.directory / "gateway.json","engine_connect_attempts") >= (direct ? 2 : 1),"No engine connectors");
        check(metric(fixture.directory / "shard1.json","router_connections") == 1 && metric(fixture.directory / "shard2.json","router_connections") == 1,"Per-shard connection churn");
        check(metric(fixture.directory / "gateway.json","commands") == one.operations + many.operations + 200 + 82,"Lost or replayed gateway command");
      }
    }
    std::cout << "BENCHMARK PASS: 6 runs, 220608 timed requests, all responses checked; output=" << argv[2] << '\n';
    return 0;
  } catch (const std::exception& e) { std::cerr << "FAIL " << e.what() << '\n'; return 1; }
}
