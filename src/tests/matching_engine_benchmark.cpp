#include "matching_engine/engine.hpp"
#include "tests/gateway_support.hpp"
#include <random>

using namespace marketlab;
using namespace marketlab::orders;
using namespace marketlab::orders::test;
namespace {
struct Bench {
  OrderConfig config;
  std::unique_ptr<MatchingEngine> engine;
  RequestId request = 1;
  std::uint64_t events = 0,trades = 0;
  OrderId accepted = 0;
  Bench() {
    config.accounts = {{"maker","p"},{"taker","p"}};
    config.shards = {{1,{"127.0.0.1",7201}}}; config.products = {{"BENCH",1000000,1}};
    engine = std::make_unique<MatchingEngine>(config,1);
  }
  template <typename T> void send(LinkKind kind,unsigned user,const T& record) {
    // Stack-backed input and span-backed event output; no benchmark allocation
    // per wire record. Decode and verify responses instead of timing a no-op sink.
    const auto input = std::span<const std::uint8_t>(reinterpret_cast<const std::uint8_t*>(&record),sizeof(record));
    engine->command({0,kind,request++,user,user},input,[&](auto,const auto&,auto bytes) {
      Header header{}; std::memcpy(&header,bytes.data(),sizeof(header)); ++events;
      if (header.messageType == MessageType::OrderUpdate) {
        const auto update = copy_record<OrderUpdate>(bytes);
        if (update.order.status == OrderStatus::Accepted) accepted = update.order.orderId;
      } else if (header.messageType == MessageType::Trade) {
        const auto trade = copy_record<Trade>(bytes); check(trade.quantity > 0,"Benchmark invalid fill"); ++trades;
      } else throw std::runtime_error("Unexpected benchmark rejection");
    });
  }
  OrderId add(Side side,Price price,Quantity quantity,unsigned user = 1) {
    send(LinkKind::NewOrder,user,Client::new_request(request,"BENCH",side,price,quantity)); return accepted;
  }
  void cancel(OrderId id) { send(LinkKind::Cancel,1,CancelRequest{{MessageType::CancelRequest,sizeof(CancelRequest),request},id}); }
};
void report(std::ostream& csv,unsigned repeat,const std::string& scenario,std::uint64_t commands,
            std::uint64_t fills,double ms,const std::vector<double>& us) {
  csv << repeat << ',' << scenario << ',' << commands << ',' << fills << ',' << ms << ',' << commands * 1000.0 / ms << ','
      << us.size() << ',' << percentile(us,.5) << ',' << percentile(us,.95) << ',' << percentile(us,.99) << '\n';
}
}
int main(int argc,char** argv) {
  try {
    check(argc == 2,"Expected results directory"); std::filesystem::create_directories(argv[1]);
    std::ofstream csv(std::filesystem::path(argv[1]) / "core_latency.csv");
    csv << "repeat,scenario,commands,fills,wall_ms,commands_per_second,latency_samples,p50_us,p95_us,p99_us\n";
    for (unsigned repeat = 0; repeat < 3; ++repeat) {
      {
        Bench b;
        for (unsigned i = 0; i < 1000; ++i) b.cancel(b.add(Side::Buy,1000000,1));
        std::vector<double> latency; latency.reserve(100000); const auto begin = md::Clock::now();
        for (unsigned i = 0; i < 50000; ++i) {
          auto start = md::Clock::now(); const auto id = b.add(Side::Buy,1000000,1); latency.push_back(elapsed_ms(start) * 1000);
          start = md::Clock::now(); b.cancel(id); latency.push_back(elapsed_ms(start) * 1000);
        }
        const auto ms = elapsed_ms(begin); check(b.engine->market_view().books.at(1).levels.empty(),"Benchmark order leak");
        report(csv,repeat,"new_cancel_reused_nodes",100000,0,ms,latency);
      }
      {
        Bench b; std::vector<OrderId> ids; ids.reserve(20000);
        for (unsigned i = 0; i < 20000; ++i) ids.push_back(b.add(Side::Buy,900001 + i,1));
        std::mt19937 random(1234); std::shuffle(ids.begin(),ids.end(),random);
        std::vector<double> latency; latency.reserve(ids.size()); const auto begin = md::Clock::now();
        for (const auto id : ids) { const auto start = md::Clock::now(); b.cancel(id); latency.push_back(elapsed_ms(start) * 1000); }
        const auto ms = elapsed_ms(begin); check(b.engine->market_view().books.at(1).levels.empty(),"Sparse price cancel leak");
        report(csv,repeat,"random_cancel_20000_price_levels",ids.size(),0,ms,latency);
      }
      {
        Bench b; std::vector<double> latency; latency.reserve(500); const auto begin = md::Clock::now();
        for (unsigned round = 0; round < 500; ++round) {
          for (unsigned i = 0; i < 256; ++i) b.add(Side::Sell,1000000 + i % 16,1);
          const auto start = md::Clock::now(); b.add(Side::Buy,1000100,256,2); latency.push_back(elapsed_ms(start) * 1000);
        }
        const auto ms = elapsed_ms(begin);
        check(b.trades == 2ULL * 500 * 256 && b.engine->market_view().books.at(1).levels.empty(),"Sweep dropped participant trades");
        // Throughput includes maker placement. Latency samples are the individual
        // aggressive commands, each matching 256 orders and emitting 1025 events.
        report(csv,repeat,"sweep_256_orders_aggressor_latency",500 * 257,500 * 256,ms,latency);
      }
      csv.flush();
    }
    std::cout << "CORE_BENCHMARK PASS repeats=3, CSV=" << (std::filesystem::path(argv[1]) / "core_latency.csv") << '\n';
    return 0;
  } catch (const std::exception& e) { std::cerr << "FAIL " << e.what() << '\n'; return 1; }
}
