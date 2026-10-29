#include "tests/gateway_support.hpp"
#include <future>
#include <mutex>

using namespace marketlab;
using namespace marketlab::orders;
using namespace marketlab::orders::test;
namespace {
void coverage(const OrderConfig& order,const md::Config& market) {
  check(order.shards.size() == 32 && order.products.size() == 1000 &&
        market.shards.size() == 32 && market.symbols.size() == 1000,"Expected 32 shards / 1000 products");
  std::array<unsigned,32> counts{};
  for (unsigned i = 0; i < 1000; ++i) {
    check(order.products[i].shard == i % 32 + 1 && market.symbols.at(i + 1) == order.products[i].symbol,"Order/market symbol mapping mismatch");
    ++counts[i % 32];
    const auto& ids = market.shards[i % 32].symbols;
    check(std::find(ids.begin(),ids.end(),i + 1) != ids.end(),"Publisher shard ownership mismatch");
  }
  for (unsigned s = 0; s < 32; ++s) {
    check(order.shards[s].id == s + 1 && order.shards[s].address.port == 7201 + s &&
          market.shards[s].port == 9201 + s && counts[s] == (s < 8 ? 32U : 31U),"Shard distribution/ports");
  }
}
void snapshot_check(const std::vector<OrderState>& snapshot,const std::set<OrderId>& ids,Quantity remaining = 10) {
  std::set<OrderId> seen;
  for (const auto& state : snapshot) {
    const OrderId id = state.orderId;
    check(seen.insert(id).second && ids.contains(id) &&
          state.remainingQuantity == remaining && state.filledQuantity == 10 - remaining &&
          state.status == (remaining == 10 ? OrderStatus::Accepted : OrderStatus::PartiallyFilled),"Incomplete or mixed snapshot");
  }
  check(seen == ids,"32-shard snapshot missing orders");
}
md::ShardConfig source_config(const Fixture& f,unsigned s) {
  md::ShardConfig source{"shard-" + std::to_string(s + 1),"tcp","127.0.0.1",f.market_ports[s],"/tmp/unused-scale",{}};
  for (unsigned i = s; i < 1000; i += 32) source.symbols.push_back(i + 1);
  return source;
}
void publisher_coverage(Fixture& f,const std::string& binary,const md::Config& profile,Quantity quantity = 10) {
  const auto file = f.directory / "publisher.conf";
  const auto query_port = free_port();
  std::ofstream out(file); out << "symbols=";
  for (unsigned i = 0; i < 1000; ++i) out << (i ? "," : "") << i + 1 << ':' << f.config.products[i].symbol;
  out << '\n';
  for (unsigned s = 0; s < 32; ++s) {
    out << "shard=shard-" << s + 1 << ",tcp,127.0.0.1," << f.market_ports[s] << ",/tmp/unused-scale-" << s;
    for (auto id : profile.shards[s].symbols) out << ',' << id;
    out << '\n';
  }
  out << "query_port=" << query_port << "\nudp_receiver=127.0.0.1," << free_port() << '\n'; out.close();
  Process publisher; publisher.start(binary,{"publish","--config",file.string()},f.directory / "publisher.log");
  md::Fd query;
  const auto until = deadline(10000);
  while (md::Clock::now() < until) {
    try {
      query = md::connect_tcp("127.0.0.1",query_port,deadline());
      Bytes request; md::append_uint(request,1,2); md::send_all(query.get(),request,deadline());
      const auto prefix = md::receive_exact(query.get(),2,deadline()); std::size_t offset = 0;
      const auto first = md::decode(md::receive_exact(query.get(),md::read_uint(prefix,offset,2),deadline()));
      check(first.levels.size() == 1 && first.levels[0].quantity == static_cast<std::uint64_t>(quantity),"Initial published level"); break;
    } catch (...) { query.reset(); std::this_thread::sleep_for(20ms); }
  }
  check(static_cast<bool>(query),"Publisher never completed a 32-shard round");
  for (unsigned id = 1; id <= 1000; ++id) {
    Bytes request; md::append_uint(request,id,2); md::send_all(query.get(),request,deadline());
    const auto prefix = md::receive_exact(query.get(),2,deadline()); std::size_t offset = 0;
    const auto message = md::decode(md::receive_exact(query.get(),md::read_uint(prefix,offset,2),deadline()));
    check(message.symbol_id == id && message.kind == md::Kind::Snapshot && message.levels.size() == 1 &&
          message.levels[0].price == 100 && message.levels[0].quantity == static_cast<std::uint64_t>(quantity) && message.levels[0].num_orders == 1,"Publisher 1000-product view differs from orders");
  }
}
void concurrent_queries(Fixture& f,Client& alice,const std::set<OrderId>& ids) {
  std::mutex errors_mutex; std::exception_ptr error;
  auto guard = [&](auto fn) { try { fn(); } catch (...) { std::lock_guard lock(errors_mutex); if (!error) error = std::current_exception(); } };
  std::atomic<unsigned> writers_done{0},views{0};
  std::vector<std::thread> threads;
  for (unsigned w = 0; w < 2; ++w) threads.emplace_back([&,w] {
    guard([&] {
      Client client(f.config.gateway_listen); client.login(w ? "carol" : "bob",w ? "carol-pass" : "bob-pass");
      for (unsigned i = w; i < 1000; i += 2) {
        const auto order = client.order(i * 2 + 10,f.config.products[i].symbol,Side::Buy,100,10);
        client.cancel(i * 2 + 11,order.order.orderId);
      }
    });
    ++writers_done;
  });
  for (unsigned r = 0; r < 4; ++r) threads.emplace_back([&,r] {
    guard([&] {
      std::vector<std::unique_ptr<md::ShardSource>> sources;
      for (unsigned s = r; s < 32; s += 4) sources.push_back(md::make_source(source_config(f,s),1000));
      unsigned rounds = 0;
      do {
        for (unsigned j = 0; j < sources.size(); ++j) {
          const auto view = sources[j]->read(deadline());
          const auto shard = r + j * 4;
          check(view.books.size() == (shard < 8 ? 32 : 31),"Missing symbols in concurrent source read");
          for (const auto& [id,b] : view.books) {
            check((id - 1) % 32 == shard && b.levels.size() == 1,"Mixed shard/state in source read");
            const auto& level = b.levels[0];
            check(level.quantity == 10 * level.num_orders && level.num_orders >= 1 && level.num_orders <= 2,"Torn TCP market view");
          }
          ++views;
        }
        ++rounds;
      } while (writers_done.load() != 2 || rounds < 5);
    });
  });
  guard([&] { for (unsigned i = 0; i < 10; ++i) snapshot_check(alice.login("alice","alice-pass",20000 + i),ids); });
  for (auto& thread : threads) thread.join();
  if (error) std::rethrow_exception(error);
  std::cout << "CONCURRENT writers=2 orders=1000 cancels=1000 login_snapshots=10 coherent_shard_views=" << views << '\n';
}
}
int main(int argc,char** argv) {
  try {
    check(argc == 7,"Expected gateway/publisher binaries, order/market profiles, results directory and topology");
    const auto original = load_order_config(argv[3]); const auto market = md::load_config(argv[4]);
    std::filesystem::create_directories(argv[5]); Cases cases(std::filesystem::path(argv[5]) / "scale.csv");
    cases.run("profile_32_shards_1000_symbols_consistent",[&] { coverage(original,market); });
    const auto begin = md::Clock::now();
    Fixture f(argv[1],argv[5],std::string(argv[6]) == "direct",false,argv[3],true);
    std::cout << "STARTUP topology=" << argv[6] << " processes=" << (f.config.direct ? 33 : 34) << " runtime_ms=" << elapsed_ms(begin) << '\n';
    Client alice(f.config.gateway_listen); alice.login("alice","alice-pass");
    std::set<OrderId> ids;
    std::vector<double> new_us,cancel_us;
    cases.run("all_1000_products_route_and_accept",[&] {
      for (unsigned i = 0; i < 1000; ++i) {
        const auto start = md::Clock::now();
        const auto update = alice.order(i + 10,f.config.products[i].symbol,Side::Buy,100,10);
        new_us.push_back(elapsed_ms(start) * 1000);
        const OrderId id = update.order.orderId;
        check((id >> 48) == i % 32 + 1 && ids.insert(id).second,"Wrong shard or duplicate orderId");
      }
      snapshot_check(alice.login("alice","alice-pass",10000),ids);
    });
    cases.run("publisher_reads_32_shards_and_queries_1000_products",[&] { publisher_coverage(f,argv[2],market); });
    cases.run("concurrent_mutations_market_reads_and_32_shard_snapshots",[&] { concurrent_queries(f,alice,ids); });
    cases.run("match_every_product_and_deliver_both_participants",[&] {
      Client bob(f.config.gateway_listen); check(bob.login("bob","bob-pass").empty(),"Unexpected Bob orders");
      std::map<std::string,TradeId> trades;
      for (unsigned i = 0; i < 1000; ++i) {
        const auto accepted = bob.order(i + 50000,f.config.products[i].symbol,Side::Sell,100,6);
        const auto trade = bob.receive_as<Trade>(MessageType::Trade);
        const auto update = bob.receive_as<OrderUpdate>(MessageType::OrderUpdate);
        check(trade.orderId == accepted.order.orderId && trade.side == Side::Sell && trade.quantity == 6 && trade.price == 100 &&
              trade.header.requestId == 0 && update.header.requestId == 0 && update.order.status == OrderStatus::Filled &&
              update.order.orderId == trade.orderId && update.order.filledQuantity == 6 && update.order.remainingQuantity == 0 &&
              update.timestamp == trade.timestamp,"Incoming participant fill delivery");
        const TradeId trade_id = trade.tradeId;
        trades.emplace(fixed_string(trade.symbol),trade_id);
      }
      std::set<OrderId> seen_trades,seen_updates;
      // Different shards may interleave messages. Correlate by order, never assume
      // a global order between frames arriving on independent shard TCP links.
      for (unsigned i = 0; i < 2000; ++i) {
        const auto bytes = alice.receive(); const auto header = copy_record<Header>(std::span(bytes).first(sizeof(Header)));
        check(header.requestId == 0,"Unsolicited fill correlation");
        if (header.messageType == MessageType::Trade) {
          const auto t = copy_record<Trade>(bytes);
          const OrderId id = t.orderId;
          check(ids.contains(id) && seen_trades.insert(id).second && t.side == Side::Buy &&
                t.quantity == 6 && t.price == 100 && t.tradeId == trades.at(fixed_string(t.symbol)),"Resting participant trade delivery");
        } else {
          const auto u = copy_record<OrderUpdate>(bytes);
          const OrderId id = u.order.orderId;
          check(ids.contains(id) && seen_updates.insert(id).second &&
                u.order.status == OrderStatus::PartiallyFilled && u.order.filledQuantity == 6 &&
                u.order.remainingQuantity == 4,"Resting participant status delivery");
        }
      }
      check(seen_trades == ids && seen_updates == ids && trades.size() == 1000,"Missing participant notifications");
      snapshot_check(alice.login("alice","alice-pass",60000),ids,4);
      publisher_coverage(f,argv[2],market,4);
    });
    cases.run("cancel_1000_then_read_empty_snapshot_and_market",[&] {
      unsigned request = 30000;
      for (const auto id : ids) {
        const auto start = md::Clock::now(); const auto canceled = alice.cancel(request++,id); cancel_us.push_back(elapsed_ms(start) * 1000);
        check(canceled.order.filledQuantity == 6,"Cancel lost executed quantity");
      }
      check(alice.login("alice","alice-pass",40000).empty(),"Read-after-cancel returned stale orders");
      for (unsigned s = 0; s < 32; ++s) {
        auto source = md::make_source(source_config(f,s),1000);
        for (const auto& [id,b] : source->read(deadline()).books) { (void)id; check(b.levels.empty(),"Read-after-cancel returned stale market"); }
      }
    });
    std::ofstream latency(f.directory / "latency.csv");
    latency << "operation,samples,p50_us,p95_us,p99_us,max_us\n";
    for (const auto& [name,values] : {std::pair{"new",new_us},std::pair{"cancel",cancel_us}})
      latency << name << ',' << values.size() << ',' << percentile(values,.5) << ',' << percentile(values,.95) << ','
              << percentile(values,.99) << ',' << *std::max_element(values.begin(),values.end()) << '\n';
    f.gateway.stop(); f.router.stop(); for (auto& shard : f.shards) shard.stop();
    for (unsigned s = 1; s <= 32; ++s) {
      check(metric(f.directory / ("shard" + std::to_string(s) + ".json"),"commands") > 0,"Idle shard not tested");
      check(metric(f.directory / ("shard" + std::to_string(s) + ".json"),"router_connections") == 1,"Unexpected link reconnect/replay");
    }
    std::cout << "SCALE PASS topology=" << argv[6] << " shards=32 products=1000 unique_ids=1000\n";
    return 0;
  } catch (const std::exception& e) { std::cerr << "FAIL " << e.what() << '\n'; return 1; }
}
