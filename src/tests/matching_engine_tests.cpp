#include "matching_engine/engine.hpp"
#include "tests/gateway_support.hpp"
#include <random>
#include <unordered_map>

using namespace marketlab;
using namespace marketlab::orders;
using namespace marketlab::orders::test;
namespace {
struct Event {
  LinkKind kind;
  LinkHeader context;
  Bytes bytes;
};
template <typename T> void append(std::vector<Event>& out,LinkKind kind,const LinkHeader& h,const T& value) {
  out.push_back({kind,h,record_bytes(value)});
}
OrderConfig configuration() {
  OrderConfig c;
  c.accounts = {{"a","p"},{"b","p"},{"c","p"},{"d","p"}};
  c.shards = {{1,{"127.0.0.1",7201}},{2,{"127.0.0.1",7202}}};
  c.products = {{"AAPL",100,1},{"FRACT",101,1},{"MAX",std::numeric_limits<Price>::max(),1},{"TINY",1,1},{"OTHER",100,2}};
  return c;
}
// Intentionally simple independent oracle: append-only order history, scanning
// every candidate to choose best price / earliest arrival. No production index,
// intrusive list or price-map matching code is reused.
class Reference {
 public:
  struct Entry { OrderState state; std::uint32_t user; };
  std::vector<Entry> history;
  std::vector<Event> events;
  explicit Reference(OrderConfig config) : config_(std::move(config)) {}
  void command(const LinkHeader& h,std::span<const std::uint8_t> bytes) {
    events.clear();
    if (h.kind == LinkKind::NewOrder) add(h,copy_record<NewOrderRequest>(bytes));
    else if (h.kind == LinkKind::Cancel) {
      const auto request = copy_record<CancelRequest>(bytes);
      auto found = std::find_if(history.begin(),history.end(),[&](const auto& e) { return e.state.orderId == request.orderId; });
      if (found == history.end() || found->user != h.user || !found->state.remainingQuantity)
        append(events,LinkKind::Event,h,rejected(request.header.requestId,RejectReason::CancelFailed));
      else {
        found->state.remainingQuantity = 0; found->state.status = OrderStatus::Canceled;
        update(*found,h,request.header.requestId);
      }
    } else {
      const auto request = copy_record<Header>(bytes);
      for (const auto& entry : history) if (entry.user == h.user && entry.state.remainingQuantity) {
        auto s = server_record<ActiveOrderSnapshot>(MessageType::ActiveOrderSnapshot,request.requestId);
        s.order = entry.state; append(events,LinkKind::Event,h,s);
      }
      auto end = server_record<ActiveOrderSnapshot>(MessageType::ActiveOrderSnapshot,request.requestId);
      end.isEnd = 1; append(events,LinkKind::SnapshotEnd,h,end);
    }
  }
  std::map<std::uint16_t,md::Book> market() const {
    std::map<std::uint16_t,md::Book> result;
    for (unsigned i = 0; i < config_.products.size(); ++i) {
      if (config_.products[i].shard != 1) continue;
      std::map<Price,std::pair<std::uint64_t,std::uint64_t>> bids,asks;
      for (const auto& e : history) if (e.state.remainingQuantity && fixed_string(e.state.symbol) == config_.products[i].symbol) {
        const Price price = e.state.price;
        auto& totals = (e.state.side == Side::Buy ? bids : asks)[price];
        totals.first += static_cast<std::uint64_t>(e.state.remainingQuantity); ++totals.second;
      }
      md::Book b{static_cast<std::uint16_t>(i + 1),{}};
      unsigned count = 0;
      for (auto it = bids.rbegin(); it != bids.rend() && count < 5; ++it,++count)
        b.levels.push_back({md::Side::Buy,it->first,it->second.first,it->second.second});
      count = 0;
      for (auto it = asks.begin(); it != asks.end() && count < 5; ++it,++count)
        b.levels.push_back({md::Side::Sell,it->first,it->second.first,it->second.second});
      result.emplace(b.symbol_id,std::move(b));
    }
    return result;
  }
 private:
  OrderConfig config_;
  std::uint64_t order_sequence_ = 1,trade_sequence_ = 1;
  void update(const Entry& entry,const LinkHeader& h,RequestId request) {
    auto record = server_record<OrderUpdate>(MessageType::OrderUpdate,request); record.order = entry.state;
    auto context = h; context.user = entry.user; append(events,LinkKind::Event,context,record);
  }
  void add(const LinkHeader& h,const NewOrderRequest& request) {
    const auto symbol = fixed_string(request.symbol);
    const auto product = std::find_if(config_.products.begin(),config_.products.end(),[&](const auto& p) { return p.shard == 1 && p.symbol == symbol; });
    if (product == config_.products.end() || request.price <= 0 || request.quantity <= 0 ||
        (request.side != Side::Buy && request.side != Side::Sell) ||
        request.price < static_cast<std::int64_t>(product->previous_close) * 9 / 10 ||
        request.price > static_cast<std::int64_t>(product->previous_close) * 11 / 10) {
      append(events,LinkKind::Event,h,rejected(request.header.requestId,RejectReason::InvalidOrder)); return;
    }
    Entry incoming{}; incoming.user = h.user;
    incoming.state.orderId = (1ULL << 48) | order_sequence_++;
    std::memcpy(incoming.state.symbol,request.symbol,32);
    incoming.state.side = request.side; incoming.state.price = request.price;
    incoming.state.originalQuantity = incoming.state.remainingQuantity = request.quantity;
    incoming.state.status = OrderStatus::Accepted; update(incoming,h,request.header.requestId);
    while (incoming.state.remainingQuantity) {
      Entry* best = nullptr;
      for (auto& candidate : history) {
        const auto& s = candidate.state;
        if (!s.remainingQuantity || s.side == request.side || fixed_string(s.symbol) != symbol) continue;
        if ((request.side == Side::Buy && s.price > request.price) || (request.side == Side::Sell && s.price < request.price)) continue;
        // Equal prices retain the earlier vector entry: FIFO independent of IDs.
        if (!best || (request.side == Side::Buy ? s.price < best->state.price : s.price > best->state.price)) best = &candidate;
      }
      if (!best) break;
      const Quantity incoming_remaining = incoming.state.remainingQuantity;
      const Quantity resting_remaining = best->state.remainingQuantity;
      const auto quantity = std::min(incoming_remaining,resting_remaining);
      const auto price = best->state.price;
      const auto id = (1ULL << 48) | trade_sequence_++;
      for (auto* participant : {&incoming,best}) {
        auto& state = participant->state;
        state.filledQuantity += quantity; state.remainingQuantity -= quantity;
        state.status = state.remainingQuantity ? OrderStatus::PartiallyFilled : OrderStatus::Filled;
        auto trade = server_record<Trade>(MessageType::Trade,0);
        trade.tradeId = id; trade.orderId = state.orderId; std::memcpy(trade.symbol,state.symbol,32);
        trade.side = state.side; trade.price = price; trade.quantity = quantity;
        const LinkHeader destination{0,LinkKind::Event,0,participant->user,0};
        append(events,LinkKind::Event,destination,trade); update(*participant,destination,0);
      }
    }
    history.push_back(incoming);
  }
};
void normalize(std::vector<Event>& events,bool snapshot) {
  for (auto& event : events) {
    // Event times differ between independent executions. Wire metadata, IDs,
    // quantities, complete payloads and destination context are compared exactly.
    std::fill(event.bytes.begin() + sizeof(Header),event.bytes.begin() + sizeof(Header) + sizeof(Timestamp),0);
  }
  if (snapshot && events.size() > 1)
    std::sort(events.begin(),events.end() - 1,[](const auto& a,const auto& b) {
      return copy_record<ActiveOrderSnapshot>(a.bytes).order.orderId < copy_record<ActiveOrderSnapshot>(b.bytes).order.orderId;
    });
}
class Harness {
 public:
  OrderConfig config = configuration();
  MatchingEngine engine{config,1};
  Reference reference{config};
  std::uint64_t token = 1;
  template <typename T> void run(std::uint32_t user,LinkKind kind,const T& request) {
    const LinkHeader context{0,kind,token++,user,100 + user};
    const auto bytes = record_bytes(request); std::vector<Event> actual;
    engine.command(context,bytes,[&](auto k,const auto& h,auto payload) { actual.push_back({k,h,Bytes(payload.begin(),payload.end())}); });
    reference.command(context,bytes);
    auto expected = reference.events;
    normalize(actual,kind == LinkKind::Snapshot); normalize(expected,kind == LinkKind::Snapshot);
    check(actual.size() == expected.size(),"Reference event count at token " + std::to_string(context.token));
    for (unsigned i = 0; i < actual.size(); ++i) {
      const auto& a = actual[i]; const auto& b = expected[i];
      check(a.kind == b.kind && a.context.token == b.context.token && a.context.user == b.context.user &&
            a.context.session == b.context.session && a.bytes == b.bytes,"Reference event/context mismatch at token " + std::to_string(context.token));
    }
  }
  void market() { check(engine.market_view().books == reference.market(),"Reference top-five aggregates mismatch"); }
  void snapshot(unsigned user) { run(user,LinkKind::Snapshot,Header{MessageType::ActiveOrderSnapshot,sizeof(Header),8}); }
};
void index_churn() {
  ActiveIndex<unsigned> index(8);
  std::array<unsigned,4096> values{};
  std::unordered_map<OrderId,unsigned*> oracle;
  std::mt19937_64 random(0x734279);
  for (unsigned step = 0; step < 100000; ++step) {
    const auto i = static_cast<unsigned>(random() % values.size());
    const OrderId id = (32ULL << 48) | (i + 1);
    if (random() % 2) {
      if (!oracle.contains(id)) { index.insert(id,&values[i]); oracle.emplace(id,&values[i]); }
    } else { index.erase(id); oracle.erase(id); }
    check(index.size() == oracle.size(),"Flat index cardinality");
    for (unsigned probe = 0; probe < (step % 257 ? 3U : 4096U); ++probe) {
      const auto j = step % 257 ? random() % values.size() : probe;
      const OrderId query = (32ULL << 48) | (j + 1);
      const auto found = oracle.find(query);
      check(index.find(query) == (found == oracle.end() ? nullptr : found->second),"Backward-shift deletion lost an order");
    }
  }
  check(index.find(0) == nullptr,"Zero ID must be absent");
}
void random_reference() {
  Harness h; std::mt19937_64 random(0x1234abcd);
  for (unsigned step = 0; step < 20000; ++step) {
    const auto user = static_cast<std::uint32_t>(random() % 4 + 1);
    const auto action = random() % 10;
    if (action < 6) {
      const auto& p = h.config.products[random() % h.config.products.size()];
      auto price = static_cast<std::int64_t>(p.previous_close);
      if (price < 1000) price += static_cast<int>(random() % 25) - 12;
      else price -= static_cast<int>(random() % 1000);
      auto quantity = static_cast<Quantity>(random() % 20 + 1);
      if (step % 37 == 0) quantity = 0;
      if (step % 43 == 0) price = 0;
      const auto side = step % 71 ? (random() % 2 ? Side::Buy : Side::Sell) : static_cast<Side>(9);
      h.run(user,LinkKind::NewOrder,Client::new_request(step % 19 + 1,p.symbol,side,static_cast<Price>(price),quantity));
    } else if (action < 9) {
      OrderId id = (1ULL << 48) | (random() % 30000);
      if (!h.reference.history.empty() && step % 4) id = h.reference.history[random() % h.reference.history.size()].state.orderId;
      h.run(user,LinkKind::Cancel,CancelRequest{{MessageType::CancelRequest,sizeof(CancelRequest),step % 19 + 1},id});
    } else h.snapshot(user);
    if (step % 97 == 0) h.market();
  }
  for (unsigned u = 1; u <= 4; ++u) h.snapshot(u);
  h.market();
}
void pool_reuse_and_sweeps() {
  Harness h;
  // Exercise >1 node block, index growth, cancellation of queue head/middle/tail,
  // reused nodes, then one aggressive order producing thousands of fill events.
  for (unsigned i = 0; i < 2500; ++i)
    h.run(i % 4 + 1,LinkKind::NewOrder,Client::new_request(i + 1,"AAPL",Side::Sell,100 + i % 5,2));
  for (unsigned i = 0; i < 2500; i += 3)
    h.run(i % 4 + 1,LinkKind::Cancel,CancelRequest{{MessageType::CancelRequest,sizeof(CancelRequest),i + 3000},(1ULL << 48) | (i + 1)});
  for (unsigned i = 0; i < 1500; ++i)
    h.run(1,LinkKind::NewOrder,Client::new_request(i + 6000,"AAPL",Side::Sell,100,1));
  h.market();
  h.run(2,LinkKind::NewOrder,Client::new_request(9000,"AAPL",Side::Buy,110,10000));
  h.market(); for (unsigned u = 1; u <= 4; ++u) h.snapshot(u);
  // Maximum quantity and 64-bit aggregate, tiny close and both limit endpoints.
  for (const auto price : {90,110}) h.run(1,LinkKind::NewOrder,Client::new_request(price,"FRACT",Side::Buy,price,std::numeric_limits<Quantity>::max()));
  h.run(1,LinkKind::NewOrder,Client::new_request(111,"FRACT",Side::Buy,111,std::numeric_limits<Quantity>::max()));
  h.run(1,LinkKind::NewOrder,Client::new_request(112,"MAX",Side::Buy,std::numeric_limits<Price>::max(),1));
  h.run(1,LinkKind::NewOrder,Client::new_request(113,"TINY",Side::Buy,1,1));
  h.market();
}
void concurrent_view() {
  auto config = configuration(); MatchingEngine engine(config,1);
  std::atomic_bool done{false}; std::atomic<unsigned> reads{0};
  std::mutex errors; std::exception_ptr error;
  auto guarded = [&](auto fn) { try { fn(); } catch (...) { std::lock_guard lock(errors); if (!error) error = std::current_exception(); } };
  std::vector<std::thread> readers;
  for (unsigned i = 0; i < 4; ++i) readers.emplace_back([&] {
    guarded([&] {
      do {
        const auto view = engine.market_view();
        for (const auto& [id,book] : view.books) {
          (void)id; md::validate_book(book);
          for (const auto& level : book.levels) check(level.quantity == 10 * level.num_orders,"Torn aggregate during command");
        }
        ++reads; std::this_thread::yield();
      } while (!done.load());
    });
  });
  guarded([&] {
    for (unsigned i = 0; i < 10000; ++i) {
      OrderId id = 0;
      const auto request = Client::new_request(i * 2 + 1,"AAPL",Side::Buy,100,10);
      engine.command({0,LinkKind::NewOrder,i * 2 + 1,1,1},record_bytes(request),[&](auto,const auto&,auto payload) { id = copy_record<OrderUpdate>(payload).order.orderId; });
      const CancelRequest cancel{{MessageType::CancelRequest,sizeof(CancelRequest),i * 2 + 2},id};
      engine.command({0,LinkKind::Cancel,i * 2 + 2,1,1},record_bytes(cancel),[](auto,const auto&,auto) {});
    }
  });
  done.store(true); for (auto& reader : readers) reader.join();
  if (error) std::rethrow_exception(error);
  check(reads > 0 && engine.market_view().books.at(1).levels.empty(),"Concurrent reader did not run or leaked an order");
  std::cout << "COHERENT_MARKET_VIEWS=" << reads << '\n';
}
}
int main(int argc,char** argv) {
  try {
    check(argc == 2,"Expected results directory"); std::filesystem::create_directories(argv[1]);
    Cases cases(std::filesystem::path(argv[1]) / "correctness.csv");
    cases.run("active_index_100000_random_insert_erase_find",index_churn);
    cases.run("20000_commands_against_independent_reference",random_reference);
    cases.run("pool_growth_reuse_fifo_multilevel_sweep_and_int_limits",pool_reuse_and_sweeps);
    cases.run("single_writer_four_concurrent_market_readers",concurrent_view);
    return 0;
  } catch (const std::exception& e) { std::cerr << "FAIL " << e.what() << '\n'; return 1; }
}
