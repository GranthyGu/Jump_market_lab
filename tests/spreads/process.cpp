#include "fixtures.hpp"
#include "matching_engine/spreads/depth.hpp"
#include "network.hpp"
#include <array>

using namespace spread_test;
namespace md = m::md;
namespace {
struct Deployment {
  std::filesystem::path root, binary;
  o::Address gateway;
  std::array<unsigned, 8> orders{}, markets{}, controls{};
  std::array<demo::Child, 8> engines;
  demo::Child coordinator, router, front;
  bool trace;
  unsigned request = 10;
  unsigned admin_port = 0;
  std::unique_ptr<demo::Trader> client;
  Deployment(const std::filesystem::path &binaries,
             const std::filesystem::path &path, bool tracing = true)
      : root(path), binary(binaries), trace(tracing) {
    std::filesystem::create_directories(root);
    demo::Ports ports;
    gateway = {"127.0.0.1", ports.take()};
    const auto router_port = ports.take(), coordinator_port = ports.take(),
               admin = ports.take();
    admin_port = admin;
    std::array<unsigned, 4> peers{};
    for (unsigned i = 0; i < 8; ++i) {
      orders[i] = ports.take();
      markets[i] = ports.take();
      controls[i] = ports.take();
    }
    for (auto &peer : peers)
      peer = ports.take();
    const auto path_config = root / "exchange.conf";
    std::ofstream config(path_config);
    config << "routing=router\nspreads=enabled\ngateway_listen=127.0.0.1,"
           << gateway.port << "\nrouter_listen=127.0.0.1," << router_port
           << "\nengine_endpoint=127.0.0.1," << router_port
           << "\ncoordinator=127.0.0.1," << coordinator_port
           << "\nuser=Alice,demo\nuser=Bob,demo\nuser=Carol,demo\nrequest_"
              "timeout_ms=30000\nmax_pending_per_client=20000\n";
    for (unsigned i = 0; i < 8; ++i)
      config << "shard=" << i + 1 << ",127.0.0.1," << orders[i] << '\n';
    for (unsigned i = 0; i < 4; ++i) {
      config << "symbol=ITEM" << i + 1 << ",100," << i + 1 << '\n';
      config << "spread_peer=" << i + 5 << ",127.0.0.1," << peers[i] << '\n';
    }
    config << "symbol=A,120,6\nsymbol=B,110,7\nsymbol=C,105,8\n"
              "symbol=SP_AB,10,5\nsymbol=SP_AC,15,5\nsymbol=BF_ABC,5,5\n";
    config.close();
    ports.release();
    for (unsigned i = 0; i < 8; ++i)
      start_engine(i);
    coordinator.launch(binary / "market_group_coordinator",
                       traced({"--config", path_config.string(), "--stats",
                               (root / "coordinator.json").string()}),
                       root / "coordinator.log");
    router.launch(binary / "market_engine_router",
                  traced({"--config", path_config.string(), "--admin-port",
                          std::to_string(admin)}),
                  root / "router.log");
    front.launch(binary / "market_gateway",
                 {"gateway", "--config", path_config.string()},
                 root / "gateway.log");
    demo::eventually(
        [&] {
          try {
            auto probe = std::make_unique<demo::Trader>(gateway);
            require(probe->login("Alice").empty(), "Fresh login is not empty");
            client = std::move(probe);
            return true;
          } catch (...) {
            return false;
          }
        },
        "Group startup/login timed out; logs: " + root.string(), 16000);
  }
  ~Deployment() {
    client.reset();
    front.stop();
    router.stop();
    coordinator.stop();
    for (auto &engine : engines)
      engine.stop();
  }
  std::vector<std::string> traced(std::vector<std::string> args) const {
    if (trace)
      args.push_back("--trace");
    return args;
  }
  void start_engine(unsigned index, std::string suffix = "") {
    engines[index].launch(
        binary / "market_matching_engine",
        traced({"--config", (root / "exchange.conf").string(), "--shard",
                std::to_string(index + 1), "--data-dir", (root / "data").string(),
                "--market-port", std::to_string(markets[index]), "--control-port",
                std::to_string(controls[index])}),
        root / ("shard-" + std::to_string(index + 1) + suffix + ".log"));
  }
  std::string admin(std::string_view command) {
    namespace rt = m::runtime;
    auto socket = rt::connect_tcp("127.0.0.1", admin_port, demo::until());
    o::Bytes bytes;
    p::integer(bytes, command.size(), 4);
    bytes.insert(bytes.end(), command.begin(), command.end());
    rt::send_all(socket.get(), bytes, demo::until());
    bytes = rt::receive_exact(socket.get(), 4, demo::until());
    p::Reader reader{bytes};
    bytes = rt::receive_exact(socket.get(), reader.number(4), demo::until());
    require(!bytes.empty() && !bytes[0], "Router admin failed");
    return {bytes.begin() + 1, bytes.end()};
  }
  m::OrderId add(const char *symbol, m::Side side, int price, int quantity) {
    return client->add(++request, symbol, side, price, quantity).order.orderId;
  }
  std::vector<m::Trade> fills(unsigned count) {
    std::vector<m::Trade> trades;
    for (unsigned i = 0; i < count; ++i) {
      auto trade = client->get<m::Trade>(m::MessageType::Trade);
      auto update = client->get<m::OrderUpdate>(m::MessageType::OrderUpdate);
      require(trade.header.requestId == 0 && update.header.requestId == 0 &&
                  trade.orderId == update.order.orderId &&
                  trade.timestamp == update.timestamp,
              "Trade/update identity, order, or shared timestamp changed "
              "through Gateway");
      trades.push_back(trade);
    }
    return trades;
  }
  std::vector<o::Bytes> history() {
    m::TradeHistoryRequest query{
        {m::MessageType::TradeHistoryRequest, sizeof(query), ++request},
        0,
        256,
        0,
        0};
    client->send(query);
    std::vector<o::Bytes> records;
    unsigned ends = 0;
    for (;;) {
      auto bytes = client->receive();
      m::Header header{};
      std::memcpy(&header, bytes.data(), sizeof(header));
      require(header.requestId == request,
              "History request correlation changed");
      if (header.messageType == m::MessageType::TradeHistoryEnd) {
        const auto end = o::copy_record<m::TradeHistoryEnd>(bytes);
        if (!end.shard) {
          require(ends == 8 && end.flags == m::history_all_shards,
                  "Wrong all-shard history termination");
          return records;
        }
        require(!(end.flags & m::history_more),
                "Unexpected history pagination in small test");
        ++ends;
      } else {
        require(m::is_history_record(header.messageType),
                "Typed history was rejected by network path");
        records.push_back(std::move(bytes));
      }
    }
  }
  md::ShardView market(unsigned shard) {
    std::vector<std::uint16_t> symbols;
    if (shard <= 4)
      symbols = {static_cast<std::uint16_t>(shard)};
    else if (shard == 5)
      symbols = {8, 9, 10};
    else
      symbols = {static_cast<std::uint16_t>(shard - 1)};
    auto source = md::make_source(
        {"s", "tcp", "127.0.0.1",
         static_cast<std::uint16_t>(markets[shard - 1]), "", symbols},
        10000);
    return source->read(demo::until());
  }
};

void baseline(Deployment &d) {
  for (unsigned i = 1; i <= 4; ++i) {
    const auto symbol = "ITEM" + std::to_string(i);
    const auto order = d.add(symbol.c_str(), m::Side::Buy, 100, 2);
    require(order >> 48 == i, "Ordinary shard routing changed");
    d.client->cancel(++d.request, order);
  }
  const auto bf = d.add("BF_ABC", m::Side::Buy, 5, 2);
  d.client->cancel(++d.request, bf);
}

void implied(Deployment &d) {
  d.add("A", m::Side::Sell, 120, 4);
  d.add("B", m::Side::Buy, 110, 4);
  demo::eventually(
      [&] {
        const auto view = d.market(5).books.at(8);
        return view.levels.size() == 1 && view.levels[0].quantity == 0 &&
               view.levels[0].implied_quantity == 4 &&
               view.levels[0].num_orders == 1;
      },
      "Public implied quote was not installed");
  const auto incoming = d.add("SP_AB", m::Side::Buy, 10, 4);
  const auto trades = d.fills(3);
  require(trades[0].orderId == incoming && trades[0].price == 10 &&
              trades[1].price == 120 && trades[2].price == 110,
          "IN execution returned incorrect owned prices");
  const auto history = d.history();
  require(history.size() == 3,
          "Self-trade typed history duplicated or lost an execution");
  unsigned legs = 0, spread = 0;
  for (const auto &bytes : history) {
    m::Header header{};
    std::memcpy(&header, bytes.data(), sizeof(header));
    if (header.messageType == m::MessageType::ImpliedLegHistory) {
      const auto leg = o::copy_record<m::ImpliedLegHistory>(bytes);
      require(leg.aggressorOrderId == incoming && leg.fillNo == 1 &&
                  leg.trade.quantity == 4,
              "Leg history lost common execution identity");
      ++legs;
    } else {
      const auto execution = o::copy_record<m::SpreadExecutionHistory>(bytes);
      require(execution.aggressorOrderId == incoming &&
                  execution.orderId == incoming && execution.fillNo == 1 &&
                  execution.a.price == 120 && execution.x.price == 110,
              "Spread history lost real counterparties");
      ++spread;
    }
  }
  require(legs == 2 && spread == 1, "Wrong typed records by owning shard");
}

void signed_and_direct(Deployment &d) {
  for (const int price : {-1, 0}) {
    d.add("A", m::Side::Sell, 110 + price, 1);
    d.add("B", m::Side::Buy, 110, 1);
    d.add("SP_AB", m::Side::Buy, price, 1);
    require(d.fills(3)[0].price == price,
            "Signed group price failed native TCP path");
  }
  d.add("SP_AB", m::Side::Buy, 0, 1);
  d.add("SP_AB", m::Side::Sell, -1, 1);
  const auto trades = d.fills(2);
  require(trades[0].price == 0 && trades[1].price == 0,
          "Signed direct book did not use maker price");
  require(d.history().size() == 10,
          "Mixed direct/typed history failed full fanout");
}

void rolling(Deployment &d) {
  for (unsigned i = 0; i < 5; ++i) {
    d.add("A", m::Side::Sell, 120 + i, 1);
    d.add("B", m::Side::Buy, 110 - i, 1);
  }
  const auto incoming = d.add("SP_AB", m::Side::Buy, 18, 5);
  const auto trades = d.fills(15);
  std::vector<int> spread_prices;
  for (const auto &trade : trades)
    if (trade.orderId == incoming)
      spread_prices.push_back(trade.price);
  require(spread_prices == std::vector<int>{10, 12, 14, 16, 18},
          "Native rolling depth skipped a source level");
  demo::Trader reconnect(d.gateway);
  require(reconnect.login("Alice", ++d.request).empty(),
          "Public active-order capture retained consumed orders");
  d.client = std::make_unique<demo::Trader>(d.gateway);
  require(d.client->login("Alice", ++d.request).empty(),
          "Repeated capture changed group state");
}

void idle_recovery(Deployment &d) {
  d.add("A", m::Side::Sell, 120, 2);
  d.add("B", m::Side::Buy, 110, 2);
  const auto before = d.history();
  d.engines[5].crash();
  demo::eventually(
      [&] { return d.admin("group-status").starts_with("PAUSED "); },
      "Idle member loss did not pause the group");
  const auto queued_request = ++d.request;
  d.client->send(demo::order_record(queued_request, "C", m::Side::Buy, 100, 1));
  demo::eventually([&] { return d.admin("group-status").ends_with(" 1"); },
                   "Unexecuted request was not retained during group pause");
  d.start_engine(5, "-recovered");
  const auto queued =
      d.client->get<m::OrderUpdate>(m::MessageType::OrderUpdate);
  require(queued.header.requestId == queued_request &&
              queued.order.status == m::OrderStatus::Accepted,
          "Retained group request did not resume exactly once");
  d.client->cancel(++d.request, queued.order.orderId);
  demo::eventually(
      [&] { return d.admin("group-status").starts_with("READY "); },
      "Complete durable member failed to rejoin", 10000);
  demo::eventually(
      [&] {
        try {
          auto client = std::make_unique<demo::Trader>(d.gateway);
          require(client->login("Alice", ++d.request).size() == 2,
                  "Recovered active orders differ");
          d.client = std::move(client);
          return true;
        } catch (...) {
          return false;
        }
      },
      "Gateway did not reconnect after group recovery", 10000);
  auto after = d.history();
  require(before.size() == after.size(), "Recovered history size changed");
  auto economic = [](auto records) {
    for (auto &record : records)
      record.erase(record.begin(), record.begin() + sizeof(m::Header));
    std::sort(records.begin(), records.end());
    return records;
  };
  require(economic(before) == economic(after),
          "Recovered typed history bytes changed");
  const auto view = d.market(5).books.at(8);
  require(view.levels.size() == 1 && view.levels[0].implied_quantity == 2,
          "Recovered real sources did not rebuild implied depth");
  d.add("SP_AB", m::Side::Buy, 10, 2);
  require(d.fills(3)[0].quantity == 2, "Rejoined group cannot execute");
}

void routes(Deployment &d) {
  namespace s = o::spreads;
  constexpr std::array<const char *, 5> names{"A", "B", "C", "SP_AB", "SP_AC"};
  constexpr std::array<int, 5> price{120, 110, 105, 10, 15};
  for (const auto &route : s::routes) {
    for (auto source : {route.first, route.second})
      d.add(names[source / 2],
            source % 2 == s::Bid ? m::Side::Buy : m::Side::Sell,
            price[source / 2], 2);
    const auto incoming =
        d.add(names[route.target],
              route.side == s::Ask ? m::Side::Buy : m::Side::Sell,
              price[route.target], 2);
    const auto fills = d.fills(3);
    require(fills[0].orderId == incoming &&
                fills[0].price == price[route.target] && fills[0].quantity == 2,
            "Native IN/OUT route lost incoming price or quantity");
  }
}

void latency(Deployment &d, demo::Suite &suite, unsigned count = 100,
             unsigned warmup = 0) {
  std::vector<double> local, implied;
  local.reserve(count);
  implied.reserve(count);
  // Four stable levels keep top-three and hasMore unchanged for the fifth
  // level.
  std::vector<m::OrderId> anchors;
  for (int price = 109; price < 113; ++price)
    anchors.push_back(d.add("C", m::Side::Sell, price, 1));
  for (unsigned i = 0; i < count + warmup; ++i) {
    const auto start = demo::Clock::now();
    const auto id = d.add("C", m::Side::Sell, 113, 1);
    if (i >= warmup)
      local.push_back(demo::us(start));
    d.client->cancel(++d.request, id);
  }
  for (auto id : anchors)
    d.client->cancel(++d.request, id);
  d.add("A", m::Side::Sell, 120, count + warmup);
  d.add("B", m::Side::Buy, 110, count + warmup);
  for (unsigned i = 0; i < count + warmup; ++i) {
    const auto start = demo::Clock::now();
    d.add("SP_AB", m::Side::Buy, 10, 1);
    require(d.fills(3).size() == 3, "Single-outstanding implied fills lost");
    if (i >= warmup)
      implied.push_back(demo::us(start));
  }
  const auto seconds = [](const auto &values) {
    double total = 0;
    for (auto value : values)
      total += value;
    return total / 1e6;
  };
  suite.metric("native-local-short-path", local, seconds(local), count,
               "Single outstanding Gateway TCP request to durable Accepted; "
               "unchanged top3/hasMore");
  suite.metric("native-implied-single-outstanding", implied, seconds(implied),
               count,
               "Single outstanding Gateway TCP request through all three shard "
               "fsyncs and all owned fills");
}

void unresolved_crash(Deployment &d) {
  d.add("A", m::Side::Sell, 120, 1);
  d.add("B", m::Side::Buy, 110, 1);
  std::istringstream before(d.admin("group-status"));
  std::string state;
  std::uint64_t epoch, join, dispatched, memory, published, queued;
  before >> state >> epoch >> join >> dispatched >> memory >> published >>
      queued;
  require(state == "READY" && dispatched == memory && memory == published,
          "Fault test did not start at a complete public boundary");
  d.engines[5].suspend();
  d.client->send(demo::order_record(++d.request, "SP_AB", m::Side::Buy, 10, 1));
  demo::eventually(
      [&] {
        const auto log = demo::read_text(d.root / "shard-7.log");
        return log.find("type=SOURCE_DONE epoch=" + std::to_string(epoch) +
                        " seq=" + std::to_string(dispatched + 1)) !=
               std::string::npos;
      },
      "B did not modify its real source while A was suspended");
  d.engines[5].crash();
  demo::eventually(
      [&] { return d.admin("group-status").starts_with("PAUSED "); },
      "Unresolved memory tail did not pause group");
  d.start_engine(5, "-unresolved");
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  std::istringstream after(d.admin("group-status"));
  std::uint64_t next_epoch, next_join, next_dispatch, next_memory, next_public,
      next_queued;
  after >> state >> next_epoch >> next_join >> next_dispatch >> next_memory >>
      next_public >> next_queued;
  require(state == "PAUSED" && next_epoch == epoch &&
              next_dispatch == dispatched + 1 && next_memory == memory &&
              next_public == published,
          "Reconnection incorrectly released partial execution state");
  std::uint8_t unexpected = 0;
  require(::recv(d.client->socket.get(), &unexpected, 1,
                 MSG_PEEK | MSG_DONTWAIT) < 0 &&
              (errno == EAGAIN || errno == EWOULDBLOCK),
          "Partial command emitted an acceptance or trade");
  const auto ordinary = d.add("ITEM1", m::Side::Buy, 100, 1);
  d.client->cancel(++d.request, ordinary);
  require(demo::read_text(d.root / "coordinator.log").find("unresolved=1") !=
              std::string::npos,
          "Coordinator did not retain the unresolved command");
}

void stress(Deployment &d, demo::Suite &suite) {
  constexpr unsigned count = 5000;
  d.add("B", m::Side::Buy, 110, count);
  std::vector<demo::Clock::time_point> started;
  started.reserve(count);
  const auto first_request = d.request + 1;
  const auto start = demo::Clock::now();
  for (unsigned i = 0; i < count; ++i) {
    started.push_back(demo::Clock::now());
    d.client->send(demo::order_record(++d.request, "A", m::Side::Sell, 120, 1));
  }
  std::vector<double> samples;
  for (unsigned i = 0; i < count; ++i) {
    const auto update =
        d.client->get<m::OrderUpdate>(m::MessageType::OrderUpdate);
    require(update.header.requestId == first_request + i &&
                update.order.status == m::OrderStatus::Accepted,
            "Pipelined group requests lost FIFO acceptance");
    samples.push_back(demo::us(started[i]));
  }
  suite.metric("native-group-pipelined-acceptance", samples,
               demo::us(start) / 1e6, count,
               "Gateway TCP request to durable Accepted; one pipelined client, "
               "includes group queue and fsync");
  const auto trade_start = demo::Clock::now();
  const auto incoming = d.add("SP_AB", m::Side::Buy, 10, count);
  const auto trades = d.fills(3 * count);
  unsigned quantity = 0;
  for (const auto &trade : trades)
    if (trade.orderId == incoming)
      quantity += trade.quantity;
  require(quantity == count, "Multipart native execution lost spread quantity");
  suite.metric("native-five-thousand-piece-command", {demo::us(trade_start)},
               demo::us(trade_start) / 1e6, count,
               "One native TCP command through all shard fsyncs and delivery "
               "of all 15000 owned fills; not per-order percentiles");
}
} // namespace

int main(int argc, char **argv) {
  require(argc == 3 || (argc == 4 && std::string_view(argv[3]) == "--benchmark"),
          "Expected binary and result directories, optionally --benchmark");
  const bool benchmark = argc == 4;
  demo::Suite suite(argv[2]);
  const auto root = suite.output / ("run-" + std::to_string(::getpid()) + "-" +
                                    std::to_string(o::unix_ns()));
  std::unique_ptr<Deployment> deployment;
  suite.test("native-eight-shards-coordinator-router-gateway-bootstrap",
             [&] {
               deployment = std::make_unique<Deployment>(argv[1], root, !benchmark);
             });
  if (benchmark) {
    if (deployment) {
      suite.test("warmed-no-trace-single-outstanding-latency",
                 [&] { latency(*deployment, suite, 2000, 200); });
      suite.test("no-trace-pipeline-and-five-thousand-piece-stress",
                 [&] { stress(*deployment, suite); });
    }
    deployment.reset();
    return suite.finish();
  }
  if (deployment) {
    suite.test("ordinary-shards-and-butterfly-reuse-original-path",
               [&] { baseline(*deployment); });
    suite.test("native-implied-market-trades-and-typed-history",
               [&] { implied(*deployment); });
    suite.test("negative-zero-and-direct-spread-native-execution",
               [&] { signed_and_direct(*deployment); });
    suite.test("native-multi-round-window-and-complete-order-capture",
               [&] { rolling(*deployment); });
    suite.test("idle-crashed-member-recovers-exact-boundary-and-rejoins",
               [&] { idle_recovery(*deployment); });
    suite.test("all-twelve-in-out-routes-across-real-peer-connections",
               [&] { routes(*deployment); });
    suite.test("single-outstanding-short-path-and-implied-latency",
               [&] { latency(*deployment, suite); });
    suite.test("native-pipeline-and-five-thousand-piece-multipart-stress",
               [&] { stress(*deployment, suite); });
    suite.test("partial-memory-crash-stays-paused-after-worker-reconnection",
               [&] { unresolved_crash(*deployment); });
  }
  deployment.reset();
  return suite.finish();
}
