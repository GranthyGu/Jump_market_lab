#pragma once
#include "fixtures.hpp"
#include "network.hpp"
#include <array>

namespace spread_test {
namespace md = m::md;
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
} // namespace spread_test
