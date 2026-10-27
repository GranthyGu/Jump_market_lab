#include "tests/gateway_support.hpp"
#include "gateway/net/engine_connections.hpp"

using namespace marketlab;
using namespace marketlab::orders;
using namespace marketlab::orders::test;
namespace {
LoginRequest login_request(const std::string& name,RequestId id = 1) {
  LoginRequest login{}; login.header = {MessageType::LoginRequest,sizeof(login),id}; put_string(login.username,name); put_string(login.password,"pass"); return login;
}
OrderState state(OrderId id) {
  OrderState s{}; s.orderId = id; put_string(s.symbol,"AAPL"); s.side = Side::Buy; s.price = 90;
  s.originalQuantity = s.remainingQuantity = 7; s.status = OrderStatus::Accepted; return s;
}
class Scripted {
 public:
  std::filesystem::path path;
  OrderConfig c;
  TcpReactor io;
  EngineLinks links;
  Process gateway;
  std::unique_ptr<md::StopThread> worker;
  std::atomic_bool frozen{false};
  std::atomic_uint commands{0};
  std::function<void(ConnectionId,const LinkHeader&,std::span<const std::uint8_t>)> script;
  Scripted(const std::string& binary,const std::filesystem::path& directory) : path(directory),io(),links(io,c) {
    std::filesystem::create_directories(path);
    c.gateway_listen = {"127.0.0.1",free_port()};
    Address engine{"127.0.0.1",free_port()};
    while (engine.port == c.gateway_listen.port) engine.port = free_port();
    c.shards = {{1,engine}}; c.accounts = {{"alice","pass"},{"probe","pass"}};
    c.products = {{"AAPL",100,1}}; c.heartbeat_ms = 100; c.link_timeout_ms = 900; c.request_timeout_ms = 700;
    std::ofstream out(path / "exchange.conf");
    out << "gateway_listen=127.0.0.1," << c.gateway_listen.port << "\nuser=alice,pass\nuser=probe,pass\nshard=1,127.0.0.1,"
        << engine.port << "\nsymbol=AAPL,100,1\nheartbeat_ms=100\nlink_timeout_ms=900\nrequest_timeout_ms=700\nreconnect_ms=50\n";
    out.close();
    io.on_accept = [&](auto id) { links.incoming(id); };
    io.on_close = [&](auto id) { links.lost(id); };
    io.on_frame = [&](auto id,auto bytes) { if (!frozen.load()) links.receive(id,bytes); };
    links.on_message = [&](auto id,const auto& h,auto bytes) {
      if (h.kind == LinkKind::Snapshot && h.user == 2) { end(id,h,copy_record<Header>(bytes).requestId); return; }
      if (script) script(id,h,bytes);
    };
    io.listen(engine,Framing::Internal,16);
    gateway.start(binary,{"gateway","--config",(path / "exchange.conf").string(),"--stats",(path / "gateway.json").string()},path / "gateway.log");
  }
  ~Scripted() { gateway.stop(); worker.reset(); }
  void start() {
    worker = std::make_unique<md::StopThread>([&](const auto& stop) {
      while (!stop.load()) { io.step(5); if (!frozen.load()) links.tick(); }
    });
    ready();
  }
  void ready() {
    const auto until = deadline(6000);
    while (md::Clock::now() < until) {
      try { Client p(c.gateway_listen); p.login("probe"); return; }
      catch (const std::exception&) { std::this_thread::sleep_for(20ms); }
    }
    throw std::runtime_error("Scripted gateway startup failed");
  }
  void end(ConnectionId id,const LinkHeader& h,RequestId request) {
    auto value = server_record<ActiveOrderSnapshot>(MessageType::ActiveOrderSnapshot,request); value.isEnd = 1;
    links.send(id,LinkKind::SnapshotEnd,h,record_bytes(value));
  }
  void item(ConnectionId id,const LinkHeader& h,RequestId request,OrderId order) {
    auto value = server_record<ActiveOrderSnapshot>(MessageType::ActiveOrderSnapshot,request); value.order = state(order);
    links.send(id,LinkKind::Event,h,record_bytes(value));
  }
};
}
int main(int argc,char** argv) {
  try {
    check(argc == 3,"Expected binary and output directory");
    std::filesystem::create_directories(argv[2]); const std::filesystem::path output = argv[2];
    Cases cases(output / "failure.csv");
    cases.run("stale_snapshot_generation_and_live_interleaving",[&] {
      Scripted env(argv[1],output / "stale_snapshot");
      LinkHeader old{}; RequestId old_request = 0;
      env.script = [&](auto id,const auto& h,auto bytes) {
        if (h.kind != LinkKind::Snapshot) return;
        const auto request = copy_record<Header>(bytes).requestId;
        if (!old.token) { old = h; old_request = request; return; }
        env.item(id,old,old_request,111); env.end(id,old,old_request);
        env.item(id,h,request,222);
        auto trade = server_record<Trade>(MessageType::Trade,0); trade.orderId = 222; trade.tradeId = 9;
        put_string(trade.symbol,"AAPL"); trade.side = Side::Buy; trade.price = 90; trade.quantity = 1;
        env.links.send(id,LinkKind::Event,{0,LinkKind::Event,0,1,0},record_bytes(trade));
        env.end(id,h,request);
      };
      env.start(); Client old_client(env.c.gateway_listen); old_client.send(login_request("alice",5));
      old_client.receive_as<LoginAccepted>(MessageType::LoginAccepted);
      Client current(env.c.gateway_listen); current.send(login_request("alice",5));
      current.receive_as<LoginAccepted>(MessageType::LoginAccepted);
      const auto snapshot = current.receive_as<ActiveOrderSnapshot>(MessageType::ActiveOrderSnapshot);
      check(!snapshot.isEnd && snapshot.order.orderId == 222 && snapshot.header.requestId == 5,"Old login snapshot leaked into new session");
      check(current.receive_as<Trade>(MessageType::Trade).orderId == 222,"Live event blocked by snapshot");
      check(current.receive_as<ActiveOrderSnapshot>(MessageType::ActiveOrderSnapshot).isEnd == 1,"Missing current terminal marker");
      old_client.expect_closed();
    });
    cases.run("unknown_outcome_no_replay_and_snapshot_recovery",[&] {
      Scripted env(argv[1],output / "no_replay");
      bool committed = false;
      env.script = [&](auto id,const auto& h,auto bytes) {
        if (h.kind == LinkKind::Snapshot) {
          const auto request = copy_record<Header>(bytes).requestId;
          if (committed) env.item(id,h,request,333);
          env.end(id,h,request);
        } else if (h.kind == LinkKind::NewOrder) { committed = true; ++env.commands; env.io.close(id); }
      };
      env.start(); Client client(env.c.gateway_listen); client.login("alice");
      client.send(Client::new_request(2,"AAPL",Side::Buy,90,7)); client.expect_closed(); env.ready();
      Client recovered(env.c.gateway_listen); const auto snapshot = recovered.login("alice");
      check(snapshot.size() == 1 && snapshot[0].orderId == 333 && env.commands.load() == 1,"Unknown-outcome order replayed/lost");
    });
    cases.run("request_timeout_without_false_rejection",[&] {
      Scripted env(argv[1],output / "request_timeout");
      env.script = [&](auto id,const auto& h,auto bytes) {
        if (h.kind == LinkKind::Snapshot) env.end(id,h,copy_record<Header>(bytes).requestId);
        else if (h.kind == LinkKind::NewOrder) ++env.commands;
      };
      env.start(); Client client(env.c.gateway_listen); client.login("alice");
      client.send(Client::new_request(2,"AAPL",Side::Buy,90,1));
      const auto start = md::Clock::now();
      // No wire response is allowed: absence of an engine response is not a rejection.
      pollfd p{client.fd.get(),POLLIN,0}; check(poll(&p,1,2500) > 0,"Request did not expire");
      std::uint8_t byte{}; check(recv(client.fd.get(),&byte,1,0) == 0,"Fabricated response on timeout");
      check(elapsed_ms(start) >= 600 && env.commands.load() == 1,"Timeout budget/replay error");
      Client healthy(env.c.gateway_listen); healthy.login("alice");
    });
    cases.run("heartbeat_detects_silent_tcp_peer",[&] {
      Scripted env(argv[1],output / "heartbeat");
      env.script = [&](auto id,const auto& h,auto bytes) { if (h.kind == LinkKind::Snapshot) env.end(id,h,copy_record<Header>(bytes).requestId); };
      env.start(); Client client(env.c.gateway_listen); client.login("alice");
      env.frozen.store(true); client.expect_closed(2500); env.frozen.store(false); env.ready();
      Client next(env.c.gateway_listen); next.login("alice");
    });
    cases.run("wrong_user_engine_response_fails_closed",[&] {
      Scripted env(argv[1],output / "bad_engine");
      env.script = [&](auto id,const auto& h,auto bytes) {
        if (h.kind == LinkKind::Snapshot) env.end(id,h,copy_record<Header>(bytes).requestId);
        else if (h.kind == LinkKind::NewOrder) {
          const auto request = copy_record<NewOrderRequest>(bytes);
          auto update = server_record<OrderUpdate>(MessageType::OrderUpdate,request.header.requestId); update.order = state(444);
          auto wrong = h; wrong.user = 2; env.links.send(id,LinkKind::Event,wrong,record_bytes(update));
        }
      };
      env.start(); Client client(env.c.gateway_listen); client.login("alice");
      client.send(Client::new_request(2,"AAPL",Side::Buy,90,1)); client.expect_closed();
    });
    std::cout << "gateway_failure_cases=" << cases.count << " PASS\n"; return 0;
  } catch (const std::exception& e) { std::cerr << "FAIL " << e.what() << '\n'; return 1; }
}
