#include "tests/gateway_support.hpp"
#include <map>
#include <tuple>

using namespace marketlab;
using namespace marketlab::orders;
using namespace marketlab::orders::test;
namespace {
struct Terminal {
  md::Fd input;
  Process process;
  std::filesystem::path log;
  Terminal(const std::string& binary,const Address& gateway,const std::string& user,
           const std::filesystem::path& output,const std::string& password = "pass") : log(output) {
    int pipe_fds[2]; check(pipe(pipe_fds) == 0,"Client stdin pipe");
    md::Fd reader(pipe_fds[0]); input.reset(pipe_fds[1]);
    check(fcntl(reader.get(),F_SETFD,FD_CLOEXEC) == 0 && fcntl(input.get(),F_SETFD,FD_CLOEXEC) == 0,"Pipe CLOEXEC");
    process.start(binary,{"--host",gateway.host,"--port",std::to_string(gateway.port),"--user",user,"--password",password},log,reader.get());
  }
  std::string text() const {
    std::ifstream in(log); return {(std::istreambuf_iterator<char>(in)),{}};
  }
  void wait_for(const std::string& expected) const {
    const auto until = deadline();
    while (md::Clock::now() < until) {
      if (text().find(expected) != std::string::npos) return;
      std::this_thread::sleep_for(10ms);
    }
    throw std::runtime_error("Missing " + expected + " in " + log.string() + ": " + text());
  }
  void send(const std::string& command) {
    const auto line = command + '\n'; std::size_t sent = 0;
    while (sent < line.size()) {
      const auto n = write(input.get(),line.data() + sent,line.size() - sent);
      if (n < 0 && errno == EINTR) continue;
      check(n > 0,"Write console command"); sent += static_cast<std::size_t>(n);
    }
  }
};
void udp_fanout(Fixture& f,const std::string& binary,const std::array<std::uint16_t,2>& market_ports) {
  std::array<md::Fd,3> receivers;
  std::array<std::uint16_t,3> ports{};
  for (unsigned i = 0; i < receivers.size(); ++i) {
    ports[i] = free_port(); receivers[i] = md::bind_udp("127.0.0.1",ports[i]);
  }
  auto query_port = free_port();
  while (query_port == market_ports[0] || query_port == market_ports[1]) query_port = free_port();
  const auto config = f.directory / "publisher.conf";
  std::ofstream out(config);
  out << "symbols=1:AAPL,2:MSFT,3:EMPTY,4:FRACT\n"
      << "shard=one,tcp,127.0.0.1," << market_ports[0] << ",/tmp/unused-one,1,3,4\n"
      << "shard=two,tcp,127.0.0.1," << market_ports[1] << ",/tmp/unused-two,2\n"
      << "query_port=" << query_port << '\n';
  for (auto port : ports) out << "udp_receiver=127.0.0.1," << port << '\n';
  out.close();
  Process publisher; publisher.start(binary,{"publish","--config",config.string()},f.directory / "publisher.log");
  using Key = std::tuple<std::uint64_t,std::uint16_t,unsigned>;
  std::array<std::map<Key,Bytes>,3> copies;
  const auto until = deadline(10000);
  // Four products: snapshots at index 0, deltas 1..5, snapshots at index 5.
  constexpr std::size_t expected = 28;
  for (;;) {
    bool complete = true; for (const auto& copy : copies) complete = complete && copy.size() == expected;
    if (complete) break;
    check(md::Clock::now() < until,"UDP targets did not receive all rounds");
    pollfd ready[3];
    for (unsigned i = 0; i < 3; ++i) ready[i] = {receivers[i].get(),POLLIN,0};
    const auto count = poll(ready,3,100); check(count >= 0 || errno == EINTR,"UDP poll");
    for (unsigned i = 0; i < 3; ++i) if (ready[i].revents & POLLIN) {
      std::array<std::uint8_t,1024> buffer{};
      const auto n = recv(receivers[i].get(),buffer.data(),buffer.size(),0); check(n > 0,"UDP receive");
      const Bytes bytes(buffer.begin(),buffer.begin() + n); const auto message = md::decode(bytes);
      if (message.index > 5) continue;
      check(copies[i].emplace(Key{message.index,message.symbol_id,static_cast<unsigned>(message.kind)},bytes).second,"Duplicate datagram");
      if (message.kind == md::Kind::Snapshot && message.index == 0) {
        if (message.symbol_id == 1 || message.symbol_id == 2) {
          check(message.levels.size() == 1 && message.levels[0].side == md::Side::Buy &&
              message.levels[0].quantity == (message.symbol_id == 1 ? 6 : 5) && message.levels[0].num_orders == 1,"Published book does not match CLI orders");
        } else check(message.levels.empty(),"Empty product has phantom levels");
      }
    }
  }
  check(copies[0] == copies[1] && copies[0] == copies[2],"UDP payloads differ across destinations");
  const auto client_config = md::load_config(config.string());
  auto query = md::connect_tcp(client_config.query.host,client_config.query.port,deadline());
  Bytes request; md::append_uint(request,1,2); md::send_all(query.get(),request,deadline());
  const auto prefix = md::receive_exact(query.get(),2,deadline()); std::size_t offset = 0;
  const auto reply = md::decode(md::receive_exact(query.get(),md::read_uint(prefix,offset,2),deadline()));
  check(reply.kind == md::Kind::Snapshot && reply.symbol_id == 1 && reply.levels.size() == 1 && reply.levels[0].quantity == 6,"TCP snapshot differs from gateway order state");
  std::cout << "UDP_TARGETS=3 datagrams_per_target=" << expected << " payloads=identical TCP_snapshot=PASS\n";
}
}
int main(int argc,char** argv) {
  try {
    check(argc == 5,"Expected gateway, console, publisher binaries and output directory");
    Fixture f(argv[1],argv[4]); Cases cases(f.directory / "manual_cli.csv");
    std::array<std::uint16_t,2> market_ports{free_port(),free_port()};
    while (market_ports[0] == market_ports[1]) market_ports[1] = free_port();
    for (unsigned i = 0; i < 2; ++i) { f.shards[i].stop(); f.start_shard(i,market_ports[i]); }
    f.wait_ready();
    Terminal alice(argv[2],f.config.gateway_listen,"alice",f.directory / "alice.log");
    Terminal bob(argv[2],f.config.gateway_listen,"bob",f.directory / "bob.log");
    Terminal carol(argv[2],f.config.gateway_listen,"carol",f.directory / "carol.log");
    cases.run("three_console_accounts_and_cross_shard_orders",[&] {
      for (auto* terminal : {&alice,&bob,&carol}) terminal->wait_for("READY");
      alice.send("buy AAPL 100 10"); alice.wait_for("remaining=10 status=Accepted");
      bob.send("sell AAPL 100 4"); bob.wait_for("remaining=0 status=Filled"); alice.wait_for("remaining=6 status=PartiallyFilled");
      carol.send("buy MSFT 200 5"); carol.wait_for("symbol=MSFT"); carol.wait_for("remaining=5 status=Accepted");
    });
    cases.run("three_udp_destinations_and_authoritative_tcp_snapshot",[&] { udp_fanout(f,argv[3],market_ports); });
    cases.run("console_takeover_snapshot_cancel_relogin_and_bad_password",[&] {
      Terminal next(argv[2],f.config.gateway_listen,"alice",f.directory / "alice-next.log"); next.wait_for("READY"); alice.wait_for("DISCONNECTED");
      next.wait_for("remaining=6 status=PartiallyFilled");
      const auto text = next.text(); const auto at = text.find("orderId="); check(at != std::string::npos,"No snapshot order ID");
      const auto id = std::stoull(text.substr(at + 8)); check((id >> 48) == 1,"Console lost 64-bit ID");
      next.send("cancel " + std::to_string(id)); next.wait_for("remaining=0 status=Canceled");
      next.send("relogin"); next.wait_for("SNAPSHOT_END requestId=3");
      carol.send("cancel-last"); carol.wait_for("remaining=0 status=Canceled");
      Terminal bad(argv[2],f.config.gateway_listen,"bob",f.directory / "bad-password.log","wrong"); bad.wait_for("REJECTED requestId=1 reason=1");
      bob.send("buy AAPL 1 1"); bob.wait_for("REJECTED requestId=3 reason=2");
      bob.send("buy AAPL 90 1"); bob.wait_for("remaining=1 status=Accepted"); bob.send("cancel-last"); bob.wait_for("remaining=0 status=Canceled");
      next.send("quit"); bob.send("quit"); carol.send("quit");
    });
    std::cout << "manual_cli_cases=" << cases.count << " PASS\n"; return 0;
  } catch (const std::exception& e) { std::cerr << "FAIL " << e.what() << '\n'; return 1; }
}
