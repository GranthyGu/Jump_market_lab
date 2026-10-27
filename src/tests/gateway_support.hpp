#pragma once
#include "gateway/config.hpp"
#include "gateway/protocol/engine_link.hpp"
#include "market_publisher/net/transport.hpp"
#include <arpa/inet.h>
#include <fcntl.h>
#include <poll.h>
#include <spawn.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>
#include <algorithm>
#include <chrono>
#include <csignal>
#include <cerrno>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <sstream>
#include <set>
#include <thread>
#include <utility>

extern char** environ;
namespace marketlab::orders::test {
using namespace std::chrono_literals;
inline void check(bool value,const std::string& description) { if (!value) throw std::runtime_error(description); }
inline auto deadline(int ms = 5000) { return md::Clock::now() + std::chrono::milliseconds(ms); }
inline double elapsed_ms(md::Clock::time_point start) { return std::chrono::duration<double,std::milli>(md::Clock::now() - start).count(); }
inline std::uint16_t free_port() {
  md::Fd fd(socket(AF_INET,SOCK_STREAM,0));
  sockaddr_in a{}; a.sin_family = AF_INET; a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  check(fd && bind(fd.get(),reinterpret_cast<sockaddr*>(&a),sizeof(a)) == 0,"Cannot reserve port");
  socklen_t length = sizeof(a);
  check(getsockname(fd.get(),reinterpret_cast<sockaddr*>(&a),&length) == 0,"getsockname");
  return ntohs(a.sin_port);
}
inline double percentile(std::vector<double> values,double p) {
  check(!values.empty(),"No timing samples"); std::sort(values.begin(),values.end());
  return values[std::min(values.size() - 1,static_cast<std::size_t>(std::ceil(p * values.size()) - 1))];
}
inline std::uint64_t metric(const std::filesystem::path& path,const std::string& name) {
  std::ifstream file(path); std::string text((std::istreambuf_iterator<char>(file)),{});
  auto at = text.find('"' + name + "\":");
  check(at != std::string::npos,"Missing metric " + name + " in " + path.string());
  return std::stoull(text.substr(at + name.size() + 3));
}
class Process {
 public:
  Process() = default;
  ~Process() { stop(); }
  Process(const Process&) = delete;
  Process& operator=(const Process&) = delete;
  void start(const std::string& binary,const std::vector<std::string>& args,const std::filesystem::path& log,int stdin_fd = -1) {
    check(pid_ == 0,"Process already running");
    std::vector<std::string> storage{binary}; storage.insert(storage.end(),args.begin(),args.end());
    std::vector<char*> argv; for (auto& s : storage) argv.push_back(s.data()); argv.push_back(nullptr);
    posix_spawn_file_actions_t actions;
    check(posix_spawn_file_actions_init(&actions) == 0,"spawn actions");
    if (stdin_fd >= 0) posix_spawn_file_actions_adddup2(&actions,stdin_fd,STDIN_FILENO);
    posix_spawn_file_actions_addopen(&actions,STDOUT_FILENO,log.c_str(),O_WRONLY|O_CREAT|O_TRUNC,0600);
    posix_spawn_file_actions_adddup2(&actions,STDOUT_FILENO,STDERR_FILENO);
    const int result = posix_spawn(&pid_,binary.c_str(),&actions,nullptr,argv.data(),environ);
    posix_spawn_file_actions_destroy(&actions);
    check(result == 0,"posix_spawn failed");
  }
  void stop(int signal = SIGTERM) {
    if (!pid_) return;
    kill(pid_,signal);
    const auto until = deadline(4000);
    int status = 0;
    while (waitpid(pid_,&status,WNOHANG) == 0 && md::Clock::now() < until) std::this_thread::sleep_for(5ms);
    if (waitpid(pid_,&status,WNOHANG) == 0) { kill(pid_,SIGKILL); waitpid(pid_,&status,0); }
    pid_ = 0;
  }
 private:
  pid_t pid_ = 0;
};
struct Client {
  md::Fd fd;
  explicit Client(const Address& a) : fd(md::connect_tcp(a.host,a.port,deadline())) {}
  void send_bytes(std::span<const std::uint8_t> bytes) { md::send_all(fd.get(),bytes,deadline()); }
  template <typename T> void send(const T& value) { send_bytes(record_bytes(value)); }
  Bytes receive(int timeout_ms = 5000) {
    const auto until = deadline(timeout_ms);
    const auto prefix = md::receive_exact(fd.get(),sizeof(Header),until);
    const auto h = copy_record<Header>(prefix);
    check(h.messageLength >= sizeof(Header) && h.messageLength <= max_client_frame,"Invalid frame length");
    auto bytes = prefix;
    const auto body = md::receive_exact(fd.get(),h.messageLength - sizeof(Header),until);
    bytes.insert(bytes.end(),body.begin(),body.end());
    check(valid_client_record(bytes,false),"Invalid server record");
    std::uint64_t timestamp{}; std::memcpy(&timestamp,bytes.data() + sizeof(Header),8);
    check(timestamp > 1'500'000'000'000'000'000ULL,"Timestamp is not Unix nanoseconds");
    return bytes;
  }
  template <typename T> T receive_as(MessageType type,int ms = 5000) {
    const auto bytes = receive(ms);
    const auto value = copy_record<T>(bytes);
    check(value.header.messageType == type,"Wrong message type");
    return value;
  }
  std::vector<OrderState> login(const std::string& name,const std::string& password = "pass",RequestId id = 1) {
    LoginRequest request{}; request.header = {MessageType::LoginRequest,sizeof(request),id};
    put_string(request.username,name); put_string(request.password,password); send(request);
    const auto accepted = receive_as<LoginAccepted>(MessageType::LoginAccepted);
    check(accepted.header.requestId == id,"Login request correlation");
    std::vector<OrderState> orders;
    for (;;) {
      const auto snapshot = receive_as<ActiveOrderSnapshot>(MessageType::ActiveOrderSnapshot);
      check(snapshot.header.requestId == id,"Snapshot correlation");
      if (snapshot.isEnd) {
        const OrderState zero{};
        check(snapshot.isEnd == 1 && std::memcmp(&zero,&snapshot.order,sizeof(zero)) == 0,"Snapshot terminal marker");
        return orders;
      }
      check(snapshot.order.status == OrderStatus::Accepted || snapshot.order.status == OrderStatus::PartiallyFilled,"Inactive snapshot record");
      orders.push_back(snapshot.order);
    }
  }
  static NewOrderRequest new_request(RequestId request,const std::string& symbol,Side side,Price price,Quantity quantity) {
    NewOrderRequest value{}; value.header = {MessageType::NewOrderRequest,sizeof(value),request};
    put_string(value.symbol,symbol); value.side = side; value.price = price; value.quantity = quantity; return value;
  }
  OrderUpdate order(RequestId id,const std::string& symbol,Side side,Price price,Quantity quantity) {
    send(new_request(id,symbol,side,price,quantity));
    const auto value = receive_as<OrderUpdate>(MessageType::OrderUpdate);
    check(value.header.requestId == id && value.order.status == OrderStatus::Accepted,"Acceptance missing or out of order");
    check(fixed_string(value.order.symbol) == symbol && value.order.side == side && value.order.price == price &&
        value.order.originalQuantity == quantity && value.order.remainingQuantity == quantity && value.order.filledQuantity == 0,"New order fields changed in transit");
    return value;
  }
  void send_cancel(RequestId request,OrderId id) { send(CancelRequest{{MessageType::CancelRequest,sizeof(CancelRequest),request},id}); }
  OrderUpdate cancel(RequestId request,OrderId id) {
    send_cancel(request,id);
    const auto value = receive_as<OrderUpdate>(MessageType::OrderUpdate);
    check(value.header.requestId == request && value.order.orderId == id && value.order.status == OrderStatus::Canceled && value.order.remainingQuantity == 0,"Cancel fields changed");
    return value;
  }
  void expect_reject(RequestId id,RejectReason reason) {
    const auto value = receive_as<RequestRejected>(MessageType::RequestRejected);
    check(value.header.requestId == id && value.reason == reason,"Wrong rejection");
  }
  void expect_closed(int timeout_ms = 3000) {
    pollfd p{fd.get(),POLLIN,0};
    check(poll(&p,1,timeout_ms) > 0,"Connection did not close in time");
    std::array<char,4096> bytes{};
    const auto until = deadline(timeout_ms);
    for (;;) {
      const auto n = recv(fd.get(),bytes.data(),bytes.size(),MSG_DONTWAIT);
      if (n == 0 || (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR)) return;
      check(md::Clock::now() < until,"Connection never reached EOF");
      if (n < 0) std::this_thread::sleep_for(2ms);
    }
  }
};

struct Fixture {
  std::filesystem::path directory;
  std::string binary;
  OrderConfig config;
  std::array<Process,2> shards;
  Process router,gateway;
  explicit Fixture(std::string executable,const std::filesystem::path& output,bool direct = true,bool trace = false)
      : directory(output), binary(std::move(executable)) {
    std::signal(SIGPIPE,SIG_IGN);
    std::filesystem::create_directories(directory);
    config.direct = direct;
    std::set<std::uint16_t> ports;
    auto port = [&] { for (;;) { const auto p = free_port(); if (ports.insert(p).second) return p; } };
    config.gateway_listen = {"127.0.0.1",port()}; config.router_listen = {"127.0.0.1",port()}; config.engine_endpoint = config.router_listen;
    config.shards = {{1,{"127.0.0.1",port()}},{2,{"127.0.0.1",port()}}};
    config.products = {{"AAPL",100,1},{"MSFT",200,2},{"EMPTY",100,1},{"FRACT",101,1}};
    config.accounts = {{"alice","pass"},{"bob","pass"},{"carol","pass"},{"probe","pass"}};
    for (unsigned i = 0; i < 96; ++i) config.accounts.push_back({"load" + std::to_string(i),"pass"});
    config.heartbeat_ms = 100; config.link_timeout_ms = 1800; config.request_timeout_ms = 5000;
    config.partial_timeout_ms = 500; config.reconnect_ms = 50;
    config.trace = trace;
    write_config();
    start_shard(0); start_shard(1);
    if (!direct) start_router();
    start_gateway(); wait_ready();
  }
  ~Fixture() { gateway.stop(); router.stop(); for (auto& shard : shards) shard.stop(); }
  void write_config() {
    std::ofstream out(directory / "exchange.conf");
    auto endpoint = [&](const std::string& key,const Address& a) { out << key << '=' << a.host << ',' << a.port << '\n'; };
    out << "routing=" << (config.direct ? "direct" : "router") << '\n';
    endpoint("gateway_listen",config.gateway_listen); endpoint("router_listen",config.router_listen); endpoint("engine_endpoint",config.engine_endpoint);
    for (const auto& a : config.accounts) out << "user=" << a.name << ',' << a.password << '\n';
    for (const auto& s : config.shards) out << "shard=" << s.id << ',' << s.address.host << ',' << s.address.port << '\n';
    for (const auto& p : config.products) out << "symbol=" << p.symbol << ',' << p.previous_close << ',' << p.shard << '\n';
    out << "heartbeat_ms=" << config.heartbeat_ms << "\nlink_timeout_ms=" << config.link_timeout_ms
        << "\nrequest_timeout_ms=" << config.request_timeout_ms << "\npartial_timeout_ms=" << config.partial_timeout_ms
        << "\nreconnect_ms=" << config.reconnect_ms << "\nmax_connections=" << config.max_connections
        << "\nmax_pending=" << config.max_pending << "\nmax_pending_per_client=" << config.max_pending_per_client
        << "\nmax_buffer_bytes=" << config.max_buffer_bytes << '\n';
  }
  std::vector<std::string> args(const std::string& role) {
    std::vector<std::string> a{role,"--config",(directory / "exchange.conf").string(),"--stats",(directory / (role + ".json")).string()};
    if (config.trace) a.push_back("--trace"); return a;
  }
  void start_shard(unsigned index,std::uint16_t market_port = 0,const std::string& mmap_path = "") {
    auto a = args("shard"); a[4] = (directory / ("shard" + std::to_string(index + 1) + ".json")).string();
    a.insert(a.end(),{"--shard",std::to_string(index + 1)});
    if (market_port) a.insert(a.end(),{"--market-port",std::to_string(market_port)});
    if (!mmap_path.empty()) a.insert(a.end(),{"--market-shm",mmap_path});
    shards[index].start(binary,a,directory / ("shard" + std::to_string(index + 1) + ".log"));
  }
  void start_router() { router.start(binary,args("router"),directory / "router.log"); }
  void start_gateway() { gateway.start(binary,args("gateway"),directory / "gateway.log"); }
  void wait_ready() {
    const auto until = deadline(10000);
    while (md::Clock::now() < until) {
      try { Client p(config.gateway_listen); p.login("probe"); return; }
      catch (const std::exception&) { std::this_thread::sleep_for(20ms); }
    }
    throw std::runtime_error("Fixture startup timed out: inspect " + directory.string());
  }
};
struct Cases {
  std::ofstream output;
  unsigned count = 0;
  explicit Cases(const std::filesystem::path& path) : output(path) { output << "case,runtime_ms,result\n"; }
  void run(const std::string& name,const std::function<void()>& fn) {
    const auto start = md::Clock::now();
    fn(); const auto time = elapsed_ms(start); ++count;
    std::cout << "CASE " << name << " runtime_ms=" << time << " PASS\n";
    output << name << ',' << time << ",PASS\n"; output.flush();
  }
};
}  // namespace marketlab::orders::test
