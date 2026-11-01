#pragma once
#include "harness.hpp"
#include "shared_data/orders/engine_link.hpp"
#include "shared_data/orders/config.hpp"
#include "shared_data/market_data/transport.hpp"
#include <arpa/inet.h>
#include <fcntl.h>
#include <spawn.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>
#include <atomic>
#include <csignal>
#include <map>
#include <set>
#include <memory>

extern char** environ;
namespace demo {
namespace ml=marketlab; namespace od=ml::orders; namespace md=ml::md;
inline auto until(int ms=5000) { return Clock::now()+std::chrono::milliseconds(ms); }
inline std::uint16_t bound_port(int fd) {
  sockaddr_in value{}; socklen_t n=sizeof(value);
  require(getsockname(fd,reinterpret_cast<sockaddr*>(&value),&n)==0,"getsockname failed"); return ntohs(value.sin_port);
}
// Retain reservations until every deployment port has been allocated.
class Ports {
  std::vector<md::Fd> held_;
 public:
  std::uint16_t take() { held_.push_back(md::listen_tcp("127.0.0.1",0)); return bound_port(held_.back().get()); }
  void release() { held_.clear(); }
};
class Child {
  pid_t pid_=0;
 public:
  ~Child() { stop(); }
  Child()=default; Child(const Child&)=delete; Child& operator=(const Child&)=delete;
  void launch(const fs::path& executable,std::vector<std::string> args,const fs::path& log) {
    require(!pid_,"Child already started");
    args.insert(args.begin(),executable.string()); std::vector<char*> argv;
    for (auto& arg:args) argv.push_back(arg.data()); argv.push_back(nullptr);
    posix_spawn_file_actions_t actions; require(posix_spawn_file_actions_init(&actions)==0,"spawn actions failed");
    posix_spawn_file_actions_addopen(&actions,1,log.c_str(),O_WRONLY|O_CREAT|O_TRUNC,0600);
    posix_spawn_file_actions_adddup2(&actions,1,2);
    const int error=posix_spawn(&pid_,executable.c_str(),&actions,nullptr,argv.data(),environ);
    posix_spawn_file_actions_destroy(&actions);
    if (error) { pid_=0; throw std::runtime_error("Cannot start "+executable.string()+": "+std::strerror(error)); }
  }
  void stop() {
    if (!pid_) return;
    kill(pid_,SIGTERM); int status=0; auto end=until(3000);
    for (;;) {
      const auto result=waitpid(pid_,&status,WNOHANG);
      if (result==pid_ || (result<0 && errno==ECHILD)) break;
      if (Clock::now()>=end) { kill(pid_,SIGKILL); waitpid(pid_,&status,0); break; }
      std::this_thread::sleep_for(10ms);
    }
    pid_=0;
  }
};
inline ml::LoginRequest login_record(const std::string& user,const std::string& password="demo",ml::RequestId id=1) {
  ml::LoginRequest r{}; r.header={ml::MessageType::LoginRequest,sizeof(r),id};
  od::put_string(r.username,user); od::put_string(r.password,password); return r;
}
inline ml::NewOrderRequest order_record(ml::RequestId id,const std::string& symbol,ml::Side side,ml::Price price,ml::Quantity qty) {
  ml::NewOrderRequest r{}; r.header={ml::MessageType::NewOrderRequest,sizeof(r),id};
  od::put_string(r.symbol,symbol); r.side=side; r.price=price; r.quantity=qty; return r;
}
inline ml::CancelRequest cancel_record(ml::RequestId request,ml::OrderId order) {
  return {{ml::MessageType::CancelRequest,sizeof(ml::CancelRequest),request},order};
}
class Trader {
 public:
  md::Fd socket;
  explicit Trader(od::Address endpoint):socket(md::connect_tcp(endpoint.host,endpoint.port,until())) {}
  void bytes(std::span<const std::uint8_t> value) { md::send_all(socket.get(),value,until()); }
  template<class T> void send(const T& value) { bytes(od::record_bytes(value)); }
  od::Bytes receive() {
    const auto end=until(); auto buffer=md::receive_exact(socket.get(),sizeof(ml::Header),end);
    const auto h=od::copy_record<ml::Header>(buffer);
    require(h.messageLength>=sizeof(h) && h.messageLength<=od::max_client_frame,"Bad client frame length");
    auto tail=md::receive_exact(socket.get(),h.messageLength-sizeof(h),end); buffer.insert(buffer.end(),tail.begin(),tail.end());
    require(od::valid_client_record(buffer,false),"Bad response encoding");
    ml::Timestamp timestamp{}; std::memcpy(&timestamp,buffer.data()+sizeof(h),8);
    require(timestamp>1'600'000'000'000'000'000ULL,"Expected Unix nanoseconds"); return buffer;
  }
  template<class T> T get(ml::MessageType type) {
    auto record=od::copy_record<T>(receive()); require(record.header.messageType==type,"Unexpected response type"); return record;
  }
  std::vector<ml::OrderState> snapshots(ml::RequestId id) {
    std::vector<ml::OrderState> records; std::set<ml::OrderId> seen;
    for (;;) {
      const auto r=get<ml::ActiveOrderSnapshot>(ml::MessageType::ActiveOrderSnapshot);
      require(r.header.requestId==id,"Snapshot request mismatch");
      if (r.isEnd) { const ml::OrderState zero{}; require(r.isEnd==1 && std::memcmp(&zero,&r.order,sizeof(zero))==0,"Bad terminal snapshot"); break; }
      require(r.order.remainingQuantity>0 && (r.order.status==ml::OrderStatus::Accepted || r.order.status==ml::OrderStatus::PartiallyFilled),"Inactive snapshot order");
      require(seen.insert(ml::OrderId(r.order.orderId)).second,"Duplicate snapshot order"); records.push_back(r.order);
    }
    return records;
  }
  std::vector<ml::OrderState> login(const std::string& user,ml::RequestId id=1) {
    send(login_record(user,"demo",id));
    require(get<ml::LoginAccepted>(ml::MessageType::LoginAccepted).header.requestId==id,"Login correlation failed"); return snapshots(id);
  }
  ml::OrderUpdate add(ml::RequestId id,const std::string& symbol,ml::Side side=ml::Side::Buy,int price=100,int qty=10) {
    send(order_record(id,symbol,side,price,qty)); auto r=get<ml::OrderUpdate>(ml::MessageType::OrderUpdate);
    require(r.header.requestId==id && r.order.status==ml::OrderStatus::Accepted,"Acceptance/correlation failed");
    require(od::fixed_string(r.order.symbol)==symbol && r.order.side==side && r.order.price==price &&
      r.order.originalQuantity==qty && r.order.remainingQuantity==qty && r.order.filledQuantity==0,"Accepted state corrupted"); return r;
  }
  ml::OrderUpdate cancel(ml::RequestId id,ml::OrderId order) {
    send(cancel_record(id,order)); auto r=get<ml::OrderUpdate>(ml::MessageType::OrderUpdate);
    require(r.header.requestId==id && r.order.orderId==order && r.order.status==ml::OrderStatus::Canceled && r.order.remainingQuantity==0,"Cancel mismatch"); return r;
  }
  void rejected(ml::RequestId id,ml::RejectReason reason) {
    auto r=get<ml::RequestRejected>(ml::MessageType::RequestRejected); require(r.header.requestId==id && r.reason==reason,"Rejection mismatch");
  }
  void closed() {
    eventually([&] { std::uint8_t b{}; const auto n=recv(socket.get(),&b,1,MSG_DONTWAIT);
      require(n<=0,"Unexpected data before connection close");
      return n==0 || (n<0 && errno!=EAGAIN && errno!=EWOULDBLOCK && errno!=EINTR);
    },"Session did not close",6000);
  }
};
inline std::string symbol(unsigned index) { return "DEMO"+std::to_string(index+1); }
struct Exchange {
  fs::path bin,dir; od::Address gateway; std::vector<od::Address> orders,markets;
  std::vector<std::unique_ptr<Child>> engines; Child front;
  unsigned products,shards;
  Exchange(fs::path binaries,fs::path directory,unsigned count,unsigned shard_count,bool start_gateway=true)
    :bin(std::move(binaries)),dir(std::move(directory)),products(count),shards(shard_count) {
    require(count && count<=2000 && shard_count && shard_count<=32 && shard_count<=count,"Demo capacity out of range");
    fs::create_directories(dir); Ports ports; gateway={"127.0.0.1",ports.take()};
    for(unsigned s=0;s<shards;++s) { orders.push_back({"127.0.0.1",ports.take()}); markets.push_back({"127.0.0.1",ports.take()}); }
    std::ofstream config(dir/"exchange.conf"); config << "gateway_listen=127.0.0.1," << gateway.port << '\n';
    for(unsigned u=0;u<40;++u) config << "user=user" << u << ",demo\n";
    for(unsigned s=0;s<shards;++s) config << "shard=" << s+1 << ",127.0.0.1," << orders[s].port << '\n';
    for(unsigned p=0;p<products;++p) config << "symbol=" << symbol(p) << ",100," << p%shards+1 << '\n';
    config.close(); ports.release();
    for(unsigned s=0;s<shards;++s) { engines.push_back(std::make_unique<Child>()); start_engine(s); }
    if(start_gateway) {
      front.launch(bin/"market_gateway",{"gateway","--config",(dir/"exchange.conf").string()},dir/"gateway.log");
      eventually([&] { try { Trader probe(gateway); probe.login("user39"); return true; } catch (...) { return false; } },"Gateway readiness timeout: "+dir.string(),12000);
    }
  }
  ~Exchange() { front.stop(); for(auto& e:engines) e->stop(); }
  void start_engine(unsigned s) {
    engines[s]->launch(bin/"market_matching_engine",{"--config",(dir/"exchange.conf").string(),"--shard",std::to_string(s+1),"--market-port",std::to_string(markets[s].port)},dir/("engine-"+std::to_string(s+1)+".log"));
  }
  md::Config market_config() const {
    md::Config c; Ports ports; c.query={"127.0.0.1",ports.take()};
    c.multicast=md::MulticastConfig{{"239.10.20.30",ports.take()},"127.0.0.1",1,true};
    for(unsigned s=0;s<shards;++s) c.shards.push_back({"source"+std::to_string(s),"tcp","127.0.0.1",markets[s].port,(dir/("unused-"+std::to_string(s))).string(),{}});
    for(unsigned p=0;p<products;++p) { c.symbols.emplace(p+1,symbol(p)); c.shards[p%shards].symbols.push_back(p+1); }
    return c;
  }
};
inline void write_market(const md::Config& c,const fs::path& path) {
  std::ofstream out(path); out << "symbols="; bool first=true;
  for(const auto& [id,name]:c.symbols) { out << (first?"":",") << id << ':' << name; first=false; } out << '\n';
  for(const auto& s:c.shards) {
    out << "shard=" << s.name << ',' << s.transport << ',' << s.host << ',' << s.port << ',' << s.mmap_path;
    for(auto id:s.symbols) out << ',' << id; out << '\n';
  }
  out << "query_host=" << c.query.host << "\nquery_port=" << c.query.port << "\nmulticast_group=" << c.multicast->group.host << ',' << c.multicast->group.port
      << "\nmulticast_interface=127.0.0.1\nsnapshot_every=" << c.snapshot_every << '\n';
  require(out.good(),"Cannot write market config");
}
inline md::Message snapshot(const md::Endpoint& address,std::uint16_t id,md::Fd* reuse=nullptr) {
  md::Fd local; if (!reuse) { local=md::connect_tcp(address.host,address.port,until()); reuse=&local; }
  md::Bytes request; md::append_uint(request,id,2); md::send_all(reuse->get(),request,until());
  auto prefix=md::receive_exact(reuse->get(),2,until()); std::size_t offset=0; auto n=md::read_uint(prefix,offset,2);
  require(n>=md::header_size && n<=md::max_snapshot_size,"Invalid snapshot size");
  auto m=md::decode(md::receive_exact(reuse->get(),n,until()));
  require(m.kind==md::Kind::Snapshot && m.symbol_id==id,"Snapshot identity mismatch"); return m;
}
inline void publisher_ready(const md::Config& c) {
  eventually([&] { try { snapshot(c.query,1); return true; } catch (...) { return false; } },"Publisher readiness timeout",12000);
}
inline md::Message datagram(int fd,int timeout_ms=3000) {
  pollfd p{fd,POLLIN,0}; require(poll(&p,1,timeout_ms)>0,"UDP receive timeout");
  std::array<std::uint8_t,md::max_delta_size+1> buffer{}; auto n=recv(fd,buffer.data(),buffer.size(),0);
  require(n>0,"UDP receive failed"); return md::decode({buffer.data(),static_cast<std::size_t>(n)});
}
inline md::Book apply(md::Book book,const md::Message& m) {
  if(m.kind==md::Kind::Snapshot) return {m.symbol_id,m.levels};
  for(const auto& x:m.levels) {
    std::erase_if(book.levels,[&](const auto& y){return x.side==y.side && x.price==y.price;}); if(x.quantity) book.levels.push_back(x);
  }
  std::sort(book.levels.begin(),book.levels.end(),[](const auto& a,const auto& b){return a.side!=b.side?a.side<b.side:(a.side==md::Side::Buy?a.price>b.price:a.price<b.price);});
  md::validate_book(book); return book;
}
// Bounded, closed-loop load. Every timed request is checked before counting it.
inline void order_load(Suite& suite,Exchange& ex,const std::string& name,unsigned users,unsigned rounds,unsigned window) {
  std::vector<std::vector<double>> samples(users); std::uint64_t total=std::uint64_t(users)*rounds*window*2;
  std::atomic<unsigned> ready{0}; std::atomic_bool go{false};
  auto started=Clock::now();
  parallel(users,[&](unsigned u) {
    Trader client(ex.gateway); require(client.login("user"+std::to_string(u)).empty(),"Load must start with empty user orders");
    ++ready;
    eventually([&]{return go.load() || ready.load()==users;},"Load startup barrier timeout");
    // The first fully ready worker releases all workers; no worker can block others after an error.
    go.store(true);
    std::set<ml::OrderId> ids; ml::RequestId sequence=10;
    for(unsigned round=0;round<rounds;++round) {
      std::vector<Clock::time_point> sent(window); std::vector<ml::NewOrderRequest> requests;
      for(unsigned j=0;j<window;++j) {
        unsigned p=(u*131+round*window+j)%ex.products;
        requests.push_back(order_record(sequence++,symbol(p),ml::Side::Buy,90,7)); sent[j]=Clock::now(); client.send(requests.back());
      }
      const auto new_base=sequence-window;
      std::vector<bool> seen(window,false); std::vector<ml::OrderId> batch(window);
      std::map<unsigned,ml::RequestId> last_per_shard;
      for(unsigned j=0;j<window;++j) {
        auto r=client.get<ml::OrderUpdate>(ml::MessageType::OrderUpdate);
        require(r.header.requestId>=new_base && r.header.requestId<sequence,"Unknown load request ID");
        const unsigned slot=static_cast<unsigned>(r.header.requestId-new_base);
        require(!seen[slot],"Duplicate load response"); seen[slot]=true; samples[u].push_back(us(sent[slot]));
        const auto& expected=requests[slot]; const ml::OrderId id=r.order.orderId;
        const unsigned p=(u*131+round*window+slot)%ex.products;
        require(r.header.requestId==expected.header.requestId && od::fixed_string(r.order.symbol)==symbol(p) &&
          r.order.status==ml::OrderStatus::Accepted && r.order.price==90 && r.order.side==ml::Side::Buy &&
          r.order.originalQuantity==7 && r.order.remainingQuantity==7 && r.order.filledQuantity==0 &&
          (id>>48)==p%ex.shards+1 && ids.insert(id).second,"Load accepted response mismatch/duplicate"); batch[slot]=id;
        const unsigned shard=p%ex.shards;
        require(last_per_shard[shard]<r.header.requestId,"Per-shard FIFO response order violated"); last_per_shard[shard]=r.header.requestId;
      }
      const auto cancel_base=sequence;
      for(unsigned j=0;j<window;++j) { sent[j]=Clock::now(); client.send(cancel_record(sequence++,batch[j])); }
      std::fill(seen.begin(),seen.end(),false);
      for(unsigned j=0;j<window;++j) {
        auto r=client.get<ml::OrderUpdate>(ml::MessageType::OrderUpdate);
        require(r.header.requestId>=cancel_base && r.header.requestId<sequence,"Unknown cancel response");
        const unsigned slot=static_cast<unsigned>(r.header.requestId-cancel_base);
        require(!seen[slot],"Duplicate cancel response"); seen[slot]=true; samples[u].push_back(us(sent[slot]));
        require(r.order.orderId==batch[slot] && r.order.status==ml::OrderStatus::Canceled &&
          r.order.originalQuantity==7 && r.order.remainingQuantity==0 && r.order.filledQuantity==0,"Load canceled response mismatch");
      }
    }
    require(client.login("user"+std::to_string(u),sequence+1).empty(),"Load leaked active orders");
  });
  const double seconds=us(started)/1e6; std::vector<double> combined;
  for(auto& values:samples) combined.insert(combined.end(),values.begin(),values.end());
  require(combined.size()==total,"Missing timed replies");
  suite.metric(name,std::move(combined),seconds,total,"Request send to validated reply, including window/client queues; rate includes login and final reconciliation; no Publisher unless integration suite");
}
} // namespace demo
