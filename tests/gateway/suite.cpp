#include "network.hpp"
using namespace demo;
namespace {
void sessions(const fs::path& bin,const fs::path& out) {
  Exchange ex(bin,out,32,2); Trader alice(ex.gateway),bob(ex.gateway);
  require(alice.login("user0").empty() && bob.login("user1").empty(),"Initial login not empty");
  auto order=alice.add(5,symbol(0));
  Trader bad(ex.gateway); bad.send(login_record("user0","wrong",5)); bad.rejected(5,ml::RejectReason::InvalidCredentials);
  require(alice.cancel(6,order.order.orderId).order.filledQuantity==0,"Bad password displaced valid session");
  order=alice.add(7,symbol(1)); Trader replacement(ex.gateway);
  auto recovered=replacement.login("user0"); alice.closed();
  require(recovered.size()==1 && recovered[0].orderId==order.order.orderId,"Session replacement lost active order");
  bob.send(cancel_record(8,order.order.orderId)); bob.rejected(8,ml::RejectReason::CancelFailed);
  replacement.socket.reset(); Trader reconnect(ex.gateway);
  require(reconnect.login("user0").size()==1,"Disconnect canceled a live order");
  reconnect.cancel(9,order.order.orderId); require(reconnect.login("user0",10).empty(),"Final snapshot not empty");
  bob.send(order_record(11,"MISSING",ml::Side::Buy,100,1)); bob.rejected(11,ml::RejectReason::InvalidOrder);
}
void framing(const fs::path& bin,const fs::path& out) {
  Exchange ex(bin,out,16,2); Trader client(ex.gateway); auto login=od::record_bytes(login_record("user0","demo",40));
  for(auto byte:login) { client.bytes({&byte,1}); std::this_thread::sleep_for(1ms); }
  require(client.get<ml::LoginAccepted>(ml::MessageType::LoginAccepted).header.requestId==40,"Fragmented login failed");
  require(client.snapshots(40).empty(),"Fragmented login snapshot failed");
  od::Bytes batch;
  for(unsigned i=0;i<64;++i) {
    auto bytes=od::record_bytes(order_record(100+i,symbol(0),ml::Side::Buy,90,1)); batch.insert(batch.end(),bytes.begin(),bytes.end());
  }
  client.bytes(batch); std::vector<ml::OrderId> orders;
  for(unsigned i=0;i<64;++i) {
    const auto r=client.get<ml::OrderUpdate>(ml::MessageType::OrderUpdate);
    require(r.header.requestId==100+i && r.order.status==ml::OrderStatus::Accepted,"Coalesced same-shard FIFO lost"); orders.push_back(ml::OrderId(r.order.orderId));
  }
  for(unsigned i=0;i<orders.size();++i) client.cancel(200+i,orders[i]);
  Trader malformed(ex.gateway); ml::Header invalid{ml::MessageType::LoginRequest,1,1}; malformed.send(invalid); malformed.closed();
  Trader healthy(ex.gateway); require(healthy.login("user1").empty(),"Malformed peer affected healthy users");
}
void backend_failure(const fs::path& bin,const fs::path& out) {
  Exchange ex(bin,out,16,2); Trader client(ex.gateway); client.login("user0");
  const auto survivor=client.add(10,symbol(1)); // shard 2 remains alive.
  ex.engines[0]->stop(); client.closed();
  ex.start_engine(0);
  eventually([&] { try { Trader probe(ex.gateway); probe.login("user39"); return true; } catch(...) { return false; } },"Backends did not reconnect",10000);
  Trader restored(ex.gateway); const auto active=restored.login("user0");
  require(active.size()==1 && active[0].orderId==survivor.order.orderId && active[0].remainingQuantity==10,"Surviving shard state missing or command replayed");
  restored.cancel(11,survivor.order.orderId);
}
}
int main(int argc,char** argv) {
  std::signal(SIGPIPE,SIG_IGN);
  try {
    require(argc==4,"Expected binary directory, result directory, scale"); fs::path bin=argv[1]; Suite suite(argv[2]); unsigned scale=std::stoul(argv[3]);
    suite.test("G01_authentication_takeover_ownership_and_relogin",[&]{sessions(bin,suite.output/"sessions");});
    suite.test("G02_fragmented_coalesced_and_malformed_TCP",[&]{framing(bin,suite.output/"framing");});
    suite.test("G03_backend_disconnect_and_surviving_state",[&]{backend_failure(bin,suite.output/"failure");});
    for(const auto& [products,shards]:{std::pair{64U,1U},std::pair{1000U,1U},std::pair{1000U,4U},std::pair{1000U,32U},std::pair{2000U,32U}}) {
      auto label=std::to_string(products)+"p_"+std::to_string(shards)+"s";
      suite.test("G04_checked_concurrent_pressure_"+label,[&] {
        Exchange ex(bin,suite.output/label,products,shards); order_load(suite,ex,"gateway_"+label,8,20*scale,16);
      });
    }
    return suite.finish();
  } catch(const std::exception& e) { std::cerr << "FATAL " << e.what() << '\n'; return 2; }
}
