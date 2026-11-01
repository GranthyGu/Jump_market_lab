#include "network.hpp"
#include "client/recovery.hpp"
using namespace demo;
namespace {
void check_public(const md::Config& config,unsigned products,unsigned quantity) {
  // Two advances ensure the chosen sampling round started after this call,
  // even if the first observed round was already in flight during trading.
  const auto baseline=snapshot(config.query,1);
  eventually([&] { try { auto m=snapshot(config.query,1); return m.epoch==baseline.epoch && m.index>=baseline.index+2; }
    catch(...) { return false; }
  },"Publisher did not complete a post-trade sampling round",6000);
  eventually([&] {
    try { auto m=snapshot(config.query,1); return quantity?m.levels.size()==1 && m.levels[0].quantity==quantity:m.levels.empty(); }
    catch(...) { return false; }
  },"Publisher did not catch up with matching state",5000);
  auto fd=md::connect_tcp(config.query.host,config.query.port,until());
  for(unsigned id=1;id<=products;++id) {
    auto m=snapshot(config.query,id,&fd);
    if(!quantity) require(m.levels.empty(),"Canceled order still public");
    else require(m.levels.size()==1 && m.levels[0].side==md::Side::Buy && m.levels[0].price==100 &&
      m.levels[0].quantity==quantity && m.levels[0].num_orders==1,"Public book disagrees with private order state");
  }
}
void fill_roundtrip(Suite& suite,const fs::path& bin,unsigned products,unsigned shards,unsigned scale) {
  auto label=std::to_string(products)+"p_"+std::to_string(shards)+"s"; auto dir=suite.output/label;
  Exchange ex(bin,dir,products,shards); auto config=ex.market_config(); write_market(config,dir/"publisher.conf");
  md::Fd udp=md::join_multicast(*config.multicast);
  std::atomic<unsigned> packets{0}; std::exception_ptr capture_error;
  auto capture=std::make_unique<md::StopThread>([&](const auto& stop) {
    try { while(!stop.load()) {
      pollfd p{udp.get(),POLLIN,0}; if(poll(&p,1,20)<=0) continue;
      auto m=datagram(udp.get()); require(m.symbol_id>=1 && m.symbol_id<=products,"Unknown public symbol"); ++packets;
    }} catch(...) { capture_error=std::current_exception(); }
  });
  Child publisher; publisher.launch(bin/"market_broadcast",{"publish","--config",(dir/"publisher.conf").string()},dir/"publisher.log"); publisher_ready(config);
  Trader alice(ex.gateway),bob(ex.gateway); alice.login("user32"); bob.login("user33");
  std::vector<ml::OrderId> ids; std::set<ml::OrderId> unique; std::vector<double> acceptance,fill_latency;
  auto phase=Clock::now();
  for(unsigned p=0;p<products;++p) {
    auto tick=Clock::now(); auto order=alice.add(100+p,symbol(p)); acceptance.push_back(us(tick)); const ml::OrderId id=order.order.orderId;
    require((id>>48)==p%shards+1 && unique.insert(id).second,"Wrong route/global order ID collision"); ids.push_back(id);
  }
  suite.metric("e2e_accept_"+label,std::move(acceptance),us(phase)/1e6,products,"Serial trading client send to Accepted through real Gateway and Engine while Publisher is active");
  check_public(config,products,10);
  std::map<ml::OrderId,std::pair<ml::TradeId,ml::Timestamp>> expected_trades;
  phase=Clock::now();
  for(unsigned p=0;p<products;++p) {
    auto tick=Clock::now(); const auto accepted=bob.add(5000+p,symbol(p),ml::Side::Sell,100,6);
    auto t=bob.get<ml::Trade>(ml::MessageType::Trade); auto status=bob.get<ml::OrderUpdate>(ml::MessageType::OrderUpdate); fill_latency.push_back(us(tick));
    require(unique.insert(ml::OrderId(accepted.order.orderId)).second && t.orderId==accepted.order.orderId && t.side==ml::Side::Sell &&
      t.price==100 && t.quantity==6 && t.header.requestId==0 && status.header.requestId==0 && status.order.orderId==t.orderId &&
      status.order.originalQuantity==6 && status.order.filledQuantity==6 && status.order.remainingQuantity==0 &&
      status.order.status==ml::OrderStatus::Filled && status.timestamp==t.timestamp,"Aggressor acceptance/fill notification mismatch");
    expected_trades.emplace(ids[p],std::pair{ml::TradeId(t.tradeId),ml::Timestamp(t.timestamp)});
  }
  suite.metric("e2e_full_fill_"+label,std::move(fill_latency),us(phase)/1e6,products,"Serial aggressor send through Accepted, Trade and Filled; receiver validates full fill; Publisher active");
  std::set<ml::OrderId> seen_trades,seen_status;
  for(unsigned n=0;n<2*products;++n) {
    auto bytes=alice.receive(); auto h=od::copy_record<ml::Header>(std::span(bytes).first(sizeof(ml::Header)));
    require(!h.requestId,"Resting participant update must be unsolicited");
    if(h.messageType==ml::MessageType::Trade) {
      auto t=od::copy_record<ml::Trade>(bytes); const auto found=expected_trades.find(ml::OrderId(t.orderId));
      require(found!=expected_trades.end() && t.tradeId==found->second.first && t.timestamp==found->second.second && t.side==ml::Side::Buy &&
        t.quantity==6 && t.price==100 && seen_trades.insert(ml::OrderId(t.orderId)).second,"Resting trade missing, duplicate or mismatched");
    } else {
      require(h.messageType==ml::MessageType::OrderUpdate,"Unexpected resting event"); auto s=od::copy_record<ml::OrderUpdate>(bytes);
      require(expected_trades.contains(ml::OrderId(s.order.orderId)) && seen_status.insert(ml::OrderId(s.order.orderId)).second &&
        s.order.status==ml::OrderStatus::PartiallyFilled && s.order.filledQuantity==6 && s.order.remainingQuantity==4 &&
        s.order.originalQuantity==10,"Resting partial status mismatch");
    }
  }
  require(seen_status.size()==products && seen_trades.size()==products,"Missing resting participant events");
  auto active=alice.login("user32",10000); require(active.size()==products,"Cross-shard login snapshot incomplete");
  for(const auto& s:active) require(expected_trades.contains(ml::OrderId(s.orderId)) && s.remainingQuantity==4 && s.filledQuantity==6,"Login snapshot disagrees with fills");
  check_public(config,products,4);
  for(unsigned p=0;p<products;++p) require(alice.cancel(20000+p,ids[p]).order.filledQuantity==6,"Cancel lost previous fills");
  require(alice.login("user32",30000).empty() && bob.login("user33",30000).empty(),"Final active snapshot not empty");
  check_public(config,products,0);
  // Parallel load while Publisher sampling, UDP reception and TCP queries coexist.
  std::atomic_bool query_done{false}; std::atomic<unsigned> queries{0}; std::exception_ptr query_error;
  auto query_worker=std::make_unique<md::StopThread>([&](const auto& stop) {
    try { auto fd=md::connect_tcp(config.query.host,config.query.port,until());
      while(!query_done.load() && !stop.load()) { auto m=snapshot(config.query,1,&fd); md::validate_book({1,m.levels}); ++queries; std::this_thread::sleep_for(3ms); }
    } catch(...) { query_error=std::current_exception(); }
  });
  order_load(suite,ex,"e2e_concurrent_"+label,8,20*scale,16);
  query_done=true; query_worker.reset(); if(query_error) std::rethrow_exception(query_error);
  // Prove final authoritative Engine state, independent of sample timing.
  for(unsigned s=0;s<shards;++s) {
    auto source=md::make_source(config.shards[s],1000); auto view=source->read(until());
    require(view.books.size()==config.shards[s].symbols.size(),"Incomplete Engine source view");
    for(const auto& [id,b]:view.books) { (void)id; require(b.levels.empty(),"Pressure left a resting order"); }
  }
  publisher.stop(); capture.reset(); if(capture_error) std::rethrow_exception(capture_error);
  require(packets>0 && queries>0,"No simultaneous UDP or TCP activity");
  std::cout << "CHECK real_engines=" << shards << " products=" << products << " verified_fills=" << products
            << " concurrent_queries=" << queries << " udp_packets=" << packets << " final_active_orders=0\n";
}
void recovery(const fs::path& bin,const fs::path& dir) {
  Exchange ex(bin,dir,16,2); auto config=ex.market_config(); write_market(config,dir/"publisher.conf");
  // Relay the real feed to a second multicast group and deliberately lose batch 2.
  auto incoming=md::join_multicast(*config.multicast); auto observer_config=config;
  observer_config.multicast->group.host="239.10.20.32"; write_market(observer_config,dir/"observer.conf");
  md::UdpSender relay(*observer_config.multicast); std::atomic<unsigned> dropped{0};
  auto proxy=std::make_unique<md::StopThread>([&](const auto& stop) {
    while(!stop.load()) {
      pollfd p{incoming.get(),POLLIN,0}; if(poll(&p,1,20)<=0) continue;
      try { auto m=datagram(incoming.get()); if(m.symbol_id==1 && m.index==2) { ++dropped; continue; } relay.send(md::encode(m)); }
      catch(const std::exception&) { /* Main thread detects missing progress below. */ }
    }
  });
  Child observer,publisher;
  observer.launch(bin/"market_broadcast",{"observe","--config",(dir/"observer.conf").string()},dir/"observer.log");
  publisher.launch(bin/"market_broadcast",{"publish","--config",(dir/"publisher.conf").string()},dir/"publisher.log");
  publisher_ready(config);
  eventually([&]{return read_text(dir/"observer.log").find("BOOK symbol=1 index=0 ")!=std::string::npos;},"Observer did not establish TCP baseline");
  Trader alice(ex.gateway); alice.login("user0"); auto accepted=alice.add(10,symbol(0));
  eventually([&]{return dropped.load()==1;},"Loss relay did not drop the selected genuine batch",5000);
  const auto cut=read_text(dir/"observer.log").size();
  eventually([&] {
    auto log=read_text(dir/"observer.log");
    return log.find("BOOK symbol=1 index=2 ")!=std::string::npos || log.find("BOOK symbol=1 index=3 ")!=std::string::npos;
  },"Observer failed to recover the lost real batch",6000);
  auto log=read_text(dir/"observer.log");
  require(log.find("event=TCP_START symbol=1",cut)!=std::string::npos || log.find("event=TCP_COMPLETE symbol=1",cut)!=std::string::npos,"No TCP recovery observed after UDP loss");
  require(log.find("quantity=10 numOfOrders=1")!=std::string::npos,"Recovered public book differs from real order");
  auto before=snapshot(config.query,1); publisher.stop();
  eventually([&]{return read_text(dir/"observer.log").find("symbol=1 state=STALE")!=std::string::npos;},"Silent feed did not become STALE",6000);
  publisher.launch(bin/"market_broadcast",{"publish","--config",(dir/"publisher.conf").string()},dir/"publisher-restarted.log"); publisher_ready(config);
  auto after=snapshot(config.query,1); require(before.epoch!=after.epoch,"Restart did not change Publisher epoch");
  eventually([&]{return read_text(dir/"observer.log").find("epoch="+md::epoch_text(after.epoch))!=std::string::npos;},"Observer did not confirm the new epoch",6000);
  alice.cancel(11,accepted.order.orderId); check_public(config,16,0);
  observer.stop(); publisher.stop(); proxy.reset();
  std::cout << "CHECK intentional_udp_loss=1 automatic_tcp_recovery=PASS silence=STALE restart_epoch=PASS\n";
}
void recovery_boundaries() {
  md::RecoveryConfig config; auto now=Clock::now(); md::MarketRecovery state({{1,"one"}},config,now);
  md::Epoch epoch{}; epoch[0]=1;
  auto message=[&](md::Kind kind,unsigned index) { return md::Message{kind,epoch,1,index,1000+index,{{md::Side::Buy,100,10+index,1}}}; };
  auto request=state.next_request(now); require(request.has_value(),"No initial recovery query"); state.complete(*request,message(md::Kind::Snapshot,0),now);
  state.udp(message(md::Kind::Delta,2),now+1ms);
  require(state.view(1).status==md::SyncStatus::GapWait && !state.next_request(now+100ms),"Reorder wait bypassed");
  auto obsolete=state.next_request(now+201ms); require(obsolete.has_value(),"Gap did not trigger recovery");
  state.udp(message(md::Kind::Delta,1),now+202ms);
  require(!state.valid(*obsolete) && state.view(1).snapshot->index==2,"UDP did not invalidate obsolete TCP task");
  state.complete(*obsolete,message(md::Kind::Snapshot,99),now+203ms);
  require(state.view(1).snapshot->index==2,"Obsolete TCP rolled forward state");
  for(unsigned i=4;i<=69;++i) state.udp(message(md::Kind::Delta,i),now+204ms);
  require(state.buffered_bytes()==0 && state.view(1).highest_seen==69 && state.view(1).status!=md::SyncStatus::Synced,"Overflow lost high watermark or falsely synchronized");
}
void snapshot_recovery() {
  md::RecoveryConfig config; auto now=Clock::now(); md::MarketRecovery state({{1,"one"}},config,now);
  md::Epoch epoch{}; epoch[0]=1;
  auto message=[&](md::Kind kind,unsigned index) {
    return md::Message{kind,epoch,1,index,1000+index,{{md::Side::Buy,100,10+index,1}}};
  };
  auto request=state.next_request(now); require(request.has_value(),"No initial snapshot query");
  state.complete(*request,message(md::Kind::Snapshot,0),now);
  state.udp(message(md::Kind::Delta,2),now+1ms); // Missing batch 1.
  auto obsolete=state.next_request(now+201ms); require(obsolete.has_value(),"Gap did not trigger recovery");
  state.udp(message(md::Kind::Snapshot,3),now+202ms); // An adaptive snapshot bridges the gap.
  require(state.view(1).snapshot->index==3 && state.view(1).status==md::SyncStatus::Synced &&
      state.buffered_bytes()==0 && !state.valid(*obsolete),"Adaptive snapshot failed to replace recovery baseline");
  state.complete(*obsolete,message(md::Kind::Snapshot,99),now+203ms);
  require(state.view(1).snapshot->index==3,"Obsolete TCP response replaced adaptive snapshot");
  state.udp(message(md::Kind::Delta,4),now+204ms);
  state.udp(message(md::Kind::Delta,6),now+205ms); // Buffer a delta after the next snapshot.
  state.udp(message(md::Kind::Snapshot,5),now+206ms);
  require(state.view(1).snapshot->index==6 && state.view(1).snapshot->levels==message(md::Kind::Snapshot,6).levels &&
      state.view(1).status==md::SyncStatus::Synced && state.buffered_bytes()==0,"Snapshot did not drain following delta");
  state.udp(message(md::Kind::Snapshot,10),now+207ms); // No same-batch delta accompanies the periodic snapshot.
  require(state.view(1).snapshot->index==10 && state.view(1).snapshot->levels==message(md::Kind::Snapshot,10).levels,
      "Periodic snapshot without a delta failed");
  state.udp(message(md::Kind::Delta,11),now+208ms);
  auto unchanged=message(md::Kind::Delta,12); unchanged.levels.clear();
  state.udp(unchanged,now+209ms);
  require(state.view(1).snapshot->index==12 && state.view(1).snapshot->timestamp==unchanged.timestamp &&
      state.view(1).snapshot->levels==message(md::Kind::Snapshot,11).levels && state.view(1).status==md::SyncStatus::Synced,
      "Delta/empty heartbeat after a snapshot failed");
}
}
int main(int argc,char** argv) {
  std::signal(SIGPIPE,SIG_IGN);
  try {
    require(argc==4,"Expected binary directory, result directory, scale"); fs::path bin=argv[1]; Suite suite(argv[2]); unsigned scale=std::stoul(argv[3]);
    suite.test("I01_recovery_races_and_bounded_buffer",recovery_boundaries);
    suite.test("I02_real_exchange_64products_2shards",[&]{fill_roundtrip(suite,bin,64,2,scale);});
    suite.test("I03_real_exchange_1000products_32shards",[&]{fill_roundtrip(suite,bin,1000,32,scale);});
    suite.test("I04_real_UDP_loss_TCP_recovery_silence_and_restart",[&]{recovery(bin,suite.output/"recovery");});
    suite.test("I05_adaptive_and_periodic_snapshot_recovery",snapshot_recovery);
    return suite.finish();
  } catch(const std::exception& e) { std::cerr << "FATAL " << e.what() << '\n'; return 2; }
}
