#include "sources.hpp"
#include <random>
using namespace demo;
namespace {
void protocol_and_difference() {
  std::mt19937 rng(1101);
  for(unsigned i=0;i<12000;++i) {
    auto before=source_book(2,i),after=source_book(2,i+1);
    if(i%3==0) after.levels.clear();
    if(i%7==0) before.levels.clear();
    if(!after.levels.empty()) after.levels[rng()%after.levels.size()].num_orders+=1;
    md::Message delta{md::Kind::Delta,{},2,i+1,md::unix_ns(),md::difference(before,after)};
    auto wire=md::encode(delta); auto decoded=md::decode(wire);
    require(wire.size()==35+21*delta.levels.size() && wire.size()<=455,"Market packet layout/size changed");
    require(apply(before,decoded)==after,"Delta reconstruction failed");
  }
  for(auto bytes:{md::Bytes(34,0),md::Bytes(36,0),md::Bytes(456,0)}) {
    bool rejected=false; try { md::decode(bytes); } catch(...) { rejected=true; } require(rejected,"Malformed UDP packet accepted");
  }
}
struct Observation {
  std::map<std::pair<std::uint64_t,std::uint16_t>,md::Bytes> packets;
  std::map<std::uint16_t,md::Book> books; std::map<std::uint16_t,std::uint64_t> applied;
  std::map<std::uint64_t,std::uint64_t> timestamps;
  std::vector<double> ages; md::Epoch epoch{}; bool initialized=false;
};
void publication(Suite& suite,const fs::path& bin,unsigned products,unsigned shards,unsigned last,unsigned scale) {
  auto name=std::to_string(products)+"p_"+std::to_string(shards)+"s"; auto dir=suite.output/name;
  Sources sources(dir,products,shards); auto& c=sources.config; write_market(c,dir/"publisher.conf");
  constexpr unsigned receivers=3;
  std::vector<md::Fd> sockets; for(unsigned r=0;r<receivers;++r) {
    sockets.push_back(md::join_multicast(*c.multicast)); int size=4*1024*1024;
    setsockopt(sockets.back().get(),SOL_SOCKET,SO_RCVBUF,&size,sizeof(size));
  }
  std::vector<Observation> observations(receivers); std::mutex error_mutex; std::exception_ptr error;
  std::vector<std::unique_ptr<md::StopThread>> collectors;
  for(unsigned r=0;r<receivers;++r) collectors.push_back(std::make_unique<md::StopThread>([&,r](const auto& stop) {
    try { while(!stop.load()) {
      pollfd p{sockets[r].get(),POLLIN,0}; if(poll(&p,1,20)<=0) continue;
      auto m=datagram(sockets[r].get()); if(m.index>last) continue;
      auto& obs=observations[r];
      if(!obs.initialized) { obs.epoch=m.epoch; obs.initialized=true; }
      require(m.epoch==obs.epoch && m.symbol_id>=1 && m.symbol_id<=products,"UDP source identity/epoch changed");
      auto [stamp,inserted]=obs.timestamps.emplace(m.index,m.timestamp);
      require(inserted || stamp->second==m.timestamp,"Batch timestamps differ between symbols");
      auto key=std::pair{m.index,m.symbol_id};
      const auto wire=md::encode(m);
      require(obs.packets.emplace(key,wire).second,"Multiple UDP records for one symbol/batch");
      const auto expected_book=source_book(m.symbol_id,m.index);
      const auto shape=m.symbol_id%6;
      const bool forced=m.index==0 || m.index%c.snapshot_every==0;
      // Derive expected types/counts from fixture scenarios, not production difference().
      const bool use_snapshot=forced || shape==2 || (shape==5 && m.index%2==1);
      require(m.kind==(use_snapshot?md::Kind::Snapshot:md::Kind::Delta),"Wrong adaptive/periodic message type");
      const auto entries=use_snapshot?expected_book.levels.size():
          (shape==0 || shape==1?0U:shape==4?1U:10U);
      require(wire.size()==35+21*entries,"Wrong adaptive message size");
      if(!obs.books.contains(m.symbol_id)) {
        require(m.index==0 && m.kind==md::Kind::Snapshot,"Initial UDP snapshot missing");
        obs.books[m.symbol_id]={m.symbol_id,m.levels};
      } else {
        require(obs.applied.at(m.symbol_id)+1==m.index,"Missing/reordered UDP batch in local fanout test");
        obs.books[m.symbol_id]=apply(obs.books.at(m.symbol_id),m);
      }
      obs.applied[m.symbol_id]=m.index;
      require(obs.books.at(m.symbol_id)==expected_book,"UDP update reconstructed wrong book");
      const auto now=md::unix_ns(); require(now>=m.timestamp,"Wall clock moved backwards"); obs.ages.push_back(double(now-m.timestamp)/1000);
    }} catch(...) { std::lock_guard lock(error_mutex); if(!error) error=std::current_exception(); }
  }));
  Child publisher; publisher.launch(bin/"market_broadcast",{"publish","--config",(dir/"publisher.conf").string()},dir/"publisher.log");
  publisher_ready(c);
  std::vector<std::vector<double>> times(8); auto load_start=Clock::now();
  parallel(8,[&](unsigned worker) {
    auto persistent=md::connect_tcp(c.query.host,c.query.port,until());
    const auto end=Clock::now()+std::chrono::seconds(3*scale);
    unsigned sequence=0;
    do {
      const auto id=static_cast<std::uint16_t>(1+(worker*137+sequence++)%products); auto tick=Clock::now();
      auto m=snapshot(c.query,id,worker<4?&persistent:nullptr); times[worker].push_back(us(tick));
      require(m.levels==source_book(id,m.index).levels,"Concurrent TCP snapshot does not match frozen batch");
      // Short-connection pressure is deliberately paced: repeated live demos
      // must not exhaust the host's ephemeral ports in TIME_WAIT.
      std::this_thread::sleep_for(worker<4?2ms:50ms);
    } while(Clock::now()<end);
  });
  const double seconds=us(load_start)/1e6;
  eventually([&] { return read_text(dir/"publisher.log").find("Published index="+std::to_string(last)+" ")!=std::string::npos; },"Missing publication round",15000);
  publisher.stop(); std::this_thread::sleep_for(100ms); collectors.clear();
  if(error) std::rethrow_exception(error);
  std::uint64_t expected=std::uint64_t(products)*(1+last);
  for(unsigned r=0;r<receivers;++r) {
    require(observations[r].packets.size()==expected,"UDP missing packet count: receiver="+std::to_string(r)+" expected="+std::to_string(expected)+" actual="+std::to_string(observations[r].packets.size()));
    require(observations[r].packets==observations[0].packets,"Subscribers received different wire bytes");
    for(unsigned id=1;id<=products;++id) require(observations[r].applied.at(id)==last,"Symbol did not reach final batch");
  }
  std::vector<double> persistent_times,new_times;
  for(unsigned w=0;w<8;++w) {
    auto& dst=w<4?persistent_times:new_times; dst.insert(dst.end(),times[w].begin(),times[w].end());
  }
  const auto persistent_count=persistent_times.size(),new_count=new_times.size();
  suite.metric("tcp_persistent_"+name,std::move(persistent_times),seconds,persistent_count,"Four clients, request to decoded validated full snapshot over existing TCP; shared pressure window");
  suite.metric("tcp_new_connection_"+name,std::move(new_times),seconds,new_count,"Four clients, connect through decoded snapshot and socket close; shared pressure window");
  const auto age_count=observations[0].ages.size();
  suite.metric("udp_age_"+name,std::move(observations[0].ages),last+1,age_count,"Same-host wall-clock batch freeze to UDP receipt; includes encoding/burst queues; rate uses nominal batch duration");
  std::vector<double> publication_times; std::istringstream lines(read_text(dir/"publisher.log"));
  for(std::string line;std::getline(lines,line);) if(line.starts_with("Published index=")) {
    auto at=line.find("sample_us="); if(at!=std::string::npos) publication_times.push_back(std::stod(line.substr(at+10)));
  }
  auto rounds=publication_times.size(); require(rounds>=last+1,"Missing publication timing records");
  suite.metric("publication_round_"+name,std::move(publication_times),rounds,rounds,"Production sample_us: shard reads through UDP submission; throughput uses nominal one-second cycles, not receiver delivery");
  auto logs=read_text(dir/"publisher.log"); require(logs.find("Source unavailable")==std::string::npos && logs.find("UDP send failed")==std::string::npos,"Publisher reported source/send failure under healthy pressure");
  std::cout << "CHECK subscribers=3 expected_each=" << expected << " missing=0 duplicates=0 corrupt=0 products=" << products << " shards=" << shards << '\n';
}
void outage(const fs::path& bin,const fs::path& dir) {
  Sources sources(dir,16,2); write_market(sources.config,dir/"publisher.conf"); Child publisher;
  publisher.launch(bin/"market_broadcast",{"publish","--config",(dir/"publisher.conf").string()},dir/"publisher.log"); publisher_ready(sources.config);
  const auto old=snapshot(sources.config.query,1); sources.fail=true;
  eventually([&]{return read_text(dir/"publisher.log").find("Source unavailable")!=std::string::npos;},"Source outage was not detected",4000);
  bool denied=false; try { snapshot(sources.config.query,1); } catch(...) { denied=true; } require(denied,"Outage returned a misleading fresh snapshot");
  sources.epoch_number=2; sources.fail=false; md::Message restored{};
  eventually([&]{try { restored=snapshot(sources.config.query,1); return restored.epoch!=old.epoch; } catch(...) {return false;}},"Publisher did not establish a new epoch",5000);
  require(restored.index==0,"Recovery should restart at batch zero");
  bool unknown=false; try { snapshot(sources.config.query,65000); } catch(...) {unknown=true;} require(unknown,"Unknown symbol query accepted");
}
}
int main(int argc,char** argv) {
  std::signal(SIGPIPE,SIG_IGN);
  try {
    require(argc==4,"Expected binary directory, result directory, scale"); fs::path bin=argv[1]; Suite suite(argv[2]); unsigned scale=std::stoul(argv[3]);
    suite.test("P01_protocol_and_12000_delta_reconstructions",protocol_and_difference);
    suite.test("P02_periodic_batch10_64products_1shard",[&]{publication(suite,bin,64,1,10,scale);});
    suite.test("P03_concurrent_query_fanout_1000products_1shard",[&]{publication(suite,bin,1000,1,3,scale);});
    suite.test("P03_concurrent_query_fanout_1000products_4shards",[&]{publication(suite,bin,1000,4,3,scale);});
    suite.test("P03_concurrent_query_fanout_1000products_32shards",[&]{publication(suite,bin,1000,32,3,scale);});
    suite.test("P04_concurrent_query_fanout_2000products_32shards",[&]{publication(suite,bin,2000,32,3,scale);});
    suite.test("P05_source_outage_query_pause_and_epoch_reset",[&]{outage(bin,suite.output/"outage");});
    return suite.finish();
  } catch(const std::exception& e) { std::cerr << "FATAL " << e.what() << '\n'; return 2; }
}
