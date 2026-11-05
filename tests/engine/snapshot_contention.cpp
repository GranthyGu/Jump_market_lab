// Fixed before the production change. Both versions use this exact workload.
#include "harness.hpp"
#include "matching_engine/engine.hpp"
#if __has_include("matching_engine/snapshot_mailbox.hpp")
#include "matching_engine/snapshot_mailbox.hpp"
#define HAS_SNAPSHOT_MAILBOX 1
#endif
#include <atomic>
#include <cstdlib>
#include <new>
#include <barrier>

// A controlled delay at the first result allocation of a snapshot call.
// Baseline: this allocation is inside state_mutex_. Mailbox: reader-side only.
thread_local bool delay_snapshot_allocation = false;
void* operator new(std::size_t size) {
  if (delay_snapshot_allocation) {
    delay_snapshot_allocation = false;
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  if (void* p = std::malloc(size ? size : 1)) return p;
  throw std::bad_alloc();
}
void* operator new[](std::size_t n) { return ::operator new(n); }
void operator delete(void* p) noexcept { std::free(p); }
void operator delete[](void* p) noexcept { std::free(p); }
void operator delete(void* p,std::size_t) noexcept { std::free(p); }
void operator delete[](void* p,std::size_t) noexcept { std::free(p); }

using namespace demo;
namespace ml=marketlab;
namespace od=ml::orders;
namespace md=ml::md;
namespace {
constexpr unsigned shards=4, products_per_shard=250, pairs_per_second=3000;
struct Scenario { const char* name; unsigned readers, period_us; bool delayed; };
constexpr Scenario scenarios[] = {
  {"no_readers",0,0,false}, {"publisher_1hz",1,1000000,false},
  {"four_readers_100hz",4,10000,false}, {"four_readers_saturated",4,0,false},
  {"reader_allocation_delay_1ms",1,10000,true}
};
od::OrderConfig profile() {
  od::OrderConfig c; c.accounts={{"trader","test"}};
  for(unsigned s=1;s<=shards;++s) c.shards.push_back({static_cast<std::uint16_t>(s),{"127.0.0.1",static_cast<std::uint16_t>(7200+s)}});
  for(unsigned i=0;i<shards*products_per_shard;++i)
    c.products.push_back({"ITEM"+std::to_string(i+1),100,static_cast<std::uint16_t>(1+i%shards)});
  return c;
}
struct Node {
  od::MatchingEngine engine;
#ifdef HAS_SNAPSHOT_MAILBOX
  od::SnapshotMailbox mailbox;
#endif
  std::vector<ml::NewOrderRequest> requests;
  std::uint64_t token=1;
  Node(const od::OrderConfig& c,unsigned shard):engine(c,shard)
#ifdef HAS_SNAPSHOT_MAILBOX
      ,mailbox(engine.symbol_count())
#endif
  {
    requests.reserve(products_per_shard);
    for(const auto& p:c.products) if(p.shard==shard) {
      ml::NewOrderRequest r{};
      r.header={ml::MessageType::NewOrderRequest,sizeof(r),1};
      od::put_string(r.symbol,p.symbol); r.quantity=5;
      for(unsigned side=0;side<2;++side) for(int level=0;level<5;++level) {
        r.side=side?ml::Side::Sell:ml::Side::Buy; r.price=side?101+level:99-level;
        call(od::LinkKind::NewOrder,r,ml::OrderStatus::Accepted);
      }
      r.side=ml::Side::Buy; r.price=99; requests.push_back(r);
    }
    // Warm the reusable order pool, index and event capacity before measuring.
    for(unsigned i=0;i<500;++i) {
      const auto id=call(od::LinkKind::NewOrder,requests[i%requests.size()],ml::OrderStatus::Accepted);
      call(od::LinkKind::Cancel,ml::CancelRequest{{ml::MessageType::CancelRequest,sizeof(ml::CancelRequest),2},id},ml::OrderStatus::Canceled);
    }
  }
  template<class T> ml::OrderId call(od::LinkKind kind,const T& request,ml::OrderStatus expected) {
    unsigned events=0; ml::OrderId id=0;
    engine.command({0,kind,token++,1,1},od::record_bytes(request),[&](auto,const auto&,auto bytes) {
      const auto r=od::copy_record<ml::OrderUpdate>(bytes);
      require(r.header.messageType==ml::MessageType::OrderUpdate && r.order.status==expected &&
          r.header.requestId==request.header.requestId && r.order.filledQuantity==0,"Unexpected trade/response");
      id=r.order.orderId; ++events;
    });
    require(events==1,"Missing or duplicate response"); return id;
  }
  void service() {
#ifdef HAS_SNAPSHOT_MAILBOX
    mailbox.service(engine);
#endif
  }
  md::ShardView read() {
#ifdef HAS_SNAPSHOT_MAILBOX
    return mailbox.read(Clock::now()+5s);
#else
    return engine.market_view();
#endif
  }
};
void validate(const md::ShardView& view,bool final=false) {
  require(view.books.size()==products_per_shard,"Missing symbols");
  for(const auto& [id,b]:view.books) {
    (void)id; md::validate_book(b); require(b.levels.size()==10,"Missing price levels");
    for(const auto& l:b.levels) require(l.quantity==5*l.num_orders && l.num_orders>0 &&
        l.num_orders<=2 && (!final || l.num_orders==1),"Torn or incorrect aggregate");
  }
}
struct Samples { std::vector<double> steps,schedule,reads; };
void metric(std::ofstream& raw,std::ofstream& summary,const Scenario& c,unsigned repeat,
            const char* kind,const std::vector<double>& values,double seconds) {
  if(values.empty()) return;
  for(std::size_t i=0;i<values.size();++i) raw<<c.name<<','<<repeat<<','<<kind<<','<<i<<','<<std::setprecision(12)<<values[i]<<'\n';
  auto sorted=values; std::sort(sorted.begin(),sorted.end());
  auto q=[&](double p){return sorted[static_cast<std::size_t>(std::ceil(p*sorted.size()))-1];};
  summary<<c.name<<','<<repeat<<','<<kind<<','<<sorted.size()<<','<<seconds<<','
      <<std::accumulate(sorted.begin(),sorted.end(),0.0)/sorted.size()<<','<<q(.5)<<','<<q(.95)<<','<<q(.99)<<','<<q(.999)<<','<<sorted.back()<<'\n';
  std::cout<<c.name<<" repeat="<<repeat<<" metric="<<kind<<" n="<<sorted.size()<<" p99_us="<<q(.99)<<" max_us="<<sorted.back()<<std::endl;
}
void run(const Scenario& c,unsigned repeat,unsigned duration_ms,std::ofstream& raw,std::ofstream& summary) {
  const auto config=profile();
  std::vector<std::unique_ptr<Node>> nodes;
  for(unsigned s=1;s<=shards;++s) nodes.push_back(std::make_unique<Node>(config,s));
  std::vector<Samples> samples(shards);
  std::vector<std::vector<double>> reads(shards*c.readers);
  const unsigned pairs=pairs_per_second*duration_ms/1000;
  for(auto& s:samples) { s.steps.reserve(2*pairs); s.schedule.reserve(pairs); }
  for(auto& r:reads) r.reserve(100000);
  std::atomic<unsigned> readers_left[shards];
  for(auto& r:readers_left) r.store(c.readers);
  std::atomic_bool failed{false}; std::exception_ptr error; std::mutex error_mutex;
  auto record_error=[&] { failed=true; std::lock_guard lock(error_mutex); if(!error) error=std::current_exception(); };
  Clock::time_point begin;
  std::barrier start(static_cast<std::ptrdiff_t>(shards*(1+c.readers)),[&]() noexcept { begin=Clock::now(); });
  std::vector<std::thread> workers;
  for(unsigned s=0;s<shards;++s) {
    workers.emplace_back([&,s] {
      start.arrive_and_wait(); auto& n=*nodes[s];
      try {
        for(unsigned i=0;i<pairs && !failed.load();++i) {
          const auto planned=begin+std::chrono::nanoseconds(std::uint64_t(i)*1000000000/pairs_per_second);
          // Service pending reads during arrival gaps. No synchronization wait is
          // hidden from the owner-step or planned-arrival measurements.
          while(Clock::now()<planned && !failed.load()) { n.service(); std::this_thread::sleep_for(20us); }
          const auto first=Clock::now();
          const auto id=n.call(od::LinkKind::NewOrder,n.requests[i%products_per_shard],ml::OrderStatus::Accepted);
          n.service(); samples[s].steps.push_back(us(first));
          const auto second=Clock::now();
          const auto canceled=n.call(od::LinkKind::Cancel,ml::CancelRequest{{ml::MessageType::CancelRequest,sizeof(ml::CancelRequest),2},id},ml::OrderStatus::Canceled);
          n.service(); require(canceled==id,"Wrong canceled ID"); samples[s].steps.push_back(us(second));
          samples[s].schedule.push_back(us(planned));
        }
      } catch(...) { record_error(); }
      // Keep the owner alive to finish the readers' last in-flight requests.
      while(readers_left[s].load()) { n.service(); std::this_thread::sleep_for(20us); }
    });
    for(unsigned r=0;r<c.readers;++r) workers.emplace_back([&,s,r] {
      start.arrive_and_wait(); auto next=begin;
      try {
        while(!failed.load() && Clock::now()<begin+std::chrono::milliseconds(duration_ms)) {
          if(c.period_us && Clock::now()<next) { std::this_thread::sleep_for(100us); continue; }
          const auto tick=Clock::now(); delay_snapshot_allocation=c.delayed;
          auto view=nodes[s]->read(); delay_snapshot_allocation=false;
          reads[s*c.readers+r].push_back(us(tick)); validate(view);
          // Finite-rate readers do not generate catch-up bursts after a delay.
          if(c.period_us) next=std::max(next+std::chrono::microseconds(c.period_us),Clock::now());
        }
      } catch(...) { delay_snapshot_allocation=false; record_error(); }
      --readers_left[s];
    });
  }
  for(auto& worker:workers) worker.join();
  if(error) std::rethrow_exception(error);
  const double seconds=us(begin)/1e6;
  std::vector<double> steps,schedule,all_reads;
  for(unsigned s=0;s<shards;++s) {
    require(samples[s].steps.size()==2*pairs,"Missing order samples");
    validate(nodes[s]->engine.market_view(),true);
    steps.insert(steps.end(),samples[s].steps.begin(),samples[s].steps.end());
    schedule.insert(schedule.end(),samples[s].schedule.begin(),samples[s].schedule.end());
  }
  for(const auto& r:reads) all_reads.insert(all_reads.end(),r.begin(),r.end());
  if(c.readers) require(all_reads.size()>=shards*c.readers,"Readers made no progress");
  metric(raw,summary,c,repeat,"owner_step",steps,seconds);
  metric(raw,summary,c,repeat,"planned_pair",schedule,seconds);
  metric(raw,summary,c,repeat,"snapshot",all_reads,seconds);
  raw.flush(); summary.flush();
}
}
int main(int argc,char** argv) {
  try {
    require(argc>=2 && argc<=4,"Usage: snapshot_contention_benchmark OUTPUT [REPEATS=3] [DURATION_MS=3000]");
    const unsigned repeats=argc>2?std::stoul(argv[2]):3, duration=argc>3?std::stoul(argv[3]):3000;
    require(repeats>0 && duration>=1000,"Invalid benchmark length"); fs::create_directories(argv[1]);
    std::ofstream raw(fs::path(argv[1])/"samples.csv"), summary(fs::path(argv[1])/"summary.csv"), metadata(fs::path(argv[1])/"metadata.txt");
    require(raw.good()&&summary.good()&&metadata.good(),"Cannot create benchmark output");
    raw<<"scenario,repeat,metric,sample,latency_us\n";
    summary<<"scenario,repeat,metric,samples,seconds,mean_us,p50_us,p95_us,p99_us,p999_us,max_us\n";
#ifdef HAS_SNAPSHOT_MAILBOX
    metadata<<"implementation=mailbox\n";
#else
    metadata<<"implementation=mutex\n";
#endif
    metadata<<"shards=4\nproducts_per_shard=250\nlevels_per_product=10\npairs_per_second_per_shard=3000\nduration_ms="<<duration<<"\nrepeats="<<repeats<<"\n";
    metadata<<"owner_step=command, event validation, and pending snapshot service; no TCP\nplanned_pair=planned arrival through completion of dependent new/cancel pair; includes backlog\n";
    for(unsigned rep=0;rep<repeats;++rep) for(unsigned k=0;k<std::size(scenarios);++k)
      run(scenarios[rep%2?std::size(scenarios)-1-k:k],rep+1,duration,raw,summary);
    std::cout<<"PASS all snapshots and responses validated\n"; return 0;
  } catch(const std::exception& e) { std::cerr<<"FAIL "<<e.what()<<'\n'; return 1; }
}
