#include "harness.hpp"
#include "network.hpp"
#include "matching_engine/engine.hpp"
#include "matching_engine/snapshot_mailbox.hpp"
#include <random>
#include <limits>

using namespace demo;
namespace {
od::OrderConfig profile(unsigned products=16) {
  od::OrderConfig c; c.accounts={{"one","demo"},{"two","demo"},{"three","demo"},{"four","demo"}};
  c.shards={{1,{"127.0.0.1",1}}};
  for(unsigned i=0;i<products;++i) c.products.push_back({symbol(i),i==1?101:100,1});
  return c;
}
struct Core {
  od::OrderConfig config; od::MatchingEngine engine; std::uint64_t token=1;
  std::vector<unsigned> recipients;
  explicit Core(unsigned products=16):config(profile(products)),engine(config,1) {}
  template<class T> std::vector<od::Bytes> call(unsigned user,od::LinkKind kind,const T& request) {
    std::vector<od::Bytes> out; recipients.clear(); const od::LinkHeader h{0,kind,token++,user,99};
    engine.command(h,od::record_bytes(request),[&](auto,const auto& context,auto payload) {
      require(context.user>0 && context.user<=4,"Engine event missing destination");
      recipients.push_back(context.user);
      out.emplace_back(payload.begin(),payload.end());
    }); return out;
  }
  std::vector<ml::OrderState> active(unsigned user) {
    auto records=call(user,od::LinkKind::Snapshot,ml::Header{ml::MessageType::ActiveOrderSnapshot,sizeof(ml::Header),700});
    require(!records.empty(),"Missing snapshot terminator"); std::vector<ml::OrderState> out;
    for(unsigned i=0;i<records.size();++i) {
      auto r=od::copy_record<ml::ActiveOrderSnapshot>(records[i]);
      require(r.header.requestId==700,"Snapshot correlation");
      if(i+1==records.size()) { ml::OrderState zero{}; require(r.isEnd==1 && std::memcmp(&r.order,&zero,sizeof(zero))==0,"Malformed terminal marker"); }
      else { require(!r.isEnd && r.order.remainingQuantity>0,"Invalid active snapshot"); out.push_back(r.order); }
    } return out;
  }
};
void validation() {
  Core c;
  for(auto r:{order_record(10,symbol(0),ml::Side::Buy,0,1),order_record(11,symbol(0),ml::Side::Buy,100,0),
      order_record(12,"UNKNOWN",ml::Side::Buy,100,1),order_record(13,symbol(0),static_cast<ml::Side>(7),100,1),
      order_record(14,symbol(1),ml::Side::Buy,89,1),order_record(15,symbol(1),ml::Side::Buy,112,1)}) {
    auto events=c.call(1,od::LinkKind::NewOrder,r);
    require(events.size()==1,"Invalid order emitted extra events"); auto reject=od::copy_record<ml::RequestRejected>(events[0]);
    require(reject.reason==ml::RejectReason::InvalidOrder && reject.header.requestId==r.header.requestId,"Invalid order not rejected");
  }
  require(c.active(1).empty(),"Invalid order changed state");
  for(int price:{90,111}) {
    auto out=c.call(1,od::LinkKind::NewOrder,order_record(price,symbol(1),ml::Side::Buy,price,std::numeric_limits<int>::max()));
    auto accepted=od::copy_record<ml::OrderUpdate>(out.at(0)); require(accepted.order.remainingQuantity==std::numeric_limits<int>::max(),"int32 quantity lost");
  }
  require(c.active(1).size()==2,"Inclusive price limit rejected");
  auto same=c.call(1,od::LinkKind::NewOrder,order_record(800,symbol(1),ml::Side::Buy,90,std::numeric_limits<int>::max()));
  (void)same; auto view=c.engine.market_view();
  require(view.books.at(2).levels.back().quantity==2ULL*std::numeric_limits<int>::max(),"Aggregate truncated to int32");
}
// Independent semantic oracle: a flat vector is scanned for best price and
// arrival rank. It does not use production price maps, queues or the ID index.
struct ModelOrder { ml::OrderId id; unsigned user,product,rank; ml::Side side; int price,original,filled,remaining; };
struct ExpectedFill { ml::OrderId aggressor,resting; int price,quantity; unsigned user_a,user_b; int fill_a,remain_a,fill_b,remain_b; };
void oracle(unsigned scale) {
  Core c; std::vector<ModelOrder> model; std::mt19937 rng(0x20261101); std::uint64_t next=1;
  for(unsigned step=0;step<8000*scale;++step) {
    unsigned user=1+rng()%4;
    if(step%4==3 && !model.empty()) {
      auto& entry=model[rng()%model.size()];
      const bool allowed=entry.remaining>0 && entry.user==user;
      auto out=c.call(user,od::LinkKind::Cancel,cancel_record(step+1,entry.id));
      require(out.size()==1,"Cancel event count");
      if(allowed) {
        auto r=od::copy_record<ml::OrderUpdate>(out[0]); require(r.order.orderId==entry.id && r.order.status==ml::OrderStatus::Canceled &&
          r.order.filledQuantity==entry.filled && r.order.remainingQuantity==0,"Oracle cancel mismatch"); entry.remaining=0;
      } else require(od::copy_record<ml::RequestRejected>(out[0]).reason==ml::RejectReason::CancelFailed,"Unauthorized/inactive cancel accepted");
    } else {
      const unsigned p=rng()%16; const auto side=rng()%2?ml::Side::Buy:ml::Side::Sell;
      const int price=95+int(rng()%11),qty=1+rng()%20;
      ModelOrder incoming{(1ULL<<48)|next++,user,p,step,side,price,qty,0,qty}; std::vector<ExpectedFill> fills;
      while(incoming.remaining) {
        ModelOrder* selected=nullptr;
        for(auto& candidate:model) {
          if(!candidate.remaining || candidate.product!=p || candidate.side==side) continue;
          if(side==ml::Side::Buy?candidate.price>price:candidate.price<price) continue;
          if(!selected || (candidate.price==selected->price?candidate.rank<selected->rank:
              (side==ml::Side::Buy?candidate.price<selected->price:candidate.price>selected->price))) selected=&candidate;
        }
        if(!selected) break;
        int traded=std::min(incoming.remaining,selected->remaining);
        incoming.filled+=traded; incoming.remaining-=traded; selected->filled+=traded; selected->remaining-=traded;
        fills.push_back({incoming.id,selected->id,selected->price,traded,user,selected->user,incoming.filled,incoming.remaining,selected->filled,selected->remaining});
      }
      auto out=c.call(user,od::LinkKind::NewOrder,order_record(step+1,symbol(p),side,price,qty));
      require(out.size()==1+4*fills.size(),"Oracle fill count mismatch");
      const auto accepted=od::copy_record<ml::OrderUpdate>(out[0]);
      require(accepted.header.requestId==step+1 && accepted.order.orderId==incoming.id && accepted.order.status==ml::OrderStatus::Accepted &&
        accepted.order.remainingQuantity==qty && accepted.order.originalQuantity==qty && accepted.order.filledQuantity==0 &&
        accepted.order.side==side && accepted.order.price==price && od::fixed_string(accepted.order.symbol)==symbol(p) &&
        c.recipients[0]==user,"Accepted must precede fills and retain fields/destination");
      for(unsigned f=0;f<fills.size();++f) {
        const auto& expected=fills[f]; auto a=od::copy_record<ml::Trade>(out[1+4*f]); auto b=od::copy_record<ml::Trade>(out[3+4*f]);
        auto sa=od::copy_record<ml::OrderUpdate>(out[2+4*f]); auto sb=od::copy_record<ml::OrderUpdate>(out[4+4*f]);
        require(a.orderId==expected.aggressor && b.orderId==expected.resting && a.tradeId==b.tradeId && a.timestamp==b.timestamp &&
          a.price==expected.price && b.price==expected.price && a.quantity==expected.quantity && b.quantity==expected.quantity &&
          a.header.requestId==0 && b.header.requestId==0,"Oracle best price/FIFO/trade mismatch");
        require(c.recipients[1+4*f]==expected.user_a && c.recipients[2+4*f]==expected.user_a &&
          c.recipients[3+4*f]==expected.user_b && c.recipients[4+4*f]==expected.user_b,"Fill delivered to wrong owner");
        for(const auto& item:{std::tuple{sa,expected.fill_a,expected.remain_a},std::tuple{sb,expected.fill_b,expected.remain_b}}) {
          const auto& [state,filled,left]=item;
          require(state.header.requestId==0 && state.order.filledQuantity==filled && state.order.remainingQuantity==left &&
            state.order.status==(left?ml::OrderStatus::PartiallyFilled:ml::OrderStatus::Filled),"Oracle partial/final status mismatch");
        }
      }
      model.push_back(incoming);
    }
    if(step%127==0) {
      for(unsigned u=1;u<=4;++u) {
        std::map<ml::OrderId,std::pair<int,int>> expected,actual;
        for(const auto& o:model) if(o.user==u && o.remaining) expected.emplace(o.id,std::pair{o.filled,o.remaining});
        for(const auto& o:c.active(u)) actual.emplace(ml::OrderId(o.orderId),std::pair{int(o.filledQuantity),int(o.remainingQuantity)});
        require(actual==expected,"Oracle active-order snapshot mismatch");
      }
      const auto view=c.engine.market_view();
      for(unsigned p=0;p<16;++p) {
        std::map<int,std::pair<std::uint64_t,std::uint64_t>> bids,asks;
        for(const auto& o:model) if(o.product==p && o.remaining) { auto& level=(o.side==ml::Side::Buy?bids:asks)[o.price]; level.first+=o.remaining; ++level.second; }
        std::vector<md::Level> expected;
        unsigned n=0; for(auto i=bids.rbegin();i!=bids.rend() && n<5;++i,++n) expected.push_back({md::Side::Buy,i->first,i->second.first,i->second.second});
        n=0; for(auto i=asks.begin();i!=asks.end() && n<5;++i,++n) expected.push_back({md::Side::Sell,i->first,i->second.first,i->second.second});
        require(view.books.at(p+1).levels==expected,"Oracle market aggregation/depth mismatch");
      }
    }
  }
}
void pressure(Suite& suite,unsigned products,unsigned scale) {
  Core c(products); std::vector<double> times; const unsigned count=20000*scale;
  for(unsigned i=0;i<256;++i) {
    auto out=c.call(1,od::LinkKind::NewOrder,order_record(1,symbol(i%products),ml::Side::Buy,90,3));
    c.call(1,od::LinkKind::Cancel,cancel_record(2,od::copy_record<ml::OrderUpdate>(out[0]).order.orderId));
  }
  auto start=Clock::now();
  for(unsigned i=0;i<count;++i) {
    auto tick=Clock::now(); auto out=c.call(1,od::LinkKind::NewOrder,order_record(2*i+1,symbol(i%products),ml::Side::Buy,90,3));
    require(out.size()==1,"Unexpected stress fill"); auto accepted=od::copy_record<ml::OrderUpdate>(out[0]); times.push_back(us(tick));
    require(accepted.header.requestId==2*i+1 && accepted.order.remainingQuantity==3,"Stress new mismatch");
    tick=Clock::now(); out=c.call(1,od::LinkKind::Cancel,cancel_record(2*i+2,accepted.order.orderId));
    auto canceled=od::copy_record<ml::OrderUpdate>(out.at(0)); times.push_back(us(tick));
    require(canceled.order.orderId==accepted.order.orderId && canceled.order.status==ml::OrderStatus::Canceled && canceled.order.remainingQuantity==0,"Stress cancel mismatch");
  }
  const double seconds=us(start)/1e6;
  require(c.active(1).empty(),"Stress leaked orders");
  for(const auto& [id,b]:c.engine.market_view().books) { (void)id; require(b.levels.empty(),"Stress left book levels"); }
  suite.metric("core_new_cancel_products_"+std::to_string(products),std::move(times),seconds,2ULL*count,"Core command plus synchronous event copy/check; no TCP; warm-up excluded");
}
void sweeps(Suite& suite,unsigned scale) {
  Core c; std::vector<double> times; const unsigned rounds=100*scale,depth=64; auto start=Clock::now();
  for(unsigned r=0;r<rounds;++r) {
    for(unsigned i=0;i<depth;++i) c.call(1,od::LinkKind::NewOrder,order_record(1,symbol(0),ml::Side::Sell,100,2));
    auto tick=Clock::now(); auto out=c.call(1,od::LinkKind::NewOrder,order_record(2,symbol(0),ml::Side::Buy,110,2*depth)); times.push_back(us(tick));
    require(out.size()==1+4*depth,"Self-trade sweep missing participant notifications");
    std::set<ml::TradeId> trades;
    for(unsigned i=0;i<depth;++i) {
      auto a=od::copy_record<ml::Trade>(out[1+4*i]),b=od::copy_record<ml::Trade>(out[3+4*i]);
      require(a.tradeId==b.tradeId && a.orderId!=b.orderId && a.quantity==2 && a.price==100,"Self-trade sweep mismatch"); trades.insert(ml::TradeId(a.tradeId));
    }
    require(trades.size()==depth && c.active(1).empty(),"Sweep duplicate trades/order leak");
  }
  suite.metric("self_trade_sweep_64_orders",std::move(times),us(start)/1e6,rounds,"Latency times only the aggressive command (64 fills); throughput is completed sweeps including resting-order setup/checks");
}
void concurrent(unsigned scale) {
  Core c(64); std::atomic_bool done{false}; std::atomic<unsigned> reads{0}; std::exception_ptr error; std::mutex mutex;
  od::SnapshotMailbox mailbox(c.engine.symbol_count());
  std::atomic<unsigned> finished{0};
  std::vector<std::unique_ptr<md::StopThread>> readers;
  for(unsigned r=0;r<4;++r) readers.push_back(std::make_unique<md::StopThread>([&](const auto& stop) {
    try { while(!done.load() && !stop.load()) {
      for(const auto& [id,b]:mailbox.read(until()).books) { (void)id; md::validate_book(b); for(const auto& l:b.levels) require(l.quantity==5*l.num_orders,"Torn concurrent market aggregate"); }
      ++reads; std::this_thread::yield();
    }} catch(...) { std::lock_guard lock(mutex); error=std::current_exception(); }
    ++finished;
  }));
  for(unsigned i=0;i<5000*scale;++i) {
    auto out=c.call(1,od::LinkKind::NewOrder,order_record(1,symbol(i%64),ml::Side::Buy,90,5));
    mailbox.service(c.engine);
    c.call(1,od::LinkKind::Cancel,cancel_record(2,od::copy_record<ml::OrderUpdate>(out[0]).order.orderId));
    mailbox.service(c.engine);
  }
  done=true;
  while(finished.load()!=4) { mailbox.service(c.engine); std::this_thread::yield(); }
  readers.clear(); if(error) std::rethrow_exception(error); require(reads>0 && c.active(1).empty(),"Concurrent test made no progress");
  std::cout << "CHECK coherent_views=" << reads << " writers=1 readers=4\n";
}
void pool_growth() {
  Core c; std::vector<ml::OrderId> ids; std::set<ml::OrderId> canceled;
  for(unsigned i=0;i<2000;++i) {
    auto out=c.call(1,od::LinkKind::NewOrder,order_record(i+1,symbol(0),ml::Side::Buy,100,1));
    ids.push_back(ml::OrderId(od::copy_record<ml::OrderUpdate>(out[0]).order.orderId));
  }
  require(c.active(1).size()==2000,"Pool growth lost live orders");
  for(unsigned i=0;i<2000;i+=2) {
    c.call(1,od::LinkKind::Cancel,cancel_record(3000+i,ids[i])); canceled.insert(ids[i]);
  }
  for(unsigned i=0;i<1000;++i) c.call(1,od::LinkKind::NewOrder,order_record(6000+i,symbol(0),ml::Side::Buy,100,1));
  auto top=c.engine.market_view().books.at(1).levels;
  require(top.size()==1 && top[0].quantity==2000 && top[0].num_orders==2000,"Pool reuse corrupted totals");
  auto out=c.call(2,od::LinkKind::NewOrder,order_record(8000,symbol(0),ml::Side::Sell,90,2000));
  require(out.size()==8001,"Sweep after pool reuse lost fills");
  std::set<ml::OrderId> filled;
  for(unsigned i=0;i<2000;++i) {
    auto t=od::copy_record<ml::Trade>(out[3+4*i]); const ml::OrderId id=t.orderId;
    require(!canceled.contains(id) && filled.insert(id).second,"Canceled/recycled order was filled twice");
    if(i<1000) require(id==ids[2*i+1],"Middle-queue cancellation/reuse broke FIFO");
  }
  require(c.active(1).empty() && c.active(2).empty(),"Pool growth/reuse leaked orders");
}
}
int main(int argc,char** argv) {
  try {
    require(argc==4,"Expected binary directory, result directory, scale"); Suite suite(argv[2]); unsigned scale=std::stoul(argv[3]);
    suite.test("E01_validation_price_limits_and_uint64_aggregation",validation);
    suite.test("E02_seeded_independent_oracle_8000_commands",[&]{oracle(scale);});
    for(unsigned count:{16U,1000U,2000U}) suite.test("E03_new_cancel_pressure_"+std::to_string(count),[&]{pressure(suite,count,scale);});
    suite.test("E04_self_trade_64_resting_orders_per_sweep",[&]{sweeps(suite,scale);});
    suite.test("E05_one_writer_four_concurrent_market_readers",[&]{concurrent(scale);});
    suite.test("E06_2000_live_orders_growth_middle_cancel_and_reuse",pool_growth);
    return suite.finish();
  } catch(const std::exception& e) { std::cerr << "FATAL " << e.what() << '\n'; return 2; }
}
