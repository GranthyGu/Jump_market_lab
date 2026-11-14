#include <chrono>
#include <condition_variable>
#include <iostream>
#include <unistd.h>
#include "matching_engine/trade_history/history_store.hpp"

namespace m = marketlab;
namespace o = m::orders;
namespace p = o::persistence;
namespace history = o::trade_history;
using Clock = std::chrono::steady_clock;
using namespace std::chrono_literals;
namespace {
double milliseconds(Clock::time_point start) { return std::chrono::duration<double,std::milli>(Clock::now()-start).count(); }
void require(bool ok,const char* why) { if (!ok) throw std::runtime_error(why); }
struct Wakeup {
  std::mutex mutex; std::condition_variable cv; bool ready = false;
  void notify() { { std::lock_guard lock(mutex); ready=true; } cv.notify_all(); }
  void wait() { std::unique_lock lock(mutex); cv.wait_for(lock,10ms,[&] { return ready; }); ready=false; }
};
struct Page { unsigned count=0; m::TradeHistoryEnd end{}; };
Page query(history::HistoryStore& store,Wakeup& wake,unsigned user,std::uint64_t through,std::uint64_t after=0) {
  bool done=false; Page page;
  m::TradeHistoryRequest request{{m::MessageType::TradeHistoryRequest,sizeof(request),1},1,256,after,through};
  store.request({0,o::LinkKind::History,1,user,1},request,through,[&](auto kind,const auto&,auto bytes) {
    m::Header header{};std::memcpy(&header,bytes.data(),sizeof(header));
    require(header.messageType != m::MessageType::RequestRejected,"benchmark history query rejected");
    if (kind==o::LinkKind::HistoryEnd) { page.end=o::copy_record<m::TradeHistoryEnd>(bytes);done=true; }
    else { auto trade=o::copy_record<m::TradeHistoryRecord>(bytes);require(trade.timestamp==1700000000000000000ULL+trade.tradeId,"original timestamp mismatch");++page.count; }
  });
  auto deadline=Clock::now()+120s;
  while (!done) { store.poll();if (!done) { require(Clock::now()<deadline,"query timeout");wake.wait(); } }
  require(page.end.count==page.count,"page count mismatch");return page;
}
void distribution(const char* name,std::vector<double> times) {
  std::sort(times.begin(),times.end());
  std::cout << name << " samples=" << times.size() << " p50_us=" << times[times.size()/2]
            << " p95_us=" << times[times.size()*95/100] << " p99_us=" << times[times.size()*99/100] << '\n';
}
}
int main(int argc,char** argv) {
  try {
    auto root=std::filesystem::path(argc>1?argv[1]:"history-benchmark")/std::to_string(getpid());
    std::uint64_t trades=argc>2?std::stoull(argv[2]):1000000;
    require(trades>=1000,"use at least 1000 trades");
    p::make_directory(root/"trades");o::OrderConfig config;
    for (unsigned i=1;i<=10000;++i) config.accounts.push_back({"user"+std::to_string(i),"demo"});
    p::File log(root/"trades/trades.log",true,true);o::Bytes bytes;
    auto generated=Clock::now();
    for (std::uint64_t first=0;first<trades;first+=512) {
      auto count=static_cast<unsigned>(std::min<std::uint64_t>(512,trades-first));
      p::start_frame(bytes,p::Kind::Trades,{first/512+1,1,first+count+1,0,0},count,count);
      for (unsigned j=0;j<count;++j) {
        auto i=first+j;p::TradeRecord record;record.id=(std::uint64_t{1}<<48)+i+1;record.timestamp=1700000000000000000ULL+record.id;
        record.price=100;record.quantity=1;record.buy_order=2*i+1;record.sell_order=2*i+2;
        auto buyer=i%5==0?1:3+i%9998;
        auto name=config.accounts[buyer-1].name;std::copy(name.begin(),name.end(),record.buyer.begin());
        std::copy_n("user2",5,record.seller.begin());std::copy_n("ITEM1",5,record.symbol.begin());p::encode_trade(bytes,record);
      }
      p::finish_frame(bytes);log.append(bytes);
    }
    log.sync();p::sync_directory(root/"trades");auto end=log.size();
    std::cout << "LOG trades=" << trades << " bytes=" << end << " write_and_sync_ms=" << milliseconds(generated) << '\n';
    Wakeup wake;
    {
      auto replay=Clock::now();history::HistoryStore store(root,config,1,end,1024,64,[&] { wake.notify(); },UINT64_MAX);
      require(query(store,wake,1,end).count==256,"hot-user first page");
      std::cout << "INDEX full_replay_through_query_ms=" << milliseconds(replay) << " cache_bytes=" << 8*1024*1024 << '\n';
      std::vector<double> hot,sparse;
      for (unsigned i=0;i<100;++i) {
        auto begin=Clock::now();query(store,wake,1,end);hot.push_back(milliseconds(begin)*1000);
        begin=Clock::now();query(store,wake,3+i,end);sparse.push_back(milliseconds(begin)*1000);
      }
      distribution("HISTORY_HOT_PAGE",hot);distribution("HISTORY_SPARSE_USER",sparse);
      auto full=Clock::now();std::uint64_t cursor=0,count=0;
      for (;;) { auto page=query(store,wake,1,end,cursor);count+=page.count;if (!(page.end.flags&m::history_more)) break;cursor=page.end.nextOffset; }
      require(count==(trades+4)/5,"full hot-user history lost trades");
      std::cout << "HISTORY_FULL records=" << count << " milliseconds=" << milliseconds(full) << '\n';
      auto close=Clock::now();store.close();
      std::cout << "CHECKPOINT milliseconds=" << milliseconds(close) << " bytes=" << std::filesystem::file_size(root/"history/index.snapshot") << '\n';
    }
    {
      auto reopen=Clock::now();history::HistoryStore store(root,config,1,end,1024,64,[&] { wake.notify(); });
      require(query(store,wake,1,end).count==256,"reopen first page");
      std::cout << "RESTORE through_first_query_ms=" << milliseconds(reopen) << '\n';
    }
    std::cout << "RESULTS " << root << '\n';
  } catch (const std::exception& e) { std::cerr << "FAIL " << e.what() << '\n';return 1; }
}
