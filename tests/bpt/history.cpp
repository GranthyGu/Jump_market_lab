#include <chrono>
#include <condition_variable>
#include <fstream>
#include <iostream>
#include <thread>
#include <unistd.h>
#include "matching_engine/persistence/durable_engine.hpp"

namespace m = marketlab;
namespace o = m::orders;
namespace p = o::persistence;
using namespace std::chrono_literals;
namespace {
void require(bool ok, const char* message) { if (!ok) throw std::runtime_error(message); }
template<class F> void eventually(F fn) {
  auto deadline = std::chrono::steady_clock::now() + 10s;
  while (!fn()) { if (std::chrono::steady_clock::now() >= deadline) throw std::runtime_error("history test timed out"); std::this_thread::sleep_for(1ms); }
}
o::OrderConfig config() {
  o::OrderConfig c;
  c.accounts = {{"Alice","demo"},{"Bob","demo"},{"Carol","demo"}};
  c.products = {{"ITEM0001",100,1}}; c.shards = {{1,{"127.0.0.1",1}}}; return c;
}
p::Options options(const std::filesystem::path& root) {
  p::Options x; x.directory = root; x.history_cache_pages = 32; x.checkpoint_interval = 1h; return x;
}
void order(p::DurableEngine& e, unsigned user, unsigned request, m::Side side, int quantity) {
  m::NewOrderRequest q{}; q.header = {m::MessageType::NewOrderRequest,sizeof(q),request};
  o::put_string(q.symbol,"ITEM0001"); q.side = side; q.quantity = quantity; q.price = 100;
  e.command({0,o::LinkKind::NewOrder,request,user,1},o::record_bytes(q),[](auto,const auto&,auto) {});
}
struct Page { std::vector<m::TradeHistoryRecord> records; m::TradeHistoryEnd end{}; m::RejectReason error{}; };
Page query(p::DurableEngine& e, unsigned user, std::uint64_t after = 0, std::uint64_t through = 0, unsigned limit = 31) {
  Page result; bool done = false;
  m::TradeHistoryRequest q{{m::MessageType::TradeHistoryRequest,sizeof(q),900000},1,static_cast<std::uint16_t>(limit),after,through};
  e.command({0,o::LinkKind::History,900000,user,1},o::record_bytes(q),[&](auto kind,const auto& h,auto bytes) {
    require(h.user == user && h.token == 900000, "history response correlation");
    m::Header header{}; std::memcpy(&header,bytes.data(),sizeof(header));
    if (header.messageType == m::MessageType::TradeHistoryRecord) result.records.push_back(o::copy_record<m::TradeHistoryRecord>(bytes));
    else if (header.messageType == m::MessageType::RequestRejected) { result.error = o::copy_record<m::RequestRejected>(bytes).reason; done = true; }
    else { require(kind == o::LinkKind::HistoryEnd,"invalid history terminal kind"); result.end = o::copy_record<m::TradeHistoryEnd>(bytes); done = true; }
  });
  eventually([&] { e.poll(); return done; }); return result;
}
std::vector<m::TradeHistoryRecord> all(p::DurableEngine& e, unsigned user) {
  std::vector<m::TradeHistoryRecord> records; std::uint64_t after = 0, through = 0;
  for (;;) {
    auto page = query(e,user,after,through); require(page.error == m::RejectReason{},"history query rejected");
    require(page.end.count == page.records.size(),"wrong page count");
    if (through) require(through == page.end.throughOffset,"moving history upper bound");
    records.insert(records.end(),page.records.begin(),page.records.end());
    if (!(page.end.flags & m::history_more)) return records;
    require(page.end.nextOffset > after,"cursor stalled"); after = page.end.nextOffset; through = page.end.throughOffset;
  }
}
void compare(const std::vector<m::TradeHistoryRecord>& actual, const std::vector<p::TradeRecord>& expected) {
  require(actual.size() == expected.size(),"history count differs from canonical log");
  for (std::size_t i = 0; i < actual.size(); ++i) {
    const auto& a = actual[i]; const auto& e = expected[i];
    require(a.tradeId == e.id && a.timestamp == e.timestamp && a.price == e.price && a.quantity == e.quantity &&
            a.buyOrderId == e.buy_order && a.sellOrderId == e.sell_order && std::memcmp(a.symbol,e.symbol.data(),32) == 0 &&
            std::memcmp(a.buyer,e.buyer.data(),32) == 0 && std::memcmp(a.seller,e.seller.data(),32) == 0,"original trade fields changed");
  }
}
std::vector<p::TradeRecord> log_records(const std::filesystem::path& root) {
  p::File file(root/"trades/trades.log",false); std::uint64_t at = 0; std::vector<p::TradeRecord> records;
  while (auto frame = p::next_frame(file,at)) { p::Reader r{frame->payload}; for (unsigned i=0;i<frame->count;++i) records.push_back(p::decode_trade(r)); }
  return records;
}
void correctness(const std::filesystem::path& root) {
  auto opt = options(root); std::vector<p::TradeRecord> expected;
  {
    p::DurableEngine e(config(),1,opt);
    require(all(e,1).empty(),"new account history nonempty");
    for (unsigned i=0;i<600;++i) order(e,2,i+1,m::Side::Sell,1);
    order(e,1,601,m::Side::Buy,600); e.drain(); expected = log_records(root);
    require(expected.size() == 600,"fixture trade count");
    compare(all(e,1),expected); compare(all(e,2),expected); require(all(e,3).empty(),"third-party history leak");
    auto first = query(e,1,0,0,23);
    order(e,2,602,m::Side::Sell,1); order(e,1,603,m::Side::Buy,1); e.drain();
    std::vector<m::TradeHistoryRecord> frozen = first.records;
    while (first.end.flags & m::history_more) {
      first = query(e,1,first.end.nextOffset,first.end.throughOffset,23);
      require(first.error == m::RejectReason{},"frozen page rejected"); frozen.insert(frozen.end(),first.records.begin(),first.records.end());
    }
    compare(frozen,expected);
    expected = log_records(root); compare(all(e,1),expected);
    order(e,3,604,m::Side::Sell,1); order(e,3,605,m::Side::Buy,1); e.drain();
    auto self = all(e,3); require(self.size() == 1 && o::fixed_string(self[0].buyer) == "Carol" && o::fixed_string(self[0].seller) == "Carol","self-trade duplicated");
    require(query(e,1,0,e.boundary().trade_end+1).error == m::RejectReason::InvalidHistoryQuery,"future cursor accepted");
    require(query(e,1,0,0,257).error == m::RejectReason::InvalidHistoryQuery,"oversized query accepted");
    require(query(e,1,0,100).error == m::RejectReason::InvalidHistoryQuery,"partial-frame cursor accepted");
    compare(all(e,1),expected); // Invalid client cursors must not disable the index.
  }
  {
    p::DurableEngine e(config(),1,opt); compare(all(e,1),expected); require(all(e,3).size()==1,"self-trade reopen");
  }
  // A bad derived checkpoint is discarded, while the trade file stays authoritative.
  auto snapshot = root/"history/index.snapshot";
  { std::fstream file(snapshot,std::ios::in|std::ios::out|std::ios::binary); file.seekp(123); file.put('\x55'); }
  {
    p::DurableEngine e(config(),1,opt); compare(all(e,1),expected);
  }
  // Existing deployments have logs but no B+ tree at all.
  std::filesystem::remove_all(root/"history");
  { p::DurableEngine e(config(),1,opt); compare(all(e,1),expected); }
  std::cout << "PASS history fields, both parties, self-trade, pagination, fixed prefix, cursor validation, checkpoint repair, legacy bootstrap\n";
}
void commit_barrier(const std::filesystem::path& root) {
  std::mutex mutex; std::condition_variable cv; bool blocked=false, released=false;
  auto opt=options(root);
  opt.hook=[&](p::Stage stage,std::uint64_t seq) {
    if (seq==2 && stage==p::Stage::BeforeOrderSync) { std::unique_lock lock(mutex); blocked=true; cv.notify_all(); cv.wait(lock,[&] { return released; }); }
  };
  p::DurableEngine e(config(),1,opt);
  struct Release { std::mutex& m; std::condition_variable& cv; bool& ready; ~Release() { { std::lock_guard lock(m); ready=true; } cv.notify_all(); } } release{mutex,cv,released};
  order(e,2,1,m::Side::Sell,1);e.drain();order(e,1,2,m::Side::Buy,1);
  eventually([&] { std::lock_guard lock(mutex); return blocked; });
  require(all(e,1).empty(),"uncommitted trade visible in history");
  { std::lock_guard lock(mutex); released=true; } cv.notify_all();e.drain();
  require(all(e,1).size()==1,"committed trade missing");
  std::cout << "PASS dual-log commit visibility barrier\n";
}
void record_corruption(const std::filesystem::path& root) {
  auto opt=options(root);p::DurableEngine e(config(),1,opt);
  order(e,2,1,m::Side::Sell,1);order(e,1,2,m::Side::Buy,1);e.drain();
  require(all(e,1).size()==1,"corruption fixture");
  {
    std::fstream log(root/"trades/trades.log",std::ios::in|std::ios::out|std::ios::binary);
    log.seekp(p::header_bytes+24);log.put('\x7f');log.flush();
  }
  auto page=query(e,1);
  require(page.error==m::RejectReason::HistoryUnavailable && page.records.empty(),"bad record was returned as successful history");
  require(query(e,1).error==m::RejectReason::HistoryUnavailable,"failed index did not reject subsequent requests");
  std::cout << "PASS per-record CRC rejects corrupted log payload without partial-page success\n";
}
}
int main(int argc,char** argv) {
  try {
    const auto root=std::filesystem::path(argc>1?argv[1]:"history-results")/std::to_string(getpid());
    correctness(root/"correctness"); commit_barrier(root/"barrier"); record_corruption(root/"corrupt-record");
  } catch (const std::exception& e) { std::cerr << "FAIL " << e.what() << '\n'; return 1; }
}
