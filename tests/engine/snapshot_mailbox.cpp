#include "harness.hpp"
#include "matching_engine/engine.hpp"
#include "matching_engine/snapshot_mailbox.hpp"
#include "shared_data/orders/tcp_reactor.hpp"
#include <optional>
#include <unistd.h>

using namespace demo;
namespace od=marketlab::orders;
namespace md=marketlab::md;
namespace ml=marketlab;
namespace {
class FakeEngine final : public od::EngineBackend {
 public:
  std::atomic_bool block{false},entered{false},release{false};
  unsigned generation=0;
  void command(const od::LinkHeader&,std::span<const std::uint8_t>,const Emit&) override {}
  std::size_t symbol_count() const noexcept override { return 1; }
  void copy_market(od::MarketSnapshot& out) noexcept override {
    entered=true;
    const auto deadline=Clock::now()+2s;
    while(block.load() && !release.load() && Clock::now()<deadline) std::this_thread::yield();
    out.epoch[0]=1;
    out.books[0].symbol_id=1; out.books[0].count=1;
    out.books[0].levels[0]={md::Side::Buy,100,++generation,1};
  }
};
struct ReadTask {
  std::optional<md::ShardView> value;
  std::exception_ptr error;
  std::thread thread;
  ReadTask(od::SnapshotMailbox& box,Clock::time_point deadline)
      :thread([&,deadline] { try { value=box.read(deadline); } catch(...) { error=std::current_exception(); } }) {}
  ~ReadTask() { if(thread.joinable()) thread.join(); }
  void join() { if(thread.joinable()) thread.join(); }
  const md::ShardView& get() { join(); if(error) std::rethrow_exception(error); require(value.has_value(),"Missing result"); return *value; }
};
void request_arrived(od::SnapshotMailbox& box) {
  pollfd p{box.request_fd(),POLLIN,0};
  require(poll(&p,1,2000)>0,"Request did not signal owner");
  unsigned char byte;
  require(::read(box.request_fd(),&byte,1)==1,"Missing notification byte");
}
void idle_wakeup() {
  FakeEngine engine; od::SnapshotMailbox box(1); od::TcpReactor reactor;
  unsigned wakes=0;
  reactor.watch_wakeup(box.request_fd(),[&] { ++wakes; box.drain_requests(); box.service(engine); });
  ReadTask read(box,Clock::now()+2s);
  reactor.step(1500);
  require(wakes==1 && read.get().books.at(1).levels[0].quantity==1,"Idle owner failed to answer");
}
void timeout_and_reuse() {
  FakeEngine engine; od::SnapshotMailbox box(1);
  for(unsigned i=0;i<3*od::SnapshotMailbox::capacity;++i) {
    bool timed_out=false;
    try { box.read(Clock::now()+1ms); }
    catch(const std::exception& e) { timed_out=std::string(e.what()).find("timeout")!=std::string::npos; }
    require(timed_out,"Queued request did not time out or leaked capacity");
  }
  box.drain_requests(); box.service(engine);
  require(engine.generation==0,"Canceled requests still copied state");
  ReadTask read(box,Clock::now()+2s); request_arrived(box); box.service(engine);
  require(read.get().books.at(1).levels[0].quantity==1,"Stale result after timeout/reuse");
}
void saturation() {
  FakeEngine engine; od::SnapshotMailbox box(1);
  std::vector<std::unique_ptr<ReadTask>> readers;
  for(unsigned i=0;i<od::SnapshotMailbox::capacity;++i) {
    readers.push_back(std::make_unique<ReadTask>(box,Clock::now()+2s)); request_arrived(box);
  }
  bool full=false;
  try { box.read(Clock::now()+1s); } catch(const std::exception& e) { full=std::string(e.what()).find("full")!=std::string::npos; }
  require(full,"Mailbox did not enforce capacity");
  box.service(engine);
  for(auto& reader:readers) require(reader->get().books.size()==1,"Saturation lost accepted request");
  require(engine.generation==od::SnapshotMailbox::capacity,"Wrong number of copies");
}
void abandon_during_copy() {
  FakeEngine engine; engine.block=true; od::SnapshotMailbox box(1);
  ReadTask expired(box,Clock::now()+100ms); request_arrived(box);
  std::thread owner([&] { box.service(engine); });
  // No assertions until the owner has joined, including failure paths.
  expired.join(); engine.release=true; owner.join();
  require(engine.entered && expired.error,"Writing request did not expire");
  engine.block=false;
  for(unsigned i=0;i<3*od::SnapshotMailbox::capacity;++i) {
    ReadTask read(box,Clock::now()+2s); request_arrived(box); box.service(engine);
    require(read.get().books.at(1).levels[0].quantity==i+2,"Abandoned slot reused early or late response leaked");
  }
}
void shutdown() {
  od::SnapshotMailbox box(1); ReadTask read(box,Clock::now()+5s); request_arrived(box);
  box.close(); read.join();
  require(bool(read.error),"Shutdown returned an unproduced view");
  bool stopped=false;
  try { box.read(Clock::now()+1s); } catch(const std::exception& e) { stopped=std::string(e.what()).find("stopped")!=std::string::npos; }
  require(stopped,"Closed mailbox accepted a new request");
}
void fresh_command_boundary() {
  od::OrderConfig c; c.accounts={{"one","test"}};
  c.shards={{1,{"127.0.0.1",1}}}; c.products={{"A",100,1},{"B",100,1}};
  od::MatchingEngine engine(c,1); od::SnapshotMailbox box(engine.symbol_count());
  ReadTask read(box,Clock::now()+2s); request_arrived(box);
  ml::NewOrderRequest order{}; order.header={ml::MessageType::NewOrderRequest,sizeof(order),1};
  od::put_string(order.symbol,"A"); order.side=ml::Side::Buy; order.price=100; order.quantity=7;
  engine.command({0,od::LinkKind::NewOrder,1,1,1},od::record_bytes(order),[](auto,const auto&,auto){});
  box.service(engine);
  const auto& view=read.get();
  require(view.books.size()==2 && view.books.at(1).levels.at(0).quantity==7 && view.books.at(2).levels.empty(),
      "Snapshot was stale or omitted an empty symbol");
}
}
int main(int argc,char** argv) {
  try {
    require(argc==2,"Expected output directory"); Suite suite(argv[1]);
    suite.test("mailbox_idle_reactor_wakeup",idle_wakeup);
    suite.test("mailbox_timeout_cancel_and_reuse",timeout_and_reuse);
    suite.test("mailbox_saturation_and_recovery",saturation);
    suite.test("mailbox_abandon_while_copying",abandon_during_copy);
    suite.test("mailbox_shutdown_wakes_reader",shutdown);
    suite.test("mailbox_fresh_command_boundary_and_empty_symbol",fresh_command_boundary);
    return suite.finish();
  } catch(const std::exception& e) { std::cerr<<e.what()<<'\n'; return 2; }
}
