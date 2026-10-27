#include "tests/gateway_support.hpp"
#include <limits>

using namespace marketlab;
using namespace marketlab::orders;
using namespace marketlab::orders::test;
namespace {
void fill(Client& client,OrderId order,OrderStatus status,Quantity quantity,Quantity filled,Price price) {
  const auto trade = client.receive_as<Trade>(MessageType::Trade);
  check(trade.header.requestId == 0 && trade.orderId == order && trade.quantity == quantity && trade.price == price,"Trade payload/routing");
  const auto update = client.receive_as<OrderUpdate>(MessageType::OrderUpdate);
  check(update.header.requestId == 0 && update.order.orderId == order && update.order.status == status &&
      update.order.filledQuantity == filled && update.timestamp == trade.timestamp,"Fill state or timestamp changed");
}
}
int main(int argc,char** argv) {
  try {
    check(argc == 4,"Expected binary, artifact directory, direct|router");
    const bool direct = std::string(argv[3]) == "direct";
    Fixture f(argv[1],argv[2],direct,true);
    Cases cases(f.directory / "integration.csv");
    cases.run("authentication_and_empty_snapshot",[&] {
      Client bad(f.config.gateway_listen);
      LoginRequest login{}; login.header = {MessageType::LoginRequest,sizeof(login),42};
      put_string(login.username,"alice"); put_string(login.password,"wrong"); bad.send(login);
      bad.expect_reject(42,RejectReason::InvalidCredentials);
      Client good(f.config.gateway_listen); check(good.login("alice","pass",42).empty(),"Expected empty snapshot");
      Client unauth(f.config.gateway_listen); unauth.send(Client::new_request(1,"AAPL",Side::Buy,90,1)); unauth.expect_closed();
    });
    cases.run("bad_credentials_preserve_online_session",[&] {
      Client alice(f.config.gateway_listen); alice.login("alice");
      Client bad(f.config.gateway_listen); LoginRequest login{};
      login.header = {MessageType::LoginRequest,sizeof(login),7}; put_string(login.username,"alice"); put_string(login.password,"wrong"); bad.send(login);
      bad.expect_reject(7,RejectReason::InvalidCredentials);
      const auto a = alice.order(8,"AAPL",Side::Buy,90,1); alice.cancel(9,a.order.orderId);
    });
    cases.run("successful_relogin_invalidates_old_connection",[&] {
      Client old(f.config.gateway_listen); old.login("alice");
      const auto a = old.order(2,"AAPL",Side::Buy,90,10);
      Client next(f.config.gateway_listen); const auto snapshot = next.login("alice","pass",2);
      check(snapshot.size() == 1 && snapshot[0].orderId == a.order.orderId,"Open order lost on takeover");
      old.expect_closed(); next.cancel(3,a.order.orderId);
    });
    cases.run("same_tcp_reauthentication_restarts_snapshot",[&] {
      Client client(f.config.gateway_listen); client.login("alice");
      const auto a = client.order(2,"AAPL",Side::Buy,90,1);
      const auto snapshot = client.login("alice","pass",2);
      check(snapshot.size() == 1 && snapshot[0].orderId == a.order.orderId,"Same-TCP reauthentication lost snapshot");
      client.cancel(3,a.order.orderId);
    });
    cases.run("invalid_orders_and_price_boundaries",[&] {
      Client alice(f.config.gateway_listen); alice.login("alice");
      RequestId id = 10;
      const std::vector<NewOrderRequest> invalid{
        Client::new_request(id++,"UNKNOWN",Side::Buy,100,1),
        Client::new_request(id++,"AAPL",Side::Buy,0,1), Client::new_request(id++,"AAPL",Side::Buy,-1,1),
        Client::new_request(id++,"AAPL",Side::Buy,100,0), Client::new_request(id++,"AAPL",Side::Buy,100,-1),
        Client::new_request(id++,"AAPL",static_cast<Side>(3),100,1),
        Client::new_request(id++,"AAPL",Side::Buy,89,1), Client::new_request(id++,"AAPL",Side::Buy,111,1),
        Client::new_request(id++,"AAPL",Side::Buy,std::numeric_limits<std::int32_t>::max(),1)};
      for (const auto& r : invalid) { alice.send(r); alice.expect_reject(r.header.requestId,RejectReason::InvalidOrder); }
      for (auto price : {90,110}) { const auto a = alice.order(id++,"AAPL",Side::Buy,price,1); alice.cancel(id++,a.order.orderId); }
      for (auto price : {90,111}) { const auto a = alice.order(id++,"FRACT",Side::Buy,price,1); alice.cancel(id++,a.order.orderId); }
      const auto big = alice.order(id++,"AAPL",Side::Buy,90,std::numeric_limits<std::int32_t>::max()); alice.cancel(id++,big.order.orderId);
    });
    cases.run("fifo_partial_fills_and_cancel_validation",[&] {
      Client alice(f.config.gateway_listen),bob(f.config.gateway_listen); alice.login("alice"); bob.login("bob");
      const auto a = alice.order(100,"AAPL",Side::Sell,100,3);
      const auto b = alice.order(101,"AAPL",Side::Sell,100,4);
      const auto buy = bob.order(100,"AAPL",Side::Buy,102,5);
      fill(bob,buy.order.orderId,OrderStatus::PartiallyFilled,3,3,100);
      fill(bob,buy.order.orderId,OrderStatus::Filled,2,5,100);
      fill(alice,a.order.orderId,OrderStatus::Filled,3,3,100);
      fill(alice,b.order.orderId,OrderStatus::PartiallyFilled,2,2,100);
      bob.send_cancel(102,b.order.orderId); bob.expect_reject(102,RejectReason::CancelFailed);
      bob.send_cancel(103,buy.order.orderId); bob.expect_reject(103,RejectReason::CancelFailed);
      bob.send_cancel(104,0); bob.expect_reject(104,RejectReason::CancelFailed);
      Client replacement(f.config.gateway_listen); const auto snapshot = replacement.login("alice");
      check(snapshot.size() == 1 && snapshot[0].orderId == b.order.orderId && snapshot[0].originalQuantity == 4 &&
          snapshot[0].filledQuantity == 2 && snapshot[0].remainingQuantity == 2,"Partially filled snapshot");
      const auto canceled = replacement.cancel(105,b.order.orderId); check(canceled.order.filledQuantity == 2,"Canceled order lost fills");
      replacement.send_cancel(106,b.order.orderId); replacement.expect_reject(106,RejectReason::CancelFailed);
      replacement.send_cancel(107,(1ULL << 48) | 123456789); replacement.expect_reject(107,RejectReason::CancelFailed);
    });
    cases.run("price_priority_and_resting_price",[&] {
      Client alice(f.config.gateway_listen),bob(f.config.gateway_listen); alice.login("alice"); bob.login("bob");
      const auto high = alice.order(1,"AAPL",Side::Sell,101,1);
      const auto low = alice.order(2,"AAPL",Side::Sell,100,1);
      const auto buy = bob.order(1,"AAPL",Side::Buy,102,1);
      fill(bob,buy.order.orderId,OrderStatus::Filled,1,1,100); fill(alice,low.order.orderId,OrderStatus::Filled,1,1,100);
      alice.cancel(3,high.order.orderId);
    });
    cases.run("self_trade_two_participant_notifications",[&] {
      Client alice(f.config.gateway_listen); alice.login("alice");
      const auto sell = alice.order(1,"MSFT",Side::Sell,200,3);
      const auto buy = alice.order(2,"MSFT",Side::Buy,200,2);
      const auto t1 = alice.receive_as<Trade>(MessageType::Trade);
      const auto u1 = alice.receive_as<OrderUpdate>(MessageType::OrderUpdate);
      const auto t2 = alice.receive_as<Trade>(MessageType::Trade);
      const auto u2 = alice.receive_as<OrderUpdate>(MessageType::OrderUpdate);
      check(t1.tradeId == t2.tradeId && t1.orderId == buy.order.orderId && t2.orderId == sell.order.orderId &&
          t1.side == Side::Buy && t2.side == Side::Sell && u1.order.status == OrderStatus::Filled &&
          u2.order.status == OrderStatus::PartiallyFilled && t1.timestamp == t2.timestamp,"Self trade participant fanout");
      check(t1.header.requestId == 0 && t2.header.requestId == 0 && u1.header.requestId == 0 && u2.header.requestId == 0,"Unsolicited correlation");
      alice.cancel(3,sell.order.orderId);
    });
    cases.run("cross_shard_ids_and_request_id_collisions",[&] {
      Client alice(f.config.gateway_listen),bob(f.config.gateway_listen); alice.login("alice"); bob.login("bob");
      const auto a = alice.order(2,"AAPL",Side::Buy,90,1), b = bob.order(2,"MSFT",Side::Buy,180,2);
      check(a.order.orderId != b.order.orderId && (a.order.orderId >> 48) == 1 && (b.order.orderId >> 48) == 2,"Global ID uniqueness/routing");
      bob.send_cancel(3,a.order.orderId); bob.expect_reject(3,RejectReason::CancelFailed);
      alice.cancel(3,a.order.orderId); bob.cancel(4,b.order.orderId);
      const auto aa = alice.order(5,"AAPL",Side::Buy,90,1), bb = alice.order(6,"MSFT",Side::Buy,180,1);
      Client next(f.config.gateway_listen); const auto snapshot = next.login("alice","pass",7);
      check(snapshot.size() == 2,"Snapshot did not cover both shards");
      next.cancel(8,aa.order.orderId); next.cancel(9,bb.order.orderId);
    });
    cases.run("disconnected_orders_remain_live",[&] {
      Client alice(f.config.gateway_listen),bob(f.config.gateway_listen); alice.login("alice"); bob.login("bob");
      const auto a = alice.order(1,"MSFT",Side::Buy,180,10); alice.fd.reset();
      const auto sell = bob.order(2,"MSFT",Side::Sell,180,3); fill(bob,sell.order.orderId,OrderStatus::Filled,3,3,180);
      Client next(f.config.gateway_listen); const auto snapshot = next.login("alice");
      check(snapshot.size() == 1 && snapshot[0].orderId == a.order.orderId && snapshot[0].filledQuantity == 3 && snapshot[0].remainingQuantity == 7,"Disconnected order stopped matching");
      next.cancel(2,a.order.orderId);
    });
    cases.run("coalesced_login_order_and_snapshot_interleaving",[&] {
      Client alice(f.config.gateway_listen);
      LoginRequest login{}; login.header = {MessageType::LoginRequest,sizeof(login),800}; put_string(login.username,"alice"); put_string(login.password,"pass");
      auto combined = record_bytes(login); const auto order = record_bytes(Client::new_request(801,"AAPL",Side::Buy,90,1));
      combined.insert(combined.end(),order.begin(),order.end()); alice.send_bytes(combined);
      check(alice.receive_as<LoginAccepted>(MessageType::LoginAccepted).header.requestId == 800,"Login response not first");
      bool end = false, accepted = false; OrderId id = 0;
      while (!end || !accepted) {
        const auto bytes = alice.receive(); Header h{}; std::memcpy(&h,bytes.data(),sizeof(h));
        if (h.messageType == MessageType::ActiveOrderSnapshot) {
          const auto s = copy_record<ActiveOrderSnapshot>(bytes); check(h.requestId == 800,"Snapshot stream ID");
          if (s.isEnd) { check(!end,"Duplicate terminal marker"); end = true; }
        } else {
          const auto update = copy_record<OrderUpdate>(bytes);
          check(h.requestId == 801 && update.order.status == OrderStatus::Accepted,"Interleaved request mismatch"); accepted = true; id = update.order.orderId;
        }
      }
      alice.cancel(802,id);
    });
    cases.run("idle_persistent_links_and_malformed_client_isolation",[&] {
      Client alice(f.config.gateway_listen); alice.login("alice"); std::this_thread::sleep_for(2100ms);
      Client partial(f.config.gateway_listen); const std::uint8_t byte = 1; partial.send_bytes(std::span(&byte,1)); partial.expect_closed();
      Client bad(f.config.gateway_listen); Header header{MessageType::NewOrderRequest,0xffffffffU,1}; bad.send(header); bad.expect_closed();
      const auto a = alice.order(2,"AAPL",Side::Buy,90,1); alice.cancel(3,a.order.orderId);
    });
    cases.run("slow_client_does_not_block_other_accounts",[&] {
      Client alice(f.config.gateway_listen),slow(f.config.gateway_listen); alice.login("alice"); slow.login("carol");
      Bytes batch;
      for (RequestId id = 1000; id < 4000; ++id) {
        const auto bytes = record_bytes(Client::new_request(id,"AAPL",Side::Buy,0,1)); batch.insert(batch.end(),bytes.begin(),bytes.end());
      }
      try { slow.send_bytes(batch); } catch (const std::exception&) { /* Pending limit may close the overloaded client. */ }
      const auto start = md::Clock::now();
      const auto a = alice.order(2,"AAPL",Side::Buy,90,1); alice.cancel(3,a.order.orderId);
      check(elapsed_ms(start) < 1500,"Slow client blocked healthy request");
    });
    cases.run("engine_link_failure_and_reconnection",[&] {
      Client alice(f.config.gateway_listen); alice.login("alice");
      const auto a = alice.order(1,"AAPL",Side::Buy,90,1);
      if (direct) { f.gateway.stop(); f.start_gateway(); }
      else { f.router.stop(); alice.expect_closed(); f.start_router(); }
      f.wait_ready(); Client next(f.config.gateway_listen); const auto snapshot = next.login("alice");
      check(snapshot.size() == 1 && snapshot[0].orderId == a.order.orderId,"Reconnect fabricated/lost live order"); next.cancel(2,a.order.orderId);
      f.shards[1].stop(); next.expect_closed(); f.start_shard(1); f.wait_ready();
      Client recovered(f.config.gateway_listen); check(recovered.login("alice").empty(),"Missing snapshot recovery");
      const auto b = recovered.order(3,"MSFT",Side::Buy,180,1); recovered.cancel(4,b.order.orderId);
    });
    cases.run("existing_market_data_tcp_and_shared_memory_adapter",[&] {
      f.shards[0].stop();
      const auto port = free_port(); const auto path = (f.directory / "market.mmap").string();
      f.start_shard(0,port,path); f.wait_ready();
      Client alice(f.config.gateway_listen); alice.login("alice");
      const auto maximum = std::numeric_limits<std::int32_t>::max();
      const auto a = alice.order(1,"AAPL",Side::Buy,90,maximum), b = alice.order(2,"AAPL",Side::Buy,90,maximum);
      md::ShardConfig shard{"gateway-adapter","tcp","127.0.0.1",port,path,{1,3,4}};
      auto tcp = md::make_source(shard,1000); const auto view = tcp->read(deadline());
      check(view.books.size() == 3 && view.books.at(3).levels.empty() && view.books.at(1).levels.size() == 1,"Market adapter coverage");
      const auto level = view.books.at(1).levels[0];
      check(level.price == 90 && level.quantity == 2ULL * maximum && level.num_orders == 2,"Market aggregates lost order information");
      shard.transport = "shm"; auto shared = md::make_source(shard,1000);
      const auto until = deadline(); bool visible = false;
      while (md::Clock::now() < until) {
        const auto shared_view = shared->read(deadline());
        if (shared_view.books.at(1) == view.books.at(1)) { visible = true; break; }
        std::this_thread::sleep_for(10ms);
      }
      check(visible,"Shared adapter did not publish matching state"); alice.cancel(3,a.order.orderId); alice.cancel(4,b.order.orderId);
    });
    f.gateway.stop(); f.router.stop(); for (auto& shard : f.shards) shard.stop();
    check(metric(f.directory / "gateway.json","commands") > 0,"No commands traversed TCP");
    check(metric(f.directory / "shard1.json","commands") > 0 && metric(f.directory / "shard2.json","commands") > 0,"Both shard interfaces not exercised");
    std::cout << "gateway_integration topology=" << argv[3] << " cases=" << cases.count << " PASS artifacts=" << f.directory << '\n';
    return 0;
  } catch (const std::exception& e) { std::cerr << "FAIL " << e.what() << '\n'; return 1; }
}
