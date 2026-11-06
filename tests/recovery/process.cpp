#include <unistd.h>

#include "network.hpp"

using namespace demo;
int main(int argc, char** argv) {
  try {
    require(argc == 3, "Usage: recovery_process_test BIN_DIRECTORY OUTPUT_DIRECTORY");
    auto output = fs::absolute(argv[2]);
    fs::create_directories(output);
    std::string temp = (output / "process-XXXXXX").string();
    require(mkdtemp(temp.data()), "mkdtemp failed");
    const fs::path root = temp;
    Exchange ex(fs::absolute(argv[1]), root, 1, 1);
    auto start = [&] {
      ex.engines[0]->launch(ex.bin / "market_matching_engine",
                            {"--config", (root / "exchange.conf").string(), "--shard", "1", "--market-port",
                             std::to_string(ex.markets[0].port), "--checkpoint-ms", "100"},
                            root / "engine-restarted.log");
    };
    ex.engines[0]->stop();
    start();
    eventually(
        [&] {
          try {
            Trader t(ex.gateway);
            t.login("user39");
            return true;
          } catch (...) {
            return false;
          }
        },
        "Persistent engine restart readiness", 12000);
    const auto market = ex.market_config();
    write_market(market, root / "publisher.conf");
    Child publisher;
    publisher.launch(ex.bin / "market_broadcast", {"publish", "--config", (root / "publisher.conf").string()},
                     root / "publisher.log");
    publisher_ready(market);
    std::uint64_t carol_id = 0;
    {
      Trader bob(ex.gateway), carol(ex.gateway), alice(ex.gateway);
      bob.login("user1");
      carol.login("user2");
      alice.login("user0");
      bob.add(10, symbol(0), ml::Side::Sell, 100, 4);
      carol_id = carol.add(11, symbol(0), ml::Side::Sell, 100, 7).order.orderId;
      alice.add(12, symbol(0), ml::Side::Buy, 100, 8);
      for (unsigned i = 0; i < 2; ++i) {
        alice.get<ml::Trade>(ml::MessageType::Trade);
        alice.get<ml::OrderUpdate>(ml::MessageType::OrderUpdate);
      }
      bob.get<ml::Trade>(ml::MessageType::Trade);
      bob.get<ml::OrderUpdate>(ml::MessageType::OrderUpdate);
      carol.get<ml::Trade>(ml::MessageType::Trade);
      carol.get<ml::OrderUpdate>(ml::MessageType::OrderUpdate);
    }
    eventually(
        [&] {
          try {
            const auto s = snapshot(market.query, 1);
            return s.levels.size() == 1 && s.levels[0].quantity == 3 && s.levels[0].num_orders == 1;
          } catch (...) {
            return false;
          }
        },
        "Publisher did not show partially filled order", 12000);
    const auto checkpoint_dir = root / ".marketlab-data/exchange/shard-1/checkpoints";
    eventually(
        [&] {
          for (const auto& f : fs::directory_iterator(checkpoint_dir))
            if (f.path().extension() == ".bin" && f.path().filename() != "book-0.bin") return true;
          return false;
        },
        "No durable checkpoint produced");
    const auto before = snapshot(market.query, 1);
    ex.engines[0]->crash();
    start();
    eventually(
        [&] {
          try {
            Trader t(ex.gateway);
            const auto orders = t.login("user2");
            return orders.size() == 1 && orders[0].orderId == carol_id && orders[0].remainingQuantity == 3 &&
                   orders[0].filledQuantity == 4;
          } catch (...) {
            return false;
          }
        },
        "Active order did not recover after kill -9", 12000);
    eventually(
        [&] {
          try {
            const auto after = snapshot(market.query, 1);
            return after.epoch != before.epoch && after.levels == before.levels;
          } catch (...) {
            return false;
          }
        },
        "Publisher did not adopt recovered engine epoch/book", 12000);
    std::cout << "PASS kill9_restart_gateway_snapshot_and_live_publisher\nRESULTS " << root << std::endl;
    return 0;
  } catch (const std::exception& e) {
    std::cerr << "FAIL " << e.what() << std::endl;
    return 1;
  }
}
