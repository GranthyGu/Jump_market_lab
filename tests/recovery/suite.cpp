#include <sys/wait.h>
#include <unistd.h>

#include <chrono>
#include <future>
#include <iostream>
#include <set>

#include "matching_engine/persistence/durable_engine.hpp"

namespace p = marketlab::orders::persistence;
namespace o = marketlab::orders;
namespace m = marketlab;
using namespace std::chrono_literals;
namespace {
void require(bool x, const char* why) {
  if (!x) throw std::runtime_error(why);
}
template <class F>
void eventually(F f) {
  auto end = std::chrono::steady_clock::now() + 5s;
  while (!f()) {
    if (std::chrono::steady_clock::now() > end) throw std::runtime_error("Test synchronization timed out");
    std::this_thread::sleep_for(1ms);
  }
}
struct Gate {
  std::mutex mutex;
  std::condition_variable cv;
  bool arrived = false, open = false;
  void enter() {
    std::unique_lock lock(mutex);
    arrived = true;
    cv.notify_all();
    cv.wait(lock, [&] { return open; });
  }
  bool reached() {
    std::lock_guard lock(mutex);
    return arrived;
  }
  void release() {
    std::lock_guard lock(mutex);
    open = true;
    cv.notify_all();
  }
};
struct Release {
  Gate& gate;
  ~Release() { gate.release(); }
};
o::OrderConfig config() {
  o::OrderConfig c;
  c.accounts = {{"Alice", "demo"}, {"Bob", "demo"}, {"Carol", "demo"}};
  c.products = {{"ITEM0001", 100, 1}};
  c.shards = {{1, {"127.0.0.1", 1}}};
  return c;
}
p::Options options(const std::filesystem::path& base, const std::string& name) {
  p::Options x;
  x.directory = base / name;
  x.checkpoint_interval = 1h;
  return x;
}
m::NewOrderRequest order(std::uint64_t req, m::Side side, int qty) {
  m::NewOrderRequest r{};
  r.header = {m::MessageType::NewOrderRequest, sizeof(r), req};
  o::put_string(r.symbol, "ITEM0001");
  r.side = side;
  r.price = 100;
  r.quantity = qty;
  return r;
}
struct Events {
  std::vector<p::Event> values;
  o::EngineBackend::Emit emit() {
    return [this](auto kind, const auto& h, auto bytes) {
      p::Event e;
      e.kind = kind;
      e.context = h;
      e.size = bytes.size();
      std::copy(bytes.begin(), bytes.end(), e.payload.begin());
      values.push_back(e);
    };
  }
  m::OrderUpdate accepted(std::size_t at = 0) {
    return o::copy_record<m::OrderUpdate>({values.at(at).payload.data(), values.at(at).size});
  }
};
void add(p::DurableEngine& e, Events& events, unsigned user, std::uint64_t request, m::Side side,
         int quantity) {
  auto r = order(request, side, quantity);
  e.command({0, o::LinkKind::NewOrder, request, user, 1}, o::record_bytes(r), events.emit());
}
void cancel(p::DurableEngine& e, Events& events, unsigned user, std::uint64_t request, std::uint64_t id) {
  m::CancelRequest r{{m::MessageType::CancelRequest, sizeof(r), request}, id};
  e.command({0, o::LinkKind::Cancel, request, user, 1}, o::record_bytes(r), events.emit());
}
void ask_quantity(p::DurableEngine& e, std::uint64_t quantity, std::uint64_t count) {
  auto v = e.market_view();
  const auto& levels = v.books.at(1).levels;
  if (!quantity) {
    require(levels.empty(), "Expected empty recovered book");
    return;
  }
  require(levels.size() == 1 && levels[0].side == m::md::Side::Sell && levels[0].quantity == quantity &&
              levels[0].num_orders == count,
          "Wrong committed/recovered market");
}
void appended_remote_products(const std::filesystem::path& root) {
  const auto opt = options(root, "appended-remote-products");
  auto original = config();
  original.shards.push_back({2, {"127.0.0.1", 2}});
  original.products.push_back({"REMOTE", 100, 2});
  std::uint64_t maker = 0;
  {
    p::DurableEngine engine(original, 1, opt);
    Events events;
    add(engine, events, 2, 1, m::Side::Sell, 10);
    engine.drain();
    maker = events.accepted().order.orderId;
    add(engine, events, 1, 2, m::Side::Buy, 4);
    engine.drain();
    engine.request_checkpoint();
    engine.wait_checkpoint();
  }

  auto reject = [&](const o::OrderConfig& changed) {
    bool failed = false;
    try {
      p::DurableEngine engine(changed, 1, opt);
    } catch (const std::runtime_error& error) {
      failed = std::string(error.what()) == "Persistence configuration/shard mismatch";
    }
    require(failed, "Incompatible config was accepted during shard expansion");
  };
  auto changed = original;
  changed.products.push_back({"NEW_LOCAL", 100, 1});
  reject(changed);
  changed = original;
  changed.products[0].previous_close = 101;
  reject(changed);
  changed = original;
  changed.products[1].symbol = "RENAMED_REMOTE";
  reject(changed);
  changed = original;
  changed.products[1].shard = 1;
  reject(changed);
  changed = original;
  std::swap(changed.products[0], changed.products[1]);
  reject(changed);
  changed = original;
  changed.products.pop_back();
  reject(changed);
  changed = original;
  std::swap(changed.accounts[0], changed.accounts[1]);
  reject(changed);

  auto expanded = original;
  expanded.shards.push_back({3, {"127.0.0.1", 3}});
  expanded.products.push_back({"NEW_REMOTE", 120, 3});
  {
    p::DurableEngine engine(expanded, 1, opt);
    ask_quantity(engine, 6, 1);
    require(engine.boundary().seq == 2 && engine.boundary().trade_count == 1,
            "Expansion lost the committed trade boundary");
    Events events;
    cancel(engine, events, 2, 3, maker);
    engine.drain();
    require(events.accepted().order.orderId == maker &&
                events.accepted().order.status == m::OrderStatus::Canceled,
            "Expansion changed the recovered order identity");
    ask_quantity(engine, 0, 0);
    engine.request_checkpoint();
    engine.wait_checkpoint();
  }
  p::DurableEngine recovered(expanded, 1, opt);
  require(recovered.boundary().seq == 3 && recovered.boundary().trade_count == 1,
          "Expanded checkpoint did not recover");
  ask_quantity(recovered, 0, 0);
}

void notification_barrier(const std::filesystem::path& root, bool block_trade) {
  Gate gate;
  std::atomic_bool other_synced = false;
  std::mutex trace_mutex;
  std::vector<std::pair<p::Stage, std::uint64_t>> trace;
  auto opt = options(root, block_trade ? "trade-barrier" : "order-barrier");
  opt.hook = [&](auto stage, auto seq) {
    {
      std::lock_guard lock(trace_mutex);
      trace.emplace_back(stage, seq);
    }
    if (seq == 2 && stage == (block_trade ? p::Stage::BeforeTradeSync : p::Stage::BeforeOrderSync))
      gate.enter();
    if (seq == 2 && stage == (block_trade ? p::Stage::OrderSynced : p::Stage::TradeSynced))
      other_synced = true;
  };
  p::DurableEngine engine(config(), 1, opt);
  Release release{gate};
  Events events;
  add(engine, events, 2, 1, m::Side::Sell, 10);
  engine.drain();
  events.values.clear();
  add(engine, events, 1, 2, m::Side::Buy, 4);
  eventually([&] { return gate.reached() && other_synced.load(); });
  engine.poll();
  require(events.values.empty(), "Business notification before both syncs");
  require(engine.boundary().seq == 1, "Visible commit advanced early");
  ask_quantity(engine, 10, 1);
  gate.release();
  engine.drain();
  require(events.values.size() == 5, "Missing accepted/trade/update notifications");
  require(events.accepted().order.status == m::OrderStatus::Accepted, "Accepted did not precede trades");
  ask_quantity(engine, 6, 1);
  std::size_t emit = trace.size(), ts = trace.size(), os = trace.size();
  {
    std::lock_guard lock(trace_mutex);
    for (std::size_t i = 0; i < trace.size(); ++i) {
      if (trace[i].second == 2) {
        if (trace[i].first == p::Stage::Emitting) emit = i;
        if (trace[i].first == p::Stage::TradeSynced) ts = i;
        if (trace[i].first == p::Stage::OrderSynced) os = i;
      }
    }
  }
  require(ts < emit && os < emit, "Recorded file-sync/notification ordering violated");
}
void restart_fifo(const std::filesystem::path& root) {
  const auto opt = options(root, "fifo");
  std::uint64_t carol = 0, last_trade = 0;
  {
    p::DurableEngine e(config(), 1, opt);
    Events v;
    add(e, v, 2, 1, m::Side::Sell, 4);
    e.drain();
    v.values.clear();
    add(e, v, 3, 2, m::Side::Sell, 7);
    e.drain();
    carol = v.accepted().order.orderId;
    v.values.clear();
    add(e, v, 1, 3, m::Side::Buy, 8);
    e.drain();
    ask_quantity(e, 3, 1);
    for (const auto& event : v.values) {
      if (event.size == sizeof(m::Trade)) {
        auto t = o::copy_record<m::Trade>({event.payload.data(), event.size});
        last_trade = std::max(last_trade, m::TradeId(t.tradeId));
      }
    }
  }
  {
    p::DurableEngine e(config(), 1, opt);
    ask_quantity(e, 3, 1);
    Events v;
    add(e, v, 2, 4, m::Side::Sell, 2);
    e.drain();
    require(v.accepted().order.orderId > carol, "Recovered order ID reused");
    v.values.clear();
    add(e, v, 1, 5, m::Side::Buy, 3);
    e.drain();
    bool matched_carol = false;
    for (const auto& event : v.values) {
      if (event.size == sizeof(m::Trade)) {
        auto t = o::copy_record<m::Trade>({event.payload.data(), event.size});
        require(t.tradeId > last_trade, "Recovered trade ID reused");
        if (event.context.user == 3 && t.orderId == carol) matched_carol = true;
      }
    }
    require(matched_carol, "FIFO changed across restart");
    ask_quantity(e, 2, 1);
  }
}
void matching_while_syncing(const std::filesystem::path& root) {
  Gate gate;
  auto opt = options(root, "pipeline");
  opt.hook = [&](auto stage, auto seq) {
    if (stage == p::Stage::BeforeTradeSync && seq == 2) gate.enter();
  };
  p::DurableEngine e(config(), 1, opt);
  Release release{gate};
  Events v;
  add(e, v, 2, 1, m::Side::Sell, 10);
  e.drain();
  v.values.clear();
  add(e, v, 1, 2, m::Side::Buy, 4);
  eventually([&] { return gate.reached(); });
  // This command must return without waiting for the previous fsync. It
  // matches the working quantity (6), while the public book still says 10.
  add(e, v, 3, 3, m::Side::Buy, 2);
  require(v.values.empty() && e.boundary().seq == 1, "Pipeline exposed an uncommitted batch");
  ask_quantity(e, 10, 1);
  gate.release();
  e.drain();
  require(v.values.size() == 10 && e.boundary().seq == 3, "Pipeline lost an accepted match");
  require(v.accepted(0).header.requestId == 2 && v.accepted(5).header.requestId == 3,
          "Pipeline notifications reordered");
  ask_quantity(e, 4, 1);
}
void checkpoint_rotation(const std::filesystem::path& root) {
  Gate gate;
  auto opt = options(root, "checkpoint");
  opt.hook = [&](auto stage, auto seq) {
    if (stage == p::Stage::BeforeSnapshotPublish && seq == 2) gate.enter();
  };
  {
    p::DurableEngine e(config(), 1, opt);
    Release release{gate};
    Events v;
    add(e, v, 2, 1, m::Side::Sell, 4);
    e.drain();
    const auto first = v.accepted().order.orderId;
    v.values.clear();
    add(e, v, 3, 2, m::Side::Sell, 7);
    e.drain();
    e.request_checkpoint();
    eventually([&] {
      e.poll();
      return gate.reached();
    });
    require(std::filesystem::exists(opt.directory / "orders/delta-1.log"),
            "Old delta removed before snapshot publication");
    require(!std::filesystem::exists(opt.directory / "checkpoints/book-2.bin"),
            "Snapshot became visible before publication");
    cancel(e, v, 2, 3, first);
    add(e, v, 1, 4, m::Side::Buy, 2);
    e.drain();
    ask_quantity(e, 5, 1);
    require(std::filesystem::file_size(opt.directory / "orders/delta-3.log") > 0, "Post-cut deltas missing");
    gate.release();
    e.wait_checkpoint();
    require(e.checkpoint_sequence() == 2, "Unexpected checkpoint boundary");
    require(!std::filesystem::exists(opt.directory / "orders/delta-1.log"), "Covered delta not reclaimed");
    require(std::filesystem::exists(opt.directory / "orders/delta-3.log"),
            "Post-checkpoint delta was deleted");
  }
  opt.hook = {};
  p::DurableEngine e(config(), 1, opt);
  ask_quantity(e, 5, 1);
  require(e.boundary().seq == 4, "Recovery lost post-checkpoint batches");
}
void bounded_wait(const std::filesystem::path& root) {
  Gate gate;
  auto opt = options(root, "bounded");
  opt.pending_slots = 1;
  opt.sync_group = 1;
  opt.hook = [&](auto stage, auto seq) {
    if (stage == p::Stage::BeforeOrderSync && seq == 1) gate.enter();
  };
  p::DurableEngine e(config(), 1, opt);
  Events v;
  add(e, v, 2, 1, m::Side::Sell, 4);
  eventually([&] { return gate.reached(); });
  std::atomic_bool entered = false;
  auto future = std::async(std::launch::async, [&] {
    entered = true;
    add(e, v, 3, 2, m::Side::Sell, 7);
    e.drain();
  });
  Release release{gate};
  eventually([&] { return entered.load(); });
  require(future.wait_for(50ms) == std::future_status::timeout, "Full result pool did not wait");
  gate.release();
  future.get();
  require(v.values.size() == 2, "Backpressure dropped or duplicated accepted orders");
  ask_quantity(e, 11, 2);
}
void sync_failure(const std::filesystem::path& root) {
  auto opt = options(root, "failure");
  opt.hook = [](auto stage, auto seq) {
    if (stage == p::Stage::BeforeOrderSync && seq == 2) throw std::runtime_error("injected sync failure");
  };
  p::DurableEngine e(config(), 1, opt);
  Events v;
  add(e, v, 2, 1, m::Side::Sell, 4);
  e.drain();
  v.values.clear();
  bool failed = false;
  try {
    add(e, v, 1, 2, m::Side::Buy, 2);
    e.drain();
  } catch (const std::exception&) {
    failed = true;
  }
  require(failed && v.values.empty(), "Persistence failure was acknowledged");
  ask_quantity(e, 4, 1);
}
void tails_and_corruption(const std::filesystem::path& root) {
  const auto opt = options(root, "tail");
  {
    p::DurableEngine e(config(), 1, opt);
    Events v;
    add(e, v, 2, 1, m::Side::Sell, 4);
    e.drain();
  }
  {
    p::File f(opt.directory / "orders/delta-1.log", true);
    const std::array<std::uint8_t, 3> tail{1, 2, 3};
    f.append(tail);
    f.sync();
  }
  {
    p::DurableEngine e(config(), 1, opt);
    ask_quantity(e, 4, 1);
  }
  {
    p::File f(opt.directory / "orders/delta-1.log", true);
    const std::uint8_t bad = 0xff;
    require(pwrite(f.fd(), &bad, 1, p::header_bytes + 1) == 1, "Could not inject corruption");
    f.sync();
  }
  bool failed = false;
  try {
    p::DurableEngine e(config(), 1, opt);
  } catch (const std::exception&) {
    failed = true;
  }
  require(failed, "Complete corrupt frame was silently ignored");
}
// Exit directly from a writer/snapshot hook: no destructor, drain or cleanup.
// Parent has no live engine threads when fork() is called.
void crash_one_sided(const std::filesystem::path& root, bool only_trade) {
  auto opt = options(root, only_trade ? "trade-only-crash" : "order-only-crash");
  const auto child = fork();
  require(child >= 0, "fork failed");
  if (!child) {
    try {
      Gate gate;
      opt.hook = [&](auto stage, auto seq) {
        if (seq != 2) return;
        if (stage == (only_trade ? p::Stage::BeforeOrderWrite : p::Stage::BeforeTradeWrite)) gate.enter();
        if (stage == (only_trade ? p::Stage::TradeSynced : p::Stage::OrderSynced)) _exit(70);
      };
      p::DurableEngine e(config(), 1, opt);
      Events v;
      add(e, v, 2, 1, m::Side::Sell, 4);
      e.drain();
      add(e, v, 1, 2, m::Side::Buy, 2);
      e.drain();
    } catch (...) {
      _exit(71);
    }
    _exit(72);
  }
  int status = 0;
  while (waitpid(child, &status, 0) < 0) require(errno == EINTR, "waitpid failed");
  require(WIFEXITED(status) && WEXITSTATUS(status) == 70, "Crash hook was not reached");
  p::DurableEngine e(config(), 1, opt);
  require(e.boundary().seq == 1, "One-sided frame recovered as committed");
  ask_quantity(e, 4, 1);
  require(std::filesystem::file_size(opt.directory / "trades/trades.log") == 0,
          "Unpaired trade tail was retained");
  Events v;
  add(e, v, 1, 3, m::Side::Buy, 2);
  e.drain();
  ask_quantity(e, 2, 1);
}
void zero_trade_order_only_crash(const std::filesystem::path& root) {
  auto opt = options(root, "zero-trade-order-only-crash");
  const auto child = fork();
  require(child >= 0, "fork failed");
  if (!child) {
    try {
      Gate gate;
      opt.hook = [&](auto stage, auto seq) {
        if (seq != 1) {
          return;
        }
        if (stage == p::Stage::BeforeTradeWrite) {
          gate.enter();
        }
        if (stage == p::Stage::OrderSynced) {
          eventually([&] { return gate.reached(); });
          _exit(70);
        }
      };
      p::DurableEngine engine(config(), 1, opt);
      Events events;
      add(engine, events, 2, 1, m::Side::Sell, 10);
      engine.drain();
    } catch (...) {
      _exit(71);
    }
    _exit(72);
  }

  int status = 0;
  while (waitpid(child, &status, 0) < 0) {
    require(errno == EINTR, "waitpid failed");
  }
  require(WIFEXITED(status) && WEXITSTATUS(status) == 70,
          "Zero-trade order sync crash hook was not reached");
  require(std::filesystem::file_size(opt.directory / "trades/trades.log") == 0,
          "Zero-trade batch unexpectedly wrote a trade frame");

  p::File delta(opt.directory / "orders/delta-1.log", false);
  std::uint64_t offset = 0;
  const auto frame = p::next_frame(delta, offset);
  require(frame && frame->boundary.seq == 1 && frame->trades == 0,
          "Order frame did not explicitly record zero expected trades");
  require(!p::next_frame(delta, offset), "Unexpected extra order frame");

  p::DurableEngine recovered(config(), 1, opt);
  require(recovered.boundary().seq == 1 && recovered.boundary().trade_count == 0 &&
              recovered.boundary().next_trade == 1,
          "Zero-trade batch was incorrectly treated as an unpaired tail");
  ask_quantity(recovered, 10, 1);
  Events events;
  add(recovered, events, 1, 2, m::Side::Buy, 4);
  recovered.drain();
  require(recovered.boundary().trade_count == 1, "Recovered active order did not match");
  ask_quantity(recovered, 6, 1);
}

void zero_trade_gaps_after_checkpoint(const std::filesystem::path& root) {
  const auto opt = options(root, "zero-trade-gaps");
  {
    p::DurableEngine engine(config(), 1, opt);
    Events events;
    add(engine, events, 2, 1, m::Side::Sell, 10);
    add(engine, events, 1, 2, m::Side::Buy, 4);
    engine.drain();
    engine.request_checkpoint();
    engine.wait_checkpoint();

    events.values.clear();
    add(engine, events, 3, 3, m::Side::Sell, 5);
    engine.drain();
    const auto canceled_order = events.accepted().order.orderId;
    cancel(engine, events, 3, 4, canceled_order);
    add(engine, events, 1, 5, m::Side::Buy, 1);
    engine.drain();
    ask_quantity(engine, 5, 1);
  }

  p::File trades(opt.directory / "trades/trades.log", false);
  std::uint64_t offset = 0;
  for (const auto sequence : {2U, 5U}) {
    const auto frame = p::next_frame(trades, offset);
    require(frame && frame->boundary.seq == sequence && frame->trades == 1,
            "Unexpected physical trade frame sequence");
  }
  require(!p::next_frame(trades, offset), "Zero-trade command created a trade frame");

  p::File delta(opt.directory / "orders/delta-3.log", false);
  offset = 0;
  for (const auto sequence : {3U, 4U, 5U}) {
    const auto frame = p::next_frame(delta, offset);
    const auto expected_trades = sequence == 5 ? 1U : 0U;
    require(frame && frame->boundary.seq == sequence && frame->trades == expected_trades,
            "Order log did not preserve zero-trade commands in sequence");
  }

  p::DurableEngine recovered(config(), 1, opt);
  require(recovered.checkpoint_sequence() == 2 && recovered.boundary().seq == 5 &&
              recovered.boundary().trade_count == 2 && recovered.boundary().next_trade == 3,
          "Recovery confused zero-trade gaps with missing trade frames");
  ask_quantity(recovered, 5, 1);
}

void crash_before_prune(const std::filesystem::path& root) {
  auto opt = options(root, "published-before-prune");
  const auto child = fork();
  require(child >= 0, "fork failed");
  if (!child) {
    try {
      opt.hook = [](auto stage, auto seq) {
        if (stage == p::Stage::BeforePrune && seq == 2) _exit(70);
      };
      p::DurableEngine e(config(), 1, opt);
      Events v;
      add(e, v, 2, 1, m::Side::Sell, 4);
      add(e, v, 1, 2, m::Side::Buy, 2);
      e.drain();
      e.request_checkpoint();
      e.wait_checkpoint();
    } catch (...) {
      _exit(71);
    }
    _exit(72);
  }
  int status = 0;
  while (waitpid(child, &status, 0) < 0) require(errno == EINTR, "waitpid failed");
  require(WIFEXITED(status) && WEXITSTATUS(status) == 70, "Snapshot crash hook was not reached");
  require(std::filesystem::exists(opt.directory / "orders/delta-1.log"),
          "Expected old delta retained at crash");
  p::DurableEngine e(config(), 1, opt);
  require(e.boundary().seq == 2 && e.checkpoint_sequence() == 2, "Published checkpoint not selected");
  ask_quantity(e, 2, 1);
  Events v;
  add(e, v, 1, 3, m::Side::Buy, 1);
  e.drain();
  ask_quantity(e, 1, 1);
}
}  // namespace
int main(int argc, char** argv) {
  try {
    auto root = std::filesystem::absolute(argc > 1 ? argv[1] : "recovery-results");
    std::filesystem::create_directories(root);
    std::string temp = (root / "run-XXXXXX").string();
    require(mkdtemp(temp.data()), "Cannot create test directory");
    root = temp;
    const std::vector<std::pair<std::string, std::function<void()>>> cases = {
        {"appended_remote_products_preserve_recovery", [&] { appended_remote_products(root); }},
        {"trade_sync_before_notifications", [&] { notification_barrier(root, true); }},
        {"order_sync_before_notifications", [&] { notification_barrier(root, false); }},
        {"restart_remaining_and_fifo", [&] { restart_fifo(root); }},
        {"continue_matching_during_previous_sync", [&] { matching_while_syncing(root); }},
        {"snapshot_publication_and_delta_rotation", [&] { checkpoint_rotation(root); }},
        {"bounded_buffers_wait_without_loss", [&] { bounded_wait(root); }},
        {"sync_failure_stops_visibility", [&] { sync_failure(root); }},
        {"incomplete_tail_and_corruption", [&] { tails_and_corruption(root); }},
        {"trade_only_crash_discards_unpaired_tail", [&] { crash_one_sided(root, true); }},
        {"order_only_crash_discards_unpaired_tail", [&] { crash_one_sided(root, false); }},
        {"zero_trade_order_only_crash_recovers", [&] { zero_trade_order_only_crash(root); }},
        {"zero_trade_gaps_after_checkpoint", [&] { zero_trade_gaps_after_checkpoint(root); }},
        {"published_snapshot_crash_before_prune", [&] { crash_before_prune(root); }}};
    for (const auto& [name, run] : cases) {
      run();
      std::cout << "PASS " << name << std::endl;
    }
    std::cout << "RESULTS " << root << std::endl;
    return 0;
  } catch (const std::exception& e) {
    std::cerr << "FAIL " << e.what() << std::endl;
    return 1;
  }
}
