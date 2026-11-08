#include <unistd.h>

#include <chrono>
#include <future>
#include <iostream>

#include "matching_engine/persistence/durable_engine.hpp"

namespace m = marketlab;
namespace o = marketlab::orders;
namespace p = marketlab::orders::persistence;
using namespace std::chrono_literals;

namespace {

void require(bool condition, const char* message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

template <class Predicate>
void eventually(Predicate ready) {
  const auto deadline = std::chrono::steady_clock::now() + 5s;
  while (!ready()) {
    if (std::chrono::steady_clock::now() >= deadline) {
      throw std::runtime_error("Test synchronization timed out");
    }
    std::this_thread::sleep_for(1ms);
  }
}

struct Gate {
  std::mutex mutex;
  std::condition_variable changed;
  bool arrived = false;
  bool open = false;

  void enter() {
    std::unique_lock lock(mutex);
    arrived = true;
    changed.notify_all();
    changed.wait(lock, [&] { return open; });
  }

  bool reached() {
    std::lock_guard lock(mutex);
    return arrived;
  }

  void release() {
    std::lock_guard lock(mutex);
    open = true;
    changed.notify_all();
  }
};

struct Release {
  Gate& gate;
  ~Release() {
    gate.release();
  }
};

o::OrderConfig config() {
  o::OrderConfig result;
  result.accounts = {{"Alice", "demo"}, {"Bob", "demo"}, {"Carol", "demo"}};
  result.products = {{"ITEM0001", 100, 1}};
  result.shards = {{1, {"127.0.0.1", 1}}};
  return result;
}

p::Options options(const std::filesystem::path& root, const std::string& name) {
  p::Options result;
  result.directory = root / name;
  result.checkpoint_interval = 1h;
  return result;
}

struct Events {
  std::vector<p::Event> values;

  o::EngineBackend::Emit emit() {
    return [this](auto kind, const auto& context, auto bytes) {
      p::Event event;
      event.kind = kind;
      event.context = context;
      event.size = bytes.size();
      std::copy(bytes.begin(), bytes.end(), event.payload.begin());
      values.push_back(event);
    };
  }

  std::vector<m::ActiveOrderSnapshot> snapshot(m::RequestId request) const {
    std::vector<m::ActiveOrderSnapshot> result;
    for (const auto& event : values) {
      const auto bytes = std::span(event.payload).first(event.size);
      const auto header = o::copy_record<m::Header>(bytes.first(sizeof(m::Header)));
      if (header.messageType == m::MessageType::ActiveOrderSnapshot &&
          header.requestId == request) {
        result.push_back(o::copy_record<m::ActiveOrderSnapshot>(bytes));
      }
    }
    return result;
  }

  std::size_t snapshot_end(m::RequestId request) const {
    for (std::size_t i = 0; i < values.size(); ++i) {
      const auto& event = values[i];
      const auto header =
          o::copy_record<m::Header>(std::span(event.payload).first(sizeof(m::Header)));
      if (event.kind == o::LinkKind::SnapshotEnd && header.requestId == request) {
        return i;
      }
    }
    return values.size();
  }
};

void add(p::DurableEngine& engine, Events& events, unsigned user, m::RequestId request,
         m::Side side, int quantity) {
  m::NewOrderRequest order{};
  order.header = {m::MessageType::NewOrderRequest, sizeof(order), request};
  o::put_string(order.symbol, "ITEM0001");
  order.side = side;
  order.price = 100;
  order.quantity = quantity;
  engine.command({0, o::LinkKind::NewOrder, request, user, 1}, o::record_bytes(order),
                 events.emit());
}

void query(p::DurableEngine& engine, Events& events, unsigned user, m::RequestId request) {
  const m::Header header{m::MessageType::ActiveOrderSnapshot, sizeof(m::Header), request};
  engine.command({0, o::LinkKind::Snapshot, request, user, 1}, o::record_bytes(header),
                 events.emit());
}

p::StoredOrder stored_order() {
  p::StoredOrder result;
  auto& state = result.state;
  state.orderId = (1ULL << 48) | 1;
  o::put_string(state.symbol, "ITEM0001");
  state.side = m::Side::Sell;
  state.price = 100;
  state.originalQuantity = 10;
  state.remainingQuantity = 10;
  state.status = m::OrderStatus::Accepted;
  std::copy_n("Bob", 3, result.username.begin());
  result.arrival = state.orderId;
  return result;
}

void compact_records_and_cow() {
  auto configuration = config();
  configuration.products.insert(configuration.products.begin(), {"OTHER", 100, 2});
  p::OrderDictionary dictionary(configuration);
  const auto original = stored_order();
  auto compact = dictionary.compact(original);
  require(compact.user == 2 && compact.symbol == 2,
          "Dictionary lost global user/product identities");
  o::Bytes before;
  o::Bytes after;
  p::encode_order(before, original);
  p::encode_order(after, dictionary.expand(compact));
  require(before == after && after.size() == p::stored_order_bytes,
          "Compact records changed the persistent order format");
  require(sizeof(p::CompactOrder) < sizeof(p::StoredOrder), "Compact order did not reduce memory");

  p::CommittedState state(configuration, 1);
  state.apply({false, compact});
  const auto frozen = state.freeze();
  compact.remaining = 6;
  compact.filled = 4;
  compact.status = m::OrderStatus::PartiallyFilled;
  state.apply({false, compact});
  const auto* writable_page = state.freeze().front().get();
  require(writable_page != frozen.front().get(), "Shared page was modified in place");
  require(frozen.front()->orders[0]->remaining == 10,
          "Snapshot version changed after a partial fill");
  require(state.copied_bytes() == sizeof(p::Page),
          "First shared-page mutation did not copy one page");

  compact.remaining = 4;
  compact.filled = 6;
  state.apply({false, compact});
  require(state.freeze().front().get() == writable_page,
          "Private page was unnecessarily copied again");
  require(state.copied_bytes() == sizeof(p::Page), "Repeated update copied the private page");
  o::MarketSnapshot market(state.symbol_count());
  state.market(market);
  const auto& book = market.books.front();
  require(book.symbol_id == 2 && book.count == 1 && book.levels[0].quantity == 4 &&
              book.levels[0].num_orders == 1,
          "Partial fill changed the level identity, quantity or order count");

  auto second = compact;
  ++second.id;
  second.arrival = second.id;
  second.original = 2;
  second.filled = 0;
  second.remaining = 2;
  second.status = m::OrderStatus::Accepted;
  state.apply({false, second});
  state.market(market);
  require(book.levels[0].quantity == 6 && book.levels[0].num_orders == 2,
          "Same-price insertion broke aggregate count");
  second.price = 101;
  state.apply({false, second});
  state.market(market);
  require(book.count == 2 && book.levels[0].quantity == 4 && book.levels[1].quantity == 2,
          "Changed-price fallback did not move the order");
  state.apply({true, compact});
  state.market(market);
  require(book.count == 1 && book.levels[0].price == 101 && book.levels[0].num_orders == 1,
          "Removing the last order left a stale level");
  state.apply({true, second});
  state.market(market);
  require(book.count == 0, "Empty committed book retained price levels");
  require(frozen.front()->orders[0]->remaining == 10, "Deletion changed the frozen snapshot");

  std::cout << "LAYOUT stored_order=" << sizeof(p::StoredOrder)
            << " compact_order=" << sizeof(p::CompactOrder) << " change=" << sizeof(p::Change)
            << " legacy_page=" << sizeof(std::array<std::optional<p::StoredOrder>, p::Page::slots>)
            << " page=" << sizeof(p::Page) << '\n';
}

void asynchronous_query(const std::filesystem::path& root, bool block_trade) {
  Gate current;
  Gate later;
  std::atomic_bool other_synced = false;
  auto opt = options(root, block_trade ? "query-trade" : "query-orders");
  opt.hook = [&](auto stage, auto sequence) {
    const auto blocked = block_trade ? p::Stage::BeforeTradeSync : p::Stage::BeforeOrderSync;
    const auto other = block_trade ? p::Stage::OrderSynced : p::Stage::TradeSynced;
    if (sequence == 2 && stage == blocked) {
      current.enter();
    }
    if (sequence == 2 && stage == other) {
      other_synced = true;
    }
    if (sequence == 3 &&
        (stage == p::Stage::BeforeTradeWrite || stage == p::Stage::BeforeOrderWrite)) {
      later.enter();
    }
  };
  p::DurableEngine engine(config(), 1, opt);
  Release release_current{current};
  Release release_later{later};
  Events events;
  add(engine, events, 2, 1, m::Side::Sell, 10);
  engine.drain();
  events.values.clear();
  add(engine, events, 1, 2, m::Side::Buy, 4);
  eventually([&] { return current.reached() && other_synced.load(); });

  // Move the single engine owner to this thread for these calls. No concurrent
  // reads of engine/events occur until future.get() transfers ownership back.
  auto future = std::async(std::launch::async, [&] {
    query(engine, events, 2, 900);  // Bob has 6 at C=2.
    query(engine, events, 1, 901);  // Alice has no active orders at C=2.
    add(engine, events, 3, 3, m::Side::Buy, 2);
    query(engine, events, 2, 902);  // Bob now has 4 at C=3.
  });
  const bool returned = future.wait_for(1s) == std::future_status::ready;
  if (!returned) {
    current.release();
    later.release();
  }
  future.get();
  require(returned, "Query blocked matching while waiting for persistence");
  require(events.values.empty() && engine.boundary().seq == 1,
          "Query or later match became visible before both writers completed");

  current.release();
  eventually([&] {
    engine.poll();
    return events.snapshot_end(901) < events.values.size();
  });
  require(engine.boundary().seq == 2, "Query waited for the later uncommitted batch");
  const auto bob = events.snapshot(900);
  const auto alice = events.snapshot(901);
  require(bob.size() == 2 && bob[0].order.remainingQuantity == 6 && bob[1].isEnd == 1,
          "Query read the later working quantity instead of its frozen values");
  require(alice.size() == 1 && alice[0].isEnd == 1,
          "Empty query omitted its ordered completion marker");
  require(events.snapshot(902).empty(), "Later query crossed its own persistence barrier");
  const auto earlier_end = events.snapshot_end(901);
  const auto before_later = events.values.size();
  require(earlier_end + 1 == before_later, "Unexpected output after first query group");

  later.release();
  engine.drain();
  const auto next = events.snapshot(902);
  require(next.size() == 2 && next[0].order.remainingQuantity == 4 && next[1].isEnd == 1,
          "Second query lost its separate version");
  const auto next_header = o::copy_record<m::Header>(
      std::span(events.values.at(before_later).payload).first(sizeof(m::Header)));
  require(next_header.messageType == m::MessageType::OrderUpdate && next_header.requestId == 3,
          "Later command notifications overtook SnapshotEnd");
  require(engine.boundary().seq == 3, "Later command did not commit after query completion");
}

void query_survives_cancel_and_reuse(const std::filesystem::path& root) {
  Gate gate;
  auto opt = options(root, "query-cancel-reuse");
  opt.hook = [&](auto stage, auto sequence) {
    if (stage == p::Stage::BeforeOrderSync && sequence == 1) {
      gate.enter();
    }
  };
  p::DurableEngine engine(config(), 1, opt);
  Release release{gate};
  Events events;
  query(engine, events, 2, 950);
  require(events.snapshot(950).size() == 1, "Initial empty query did not complete immediately");
  events.values.clear();
  add(engine, events, 2, 1, m::Side::Sell, 10);
  eventually([&] { return gate.reached(); });
  query(engine, events, 2, 951);
  const auto first_id = (1ULL << 48) | 1;
  const m::CancelRequest cancel{{m::MessageType::CancelRequest, sizeof(m::CancelRequest), 2},
                                first_id};
  engine.command({0, o::LinkKind::Cancel, 2, 2, 1}, o::record_bytes(cancel), events.emit());
  query(engine, events, 2, 952);
  add(engine, events, 2, 3, m::Side::Sell, 7);
  query(engine, events, 2, 953);
  require(events.values.empty(), "Zero-trade query bypassed order durability");
  gate.release();
  engine.drain();
  const auto first = events.snapshot(951);
  const auto empty = events.snapshot(952);
  const auto last = events.snapshot(953);
  require(first.size() == 2 && first[0].order.orderId == first_id &&
              first[0].order.remainingQuantity == 10,
          "Canceled/reused working node changed an earlier query result");
  require(empty.size() == 1 && empty[0].isEnd == 1, "Cancel did not update the user order count");
  require(last.size() == 2 && last[0].order.orderId == first_id + 1 &&
              last[0].order.remainingQuantity == 7,
          "Reused working node was missing from the later query");
  require(events.snapshot_end(951) < events.snapshot_end(952) &&
              events.snapshot_end(952) < events.snapshot_end(953),
          "Queries at distinct zero-trade boundaries were reordered");
  require(std::filesystem::file_size(opt.directory / "trades/trades.log") == 0,
          "Zero-trade queries unexpectedly required trade frames");
}

void query_capacity(const std::filesystem::path& root, bool limit_bytes) {
  Gate gate;
  auto opt = options(root, limit_bytes ? "query-byte-limit" : "query-slot-limit");
  opt.query_slots = limit_bytes ? 4 : 1;
  opt.query_bytes = limit_bytes ? sizeof(m::OrderState) : 1024;
  opt.hook = [&](auto stage, auto sequence) {
    if (stage == p::Stage::BeforeOrderSync && sequence == 1) {
      gate.enter();
    }
  };
  p::DurableEngine engine(config(), 1, opt);
  Release release{gate};
  Events events;
  add(engine, events, 2, 1, m::Side::Sell, 10);
  eventually([&] { return gate.reached(); });
  query(engine, events, 2, 910);
  std::atomic_bool entered = false;
  auto future = std::async(std::launch::async, [&] {
    entered = true;
    query(engine, events, 2, 911);
  });
  // This guard is destroyed before the future, including on assertion failure.
  Release release_future{gate};
  eventually([&] { return entered.load(); });
  require(future.wait_for(50ms) == std::future_status::timeout,
          "Full query buffer did not apply backpressure");
  gate.release();
  future.get();
  engine.drain();
  require(events.snapshot(910).size() == 2 && events.snapshot(911).size() == 2,
          "Query backpressure lost or duplicated a reply");
  require(events.snapshot_end(910) < events.snapshot_end(911), "Query admission order changed");
  query(engine, events, 2, 912);
  require(events.snapshot(912).size() == 2, "Completed query did not release buffer capacity");
}

void query_sync_failure(const std::filesystem::path& root) {
  Gate gate;
  auto opt = options(root, "query-sync-failure");
  opt.hook = [&](auto stage, auto sequence) {
    if (stage == p::Stage::BeforeOrderSync && sequence == 2) {
      gate.enter();
      throw std::runtime_error("Injected query-target sync failure");
    }
  };
  p::DurableEngine engine(config(), 1, opt);
  Release release{gate};
  Events events;
  add(engine, events, 2, 1, m::Side::Sell, 10);
  engine.drain();
  events.values.clear();
  add(engine, events, 1, 2, m::Side::Buy, 4);
  eventually([&] { return gate.reached(); });
  query(engine, events, 2, 920);
  gate.release();
  bool failed = false;
  try {
    engine.drain();
  } catch (const std::runtime_error&) {
    failed = true;
  }
  require(failed && events.values.empty(), "Failed target produced a successful snapshot or event");
}

void oversized_query(const std::filesystem::path& root) {
  auto opt = options(root, "oversized-query");
  opt.query_bytes = sizeof(m::OrderState);
  p::DurableEngine engine(config(), 1, opt);
  Events events;
  add(engine, events, 2, 1, m::Side::Sell, 1);
  add(engine, events, 2, 2, m::Side::Sell, 1);
  engine.drain();
  events.values.clear();
  bool failed = false;
  try {
    query(engine, events, 2, 930);
  } catch (const std::runtime_error&) {
    failed = true;
  }
  require(failed && events.values.empty(), "Oversized query sent a partial success");
}

void legacy_files_and_checkpoint(const std::filesystem::path& root) {
  const auto opt = options(root, "legacy-format");
  const auto original = stored_order();
  {
    p::Storage storage(opt, config(), 1);
    storage.recover();
    // Write a pre-optimization PUT with its original name-based wire format.
    o::Bytes bytes;
    const p::Boundary boundary{1, 2, 1, 0, 0};
    p::start_frame(bytes, p::Kind::Orders, boundary, 0, 1);
    p::integer(bytes, 1, 1);
    p::encode_order(bytes, original);
    p::finish_frame(bytes);
    p::File delta(storage.delta_path(1), true, true);
    delta.append(bytes);
    delta.sync();
    p::sync_directory(opt.directory / "orders");
  }
  {
    p::DurableEngine engine(config(), 1, opt);
    Events events;
    query(engine, events, 2, 940);
    require(events.snapshot(940).at(0).order.remainingQuantity == 10,
            "Legacy active order did not restore into compact state");
    add(engine, events, 1, 2, m::Side::Buy, 4);
    engine.drain();

    p::File delta(opt.directory / "orders/delta-2.log", false);
    std::uint64_t offset = 0;
    const auto frame = p::next_frame(delta, offset);
    require(frame && frame->trades == 1, "New order log lost expected trade count");
    p::Reader reader{frame->payload};
    require(reader.number(1) == 1, "Partial fill did not write a PUT");
    const auto updated = p::decode_order(reader);
    require(updated.username == original.username &&
                updated.state.orderId == original.state.orderId &&
                updated.arrival == original.arrival && updated.state.remainingQuantity == 6 &&
                updated.state.filledQuantity == 4 &&
                o::fixed_string(updated.state.symbol) == "ITEM0001",
            "Order writer did not expand compact identities into the original format");
    require(reader.number(1) == 0, "Fully filled incoming order did not write DELETE");
    reader.number(8);
    reader.finish();
    engine.request_checkpoint();
    engine.wait_checkpoint();
  }
  {
    p::DurableEngine recovered(config(), 1, opt);
    Events events;
    query(recovered, events, 2, 941);
    const auto result = events.snapshot(941);
    require(recovered.checkpoint_sequence() == 2 && result.size() == 2 &&
                result[0].order.orderId == original.state.orderId &&
                result[0].order.remainingQuantity == 6 && result[0].order.filledQuantity == 4,
            "Compact checkpoint did not preserve identity and remaining quantity");
    add(recovered, events, 1, 3, m::Side::Buy, 6);
    recovered.drain();
    query(recovered, events, 2, 942);
    require(events.snapshot(942).size() == 1 && events.snapshot(942)[0].isEnd == 1,
            "User index retained a fully filled restored order");
  }
}

}  // namespace

int main(int argc, char** argv) {
  try {
    auto root = std::filesystem::absolute(argc > 1 ? argv[1] : "optimization-results");
    std::filesystem::create_directories(root);
    std::string temporary = (root / "run-XXXXXX").string();
    require(mkdtemp(temporary.data()), "Cannot create isolated test directory");
    root = temporary;
    const std::vector<std::pair<std::string, std::function<void()>>> cases = {
        {"compact_records_level_updates_and_cow", compact_records_and_cow},
        {"async_query_waits_for_trade_writer", [&] { asynchronous_query(root, true); }},
        {"async_query_waits_for_order_writer", [&] { asynchronous_query(root, false); }},
        {"query_survives_cancel_and_slot_reuse", [&] { query_survives_cancel_and_reuse(root); }},
        {"query_slot_backpressure", [&] { query_capacity(root, false); }},
        {"query_byte_backpressure", [&] { query_capacity(root, true); }},
        {"query_target_sync_failure", [&] { query_sync_failure(root); }},
        {"oversized_query_fails_without_partial_reply", [&] { oversized_query(root); }},
        {"legacy_disk_format_and_compact_checkpoint", [&] { legacy_files_and_checkpoint(root); }}};
    for (const auto& [name, run] : cases) {
      run();
      std::cout << "PASS " << name << std::endl;
    }
    std::cout << "RESULTS " << root << std::endl;
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "FAIL " << error.what() << std::endl;
    return 1;
  }
}
