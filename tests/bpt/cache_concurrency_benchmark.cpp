#include <algorithm>
#include <barrier>
#include <chrono>
#include <cstring>
#include <exception>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <map>
#include <mutex>
#include <numeric>
#include <random>
#include <string>
#include <thread>
#include <vector>
#include "matching_engine/trade_history/index/trade_index.hpp"

using namespace marketlab::orders::bpt;
using Clock = std::chrono::steady_clock;
using Oracle = std::vector<std::vector<TradePosition>>;
namespace {
void require(bool ok, const char* message) {
  if (!ok) throw std::runtime_error(message);
}
double seconds(Clock::time_point start, Clock::time_point end) {
  return std::chrono::duration<double>(end - start).count();
}
std::uint32_t user_for(std::uint64_t i) { return i % 5 == 0 ? 1 : i % 10000 + 2; }
TradePosition position_for(std::uint64_t i) { return {i * 200 + 48, (1U << 24) | 136, static_cast<std::uint32_t>(i)}; }
Oracle oracle_for(std::uint64_t count) {
  Oracle expected(10002);
  for (std::uint64_t i = 0; i < count; ++i)
    expected[user_for(i)].push_back(position_for(i));
  return expected;
}
Oracle first_pages(const Oracle& expected) {
  Oracle pages = expected;
  for (auto& page : pages) if (page.size() > 256) page.resize(256);
  return pages;
}
void evict(TradeIndex& tree, bool no_cache) {
  if (no_cache) tree.evict_unpinned_pages();
}
void verify_all(TradeIndex& tree, const Oracle& expected, bool no_cache) {
  for (std::uint32_t user = 0; user < expected.size(); ++user) {
    std::size_t offset = 0;
    std::uint64_t after = 0;
    for (;;) {
      const auto chunk = tree.query(user, after, UINT64_MAX, 256);
      evict(tree, no_cache);
      require(offset + chunk.size() <= expected[user].size(), "extra query records");
      require(std::equal(chunk.begin(), chunk.end(), expected[user].begin() + offset),
              "query differs from oracle");
      offset += chunk.size();
      if (chunk.empty()) break;
      after = chunk.back().Get();
    }
    require(offset == expected[user].size(), "missing query records");
  }
}
using Result = std::map<std::string, double>;
void percentiles(Result& result, const std::string& prefix, std::vector<double>& samples) {
  require(!samples.empty(), "no latency samples");
  std::sort(samples.begin(), samples.end());
  for (int p : {50, 95, 99})
    result[prefix + "_p" + std::to_string(p) + "_us"] = samples[(samples.size() - 1) * p / 100];
  result[prefix + "_samples"] = samples.size();
}
void emit(const std::string& mode, const Result& result) {
  std::cout << std::setprecision(12) << "{\"mode\":\"" << mode << "\",\"correct\":true";
  for (const auto& [key, value] : result) std::cout << ",\"" << key << "\":" << value;
  std::cout << "}\n";
}

// Verify cache discard semantics independently from B+ tree behavior.
void cache_control_test(const std::filesystem::path& root) {
  PageFile file(root / "cache-control.pages", true);
  bustub::BufferPoolManager pool(4, &file);
  auto id = pool.NewPage();
  auto pinned = pool.WritePage(id);
  std::memcpy(pinned.GetDataMut(), "persistent", 11);
  require(pool.EvictUnpinnedPages() == 0, "evicted a pinned writer");
  require(file.metrics().writes == 0, "wrote a pinned page");
  pinned.Drop();
  require(pool.EvictUnpinnedPages() == 1, "dirty page not evicted");
  require(file.metrics().writes == 1, "dirty page not written exactly once");
  require(!pool.GetPinCount(id).has_value(), "mapping retained after eviction");
  auto reads = file.metrics().reads;
  auto reader = pool.ReadPage(id);
  require(std::memcmp(reader.GetData(), "persistent", 11) == 0, "eviction lost page content");
  require(file.metrics().reads == reads + 1, "evicted page was not reloaded");
  // Exercise eviction from another thread while a read guard remains pinned.
  std::thread eviction([&] { require(pool.EvictUnpinnedPages() == 0, "evicted pinned reader"); });
  eviction.join();
  reader.Drop();
  require(pool.EvictUnpinnedPages() == 1, "clean page not evicted");
  require(file.metrics().writes == 1, "clean eviction performed an unnecessary write");
  require(pool.NewPage() == id + 1, "eviction changed page allocation");
  file.sync();

  const auto expected = oracle_for(4000);
  const auto path = root / "no-cache-splits.pages";
  {
    TradeIndex tree(path, 32, false, 7, 5);
    evict(tree, true);
    std::vector<std::uint64_t> order(4000);
    std::iota(order.begin(), order.end(), 0);
    std::shuffle(order.begin(), order.end(), std::mt19937(731));
    for (auto i : order) {
      require(tree.insert(user_for(i), position_for(i)), "no-cache insert failed");
      evict(tree, true);
    }
    require(!tree.insert(user_for(3), position_for(3)), "duplicate accepted after eviction");
    evict(tree, true);
    auto first = tree.query(1, 0, 1000 * 200 + 48, 256);
    evict(tree, true);
    reads = tree.metrics().reads;
    require(first == tree.query(1, 0, 1000 * 200 + 48, 256), "reloaded range changed");
    require(first.size() == 200, "snapshot boundary incorrect");
    require(tree.metrics().reads > reads, "no-cache query reused resident pages");
    evict(tree, true);
    require(tree.evict_unpinned_pages() == 0, "pages retained between operations");
    verify_all(tree, expected, true);
    tree.flush();
  }
  TradeIndex reopened(path, 32, true, 7, 5);
  verify_all(reopened, expected, true);
}

Result cache_benchmark(const std::filesystem::path& root, bool no_cache,
                       std::uint64_t count, std::uint64_t queries) {
  const auto expected = oracle_for(count);
  const auto pages = first_pages(expected);
  const auto path = root / "index.pages";
  // Both modes allocate the same pool. In no-cache mode, no page survives an
  // operation boundary. Thus only page retention changes, not allocation/layout.
  constexpr std::size_t capacity = 1024;
  Result result{{"entries", count}, {"queries", queries}, {"frame_capacity", capacity},
                {"retained_cache_bytes", no_cache ? 0 : capacity * bustub::BUSTUB_PAGE_SIZE}};
  {
    TradeIndex tree(path, capacity);
    evict(tree, no_cache);
    auto before = tree.metrics();
    std::vector<double> insert_latency;
    insert_latency.reserve((count + 63) / 64);
    auto start = Clock::now();
    for (std::uint64_t i = 0; i < count; ++i) {
      auto op = i % 64 == 0 ? Clock::now() : Clock::time_point{};
      require(tree.insert(user_for(i), position_for(i)), "cache benchmark insert failed");
      evict(tree, no_cache);
      if (i % 64 == 0) insert_latency.push_back(seconds(op, Clock::now()) * 1e6);
    }
    auto appended = Clock::now();
    auto inserted = tree.metrics();
    tree.flush();
    auto synced = Clock::now();
    auto flushed = tree.metrics();
    result["insert_ops_s"] = count / seconds(start, appended);
    result["insert_sync_s"] = seconds(start, synced);
    result["flush_ms"] = seconds(appended, synced) * 1e3;
    result["insert_reads"] = inserted.reads - before.reads;
    result["insert_writes"] = inserted.writes - before.writes;
    result["flush_writes"] = flushed.writes - inserted.writes;
    percentiles(result, "insert", insert_latency);
    std::vector<double> query_latency;
    query_latency.reserve(queries);
    auto query_start = Clock::now();
    for (std::uint64_t i = 0; i < queries; ++i) {
      auto user = user_for(i);
      auto op = Clock::now();
      auto chunk = tree.query(user, 0, UINT64_MAX, 256);
      evict(tree, no_cache);
      query_latency.push_back(seconds(op, Clock::now()) * 1e6);
      require(chunk == pages[user], "cache benchmark query mismatch");
    }
    auto query_end = Clock::now();
    auto queried = tree.metrics();
    result["query_ops_s"] = queries / seconds(query_start, query_end);
    result["query_reads"] = queried.reads - flushed.reads;
    result["query_writes"] = queried.writes - flushed.writes;
    result["index_pages"] = tree.page_count();
    percentiles(result, "query", query_latency);
    // Validation is outside measured phases, including a complete paged scan.
    verify_all(tree, expected, no_cache);
  }
  TradeIndex reopened(path, capacity, true);
  verify_all(reopened, expected, no_cache);
  return result;
}

struct WorkerResult {
  std::vector<double> latency;
  double elapsed = 0;
};
Result concurrency_benchmark(const std::filesystem::path& root, bool serial,
                             std::uint64_t seed, std::uint64_t inserts, std::uint64_t queries,
                             unsigned writers, unsigned readers) {
  const auto expected = oracle_for(seed + inserts);
  const auto pages = first_pages(oracle_for(seed));
  const auto path = root / "index.pages";
  Result result{{"seed_entries", seed}, {"inserts", inserts}, {"queries", queries},
                {"writers", writers}, {"readers", readers}, {"cache_bytes", 8 * 1024 * 1024}};
  {
    TradeIndex tree(path, 1024);
    for (std::uint64_t i = 0; i < seed; ++i)
      require(tree.insert(user_for(i), position_for(i)), "prefill insert failed");
    tree.flush();
    // Identical warm-up in both modes, excluded from the measurements.
    for (std::uint64_t i = 0; i < 3000; ++i) tree.query(user_for(i), 0, UINT64_MAX, 256);
    auto before = tree.metrics();
    std::mutex tree_mutex, error_mutex;
    std::exception_ptr error;
    std::barrier start_barrier(static_cast<std::ptrdiff_t>(writers + readers + 1));
    std::vector<WorkerResult> stats(writers + readers);
    for (unsigned t = 0; t < writers + readers; ++t)
      stats[t].latency.reserve(t < writers ? (inserts / writers + 63) / 64 : queries / readers + 1);
    std::vector<std::thread> threads;
    Clock::time_point start;
    for (unsigned t = 0; t < writers + readers; ++t) {
      threads.emplace_back([&, t] {
        start_barrier.arrive_and_wait();
        try {
          if (t < writers) {
            std::uint64_t local = 0;
            for (std::uint64_t i = t; i < inserts; i += writers, ++local) {
              auto op = local % 64 == 0 ? Clock::now() : Clock::time_point{};
              {
                std::unique_lock lock(tree_mutex, std::defer_lock);
                if (serial) lock.lock();
                require(tree.insert(user_for(seed + i), position_for(seed + i)), "parallel insert failed");
              }
              if (local % 64 == 0) stats[t].latency.push_back(seconds(op, Clock::now()) * 1e6);
            }
          } else {
            for (std::uint64_t i = t - writers; i < queries; i += readers) {
              auto user = user_for(i);
              auto op = Clock::now();
              std::vector<TradePosition> chunk;
              {
                std::unique_lock lock(tree_mutex, std::defer_lock);
                if (serial) lock.lock();
                // A stable history boundary gives both modes exactly the same result set.
                chunk = tree.query(user, 0, seed * 200 + 48, 256);
              }
              stats[t].latency.push_back(seconds(op, Clock::now()) * 1e6);
              require(chunk == pages[user], "concurrent query mismatch");
            }
          }
        } catch (...) {
          std::lock_guard lock(error_mutex);
          if (!error) error = std::current_exception();
        }
        stats[t].elapsed = seconds(start, Clock::now());
      });
    }
    start = Clock::now();
    start_barrier.arrive_and_wait();
    for (auto& thread : threads) thread.join();
    auto finished = Clock::now();
    if (error) std::rethrow_exception(error);
    auto after = tree.metrics();
    tree.flush();
    auto synced = Clock::now();
    result["mixed_ops_s"] = (inserts + queries) / seconds(start, finished);
    result["mixed_s"] = seconds(start, finished);
    result["mixed_sync_s"] = seconds(start, synced);
    result["mixed_reads"] = after.reads - before.reads;
    result["mixed_writes"] = after.writes - before.writes;
    result["flush_ms"] = seconds(finished, synced) * 1e3;
    result["index_pages"] = tree.page_count();
    std::vector<double> insert_latency, query_latency;
    double last_writer = 0, last_reader = 0;
    for (unsigned t = 0; t < stats.size(); ++t) {
      auto& latencies = t < writers ? insert_latency : query_latency;
      latencies.insert(latencies.end(), stats[t].latency.begin(), stats[t].latency.end());
      auto& last = t < writers ? last_writer : last_reader;
      last = std::max(last, stats[t].elapsed);
    }
    result["last_writer_s"] = last_writer;
    result["last_reader_s"] = last_reader;
    percentiles(result, "insert", insert_latency);
    percentiles(result, "query", query_latency);
    verify_all(tree, expected, false);
  }
  TradeIndex reopened(path, 1024, true);
  verify_all(reopened, expected, false);
  return result;
}
}

int main(int argc, char** argv) {
  try {
    if (argc < 3) throw std::runtime_error(
        "usage: bpt_cache_concurrency_benchmark ROOT verify | ROOT cache-none|cache-8m ENTRIES QUERIES | "
        "ROOT serial|concurrent SEED INSERTS QUERIES WRITERS READERS");
    std::filesystem::path root = argv[1];
    std::filesystem::create_directories(root);
    std::string mode = argv[2];
    if (mode == "verify") {
      cache_control_test(root);
      emit("cache-none", cache_benchmark(root, true, 6000, 200));
      emit("cache-8m", cache_benchmark(root, false, 6000, 200));
      for (bool serial : {true, false})
        emit(serial ? "serial" : "concurrent", concurrency_benchmark(root, serial, 12000, 6000, 3000, 6, 4));
    } else if ((mode == "cache-none" || mode == "cache-8m") && argc == 5) {
      auto entries = std::stoull(argv[3]), queries = std::stoull(argv[4]);
      require(entries > 0 && queries > 0, "counts must be positive");
      emit(mode, cache_benchmark(root, mode == "cache-none", entries, queries));
    } else if ((mode == "serial" || mode == "concurrent") && argc == 8) {
      auto seed = std::stoull(argv[3]), inserts = std::stoull(argv[4]), queries = std::stoull(argv[5]);
      auto writers = std::stoul(argv[6]), readers = std::stoul(argv[7]);
      require(seed > 0 && inserts > 0 && queries > 0 && writers > 0 && readers > 0,
              "counts and thread counts must be positive");
      emit(mode, concurrency_benchmark(root, mode == "serial", seed, inserts, queries, writers, readers));
    } else {
      throw std::runtime_error("invalid benchmark arguments");
    }
  } catch (const std::exception& e) {
    std::cerr << "FAIL: " << e.what() << '\n';
    return 1;
  }
}
