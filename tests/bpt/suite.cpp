#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <map>
#include <mutex>
#include <random>
#include <set>
#include <thread>
#include <vector>
#include "matching_engine/trade_history/index/trade_index.hpp"

using namespace marketlab::orders::bpt;
using Clock = std::chrono::steady_clock;
namespace {
void require(bool ok, const char* message) { if (!ok) throw std::runtime_error(message); }
std::vector<TradePosition> all(TradeIndex& tree, std::uint32_t user, std::size_t page = 31) {
  std::vector<TradePosition> values;
  std::uint64_t after = 0;
  for (;;) {
    auto chunk = tree.query(user, after, UINT64_MAX, page);
    if (chunk.empty()) break;
    for (const auto& p : chunk) { require(p.Get() > after, "cursor did not advance"); after = p.Get(); values.push_back(p); }
  }
  return values;
}
void duplicate_and_reopen(const std::filesystem::path& root) {
  std::map<std::uint32_t, std::vector<TradePosition>> oracle;
  std::mt19937 random(731);
  std::vector<std::pair<std::uint32_t, TradePosition>> input;
  for (std::uint64_t i = 0; i < 12000; ++i) {
    std::uint32_t user = i % 3 == 0 ? UINT32_MAX : random() % 29;
    TradePosition pos{i * 1000, static_cast<std::uint32_t>(i % 3), 0};
    input.emplace_back(user, pos); oracle[user].push_back(pos);
  }
  std::shuffle(input.begin(), input.end(), random);
  {
    TradeIndex tree(root / "duplicates.pages", 24, false, 7, 5);
    for (const auto& [user, pos] : input) require(tree.insert(user, pos), "missing insertion");
    for (const auto& [user, pos] : input) require(!tree.insert(user, pos), "duplicate pair accepted twice");
    for (const auto& [user, expected] : oracle) require(all(tree, user) == expected, "random duplicate range differs from oracle");
    require(all(tree, 1000000).empty(), "absent user returned trades");
    auto prefix = tree.query(UINT32_MAX, 0, 6000 * 1000, 12000);
    for (const auto& pos : prefix) require(pos.Get() < 6000 * 1000, "upper boundary leaked");
    require(prefix.size() == 2000, "prefix missing entries");
    tree.flush();
    auto m = tree.metrics();
    require(tree.page_count() > 24 && m.reads > 24 && m.writes > 24, "test did not exercise eviction");
    std::cout << "duplicate_pages=" << tree.page_count() << " cache_pages=24 disk_reads=" << m.reads << " disk_writes=" << m.writes << '\n';
  }
  {
    TradeIndex tree(root / "duplicates.pages", 16, true, 7, 5);
    for (const auto& [user, expected] : oracle) require(all(tree, user, 19) == expected, "reopened range differs from oracle");
    require(tree.insert(UINT32_MAX, {90000000, 0, 0}), "insert after reopen failed");
    tree.flush();
  }
  std::cout << "PASS duplicate keys, page boundaries, cache eviction, cursor, reopen\n";
}
void concurrent(const std::filesystem::path& root) {
  constexpr unsigned writers = 6, readers = 4, count = 6000;
  TradeIndex tree(root / "concurrent.pages", 128, false, 13, 9);
  std::atomic<bool> start{false}, finished{false};
  std::atomic<std::uint64_t> scans{0};
  std::mutex error_mutex;
  std::exception_ptr error;
  auto run = [&](auto fn) {
    while (!start.load()) std::this_thread::yield();
    try { fn(); } catch (...) { std::lock_guard lock(error_mutex); if (!error) error = std::current_exception(); }
  };
  std::vector<std::thread> writes, reads;
  for (unsigned t = 0; t < readers; ++t) reads.emplace_back([&] { run([&] {
    while (!finished.load()) {
      auto page = tree.query(7, 0, UINT64_MAX, 113);
      for (std::size_t i = 1; i < page.size(); ++i) require(page[i-1].Get() < page[i].Get(), "concurrent scan order/duplicate");
      ++scans;
    }
  }); });
  for (unsigned t = 0; t < writers; ++t) writes.emplace_back([&, t] { run([&] {
    for (unsigned i = 0; i < count; ++i) {
      TradePosition pos{(static_cast<std::uint64_t>(i) * writers + t) * 1000, 0, 0};
      require(tree.insert(7, pos), "concurrent hot-user insert lost");
      require(tree.insert(100 + t, pos), "concurrent independent-user insert lost");
    }
  }); });
  const auto begin = Clock::now(); start = true;
  for (auto& t : writes) t.join();
  finished = true;
  for (auto& t : reads) t.join();
  if (error) std::rethrow_exception(error);
  auto hot = all(tree, 7);
  require(hot.size() == writers * count, "concurrent final hot-user count");
  for (unsigned t = 0; t < writers; ++t) {
    auto own = all(tree, 100 + t);
    require(own.size() == count, "concurrent final independent-user count");
    for (unsigned i = 0; i < count; ++i) require(own[i].frame == (static_cast<std::uint64_t>(i) * writers + t) * 1000, "wrong user value");
  }
  tree.flush();
  double seconds = std::chrono::duration<double>(Clock::now() - begin).count();
  std::cout << "PASS concurrency writers=" << writers << " readers=" << readers << " entries=" << writers * count * 2
            << " scans=" << scans << " seconds=" << seconds << '\n';
}
void benchmark(const std::filesystem::path& root, std::size_t n, std::size_t cache_pages) {
  TradeIndex tree(root / "stress.pages", cache_pages);
  std::vector<double> inserts;
  auto begin = Clock::now();
  for (std::uint64_t i = 0; i < n; ++i) {
    auto user = i % 5 == 0 ? 1 : static_cast<std::uint32_t>(i % 10000 + 2);
    auto sampled = i % 64 == 0;
    auto tick = sampled ? Clock::now() : Clock::time_point{};
    require(tree.insert(user, {i * 200, 0, 0}), "stress insertion failed");
    if (sampled) inserts.push_back(std::chrono::duration<double,std::micro>(Clock::now()-tick).count());
  }
  auto appended = Clock::now(); tree.flush(); auto durable = Clock::now();
  std::vector<double> latencies;
  std::size_t hits = 0;
  for (unsigned i = 0; i < 3000; ++i) {
    auto q = Clock::now(); auto page = tree.query(i % 5 == 0 ? 1 : i % 10000 + 2, 0, UINT64_MAX, 256); hits += page.size();
    latencies.push_back(std::chrono::duration<double, std::micro>(Clock::now() - q).count());
  }
  std::sort(latencies.begin(), latencies.end()); auto m = tree.metrics();
  auto full_begin = Clock::now();
  require(all(tree, 1, 256).size() == (n + 4) / 5, "stress full-history count");
  auto full_ms = std::chrono::duration<double,std::milli>(Clock::now()-full_begin).count();
  std::sort(inserts.begin(),inserts.end());
  TradeIndex reopened(root / "stress.pages",cache_pages,true);
  auto first_begin = Clock::now();
  require(!reopened.query(1,0,UINT64_MAX,256).empty(),"first page after reopen missing");
  auto first_us = std::chrono::duration<double,std::micro>(Clock::now()-first_begin).count();
  std::cout << "BENCH entries=" << n << " insert_per_second=" << n / std::chrono::duration<double>(appended - begin).count()
            << " insert_and_sync_seconds=" << std::chrono::duration<double>(durable - begin).count()
            << " flush_ms=" << std::chrono::duration<double,std::milli>(durable-appended).count()
            << " insert_sample_p50_us=" << inserts[inserts.size()/2] << " insert_sample_p95_us=" << inserts[inserts.size()*95/100]
            << " insert_sample_p99_us=" << inserts[inserts.size()*99/100]
            << " query_p50_us=" << latencies[1500] << " query_p95_us=" << latencies[2850] << " query_p99_us=" << latencies[2970]
            << " full_hot_user_ms=" << full_ms << " reopen_first_page_us=" << first_us
            << " cache_bytes=" << cache_pages * bustub::BUSTUB_PAGE_SIZE << " pages=" << tree.page_count()
            << " reads=" << m.reads << " writes=" << m.writes << " query_values=" << hits << '\n';
}
}
int main(int argc, char** argv) {
  try {
    auto root = std::filesystem::path(argc > 1 ? argv[1] : "bpt-results"); std::filesystem::create_directories(root);
    if (argc < 4 || std::string(argv[3]) != "benchmark-only") { duplicate_and_reopen(root); concurrent(root); }
    benchmark(root, argc > 2 ? std::stoull(argv[2]) : 300000, argc > 4 ? std::stoull(argv[4]) : 128);
  } catch (const std::exception& e) { std::cerr << "FAIL " << e.what() << '\n'; return 1; }
}
