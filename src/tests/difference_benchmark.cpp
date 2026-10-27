#include "tests/stress_support.hpp"

#include <chrono>
#include <iomanip>
#include <iostream>
#include <thread>

using namespace marketlab::md;
using namespace marketlab::md::test;
namespace {
using Diff = std::vector<Level> (*)(const Book&, const Book&);
using Pair = std::pair<Book, Book>;
struct Result { double ns_per_call; std::uint64_t checksum; };

Result measure(Diff diff, const std::vector<Pair>& cases, std::size_t calls, unsigned workers) {
  std::vector<std::uint64_t> checksums(workers);
  std::vector<std::thread> threads;
  auto run = [&](unsigned worker) {
    std::uint64_t checksum = 0;
    for (std::size_t i = worker; i < calls; i += workers) {
      const auto& [before, after] = cases[i % cases.size()];
      const auto changes = diff(before, after);
      checksum += changes.size();
      for (const auto& level : changes) checksum += level.price + level.quantity + level.num_orders;
    }
    checksums[worker] = checksum;
  };
  const auto started = Clock::now();
  if (workers == 1) run(0);
  else {
    for (unsigned i = 0; i < workers; ++i) threads.emplace_back(run, i);
    for (auto& thread : threads) thread.join();
  }
  const auto elapsed = std::chrono::duration<double, std::nano>(Clock::now() - started).count();
  std::uint64_t sum = 0;
  for (const auto value : checksums) sum += value;
  return {elapsed / static_cast<double>(calls), sum};
}

void report(const std::string& name, const std::vector<Pair>& cases, std::size_t calls, unsigned workers = 1) {
  for (const auto& [before, after] : cases)
    require(reference_difference(before, after) == difference(before, after), "Benchmark outputs differ");
  (void)measure(reference_difference, cases, 1000, 1);
  (void)measure(map_with_linear_validation, cases, 1000, 1);
  (void)measure(difference, cases, 1000, 1);
  std::vector<double> old_times, middle_times, new_times;
  for (unsigned round = 0; round < 7; ++round) {
    Result old_result{}, middle_result{}, new_result{};
    if (round % 3 == 0) {
      old_result = measure(reference_difference, cases, calls, workers);
      middle_result = measure(map_with_linear_validation, cases, calls, workers);
      new_result = measure(difference, cases, calls, workers);
    } else if (round % 3 == 1) {
      new_result = measure(difference, cases, calls, workers);
      old_result = measure(reference_difference, cases, calls, workers);
      middle_result = measure(map_with_linear_validation, cases, calls, workers);
    } else {
      middle_result = measure(map_with_linear_validation, cases, calls, workers);
      new_result = measure(difference, cases, calls, workers);
      old_result = measure(reference_difference, cases, calls, workers);
    }
    require(old_result.checksum == new_result.checksum && old_result.checksum == middle_result.checksum,
            "Benchmark checksums differ");
    old_times.push_back(old_result.ns_per_call);
    middle_times.push_back(middle_result.ns_per_call);
    new_times.push_back(new_result.ns_per_call);
  }
  const auto old_ns = percentile(old_times, .5), new_ns = percentile(new_times, .5);
  const auto middle_ns = percentile(middle_times, .5);
  std::cout << name << ',' << cases.size() << ',' << calls << ',' << workers << ','
            << old_ns << ',' << new_ns << ',' << old_ns / new_ns << ',' << (1 - new_ns / old_ns) * 100 << ','
            << old_ns * calls / 1e6 << ',' << new_ns * calls / 1e6 << ',' << middle_ns << ','
            << old_ns / middle_ns << ',' << middle_ns / new_ns << '\n' << std::flush;
}

std::vector<Pair> catalog(unsigned products) {
  std::vector<Pair> cases;
  cases.reserve(products);
  for (unsigned i = 1; i <= products; ++i) {
    const auto id = static_cast<std::uint16_t>(i);
    auto before = i % 19 == 0 ? Book{id, {}} : full_book(id, static_cast<int>(i % 17));
    auto after = before;
    if (i % 10 == 7 && !after.levels.empty()) ++after.levels[0].quantity;
    if (i % 10 == 8 && !after.levels.empty()) ++after.levels[4].num_orders;
    if (i % 10 == 9) after = full_book(id, 100, 1);
    cases.emplace_back(std::move(before), std::move(after));
  }
  return cases;
}
}  // namespace

int main() {
  try {
    std::cerr << "Release benchmark: 7 rounds, rotating order; includes validation and result lifetime.\n"
              << "hardware_concurrency=" << std::thread::hardware_concurrency() << '\n';
    std::cout << std::fixed << std::setprecision(2)
              << "case,books,calls_per_round,workers,old_ns,new_ns,speedup,reduction_pct,old_total_ms,new_total_ms,linear_map_ns,validation_speedup,merge_speedup\n";
    const auto full = full_book(1);
    auto quantity = full, orders = full, boundary = full;
    ++quantity.levels[0].quantity;
    ++orders.levels[4].num_orders;
    boundary.levels[4] = {Side::Buy, 95, 10, 2};
    const Book empty{1, {}};
    const Book buy{1, {{Side::Buy, 100, 10, 2}}}, sell{1, {{Side::Sell, 100, 10, 2}}};
    const std::vector<std::pair<std::string, Pair>> profiles{
      {"empty_unchanged", {empty, empty}}, {"full_unchanged", {full, full}},
      {"quantity_only", {full, quantity}}, {"orders_only", {full, orders}},
      {"fifth_replacement", {full, boundary}}, {"full_replacement", {full, full_book(1, 100)}},
      {"empty_to_full", {empty, full}}, {"full_to_empty", {full, empty}},
      {"same_price_side_change", {buy, sell}}};
    for (const auto& [name, pair] : profiles) report(name, {pair}, 100000);
    const auto small = catalog(10000);
    report("catalog_10000_mixed", small, small.size());
    const auto large = catalog(60000);
    report("catalog_60000_mixed", large, large.size());
    report("catalog_60000_mixed_parallel", large, large.size(), 8);
  } catch (const std::exception& e) { std::cerr << "BENCHMARK FAILURE: " << e.what() << '\n'; return 1; }
}
