#include "tests/stress_support.hpp"

#include <chrono>
#include <iostream>

using namespace marketlab::md;
using namespace marketlab::md::test;
namespace {
void check_pair(const Book& before, const Book& after, const Epoch& epoch, bool check_wire) {
  const auto expected = reference_difference(before, after);
  const auto actual = difference(before, after);
  require(actual == expected, "Difference differs from the previous map implementation");
  require(apply_changes(before, actual) == after, "Delta does not reconstruct the next view");
  validate_levels(actual, true);
  if (check_wire) {
    const auto bytes = encode({Kind::Delta, epoch, before.symbol_id, 1, 1, actual});
    require(decode(bytes).levels == actual, "Delta wire round trip failed");
  }
}

void exhaustive(const Epoch& epoch) {
  std::vector<Book> books;
  for (unsigned mask = 0; mask < 1024; ++mask) {
    const auto full = full_book(1);
    Book book{1, {}};
    for (unsigned i = 0; i < 10; ++i) if (mask & (1U << i)) book.levels.push_back(full.levels[i]);
    books.push_back(std::move(book));
  }
  for (const auto& before : books)
    for (const auto& after : books) check_pair(before, after, epoch, false);
  std::cout << "exhaustive_pairs=1048576 mismatches=0\n";
}

void random_and_boundaries(const Epoch& epoch) {
  std::mt19937_64 random(2026102601ULL);
  for (unsigned i = 0; i < 100000; ++i) {
    const auto id = static_cast<std::uint16_t>(1 + random() % 65535);
    auto before = random_book(random, id);
    auto after = random_book(random, id);
    if (i % 5 == 0) after = before;
    if (i % 5 == 1) after.levels.clear();
    if (i % 5 == 2) before.levels.clear();
    if (i % 5 == 3 && !before.levels.empty()) {
      after = before;
      auto& changed = after.levels[random() % after.levels.size()];
      changed.quantity = changed.quantity == 1 ? 2 : 1;
      changed.num_orders = changed.num_orders == 1 ? 2 : 1;
    }
    check_pair(before, after, epoch, true);
  }
  const auto max = std::numeric_limits<std::uint64_t>::max();
  const Book boundary{65535, {{Side::Buy, std::numeric_limits<std::int32_t>::max(), max, max},
                             {Side::Sell, 1, max, max}}};
  check_pair({65535, {}}, boundary, epoch, true);
  check_pair(boundary, {65535, {}}, epoch, true);
  const auto bytes = encode({Kind::Snapshot, epoch, 65535, max, max, boundary.levels});
  const auto decoded = decode(bytes);
  require(decoded.index == max && decoded.timestamp == max && decoded.levels == boundary.levels,
          "Integer boundary lost precision");
  std::cout << "random_pairs=100000 uint_boundaries=passed mismatches=0\n";
}

void validation_and_wire_fuzz(const Epoch& epoch) {
  std::mt19937_64 random(2026102602ULL);
  for (unsigned i = 0; i < 50000; ++i) {
    const bool delta = i % 2;
    std::vector<Level> levels;
    const auto size = random() % 23;
    for (std::size_t j = 0; j < size; ++j)
      levels.push_back({static_cast<Side>(random() % 4), static_cast<std::int32_t>(random() % 8) - 1,
                        random() % 3, random() % 3});
    if (i % 3 == 0) std::sort(levels.begin(), levels.end(), wire_order);
    const auto old_accepts = accepts([&] { reference_validate_levels(levels, delta); });
    const auto new_accepts = accepts([&] { validate_levels(levels, delta); });
    require(old_accepts == new_accepts, "Validation acceptance changed");
  }

  const auto valid = encode({Kind::Snapshot, epoch, 1, 9, 123, full_book(1).levels});
  std::size_t malformed_checks = 0;
  auto reject = [&](const Bytes& bytes) {
    require(!accepts([&] { (void)decode(bytes); }), "Malformed wire was accepted");
    ++malformed_checks;
  };
  for (std::size_t n = 0; n < valid.size(); ++n) {
    // Removing complete trailing levels is legal: the public protocol has no count field.
    if (n >= header_size && (n - header_size) % level_size == 0) continue;
    reject(Bytes(valid.begin(), valid.begin() + static_cast<std::ptrdiff_t>(n)));
  }
  for (const auto& [offset, value] : std::array<std::pair<std::size_t, std::uint8_t>, 5>{{
           {0, 0}, {0, 3}, {header_size, 0}, {header_size, 3}, {header_size + 4, 255}}}) {
    auto bad = valid;
    bad[offset] = value;
    reject(bad);
  }
  auto bad = valid;
  bad[17] = bad[18] = 0;
  reject(bad);
  bad = valid;
  std::fill(bad.begin() + 27, bad.begin() + 35, 0);
  reject(bad);
  bad = valid;
  std::copy_n(bad.begin() + header_size, level_size, bad.begin() + header_size + level_size);
  reject(bad);

  const auto shard = encode_shard({epoch, {{1, full_book(1)}, {65535, {65535, {}}}}});
  for (std::size_t n = 0; n < shard.size(); ++n) {
    require(!accepts([&] { (void)decode_shard(std::span(shard).first(n), {1, 65535}); }),
            "Truncated shard frame accepted");
    ++malformed_checks;
  }
  bad = shard;
  bad.push_back(0);
  require(!accepts([&] { (void)decode_shard(bad, {1, 65535}); }), "Trailing shard byte accepted");
  require(!accepts([&] { (void)decode_shard(shard, {1, 2}); }), "Wrong shard ownership accepted");

  const auto delta = encode({Kind::Delta, epoch, 1, 1, 123, difference(full_book(1), full_book(1, 100))});
  require(delta.size() == max_delta_size, "Maximum delta is not 455 bytes");
  require(decode(delta).levels.size() == 20, "Maximum delta lost entries");
  require(valid.size() == max_snapshot_size, "Maximum snapshot is not 245 bytes");
  require(encode({Kind::Delta, epoch, 1, 1, 123, {}}).size() == 35, "Heartbeat is not 35 bytes");
  std::cout << "validation_fuzz=50000 mismatches=0 malformed_wire_checks=" << malformed_checks + 2
            << " size_boundaries=35/245/455_passed\n";
}
}  // namespace

int main() {
  try {
    const auto started = Clock::now();
    const auto epoch = new_epoch();
    exhaustive(epoch);
    random_and_boundaries(epoch);
    validation_and_wire_fuzz(epoch);
    std::cout << "algorithm_stress_ms=" << std::chrono::duration<double, std::milli>(Clock::now() - started).count() << '\n';
  } catch (const std::exception& e) { std::cerr << "STRESS FAILURE: " << e.what() << '\n'; return 1; }
}
