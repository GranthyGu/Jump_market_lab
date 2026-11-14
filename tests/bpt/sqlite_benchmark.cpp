// Compare index-only workloads, not authoritative trade-log durability.
#include <sqlite3.h>
#include <algorithm>
#include <chrono>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>
#include "matching_engine/trade_history/index/trade_index.hpp"

using namespace marketlab::orders::bpt;
using Clock = std::chrono::steady_clock;
using Oracle = std::vector<std::vector<TradePosition>>;
namespace {
void require(bool ok, const char* message) { if (!ok) throw std::runtime_error(message); }
double seconds(Clock::time_point a, Clock::time_point b) {
  return std::chrono::duration<double>(b - a).count();
}
std::uint32_t user_for(std::uint64_t i) { return i % 5 == 0 ? 1 : i % 10000 + 2; }
TradePosition position_for(std::uint64_t i) { return {i * 200, 0, static_cast<std::uint32_t>(i)}; }
struct DatabaseCloser { void operator()(sqlite3* p) const { sqlite3_close_v2(p); } };
struct StatementCloser { void operator()(sqlite3_stmt* p) const { sqlite3_finalize(p); } };
using Statement = std::unique_ptr<sqlite3_stmt, StatementCloser>;

class SqliteIndex {
 public:
  SqliteIndex(const std::filesystem::path& path, std::uint64_t batch, bool reopen = false)
      : batch_(batch) {
    sqlite3* raw = nullptr;
    int rc = sqlite3_open_v2(path.c_str(), &raw,
        SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_NOMUTEX, nullptr);
    db_.reset(raw);
    check(rc);
    if (!reopen) sql("PRAGMA page_size=8192");
    sql("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA cache_size=1024;"
        "PRAGMA mmap_size=0; PRAGMA wal_autocheckpoint=1000;"
        "PRAGMA fullfsync=OFF; PRAGMA checkpoint_fullfsync=OFF;");
    if (!reopen) sql(
        "CREATE TABLE user_trades ("
        "user_id INTEGER NOT NULL, record_offset INTEGER NOT NULL,"
        "ordinal INTEGER NOT NULL, checksum INTEGER NOT NULL,"
        "PRIMARY KEY(user_id, record_offset)) WITHOUT ROWID");
    insert_ = prepare("INSERT INTO user_trades VALUES(?1, ?2, ?3, ?4)");
    query_ = prepare("SELECT record_offset,ordinal,checksum FROM user_trades "
        "WHERE user_id=?1 AND record_offset>?2 AND record_offset<?3 "
        "ORDER BY record_offset LIMIT ?4");
    require(scalar("PRAGMA page_size") == 8192, "SQLite page size differs");
    require(scalar("PRAGMA cache_size") == 1024, "SQLite cache target differs");
    require(scalar("PRAGMA synchronous") == 2, "SQLite synchronous is not FULL");
    require(scalar("PRAGMA mmap_size") == 0, "SQLite mmap must be disabled");
    auto plan = prepare("EXPLAIN QUERY PLAN SELECT record_offset,ordinal,checksum FROM user_trades "
        "WHERE user_id=1 AND record_offset>0 AND record_offset<200000000 "
        "ORDER BY record_offset LIMIT 256");
    require(sqlite3_step(plan.get()) == SQLITE_ROW, "missing SQLite query plan");
    query_plan_ = reinterpret_cast<const char*>(sqlite3_column_text(plan.get(), 3));
    require(query_plan_.find("SEARCH user_trades USING PRIMARY KEY") != std::string::npos,
            "SQLite query did not seek the primary key");
  }
  bool insert(std::uint32_t user, TradePosition p) {
    if (!transaction_) { sql("BEGIN IMMEDIATE"); transaction_ = true; }
    check(sqlite3_bind_int64(insert_.get(), 1, user));
    check(sqlite3_bind_int64(insert_.get(), 2, p.Get()));
    check(sqlite3_bind_int64(insert_.get(), 3, p.ordinal));
    check(sqlite3_bind_int64(insert_.get(), 4, p.checksum));
    int rc = sqlite3_step(insert_.get());
    sqlite3_reset(insert_.get());
    if (rc == SQLITE_CONSTRAINT) return false;
    check(rc, SQLITE_DONE);
    if (++pending_ == batch_) commit();
    return true;
  }
  std::vector<TradePosition> query(std::uint32_t user, std::uint64_t after,
                                   std::uint64_t through, std::size_t limit) {
    std::vector<TradePosition> result;
    result.reserve(limit);
    check(sqlite3_bind_int64(query_.get(), 1, user));
    check(sqlite3_bind_int64(query_.get(), 2, after));
    check(sqlite3_bind_int64(query_.get(), 3, std::min<std::uint64_t>(through, INT64_MAX)));
    check(sqlite3_bind_int64(query_.get(), 4, limit));
    int rc;
    while ((rc = sqlite3_step(query_.get())) == SQLITE_ROW) {
      auto offset = static_cast<std::uint64_t>(sqlite3_column_int64(query_.get(), 0));
      auto ordinal = static_cast<std::uint32_t>(sqlite3_column_int64(query_.get(), 1));
      auto crc = static_cast<std::uint32_t>(sqlite3_column_int64(query_.get(), 2));
      result.push_back({offset - 48 - static_cast<std::uint64_t>(ordinal) * 136, ordinal, crc});
    }
    sqlite3_reset(query_.get());
    check(rc, SQLITE_DONE);
    return result;
  }
  void flush() {
    commit();
    check(sqlite3_wal_checkpoint_v2(db_.get(), nullptr, SQLITE_CHECKPOINT_TRUNCATE, nullptr, nullptr));
  }
  void integrity_check() {
    auto statement = prepare("PRAGMA integrity_check");
    require(sqlite3_step(statement.get()) == SQLITE_ROW, "integrity_check failed");
    require(std::string(reinterpret_cast<const char*>(sqlite3_column_text(statement.get(), 0))) == "ok",
            "SQLite integrity check reports corruption");
  }
  const std::string& query_plan() const { return query_plan_; }
 private:
  void check(int rc, int expected = SQLITE_OK) {
    if (rc != expected) throw std::runtime_error(sqlite3_errmsg(db_.get()));
  }
  void sql(const char* text) { check(sqlite3_exec(db_.get(), text, nullptr, nullptr, nullptr)); }
  Statement prepare(const char* text) {
    sqlite3_stmt* raw = nullptr;
    check(sqlite3_prepare_v2(db_.get(), text, -1, &raw, nullptr));
    return Statement(raw);
  }
  sqlite3_int64 scalar(const char* text) {
    auto stmt = prepare(text);
    check(sqlite3_step(stmt.get()), SQLITE_ROW);
    return sqlite3_column_int64(stmt.get(), 0);
  }
  void commit() {
    if (transaction_) { sql("COMMIT"); transaction_ = false; pending_ = 0; }
  }
  std::unique_ptr<sqlite3, DatabaseCloser> db_;
  Statement insert_, query_;
  std::uint64_t batch_, pending_ = 0;
  bool transaction_ = false;
  std::string query_plan_;
};

using Metrics = std::map<std::string, double>;
void percentiles(Metrics& m, const std::string& name, std::vector<double>& v) {
  std::sort(v.begin(), v.end());
  for (int p : {50, 95, 99}) m[name + "_p" + std::to_string(p) + "_us"] = v[(v.size()-1)*p/100];
}
template<class Index>
void verify(Index& index, const Oracle& oracle) {
  for (std::uint32_t user = 0; user < oracle.size(); ++user) {
    std::uint64_t after = 0;
    std::size_t at = 0;
    for (;;) {
      auto page = index.query(user, after, INT64_MAX, 256);
      require(at + page.size() <= oracle[user].size(), "extra query positions");
      require(std::equal(page.begin(), page.end(), oracle[user].begin() + at), "query oracle mismatch");
      at += page.size();
      if (page.empty()) break;
      after = page.back().Get();
    }
    require(at == oracle[user].size(), "missing query positions");
  }
}
template<class Factory>
Metrics benchmark(const std::filesystem::path& path, Factory factory,
                  std::uint64_t count, std::uint64_t queries) {
  Oracle oracle(10002);
  for (std::uint64_t i = 0; i < count; ++i) oracle[user_for(i)].push_back(position_for(i));
  auto first_pages = oracle;
  for (auto& page : first_pages) if (page.size() > 256) page.resize(256);
  Metrics m{{"entries", count}, {"queries", queries}};
  {
    auto index = factory(false);
    std::vector<double> insert_us, query_us;
    insert_us.reserve((count + 63) / 64); query_us.reserve(queries);
    auto start = Clock::now();
    for (std::uint64_t i = 0; i < count; ++i) {
      auto op = i % 64 == 0 ? Clock::now() : Clock::time_point{};
      require(index->insert(user_for(i), position_for(i)), "insert failed");
      if (i % 64 == 0) insert_us.push_back(seconds(op, Clock::now()) * 1e6);
    }
    auto appended = Clock::now();
    index->flush();
    auto finished = Clock::now();
    m["build_s"] = seconds(start, finished);
    m["build_entries_s"] = count / m["build_s"];
    m["insert_loop_s"] = seconds(start, appended);
    m["final_flush_ms"] = seconds(appended, finished) * 1e3;
    m["main_file_bytes"] = std::filesystem::file_size(path);
    m["wal_bytes_after_flush"] = std::filesystem::exists(path.string()+"-wal")
        ? std::filesystem::file_size(path.string()+"-wal") : 0;
    auto qstart = Clock::now();
    for (std::uint64_t i = 0; i < queries; ++i) {
      auto user = user_for(i);
      auto op = Clock::now();
      auto page = index->query(user, 0, INT64_MAX, 256);
      query_us.push_back(seconds(op, Clock::now()) * 1e6);
      require(page == first_pages[user], "timed query oracle mismatch");
    }
    m["query_pages_s"] = queries / seconds(qstart, Clock::now());
    percentiles(m, "insert", insert_us); percentiles(m, "query", query_us);
    auto hot_start = Clock::now();
    std::uint64_t after = 0;
    std::size_t seen = 0;
    for (;;) {
      auto page = index->query(1, after, INT64_MAX, 256);
      require(seen + page.size() <= oracle[1].size() &&
          std::equal(page.begin(), page.end(), oracle[1].begin() + seen), "hot-user query mismatch");
      seen += page.size();
      if (page.empty()) break;
      after = page.back().Get();
    }
    m["hot_full_query_ms"] = seconds(hot_start, Clock::now()) * 1e3;
    m["hot_entries"] = seen;
    require(seen == oracle[1].size(), "hot-user missing records");
    verify(*index, oracle);
    // Exercise duplicate rejection and the captured upper boundary outside timing.
    require(!index->insert(user_for(0), position_for(0)), "duplicate index entry accepted");
    auto prefix = index->query(1, 0, position_for(10).Get(), 256);
    require(prefix.size() == 2 && prefix[0] == position_for(0) && prefix[1] == position_for(5),
            "query boundary mismatch");
    index->flush();
  }
  auto reopened = factory(true);
  verify(*reopened, oracle);
  return m;
}
void run(const std::filesystem::path& root, const std::string& mode,
         std::uint64_t entries, std::uint64_t queries) {
  std::filesystem::create_directories(root);
  auto path = root / (mode == "bpt" ? "index.pages" : "index.sqlite");
  require(!std::filesystem::exists(path), "use a new output directory for each run");
  Metrics metrics;
  std::string plan;
  if (mode == "bpt") {
    metrics = benchmark(path, [&](bool reopen) {
      return std::make_unique<TradeIndex>(path, 1024, reopen);
    }, entries, queries);
  } else {
    const auto batch = mode == "sqlite-bulk" ? entries : 1000;
    metrics = benchmark(path, [&](bool reopen) {
      return std::make_unique<SqliteIndex>(path, batch, reopen);
    }, entries, queries);
    SqliteIndex check(path, batch, true);
    check.integrity_check(); plan = check.query_plan();
    metrics["transaction_batch"] = batch;
  }
  std::cout << std::setprecision(12) << "{\"mode\":\"" << mode << "\",\"correct\":true"
            << ",\"sqlite_version\":\"" << sqlite3_libversion() << "\",\"query_plan\":\"" << plan << "\"";
  for (const auto& [key,value] : metrics) std::cout << ",\"" << key << "\":" << value;
  std::cout << "}\n";
}
}
int main(int argc, char** argv) {
  try {
    require(argc == 5, "usage: bpt_sqlite_benchmark ROOT bpt|sqlite-bulk|sqlite-batch|verify ENTRIES QUERIES");
    auto entries = std::stoull(argv[3]), queries = std::stoull(argv[4]);
    require(entries >= 11 && entries <= UINT32_MAX && queries > 0, "invalid workload size");
    std::string mode = argv[2];
    if (mode == "verify") {
      // A distinct root is required because the benchmark never overwrites data.
      auto unique = std::to_string(Clock::now().time_since_epoch().count());
      for (const char* m : {"bpt", "sqlite-bulk", "sqlite-batch"})
        run(std::filesystem::path(argv[1]) / unique / m, m, entries, queries);
    } else {
      require(mode == "bpt" || mode == "sqlite-bulk" || mode == "sqlite-batch", "unknown mode");
      run(argv[1], mode, entries, queries);
    }
  } catch (const std::exception& e) {
    std::cerr << "FAIL: " << e.what() << '\n';
    return 1;
  }
}
