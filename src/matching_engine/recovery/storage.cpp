#include "matching_engine/recovery/storage.hpp"

#include <algorithm>
#include <map>
#include <unordered_map>

namespace marketlab::orders::persistence {
namespace {
std::vector<std::uint64_t> fingerprints(const OrderConfig& c, std::uint16_t shard) {
  std::uint64_t h = 1469598103934665603ULL;
  auto add = [&](const std::string& s) {
    for (unsigned char b : s) {
      h ^= b;
      h *= 1099511628211ULL;
    }
    h ^= 255;
    h *= 1099511628211ULL;
  };
  add("parallel-v2");
  add(std::to_string(shard));
  for (const auto& u : c.accounts)
    add(u.name);
  std::vector<std::uint64_t> values{h};
  for (const auto& p : c.products) {
    add(p.symbol);
    add(std::to_string(p.previous_close));
    add(std::to_string(p.shard));
    // Only prefixes containing every local product are compatible. Appending
    // other shards' products preserves existing IDs and this shard's books.
    if (p.shard == shard)
      values.clear();
    values.push_back(h);
  }
  return values;
}

std::uint32_t chain(std::uint32_t prev, std::uint32_t next) {
  Bytes b;
  integer(b, next, 4);
  return crc32c(b, prev);
}

void check_id(std::uint64_t id, std::uint16_t shard, std::uint64_t next) {
  if ((id >> 48) != shard || !(id & ((1ULL << 48) - 1)) || (id & ((1ULL << 48) - 1)) >= next)
    throw std::runtime_error("Persisted ID outside shard/counter range");
}
}  // namespace

Storage::Storage(const Options& o, const OrderConfig& c, std::uint16_t shard)
    : root_(o.directory),
      lock_(root_),
      options_(o),
      shard_(shard),
      fingerprints_(fingerprints(c, shard)),
      dictionary_(c) {
  make_directory(root_ / "trades");
  make_directory(root_ / "orders");
  make_directory(root_ / "checkpoints");
  sync_directory(root_);
}

void Storage::checkpoint(const Boundary& b, const FrozenPages& pages, bool prune) {
  const auto dir = root_ / "checkpoints";
  const auto final = dir / ("book-" + std::to_string(b.seq) + ".bin");
  const auto temp = dir / ("book-" + std::to_string(b.seq) + ".tmp");
  File file(temp, true, true);
  file.truncate(0);
  Bytes scratch;
  scratch.reserve(64 * 1024);
  std::uint32_t checksum = 0;
  std::uint64_t total = 0, page_count = 0;
  auto append = [&] {
    finish_frame(scratch);
    checksum = chain(checksum, crc32c(std::span(scratch).first(scratch.size() - trailer_bytes)));
    file.append(scratch);
  };
  start_frame(scratch, Kind::SnapshotMeta, b, 0, 0);
  integer(scratch, fingerprints_.back(), 8);
  integer(scratch, shard_, 2);
  integer(scratch, b.trade_end, 8);
  integer(scratch, b.trade_count, 8);
  append();
  for (const auto& page : pages) {
    std::uint32_t count = 0;
    for (const auto& o : page->orders)
      if (o)
        ++count;
    if (!count)
      continue;
    start_frame(scratch, Kind::SnapshotPage, b, 0, count);
    for (const auto& o : page->orders)
      if (o)
        encode_order(scratch, dictionary_.expand(*o));
    append();
    total += count;
    ++page_count;
  }
  start_frame(scratch, Kind::SnapshotEnd, b, 0, 0);
  integer(scratch, total, 8);
  integer(scratch, page_count, 8);
  integer(scratch, checksum, 4);
  finish_frame(scratch);
  file.append(scratch);
  file.sync();
  if (options_.hook)
    options_.hook(Stage::BeforeSnapshotPublish, b.seq);
  std::filesystem::rename(temp, final);
  sync_directory(dir);
  if (options_.hook)
    options_.hook(Stage::SnapshotPublished, b.seq);
  if (!prune)
    return;
  if (options_.hook)
    options_.hook(Stage::BeforePrune, b.seq);
  // The owner inserts a cut marker before C+1 and waits for rotation before
  // issuing this job. Thus every old generation starting <= C ends <= C.
  for (const auto& [first, path] : numbered_files(root_ / "orders", "delta-", ".log"))
    if (first <= b.seq)
      std::filesystem::remove(path);
  sync_directory(root_ / "orders");
  for (const auto& [seq, path] : numbered_files(dir, "book-", ".bin"))
    if (seq < b.seq)
      std::filesystem::remove(path);
  sync_directory(dir);
}

Recovered Storage::read_snapshot(const std::filesystem::path& path) const {
  File file(path, false);
  std::uint64_t at = 0, total = 0, pages = 0;
  std::uint32_t checksum = 0;
  Recovered out;
  auto first = next_frame(file, at);
  if (!first || first->kind != Kind::SnapshotMeta)
    throw std::runtime_error("Missing snapshot metadata");
  Reader m{first->payload};
  const auto persisted_fingerprint = m.number(8);
  if (std::find(fingerprints_.begin(), fingerprints_.end(), persisted_fingerprint) ==
          fingerprints_.end() || m.number(2) != shard_)
    throw std::runtime_error("Persistence configuration/shard mismatch");
  out.boundary = first->boundary;
  out.boundary.trade_end = m.number(8);
  out.boundary.trade_count = m.number(8);
  m.finish();
  out.checkpoint = out.boundary.seq;
  checksum = chain(checksum, first->checksum);
  for (;;) {
    auto f = next_frame(file, at);
    if (!f)
      throw std::runtime_error("Incomplete published checkpoint");
    if (f->boundary.seq != out.boundary.seq || f->boundary.next_order != out.boundary.next_order ||
        f->boundary.next_trade != out.boundary.next_trade)
      throw std::runtime_error("Inconsistent checkpoint frame");
    Reader r{f->payload};
    if (f->kind == Kind::SnapshotEnd) {
      if (r.number(8) != total || r.number(8) != pages || r.number(4) != checksum ||
          at != file.size())
        throw std::runtime_error("Checkpoint completeness mismatch");
      r.finish();
      break;
    }
    if (f->kind != Kind::SnapshotPage || f->count > Page::slots)
      throw std::runtime_error("Unexpected checkpoint frame");
    for (unsigned i = 0; i < f->count; ++i)
      out.orders.push_back(decode_order(r));
    r.finish();
    total += f->count;
    ++pages;
    checksum = chain(checksum, f->checksum);
  }
  return out;
}

Recovered Storage::recover() {
  auto snapshots = numbered_files(root_ / "checkpoints", "book-", ".bin");
  if (snapshots.empty()) {
    if ((std::filesystem::exists(trade_path()) && std::filesystem::file_size(trade_path())) ||
        !numbered_files(root_ / "orders", "delta-", ".log").empty())
      throw std::runtime_error("Missing recovery root in nonempty data directory");
    File trades_file(trade_path(), true, true);
    trades_file.sync();
    sync_directory(root_ / "trades");
    checkpoint({}, {}, false);
    snapshots = numbered_files(root_ / "checkpoints", "book-", ".bin");
  }
  auto out = read_snapshot(snapshots.back().second);
  if (out.boundary.seq != snapshots.back().first)
    throw std::runtime_error("Checkpoint name mismatch");
  std::unordered_map<OrderId, StoredOrder> active;
  for (const auto& o : out.orders) {
    check_id(o.state.orderId, shard_, out.boundary.next_order);
    if (!active.emplace(OrderId(o.state.orderId), o).second)
      throw std::runtime_error("Duplicate checkpoint order");
  }
  File tf(trade_path(), true);
  if (tf.size() < out.boundary.trade_end)
    throw std::runtime_error("Missing checkpoint trade prefix");
  if (out.boundary.trade_end) {
    if (out.boundary.trade_end < header_bytes + trailer_bytes)
      throw std::runtime_error("Invalid checkpoint trade offset");
    const auto tail = tf.read(out.boundary.trade_end - 8, 8);
    Reader r{tail};
    if (r.number(8) != frame_end)
      throw std::runtime_error("Invalid checkpoint trade boundary");
  }

  struct TradeInfo {
    Boundary b;
    std::uint32_t count;
    std::uint64_t end;
  };

  std::map<std::uint64_t, TradeInfo> trades;
  auto tp = out.boundary.trade_end;
  std::uint64_t previous = out.boundary.seq;
  while (auto f = next_frame(tf, tp)) {
    if (f->kind != Kind::Trades || f->boundary.seq <= previous || f->count != f->trades ||
        !f->trades)
      throw std::runtime_error("Invalid trade log ordering");
    if (f->payload.size() != std::uint64_t(f->count) * stored_trade_bytes)
      throw std::runtime_error("Invalid trade payload count");
    Reader r{f->payload};
    for (unsigned i = 0; i < f->count; ++i) {
      auto t = decode_trade(r);
      check_id(t.id, shard_, f->boundary.next_trade);
      check_id(t.buy_order, shard_, f->boundary.next_order);
      check_id(t.sell_order, shard_, f->boundary.next_order);
    }
    r.finish();
    previous = f->boundary.seq;
    trades.emplace(previous, TradeInfo{f->boundary, f->count, f->end});
  }
  bool orphan = false;
  std::vector<std::pair<std::filesystem::path, std::uint64_t>> cleanup;
  const auto deltas = numbered_files(root_ / "orders", "delta-", ".log");
  for (const auto& [first, path] : deltas) {
    File file(path, true);
    std::uint64_t at = 0, keep = 0, seen = 0;
    while (auto f = next_frame(file, at)) {
      if (f->kind != Kind::Orders || f->boundary.seq < first ||
          (seen && f->boundary.seq != seen + 1))
        throw std::runtime_error("Invalid order log ordering");
      seen = f->boundary.seq;
      if (f->boundary.seq <= out.checkpoint) {
        keep = at;
        continue;
      }
      if (f->boundary.seq != out.boundary.seq + 1 && !orphan)
        throw std::runtime_error("Missing order log batch");
      const auto trade = trades.find(f->boundary.seq);
      const bool paired = f->trades == 0 || trade != trades.end();
      if (!paired) {
        orphan = true;
        continue;
      }
      if (orphan) {
        if (f->trades && trade != trades.end())
          throw std::runtime_error("Paired batch after incomplete prefix");
        continue;
      }
      if (f->boundary.next_order < out.boundary.next_order ||
          f->boundary.next_trade != out.boundary.next_trade + f->trades)
        throw std::runtime_error("Nonmonotonic committed ID counters");
      if (f->trades) {
        const auto& t = trade->second;
        if (t.count != f->trades || t.b.next_order != f->boundary.next_order ||
            t.b.next_trade != f->boundary.next_trade)
          throw std::runtime_error("Mismatched parallel log frames");
        out.boundary.trade_end = t.end;
        out.boundary.trade_count += t.count;
      } else if (trade != trades.end()) {
        throw std::runtime_error("Unexpected trade for empty batch");
      }
      Reader r{f->payload};
      if (f->count > f->payload.size() / 9)
        throw std::runtime_error("Invalid delta record count");
      for (unsigned i = 0; i < f->count; ++i) {
        const auto kind = r.number(1);
        if (kind == 0) {
          const auto id = r.number(8);
          check_id(id, shard_, f->boundary.next_order);
          active.erase(id);
        } else if (kind == 1) {
          auto o = decode_order(r);
          check_id(o.state.orderId, shard_, f->boundary.next_order);
          active[OrderId(o.state.orderId)] = o;
        } else {
          throw std::runtime_error("Invalid order delta kind");
        }
      }
      r.finish();
      out.boundary.seq = f->boundary.seq;
      out.boundary.next_order = f->boundary.next_order;
      out.boundary.next_trade = f->boundary.next_trade;
      keep = at;
    }
    cleanup.emplace_back(path, keep);
  }
  for (const auto& [path, keep] : cleanup) {
    File file(path, true);
    if (keep != file.size())
      file.truncate(keep);
    else
      file.sync();
    if (!keep)
      std::filesystem::remove(path);
  }
  if (tf.size() != out.boundary.trade_end)
    tf.truncate(out.boundary.trade_end);
  else
    tf.sync();
  sync_directory(root_ / "orders");
  out.orders.clear();
  out.orders.reserve(active.size());
  for (auto& [id, o] : active) {
    (void)id;
    out.orders.push_back(std::move(o));
  }
  std::sort(out.orders.begin(), out.orders.end(), [](const auto& a, const auto& b) {
    return a.arrival < b.arrival;
  });
  return out;
}
}  // namespace marketlab::orders::persistence
