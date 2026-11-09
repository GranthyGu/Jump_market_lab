#include "history_store.hpp"

#include <iostream>
#include <utility>

namespace marketlab::orders::bpt {
using namespace persistence;

namespace {
constexpr std::uint64_t snapshot_magic = 0x315450424c4dULL;
constexpr std::size_t footer_size = 80;
constexpr std::size_t copy_chunk = 1024 * 1024;

std::uint32_t copy_pages(const File& source, File& destination, std::uint64_t size) {
  std::uint32_t crc = 0;
  for (std::uint64_t at = 0; at < size;) {
    auto length = static_cast<std::size_t>(std::min<std::uint64_t>(copy_chunk, size - at));
    auto bytes = source.read(at, length);
    if (bytes.size() != length)
      throw std::runtime_error("truncated index checkpoint");
    crc = crc32c(bytes, crc);
    destination.append(bytes);
    at += bytes.size();
  }
  return crc;
}
}

HistoryStore::HistoryStore(const std::filesystem::path& root,
                           const OrderConfig& config,
                           std::uint16_t shard,
                           std::uint64_t recovered_end,
                           std::size_t cache_pages,
                           std::size_t query_slots,
                           std::function<void()> wakeup,
                           std::uint64_t checkpoint_bytes)
    : root_(root / "history"),
      log_path_(root / "trades" / "trades.log"),
      working_path_(root_ / "index.pages"),
      snapshot_path_(root_ / "index.snapshot"),
      shard_(shard),
      wakeup_(std::move(wakeup)),
      query_slots_(query_slots),
      checkpoint_bytes_(checkpoint_bytes),
      target_(recovered_end) {
  if (cache_pages < 32 || !query_slots || !checkpoint_bytes)
    throw std::invalid_argument("invalid history options");
  make_directory(root_);
  names_.emplace_back();
  for (const auto& account : config.accounts) {
    users_.emplace(account.name, static_cast<std::uint32_t>(names_.size()));
    names_.push_back(account.name);
    for (unsigned char ch : account.name) {
      fingerprint_ ^= ch;
      fingerprint_ *= 1099511628211ULL;
    }
    fingerprint_ ^= 0xff;
    fingerprint_ *= 1099511628211ULL;
  }
  fingerprint_ ^= shard_;
  bool restored = restore(recovered_end);
  index_ = std::make_unique<TradeIndex>(working_path_, cache_pages, restored);
  std::cout << "HISTORY_INDEX shard=" << shard_ << " restored=" << restored
            << " indexed_bytes=" << indexed_ << " committed_bytes=" << recovered_end
            << " cache_pages=" << cache_pages << std::endl;
  try {
    index_thread_ = std::thread([this] {
      index_worker();
    });
    for (int i = 0; i < 2; ++i)
      readers_.emplace_back([this] {
        query_worker();
      });
  } catch (...) {
    close();
    throw;
  }
}

HistoryStore::~HistoryStore() {
  close();
}

bool HistoryStore::restore(std::uint64_t recovered_end) {
  if (!std::filesystem::exists(snapshot_path_))
    return false;
  try {
    File snapshot(snapshot_path_, false);
    if (snapshot.size() < footer_size + bustub::BUSTUB_PAGE_SIZE)
      throw std::runtime_error("short index checkpoint");
    const auto bytes = snapshot.read(snapshot.size() - footer_size, footer_size);
    Reader meta{bytes};
    if (meta.number(8) != snapshot_magic || meta.number(8) != 2 || meta.number(8) != fingerprint_)
      throw std::runtime_error("incompatible index checkpoint");
    const auto end = meta.number(8), last_frame = meta.number(8), first_crc = meta.number(8),
               last_crc = meta.number(8);
    const auto page_bytes = meta.number(8), pages_crc = meta.number(8), meta_crc = meta.number(8);
    if (meta_crc != crc32c(std::span(bytes).first(footer_size - 8)) || end > recovered_end ||
        page_bytes != snapshot.size() - footer_size || page_bytes % bustub::BUSTUB_PAGE_SIZE ||
        page_bytes / bustub::BUSTUB_PAGE_SIZE >= INT32_MAX)
      throw std::runtime_error("invalid index checkpoint metadata");
    if (end) {
      File log(log_path_, false);
      std::uint64_t pos = 0;
      auto first = next_frame(log, pos);
      pos = last_frame;
      auto last = next_frame(log, pos);
      if (!first || !last || first->checksum != first_crc || last->checksum != last_crc ||
          last->end != end)
        throw std::runtime_error("index checkpoint does not match trade log");
    }
    File work(working_path_, true, true);
    work.truncate(0);
    if (copy_pages(snapshot, work, page_bytes) != pages_crc)
      throw std::runtime_error("index checkpoint checksum mismatch");
    auto header = work.read(0, sizeof(bustub::page_id_t));
    bustub::page_id_t root{};
    std::memcpy(&root, header.data(), sizeof(root));
    if (root != bustub::INVALID_PAGE_ID &&
        (root <= 0 || static_cast<std::uint64_t>(root) >= page_bytes / bustub::BUSTUB_PAGE_SIZE))
      throw std::runtime_error("invalid index root");
    indexed_ = checkpoint_end_ = end;
    last_frame_ = last_frame;
    first_crc_ = first_crc;
    last_crc_ = last_crc;
    return true;
  } catch (const std::exception& e) {
    std::cerr << "HISTORY_REBUILD shard=" << shard_ << " reason=" << e.what() << std::endl;
    return false;
  }
}

void HistoryStore::checkpoint(std::uint64_t end) {
  // Only the index writer calls this between complete committed frames. Readers
  // keep running; no structural mutation can race with the copied page image.
  index_->flush();
  const auto temporary = root_ / "index.snapshot.tmp";
  File source(working_path_, false), destination(temporary, true, true);
  destination.truncate(0);
  const auto page_bytes = source.size();
  const auto pages_crc = copy_pages(source, destination, page_bytes);
  Bytes footer;
  for (const std::uint64_t v : {snapshot_magic,
                                std::uint64_t{2},
                                fingerprint_,
                                end,
                                last_frame_,
                                std::uint64_t{first_crc_},
                                std::uint64_t{last_crc_},
                                page_bytes,
                                std::uint64_t{pages_crc}})
    integer(footer, v, 8);
  integer(footer, crc32c(footer), 8);
  destination.append(footer);
  destination.sync();
  std::filesystem::rename(temporary, snapshot_path_);
  sync_directory(root_);
  checkpoint_end_ = end;
}

void HistoryStore::publish(std::uint64_t end) {
  {
    std::lock_guard lock(mutex_);
    target_ = std::max(target_, end);
  }
  ready_.notify_all();
}

void HistoryStore::report_failure(std::exception_ptr error) {
  {
    std::lock_guard lock(mutex_);
    if (!failure_)
      failure_ = error;
    // Even if a reader could not open its file, queued requests must complete
    // with an explicit error instead of waiting indefinitely for a dead worker.
    while (!requests_.empty()) {
      Answer answer;
      answer.task = std::move(requests_.front());
      requests_.pop_front();
      answer.error = RejectReason::HistoryUnavailable;
      answers_.push_back(std::move(answer));
    }
  }
  try {
    std::rethrow_exception(error);
  } catch (const std::exception& e) {
    std::cerr << "HISTORY_UNAVAILABLE shard=" << shard_ << " reason=" << e.what() << std::endl;
  } catch (...) {
    std::cerr << "HISTORY_UNAVAILABLE shard=" << shard_ << std::endl;
  }
  ready_.notify_all();
  wakeup_();
}

void HistoryStore::index_worker() {
  try {
    File log(log_path_, false);
    for (;;) {
      std::uint64_t end, position;
      {
        std::unique_lock lock(mutex_);
        ready_.wait(lock, [&] {
          return stopping_ || failure_ || target_ > indexed_;
        });
        if (failure_)
          return;
        end = target_;
        position = indexed_;
        if (stopping_ && position == end) {
          lock.unlock();
          checkpoint(end);
          return;
        }
      }
      while (position < end) {
        auto frame = next_frame(log, position);
        if (!frame || frame->kind != Kind::Trades || frame->end > end ||
            frame->count != frame->trades ||
            frame->payload.size() != static_cast<std::size_t>(frame->count) * stored_trade_bytes)
          throw std::runtime_error("invalid committed trade frame while indexing");
        Reader records{frame->payload};
        for (std::uint32_t i = 0; i < frame->count; ++i) {
          auto trade = decode_trade(records);
          auto buyer = users_.at(fixed_string(trade.buyer)),
               seller = users_.at(fixed_string(trade.seller));
          const auto raw =
              std::span(frame->payload)
                  .subspan(static_cast<std::size_t>(i) * stored_trade_bytes, stored_trade_bytes);
          const TradePosition where{frame->start, i, crc32c(raw)};
          index_->insert(buyer, where);
          if (buyer != seller)
            index_->insert(seller, where);
        }
        if (frame->start == 0)
          first_crc_ = frame->checksum;
        last_frame_ = frame->start;
        last_crc_ = frame->checksum;
        {
          std::lock_guard lock(mutex_);
          indexed_ = position;
        }
        ready_.notify_all();
      }
      if (position - checkpoint_end_ >= checkpoint_bytes_)
        checkpoint(position);
    }
  } catch (...) {
    report_failure(std::current_exception());
  }
}

void HistoryStore::request(const LinkHeader& context,
                           const TradeHistoryRequest& request,
                           std::uint64_t committed_end,
                           const Emit& emit) {
  auto q = request;
  RejectReason error{};
  if ((q.shard && q.shard != shard_) || !q.limit || q.limit > trade_history_page_limit ||
      q.afterOffset > q.throughOffset || (!q.throughOffset && q.afterOffset) ||
      q.throughOffset > committed_end || context.user == 0 || context.user >= names_.size()) {
    error = RejectReason::InvalidHistoryQuery;
  } else {
    if (!q.throughOffset)
      q.throughOffset = committed_end;
    std::lock_guard lock(mutex_);
    if (failure_ || stopping_)
      error = RejectReason::HistoryUnavailable;
    else if (outstanding_ >= query_slots_)
      error = RejectReason::HistoryBusy;
    else {
      ++outstanding_;
      requests_.push_back({context, q, emit});
    }
  }
  if (error != RejectReason{})
    emit(LinkKind::Event, context, record_bytes(rejected(q.header.requestId, error)));
  else
    ready_.notify_all();
}

void HistoryStore::query_worker() {
  try {
    File log(log_path_, false);
    for (;;) {
      Answer answer;
      {
        std::unique_lock lock(mutex_);
        ready_.wait(lock, [&] {
          return stopping_ || (!requests_.empty() &&
                               (failure_ || indexed_ >= requests_.front().request.throughOffset));
        });
        if (stopping_)
          return;
        answer.task = std::move(requests_.front());
        requests_.pop_front();
        if (failure_)
          answer.error = RejectReason::HistoryUnavailable;
      }
      try {
        const auto& q = answer.task.request;
        if (answer.error == RejectReason{}) {
          if (q.throughOffset) {
            if (q.throughOffset < header_bytes + trailer_bytes)
              throw std::invalid_argument("short history boundary");
            const auto footer = log.read(q.throughOffset - 8, 8);
            if (footer.size() != 8 || Reader{footer}.number(8) != frame_end)
              throw std::invalid_argument("history cursor is not a frame boundary");
          }
          auto positions =
              index_->query(answer.task.context.user, q.afterOffset, q.throughOffset, q.limit + 1);
          answer.more = positions.size() > q.limit;
          if (answer.more)
            positions.resize(q.limit);
          Bytes window;
          std::uint64_t window_at = 0;
          std::size_t next_position = 0;
          for (const auto& position : positions) {
            const auto offset = position.Get();
            if (offset > q.throughOffset || q.throughOffset - offset < stored_trade_bytes)
              throw std::runtime_error("trade index extends past committed boundary");
            if (window.empty() || offset < window_at ||
                offset - window_at + stored_trade_bytes > window.size()) {
              // Coalesce nearby positions into at most 8 KiB. Sparse values
              // still read only their own 136-byte record, not a whole frame.
              window_at = offset;
              auto end = offset + stored_trade_bytes;
              for (std::size_t i = next_position + 1; i < positions.size(); ++i) {
                auto next = positions[i].Get();
                if (next < end || next - end > 4096 || next - offset + stored_trade_bytes > 8192)
                  break;
                end = next + stored_trade_bytes;
              }
              window = log.read(window_at, end - window_at);
              if (window.size() != end - window_at)
                throw std::runtime_error("truncated historical trade");
            }
            const auto raw = std::span(window).subspan(offset - window_at, stored_trade_bytes);
            if (crc32c(raw) != position.checksum)
              throw std::runtime_error("historical trade checksum mismatch");
            Reader r{raw};
            auto record = decode_trade(r);
            const auto& name = names_.at(answer.task.context.user);
            if (fixed_string(record.buyer) != name && fixed_string(record.seller) != name)
              throw std::runtime_error("trade index participant mismatch");
            answer.records.push_back(record);
            answer.next = position.Get();
            ++next_position;
          }
        }
      } catch (const std::invalid_argument&) {
        answer.records.clear();
        answer.error = RejectReason::InvalidHistoryQuery;
      } catch (...) {
        answer.records.clear();
        answer.error = RejectReason::HistoryUnavailable;
        report_failure(std::current_exception());
      }
      {
        std::lock_guard lock(mutex_);
        answers_.push_back(std::move(answer));
      }
      wakeup_();
    }
  } catch (...) {
    report_failure(std::current_exception());
  }
}

void HistoryStore::poll() {
  std::deque<Answer> answers;
  {
    std::lock_guard lock(mutex_);
    answers.swap(answers_);
    outstanding_ -= answers.size();
  }
  for (const auto& answer : answers) {
    const auto& task = answer.task;
    if (answer.error != RejectReason{}) {
      task.emit(LinkKind::Event,
                task.context,
                record_bytes(rejected(task.request.header.requestId, answer.error)));
      continue;
    }
    for (const auto& trade : answer.records) {
      TradeHistoryRecord record{};
      record.header = {
          MessageType::TradeHistoryRecord, sizeof(record), task.request.header.requestId};
      record.timestamp = trade.timestamp;
      record.tradeId = trade.id;
      record.price = trade.price;
      record.quantity = trade.quantity;
      record.buyOrderId = trade.buy_order;
      record.sellOrderId = trade.sell_order;
      std::memcpy(record.symbol, trade.symbol.data(), 32);
      std::memcpy(record.buyer, trade.buyer.data(), 32);
      std::memcpy(record.seller, trade.seller.data(), 32);
      task.emit(LinkKind::Event, task.context, record_bytes(record));
    }
    auto end =
        server_record<TradeHistoryEnd>(MessageType::TradeHistoryEnd, task.request.header.requestId);
    end.shard = shard_;
    end.flags = answer.more ? history_more : 0;
    end.count = answer.records.size();
    end.nextOffset = answer.next ? answer.next : task.request.afterOffset;
    end.throughOffset = task.request.throughOffset;
    task.emit(LinkKind::HistoryEnd, task.context, record_bytes(end));
  }
}

bool HistoryStore::pending() const {
  std::lock_guard lock(mutex_);
  return outstanding_ != 0;
}

void HistoryStore::close() noexcept {
  {
    std::lock_guard lock(mutex_);
    stopping_ = true;
  }
  ready_.notify_all();
  for (auto& reader : readers_)
    if (reader.joinable())
      reader.join();
  if (index_thread_.joinable())
    index_thread_.join();
}
}  // namespace marketlab::orders::bpt
