#include "matching_engine/persistence/durable_engine.hpp"

#include <iostream>
#include <utility>

namespace marketlab::orders::persistence {
namespace {
Options validate(Options o) {
  if (o.directory.empty() || !o.pending_slots || o.pending_slots > 65536 || !o.sync_group ||
      o.sync_group > o.pending_slots || o.command_bytes < 4096 ||
      o.command_bytes > maximum_payload || o.pending_bytes < o.command_bytes ||
      o.cow_bytes < sizeof(Page) || !o.query_slots || o.query_slots > 65536 ||
      o.query_bytes < sizeof(OrderState) || o.checkpoint_interval.count() <= 0)
    throw std::invalid_argument("Invalid persistence buffer/checkpoint options");
  return o;
}
}  // namespace

DurableEngine::DurableEngine(const OrderConfig& c, std::uint16_t shard, Options o)
    : options_(validate(std::move(o))),
      shard_(shard),
      storage_(options_, c, shard),
      working_(c, shard),
      committed_(c, shard),
      trade_queue_(options_.pending_slots),
      order_queue_(options_.pending_slots) {
  auto recovered = storage_.recover();
  boundary_ = recovered.boundary;
  initial_ = boundary_;
  executed_ = boundary_.seq;
  checkpoint_seq_ = recovered.checkpoint;
  working_.restore(recovered.orders, boundary_.next_order, boundary_.next_trade);
  for (const auto& order : recovered.orders) {
    committed_.apply({false, storage_.dictionary().compact(order)});
  }
  File initial(storage_.delta_path(executed_ + 1), true, true);
  initial.sync();
  sync_directory(storage_.root() / "orders");
  history_ = std::make_unique<trade_history::HistoryStore>(
      storage_.root(),
      c,
      shard,
      boundary_.trade_end,
      options_.history_cache_pages,
      options_.query_slots,
      [this] {
        wakeup_.notify();
      },
      options_.history_checkpoint_bytes);
  std::cout << "RECOVERY_READY shard=" << shard << " batch=" << boundary_.seq
            << " checkpoint=" << checkpoint_seq_ << " active_orders=" << recovered.orders.size()
            << " trades=" << boundary_.trade_count << " data=" << storage_.root() << std::endl;
  try {
    trade_thread_ = std::thread([this] {
      writer(true);
    });
    order_thread_ = std::thread([this] {
      writer(false);
    });
    snapshot_thread_ = std::thread([this] {
      snapshot_worker();
    });
  } catch (...) {
    stop();
    throw;
  }
}

DurableEngine::~DurableEngine() {
  stop();
}

void DurableEngine::stop() noexcept {
  trade_queue_.close();
  order_queue_.close();
  snapshot_queue_.close();
  if (trade_thread_.joinable())
    trade_thread_.join();
  if (order_thread_.joinable())
    order_thread_.join();
  if (snapshot_thread_.joinable())
    snapshot_thread_.join();
  if (history_)
    history_->close();
}

void DurableEngine::hook(Stage s, std::uint64_t seq) {
  if (options_.hook)
    options_.hook(s, seq);
}

void DurableEngine::fail(std::exception_ptr error) {
  {
    std::lock_guard lock(completion_mutex_);
    if (!error_)
      error_ = error;
    failed_.store(true, std::memory_order_release);
  }
  trade_queue_.close();
  order_queue_.close();
  snapshot_queue_.close();
  completion_.notify_all();
  wakeup_.notify();
}

void DurableEngine::check_error() {
  if (failed_.load(std::memory_order_acquire)) {
    std::lock_guard lock(completion_mutex_);
    std::rethrow_exception(error_);
  }
}

void DurableEngine::wait_progress() {
  std::unique_lock lock(completion_mutex_);
  completion_.wait_for(lock, std::chrono::milliseconds(10));
}

void DurableEngine::writer(bool trades) {
  try {
    auto& queue = trades ? trade_queue_ : order_queue_;
    File file(trades ? storage_.trade_path() : storage_.delta_path(initial_.seq + 1), true);
    // Only this writer updates these local offsets. Main observes published receipts.
    std::uint64_t trade_count = initial_.trade_count;
    Bytes scratch;
    scratch.reserve(256 * 1024);
    std::vector<Batch*> group;
    group.reserve(options_.sync_group);
    std::optional<Job> deferred;
    while (!failed_.load(std::memory_order_acquire)) {
      auto first = deferred ? std::exchange(deferred, std::nullopt) : queue.pop();
      if (!first)
        break;
      if (first->cut) {
        auto* cut = first->cut;
        file.sync();
        file = File(storage_.delta_path(cut->seq + 1), true, true);
        file.sync();
        sync_directory(storage_.root() / "orders");
        delta_bytes_.store(0, std::memory_order_relaxed);
        cut->done.store(true, std::memory_order_release);
        completion_.notify_all();
        wakeup_.notify();
        continue;
      }
      group.clear();
      group.push_back(first->batch);
      while (group.size() < options_.sync_group) {
        auto job = queue.try_pop();
        if (!job)
          break;
        if (job->cut) {
          deferred = job;
          break;
        }
        group.push_back(job->batch);
      }
      bool wrote = false;
      for (auto* batch : group) {
        const auto& r = batch->result;
        hook(trades ? Stage::BeforeTradeWrite : Stage::BeforeOrderWrite, batch->b.seq);
        // Reserve once per frame before encoding. PUT is the largest delta;
        // the upper bound avoids a second scan just to count DELETE records.
        const auto payload_bound =
            trades ? (batch->replay ? batch->replay->payload.size()
                                    : r.trades.size() * stored_trade_bytes +
                                          r.executions.size() *
                                              (maximum_execution_bytes + execution_prefix_bytes))
                   : r.changes.size() * (1 + stored_order_bytes);
        scratch.reserve(header_bytes + payload_bound + trailer_bytes);
        if (trades) {
          if (batch->records()) {
            const auto kind = batch->replay
                                  ? batch->replay->kind
                                  : (r.executions.empty() ? Kind::Trades : Kind::TradesV2);
            start_frame(scratch, kind, batch->b, batch->records(), batch->records());
            if (batch->replay) {
              // Preserve V1/V2 layout, record offsets and historical cursors.
              scratch.insert(
                  scratch.end(), batch->replay->payload.begin(), batch->replay->payload.end());
            } else if (!r.executions.empty()) {
              for (const auto& t : r.executions)
                encode_execution(scratch, t);
            } else {
              for (const auto& t : r.trades)
                encode_trade(scratch, t);
            }
            finish_frame(scratch);
            file.append(scratch);
            wrote = true;
            trade_count += batch->records();
            hook(Stage::TradeWritten, batch->b.seq);
          }
          batch->trade_end = file.append_position();
          batch->trade_count = trade_count;
        } else {
          start_frame(scratch, Kind::Orders, batch->b, batch->records(), r.changes.size());
          for (const auto& change : r.changes) {
            integer(scratch, change.erase ? 0 : 1, 1);
            if (change.erase)
              integer(scratch, change.order.id, 8);
            else
              encode_order(scratch, storage_.dictionary().expand(change.order));
          }
          finish_frame(scratch);
          file.append(scratch);
          delta_bytes_.fetch_add(scratch.size(), std::memory_order_relaxed);
          wrote = true;
          hook(Stage::OrderWritten, batch->b.seq);
        }
      }
      if (wrote) {
        hook(trades ? Stage::BeforeTradeSync : Stage::BeforeOrderSync, group.back()->b.seq);
        file.sync();
      }
      for (auto* batch : group) {
        hook(trades ? Stage::TradeSynced : Stage::OrderSynced, batch->b.seq);
        batch->done.fetch_or(trades ? 1U : 2U, std::memory_order_release);
      }
      completion_.notify_all();
      wakeup_.notify();
    }
  } catch (...) {
    fail(std::current_exception());
  }
}

void DurableEngine::snapshot_worker() {
  try {
    while (auto work = snapshot_queue_.pop()) {
      if (failed_.load(std::memory_order_acquire))
        break;
      storage_.checkpoint((*work)->b, (*work)->pages);
      (*work)->done.store(true, std::memory_order_release);
      completion_.notify_all();
      wakeup_.notify();
    }
  } catch (...) {
    fail(std::current_exception());
  }
}

void DurableEngine::service_checkpoint() {
  if (snapshot_ && snapshot_->done.load(std::memory_order_acquire)) {
    checkpoint_seq_ = snapshot_->b.seq;
    std::cout << "CHECKPOINT_READY batch=" << checkpoint_seq_
              << " active_generation=" << checkpoint_seq_ + 1 << std::endl;
    snapshot_.reset();
    cut_.reset();
    snapshot_sent_ = false;
    committed_.reset_copy_count();
    last_checkpoint_ = std::chrono::steady_clock::now();
  }
  if (!cut_)
    return;
  if (!snapshot_ && boundary_.seq == cut_->seq) {
    snapshot_ = std::make_unique<Snapshot>();
    snapshot_->b = boundary_;
    snapshot_->pages = committed_.freeze();
    committed_.reset_copy_count();
  }
  if (snapshot_ && !snapshot_sent_ && cut_->done.load(std::memory_order_acquire)) {
    if (!snapshot_queue_.push(snapshot_.get()))
      check_error();
    snapshot_sent_ = true;
  }
}

void DurableEngine::poll() {
  wakeup_.drain();
  check_error();
  service_checkpoint();
  emit_queries();
  history_->poll();
  while (!pending_.empty() && pending_.front()->done.load(std::memory_order_acquire) == 3) {
    auto& batch = *pending_.front();
    for (const auto& change : batch.result.changes) {
      while (snapshot_ && committed_.needs_copy(change) &&
             committed_.copied_bytes() + sizeof(Page) > options_.cow_bytes) {
        service_checkpoint();
        if (!snapshot_)
          break;
        wait_progress();
        check_error();
      }
      committed_.apply(change);
    }
    boundary_ = batch.b;
    boundary_.trade_end = batch.trade_end;
    boundary_.trade_count = batch.trade_count;
    history_->publish(boundary_.trade_end);
    service_checkpoint();
    hook(Stage::Emitting, boundary_.seq);
    if (batch.complete)
      batch.complete(boundary_, batch.result.events);
    else
      for (const auto& event : batch.result.events)
        batch.emit(event.kind, event.context, {event.payload.data(), event.size});
    // SnapshotEnd for C must enter the output stream before any event from C+1.
    emit_queries();
    pending_bytes_ -= batch.bytes();
    auto recycled = std::move(pending_.front());
    pending_.pop_front();
    recycled->emit = {};
    recycled->complete = {};
    recycled->replay.reset();
    recycled->result.clear();
    if (recycled->result.bytes() <= 256 * 1024 && free_.size() < 16)
      free_.push_back(std::move(recycled));
  }
  if (!cut_ && executed_ > checkpoint_seq_ &&
      (std::chrono::steady_clock::now() - last_checkpoint_ >= options_.checkpoint_interval ||
       delta_bytes_.load(std::memory_order_relaxed) >= options_.checkpoint_bytes))
    request_checkpoint();
}

void DurableEngine::command(const LinkHeader& h,
                            std::span<const std::uint8_t> bytes,
                            const Emit& emit) {
  try {
    poll();
    if (h.kind == LinkKind::History) {
      history_->request(h, copy_record<TradeHistoryRequest>(bytes), boundary_.trade_end, emit);
      return;
    }
    if (h.kind == LinkKind::Snapshot) {
      capture_query(h, copy_record<Header>(bytes), emit);
      return;
    }
    if (group_batch_)
      throw std::logic_error("Ordinary command during open group batch");
    while (!has_capacity()) {
      wait_progress();
      poll();
    }
    auto batch = acquire_batch();
    batch->emit = emit;
    working_.execute(h, bytes, batch->result);
    batch->b = {executed_ + 1, working_.next_order(), working_.next_trade(), 0, 0};
    enqueue(std::move(batch));
  } catch (...) {
    fail(std::current_exception());
    throw;
  }
}

bool DurableEngine::has_capacity() const {
  return pending_.size() < options_.pending_slots &&
         pending_bytes_ <= options_.pending_bytes - options_.command_bytes;
}

std::unique_ptr<DurableEngine::Batch> DurableEngine::acquire_batch() {
  if (executed_ == UINT64_MAX)
    throw std::runtime_error("Persistence sequence exhausted");
  auto batch = free_.empty() ? std::make_unique<Batch>() : std::move(free_.back());
  if (!free_.empty())
    free_.pop_back();
  batch->result.clear();
  batch->result.limit = options_.command_bytes;
  batch->done.store(0, std::memory_order_relaxed);
  return batch;
}

void DurableEngine::enqueue(std::unique_ptr<Batch> batch) {
  if (!batch->result.trades.empty() && !batch->result.executions.empty())
    throw std::logic_error("Mixed legacy and typed result vectors");
  if (batch->bytes() > options_.command_bytes || batch->b.seq != executed_ + 1)
    throw std::runtime_error("Invalid batch budget/sequence");
  auto* handle = batch.get();
  pending_bytes_ += batch->bytes();
  pending_.push_back(std::move(batch));
  if (!trade_queue_.push({handle, nullptr}) || !order_queue_.push({handle, nullptr})) {
    check_error();
    throw std::runtime_error("Persistence writer stopped");
  }
  ++executed_;
}

Result* DurableEngine::begin_group(DurableResult complete) {
  poll();
  if (group_batch_)
    throw std::logic_error("Group batch already open");
  if (!has_capacity())
    return nullptr;
  group_batch_ = acquire_batch();
  group_batch_->complete = std::move(complete);
  return &group_batch_->result;
}

std::uint64_t DurableEngine::seal_group() {
  if (!group_batch_)
    throw std::logic_error("No open group batch");
  group_batch_->b = {executed_ + 1, working_.next_order(), working_.next_trade(), 0, 0};
  enqueue(std::move(group_batch_));
  return executed_;
}

void DurableEngine::capture_query(const LinkHeader& context,
                                  const Header& request,
                                  const Emit& emit) {
  const auto count = working_.active_order_count(context.user);
  if (count > options_.query_bytes / sizeof(OrderState)) {
    // A single result cannot become smaller by waiting. Fail explicitly rather
    // than deadlocking, allocating without a bound, or sending a partial success.
    throw std::runtime_error("Single order query exceeds query buffer budget");
  }
  const auto bytes = count * sizeof(OrderState);
  while (queries_.size() >= options_.query_slots || bytes > options_.query_bytes - query_bytes_) {
    wait_progress();
    poll();
  }

  Query query;
  query.target = executed_;
  query.context = context;
  query.request = request;
  query.count = count;
  query.emit = emit;
  if (count) {
    query.orders = std::make_unique_for_overwrite<OrderState[]>(count);
    working_.copy_active_orders(context.user, {query.orders.get(), count});
  }
  // Own values, never pointers into mutable working orders. No disk wait on
  // the normal path: command() returns while the target is still uncommitted.
  queries_.push_back(std::move(query));
  query_bytes_ += bytes;
  emit_queries();
}

void DurableEngine::emit_queries() {
  while (!queries_.empty() && queries_.front().target <= boundary_.seq) {
    const auto& query = queries_.front();
    for (std::size_t i = 0; i < query.count; ++i) {
      auto record = server_record<ActiveOrderSnapshot>(MessageType::ActiveOrderSnapshot,
                                                       query.request.requestId);
      record.order = query.orders[i];
      query.emit(LinkKind::Event, query.context, record_bytes(record));
    }
    auto end = server_record<ActiveOrderSnapshot>(MessageType::ActiveOrderSnapshot,
                                                  query.request.requestId);
    end.isEnd = 1;
    query.emit(LinkKind::SnapshotEnd, query.context, record_bytes(end));
    query_bytes_ -= query.count * sizeof(OrderState);
    queries_.pop_front();
  }
}

void DurableEngine::drain() {
  while (!pending_.empty() || history_->pending()) {
    poll();
    if (!pending_.empty() || history_->pending())
      wait_progress();
  }
  check_error();
  service_checkpoint();
}

void DurableEngine::request_checkpoint() {
  check_error();
  if (checkpoints_frozen_ || cut_ || executed_ <= checkpoint_seq_)
    return;
  cut_ = std::make_unique<Cut>();
  cut_->seq = executed_;
  if (!order_queue_.push({nullptr, cut_.get()}))
    check_error();
  service_checkpoint();
}

bool DurableEngine::idle() const {
  return !group_batch_ && pending_.empty() && queries_.empty() && !history_->pending() &&
         history_->caught_up(boundary_.trade_end);
}

void DurableEngine::replicate(const Frame& orders, const std::optional<Frame>& trades) {
  if (group_batch_ || orders.kind != Kind::Orders || orders.boundary.seq != executed_ + 1 ||
      orders.boundary.next_order < working_.next_order() ||
      orders.boundary.next_trade != working_.next_trade() + orders.trades ||
      bool(trades) != bool(orders.trades))
    throw std::runtime_error("Replication sequence/counter gap");
  if (trades && ((trades->kind != Kind::Trades && trades->kind != Kind::TradesV2) ||
                 trades->boundary.seq != orders.boundary.seq || trades->count != orders.trades ||
                 trades->trades != orders.trades ||
                 trades->boundary.next_order != orders.boundary.next_order ||
                 trades->boundary.next_trade != orders.boundary.next_trade))
    throw std::runtime_error("Replication parallel-log mismatch");
  while (pending_.size() >= options_.pending_slots ||
         pending_bytes_ > options_.pending_bytes - options_.command_bytes) {
    wait_progress();
    poll();
  }
  auto batch = std::make_unique<Batch>();
  batch->b = orders.boundary;
  batch->result.limit = options_.command_bytes;
  Reader reader{orders.payload};
  for (std::uint32_t i = 0; i < orders.count; ++i) {
    Change change;
    const auto kind = reader.number(1);
    if (kind == 0) {
      change.erase = true;
      change.order.id = reader.number(8);
    } else if (kind == 1) {
      change.order = storage_.dictionary().compact(decode_order(reader, true));
    } else {
      throw std::runtime_error("Invalid replicated delta");
    }
    batch->result.add(batch->result.changes, change);
  }
  reader.finish();
  if (trades) {
    for (const auto& t : decode_trades(*trades))
      storage_.dictionary().validate_execution(t.record, trades->boundary, shard_);
    batch->replay = *trades;
  }
  if (batch->bytes() > options_.command_bytes)
    throw std::runtime_error("Replicated batch exceeds command budget");
  working_.replicate(batch->result.changes, storage_.dictionary(), batch->b);
  enqueue(std::move(batch));
}

void DurableEngine::wait_checkpoint() {
  drain();
  while (cut_) {
    poll();
    if (cut_)
      wait_progress();
  }
}
}  // namespace marketlab::orders::persistence
