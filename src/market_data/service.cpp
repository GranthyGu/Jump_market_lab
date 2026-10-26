#include "market_data/service.hpp"

#include <future>
#include <iostream>
#include <limits>
#include <thread>

namespace marketlab::md {
namespace {
void sleep_until_or_stop(Deadline deadline, const std::atomic_bool& stop, const std::atomic_bool& cancelled) {
  while (!stop.load() && !cancelled.load() && Clock::now() < deadline)
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
}
}  // namespace

Publisher::Publisher(Config config) : config_(std::move(config)), sender_(config_.receivers) {
  for (const auto& shard : config_.shards) sources_.push_back(make_source(shard, config_.shm_max_age_ms));
  set_latest(nullptr);
}
void Publisher::sample_loop(const std::atomic_bool& stop, const std::atomic_bool& cancelled) {
  std::vector<Epoch> previous_sources;
  auto next = Clock::now();
  while (!stop.load() && !cancelled.load()) {
    const auto round_started = Clock::now();
    try {
      const auto deadline = round_started + std::chrono::milliseconds(config_.source_timeout_ms);
      std::vector<std::future<ShardView>> pending;
      pending.reserve(sources_.size());
      // One asynchronous operation per shard; all share the same round deadline.
      for (auto& source : sources_) {
        auto* ptr = source.get();
        pending.push_back(std::async(std::launch::async, [ptr, deadline] { return ptr->read(deadline); }));
      }
      std::vector<Epoch> epochs;
      std::map<std::uint16_t, Book> books;
      std::exception_ptr failure;
      for (auto& task : pending) {
        try {
          auto view = task.get();
          epochs.push_back(view.epoch);
          books.merge(view.books);
          if (!view.books.empty()) throw std::runtime_error("Overlapping shard symbols");
        } catch (...) { if (!failure) failure = std::current_exception(); }
      }
      if (failure) std::rethrow_exception(failure);
      if (books.size() != config_.symbols.size()) throw std::runtime_error("Incomplete round");
      const auto before = latest();
      const bool reset = !before || epochs != previous_sources;
      if (!reset && before->index == std::numeric_limits<std::uint64_t>::max())
        throw std::runtime_error("Batch index exhausted");
      auto batch = std::make_shared<Batch>();
      batch->epoch = reset ? new_epoch() : before->epoch;
      batch->index = reset ? 0 : before->index + 1;
      batch->timestamp = unix_ns();
      batch->books = std::move(books);
      std::vector<Bytes> outgoing;
      for (const auto& [id, book] : batch->books) {
        auto snapshot = encode({Kind::Snapshot, batch->epoch, id, batch->index, batch->timestamp, book.levels});
        batch->snapshots.emplace(id, snapshot);
        if (!reset) outgoing.push_back(encode({Kind::Delta, batch->epoch, id, batch->index,
            batch->timestamp, difference(before->books.at(id), book)}));
        if (reset || batch->index % config_.snapshot_every == 0) outgoing.push_back(std::move(snapshot));
      }
      // Commit the complete batch before exposing its UDP records to receivers.
      set_latest(batch);
      previous_sources = std::move(epochs);
      for (const auto& message : outgoing) sender_.send(message);
      Log(std::cout) << "Published index=" << batch->index << " epoch=" << epoch_text(batch->epoch)
          << " symbols=" << batch->books.size() << " packets=" << outgoing.size()
          << " sample_us=" << std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - round_started).count() << '\n';
    } catch (const std::exception& e) {
      set_latest(nullptr);
      previous_sources.clear();
      // All asynchronous operations have joined before connections are reset.
      for (auto& source : sources_) source->disconnect();
      Log(std::cerr) << "Source unavailable; publication/query paused: " << e.what() << '\n';
    }
    next += publication_period;
    if (Clock::now() > next) {
      Log(std::cerr) << "Publication deadline missed; no synthetic catch-up batches generated\n";
      next = Clock::now() + publication_period;
    }
    sleep_until_or_stop(next, stop, cancelled);
  }
}
void Publisher::run(const std::atomic_bool& stop) {
  auto listener = listen_tcp(config_.query.host, config_.query.port);
  Log(std::cout) << "Snapshot TCP " << config_.query.host << ':' << config_.query.port << '\n';
  StopThread sampler([&](const std::atomic_bool& cancelled) { sample_loop(stop, cancelled); });
  serve_requests(std::move(listener), 2, 2 + max_snapshot_size, config_.query_timeout_ms,
        config_.max_query_connections, [this](std::span<const std::uint8_t> request) {
          std::size_t offset = 0;
          const auto id = static_cast<std::uint16_t>(read_uint(request, offset, 2));
          const auto batch = latest();
          if (!batch || !batch->snapshots.contains(id)) return Bytes{};
          const auto& snapshot = batch->snapshots.at(id);
          Bytes response;
          append_uint(response, snapshot.size(), 2);
          response.insert(response.end(), snapshot.begin(), snapshot.end());
          return response;
        }, stop);
}

MockShard::MockShard(const Config& config, ShardConfig shard, bool is_static)
    : config_(config), shard_(std::move(shard)), epoch_(new_epoch()), static_(is_static), current_(generate(0)) {}
ShardView MockShard::generate(std::uint64_t step) const {
  ShardView out{epoch_, {}};
  for (const auto id : shard_.symbols) {
    Book book{id, {}};
    if (config_.symbols.at(id) != "EMPTY") {
      const auto shift = static_cast<std::int32_t>((step / 3) % 3);
      const std::int32_t center = 10000 + static_cast<std::int32_t>(id) * 100 + shift;
      // A quantity change and an order-count-only change exercise both aggregates.
      for (int i = 0; i < static_cast<int>(depth); ++i)
        book.levels.push_back({Side::Buy, center - i, 100 + (i == 0 ? step % 5 : 0),
            static_cast<std::uint64_t>(2 + (i == 1 ? step % 2 : 0))});
      for (int i = 0; i < static_cast<int>(depth); ++i)
        book.levels.push_back({Side::Sell, center + 10 + i, static_cast<std::uint64_t>(200 + i), 4});
    }
    validate_book(book);
    out.books.emplace(id, std::move(book));
  }
  return out;
}
void MockShard::run(const std::atomic_bool& stop) {
  // The cached view is copied under the mutex, then encoded/sent without it.
  SnapshotProvider provider = [this] { std::lock_guard lock(mutex_); return current_; };
  SharedViewWriter shared(shard_.mmap_path, shard_.symbols.size());
  shared.publish(provider());
  auto listener = listen_tcp(shard_.host, shard_.port);
  Log(std::cout) << "Mock " << shard_.name << " TCP=" << shard_.host << ':' << shard_.port
      << " mmap=" << shard_.mmap_path << " source_epoch=" << epoch_text(epoch_) << '\n';
  StopThread updater([&](const std::atomic_bool& cancelled) {
    const auto started = Clock::now();
    while (!stop.load() && !cancelled.load()) {
      const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - started).count();
      auto view = generate(static_ ? 0 : static_cast<std::uint64_t>(elapsed / config_.mock_step_ms));
      { std::lock_guard lock(mutex_); current_ = view; }
      try { shared.publish(view); }
      catch (const std::exception& e) { Log(std::cerr) << "Shared-view update failed: " << e.what() << '\n'; }
      sleep_until_or_stop(Clock::now() + std::chrono::milliseconds(config_.mock_update_ms), stop, cancelled);
    }
  });
  serve_shard(std::move(listener), provider, stop, config_.query_timeout_ms, config_.max_query_connections);
}
}  // namespace marketlab::md
