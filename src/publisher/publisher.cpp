#include "publisher/publisher.hpp"

#include <future>
#include <iostream>
#include <limits>
#include <thread>

namespace marketlab::md {
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
          << " symbols=" << batch->books.size() << " packets=" << outgoing.size() << " timestamp_ns=" << batch->timestamp
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

}  // namespace marketlab::md
