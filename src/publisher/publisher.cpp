#include "publisher/publisher.hpp"
#include "shared_data/market_data/availability.hpp"

#include <algorithm>
#include <future>
#include <iostream>
#include <limits>
#include <thread>

namespace marketlab::md {
Publisher::Publisher(Config config)
    : config_(std::move(config)),
      sender_(config_) {
  for (const auto& shard : config_.shards)
    sources_.push_back(make_source(shard, config_.shm_max_age_ms));
  set_latest(nullptr);
}

void Publisher::sample_loop(const std::atomic_bool& stop, const std::atomic_bool& cancelled) {
  std::vector<Epoch> previous_sources;
  AvailabilityNotice availability("PUBLISHER");
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
        pending.push_back(std::async(std::launch::async, [ptr, deadline] {
          return ptr->read(deadline);
        }));
      }
      std::vector<Epoch> epochs;
      std::map<std::uint16_t, Book> books;
      std::exception_ptr failure;
      for (auto& task : pending) {
        try {
          auto view = task.get();
          epochs.push_back(view.epoch);
          books.merge(view.books);
          if (!view.books.empty())
            throw std::runtime_error("Overlapping shard symbols");
        } catch (...) {
          if (!failure)
            failure = std::current_exception();
        }
      }
      if (failure)
        std::rethrow_exception(failure);
      if (books.size() != config_.symbols.size())
        throw std::runtime_error("Incomplete round");
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
        auto snapshot =
            encode({Kind::Snapshot, batch->epoch, id, batch->index, batch->timestamp, book.levels});
        batch->snapshots.emplace(id, snapshot);
        if (reset || batch->index % config_.snapshot_every == 0) {
          outgoing.push_back(std::move(snapshot));
        } else {
          auto delta = encode({Kind::Delta,
                               batch->epoch,
                               id,
                               batch->index,
                               batch->timestamp,
                               difference(before->books.at(id), book)});
          // Send exactly one record per symbol. Keep equal-size and empty deltas.
          outgoing.push_back(delta.size() > snapshot.size() ? std::move(snapshot)
                                                            : std::move(delta));
        }
      }
      // Commit the complete batch before exposing its UDP records to receivers.
      set_latest(batch);
      previous_sources = std::move(epochs);
      const auto retries_before = sender_.submission_retries();
      const auto submission_deadline = std::min(round_started + publication_period,
                                                Clock::now() + std::chrono::milliseconds(200));
      for (const auto& message : outgoing)
        sender_.send(message, submission_deadline);
      availability.recovered();
      Log(std::cout) << console::cyan << "Published index=" << batch->index
                     << " symbols=" << batch->books.size() << " packets=" << outgoing.size()
                     << console::reset << console::dim << " sample_us="
                     << std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() -
                                                                              round_started)
                            .count()
                     << " udp_submission_retries=" << sender_.submission_retries() - retries_before
                     << console::reset << '\n';
    } catch (const std::exception& e) {
      set_latest(nullptr);
      previous_sources.clear();
      // All asynchronous operations have joined before connections are reset.
      for (auto& source : sources_)
        source->disconnect();
      availability.unavailable(std::string("Source unavailable; publication/query paused: ") +
                               e.what());
    }
    next += publication_period;
    if (Clock::now() > next) {
      if (!availability.active())
        Log(std::cerr) << console::Label{"WARNING", console::yellow}
                       << " Publication deadline missed; no synthetic catch-up batches generated\n";
      next = Clock::now() + publication_period;
    }
    sleep_until_or_stop(next, stop, cancelled);
  }
}

void Publisher::run(const std::atomic_bool& stop) {
  auto listener = listen_tcp(config_.query.host, config_.query.port);
  Log(std::cout) << "Snapshot TCP " << config_.query.host << ':' << config_.query.port << '\n';
  if (config_.multicast) {
    const auto& multicast = *config_.multicast;
    Log(std::cout) << "Market data UDP multicast " << multicast.group.host << ':'
                   << multicast.group.port
                   << " interface=" << multicast.interface << " ttl=" << multicast.ttl
                   << " loopback=" << multicast.loopback << " (one datagram per message)\n";
  }
  StopThread sampler([&](const std::atomic_bool& cancelled) {
    sample_loop(stop, cancelled);
  });
  serve_requests(
      std::move(listener),
      2,
      2 + max_snapshot_size,
      config_.query_timeout_ms,
      config_.max_query_connections,
      [this](std::span<const std::uint8_t> request) {
        std::size_t offset = 0;
        const auto id = static_cast<std::uint16_t>(read_uint(request, offset, 2));
        const auto batch = latest();
        if (!batch || !batch->snapshots.contains(id))
          return Bytes{};
        const auto& snapshot = batch->snapshots.at(id);
        Bytes response;
        append_uint(response, snapshot.size(), 2);
        response.insert(response.end(), snapshot.begin(), snapshot.end());
        return response;
      },
      stop);
}

}  // namespace marketlab::md
