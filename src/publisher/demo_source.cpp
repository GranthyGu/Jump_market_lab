#include "publisher/demo_source.hpp"

#include <iostream>
#include <stdexcept>

namespace marketlab::md {
SyntheticMarketSource::SyntheticMarketSource(const Config& config,
                                             ShardConfig shard,
                                             bool is_static)
    : config_(config),
      shard_(std::move(shard)),
      epoch_(new_epoch()),
      static_(is_static),
      current_(generate(0)) {
}

ShardView SyntheticMarketSource::generate(std::uint64_t step) const {
  ShardView out{epoch_, {}};
  for (const auto id : shard_.symbols) {
    Book book{id, {}};
    if (config_.symbols.at(id) != "EMPTY") {
      const auto shift = static_cast<std::int32_t>((step / 3) % 3);
      const std::int32_t center = 10000 + static_cast<std::int32_t>(id) * 100 + shift;
      // A quantity change and an order-count-only change exercise both aggregates.
      for (int i = 0; i < static_cast<int>(depth); ++i)
        book.levels.push_back({Side::Buy,
                               center - i,
                               100 + (i == 0 ? step % 5 : 0),
                               static_cast<std::uint64_t>(2 + (i == 1 ? step % 2 : 0))});
      for (int i = 0; i < static_cast<int>(depth); ++i)
        book.levels.push_back(
            {Side::Sell, center + 10 + i, static_cast<std::uint64_t>(200 + i), 4});
    }
    validate_book(book);
    out.books.emplace(id, std::move(book));
  }
  return out;
}

void SyntheticMarketSource::run(const std::atomic_bool& stop) {
  // The cached view is copied under the mutex, then encoded/sent without it.
  SnapshotProvider provider = [this] {
    std::lock_guard lock(mutex_);
    return current_;
  };
  SharedViewWriter shared(shard_.mmap_path, shard_.symbols.size());
  shared.publish(provider());
  auto listener = listen_tcp(shard_.host, shard_.port);
  Log(std::cout) << "Mock " << shard_.name << " TCP=" << shard_.host << ':' << shard_.port
                 << " mmap=" << shard_.mmap_path << " source_epoch=" << epoch_text(epoch_) << '\n';
  StopThread updater([&](const std::atomic_bool& cancelled) {
    const auto started = Clock::now();
    while (!stop.load() && !cancelled.load()) {
      const auto elapsed =
          std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - started).count();
      auto view =
          generate(static_ ? 0 : static_cast<std::uint64_t>(elapsed / config_.mock_step_ms));
      {
        std::lock_guard lock(mutex_);
        current_ = view;
      }
      try {
        shared.publish(view);
      } catch (const std::exception& e) {
        Log(std::cerr) << "Shared-view update failed: " << e.what() << '\n';
      }
      sleep_until_or_stop(
          Clock::now() + std::chrono::milliseconds(config_.mock_update_ms), stop, cancelled);
    }
  });
  serve_shard(
      std::move(listener), provider, stop, config_.query_timeout_ms, config_.max_query_connections);
}

void run_demo_sources(const Config& config,
                      const std::string& shard_name,
                      bool is_static,
                      std::atomic_bool& stop) {
  std::vector<std::unique_ptr<SyntheticMarketSource>> engines;
  std::vector<std::thread> workers;
  std::atomic_bool failed{false};
  for (const auto& shard : config.shards)
    if (shard_name.empty() || shard.name == shard_name)
      engines.push_back(std::make_unique<SyntheticMarketSource>(config, shard, is_static));
  if (engines.empty())
    throw std::runtime_error("No matching shard");
  for (auto& engine : engines) {
    auto* ptr = engine.get();
    workers.emplace_back([&, ptr] {
      try {
        ptr->run(stop);
      } catch (const std::exception& e) {
        Log(std::cerr) << "Mock failed: " << e.what() << '\n';
        failed.store(true);
        stop.store(true);
      }
    });
  }
  for (auto& worker : workers)
    worker.join();
  if (failed.load())
    throw std::runtime_error("A mock shard failed");
}
}  // namespace marketlab::md
