#include <unistd.h>

#include <algorithm>
#include <barrier>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <limits>
#include <mutex>
#include <thread>
#include <vector>

#include "common.hpp"
#include "matching_engine/persistence/durable_engine.hpp"

namespace b = marketlab::bench;
namespace p = marketlab::orders::persistence;
namespace o = marketlab::orders;
namespace m = marketlab;

namespace {

constexpr std::size_t total_symbols = 256;

// Removes only the unique directory created by this benchmark invocation.
struct TemporaryDirectory {
    std::filesystem::path path;

    explicit TemporaryDirectory(const std::filesystem::path &parent) {
        std::filesystem::create_directories(parent);
        auto pattern = (parent / "engine-run-XXXXXX").string();
        b::require(mkdtemp(pattern.data()) != nullptr, "mkdtemp failed");
        path = pattern;
    }

    ~TemporaryDirectory() {
        std::error_code ignored;
        std::filesystem::remove_all(path, ignored);
    }
};

struct ShardResult {
    std::vector<double> latency_us;
    std::uint64_t accepted = 0;
    std::uint64_t trade_legs = 0;
    std::uint64_t filled = 0;
    std::uint64_t order_syncs = 0;
    std::uint64_t trade_syncs = 0;
};

o::OrderConfig configuration(std::size_t shards) {
    o::OrderConfig config;
    config.accounts = {{"Alice", "benchmark"}, {"Bob", "benchmark"}};
    for (std::size_t index = 0; index < shards; ++index) {
        const auto shard = static_cast<std::uint16_t>(index + 1);
        config.shards.push_back({shard, {"127.0.0.1", 1}});
        for (std::size_t symbol = 0; symbol < total_symbols / shards; ++symbol) {
            config.products.push_back(
                {"S" + std::to_string(shard) + "P" + std::to_string(symbol), 100, shard});
        }
    }
    return config;
}

void submit(p::DurableEngine &engine, const std::string &symbol, std::uint64_t request,
            m::Side side, const o::EngineBackend::Emit &emit) {
    m::NewOrderRequest order{};
    order.header = {m::MessageType::NewOrderRequest, sizeof(order), request};
    o::put_string(order.symbol, symbol);
    order.side = side;
    order.price = 100;
    order.quantity = 1;
    const std::uint32_t user = side == m::Side::Buy ? 1 : 2;
    engine.command({0, o::LinkKind::NewOrder, request, user, 1}, o::record_bytes(order), emit);
}

void verify_book(p::DurableEngine &engine, std::uint64_t expected) {
    const auto view = engine.market_view();
    std::uint64_t quantity = 0;
    std::uint64_t orders = 0;
    for (const auto &[symbol, book] : view.books) {
        (void)symbol;
        for (const auto &level : book.levels) {
            b::require(level.side == m::md::Side::Sell && level.price == 100,
                       "Unexpected final book side or price");
            quantity += level.quantity;
            orders += level.num_orders;
        }
    }
    b::require(quantity == expected && orders == expected,
               "Final book quantity or active order count mismatch");
}

double percentile(const std::vector<double> &sorted, double fraction) {
    b::require(!sorted.empty(), "No latency samples");
    return sorted[static_cast<std::size_t>(fraction * (sorted.size() - 1))];
}

} // namespace

int main(int argc, char **argv) {
    try {
        b::require(argc == 5, "Usage: engine_benchmark SHARDS COMMANDS ACTIVE_ORDERS TEMP_PARENT");
        const auto shards = std::stoull(argv[1]);
        const auto total = std::stoull(argv[2]);
        const auto active = std::stoull(argv[3]);
        b::require(shards >= 1 && shards <= 32 && total_symbols % shards == 0 &&
                       total % (2 * shards) == 0 && active % shards == 0 && active >= total_symbols,
                   "Invalid workload dimensions");
        TemporaryDirectory directory(argv[4]);
        const auto config = configuration(shards);
        const auto commands = total / shards;
        const auto seed = active / shards;
        const auto symbols = total_symbols / shards;
        std::barrier ready(static_cast<std::ptrdiff_t>(shards + 1));
        std::barrier start(static_cast<std::ptrdiff_t>(shards + 1));
        std::barrier finished(static_cast<std::ptrdiff_t>(shards + 1));
        // Keep destruction, book validation and checkpoint/recovery outside timing.
        std::barrier measured(static_cast<std::ptrdiff_t>(shards + 1));
        std::vector<ShardResult> results(shards);
        std::vector<std::thread> owners;
        std::mutex error_mutex;
        std::exception_ptr error;

        for (std::size_t index = 0; index < shards; ++index) {
            owners.emplace_back([&, index] {
                bool started = false;
                bool timed = false;
                bool completed = false;
                bool released = false;
                try {
                    auto &result = results[index];
                    result.latency_us.reserve(commands / 127 + 1);
                    const auto shard = static_cast<std::uint16_t>(index + 1);
                    p::Options options;
                    options.directory = directory.path / ("shard-" + std::to_string(shard));
                    options.checkpoint_interval = std::chrono::hours(1);
                    options.checkpoint_bytes = std::numeric_limits<std::size_t>::max();
                    // Written by the respective writer thread; read after drain.
                    std::atomic<std::uint64_t> order_syncs{0};
                    std::atomic<std::uint64_t> trade_syncs{0};
                    options.hook = [&](p::Stage stage, std::uint64_t) {
                        if (stage == p::Stage::BeforeOrderSync) {
                            order_syncs.fetch_add(1, std::memory_order_relaxed);
                        } else if (stage == p::Stage::BeforeTradeSync) {
                            trade_syncs.fetch_add(1, std::memory_order_relaxed);
                        }
                    };
                    std::vector<b::Clock::time_point> submitted(commands / 127 + 1);
                    {
                        p::DurableEngine engine(config, shard, options);
                        std::uint64_t seed_accepted = 0;
                        const o::EngineBackend::Emit seed_emit = [&](auto, const auto &,
                                                                     auto bytes) {
                            const auto header =
                                o::copy_record<m::Header>(bytes.first(sizeof(m::Header)));
                            b::require(header.messageType == m::MessageType::OrderUpdate,
                                       "Unexpected event during seed");
                            const auto update = o::copy_record<m::OrderUpdate>(bytes);
                            b::require(update.order.status == m::OrderStatus::Accepted,
                                       "Seed order rejected or matched");
                            ++seed_accepted;
                        };
                        for (std::uint64_t i = 0; i < seed; ++i) {
                            submit(engine, config.products[index * symbols + i % symbols].symbol,
                                   i + 1, m::Side::Sell, seed_emit);
                        }
                        engine.drain();
                        b::require(seed_accepted == seed, "Missing seed Accepted");
                        order_syncs = 0;
                        trade_syncs = 0;

                        const o::EngineBackend::Emit emit = [&](auto, const auto &, auto bytes) {
                            const auto header =
                                o::copy_record<m::Header>(bytes.first(sizeof(m::Header)));
                            if (header.messageType == m::MessageType::Trade) {
                                ++result.trade_legs;
                                return;
                            }
                            b::require(header.messageType == m::MessageType::OrderUpdate,
                                       "Unexpected rejection or event");
                            const auto update = o::copy_record<m::OrderUpdate>(bytes);
                            if (update.order.status == m::OrderStatus::Accepted) {
                                ++result.accepted;
                                const auto position = update.header.requestId - seed - 1;
                                b::require(position < commands, "Accepted has wrong request ID");
                                if (position % 127 == 0) {
                                    result.latency_us.push_back(
                                        std::chrono::duration<double, std::micro>(
                                            b::Clock::now() - submitted[position / 127])
                                            .count());
                                }
                            } else {
                                b::require(update.order.status == m::OrderStatus::Filled,
                                           "Unexpected order update status");
                                ++result.filled;
                            }
                        };
                        ready.arrive_and_wait();
                        started = true;
                        start.arrive_and_wait();
                        timed = true;
                        for (std::uint64_t i = 0; i < commands; ++i) {
                            if (i % 127 == 0) {
                                submitted[i / 127] = b::Clock::now();
                            }
                            const auto side = i % 2 == 0 ? m::Side::Sell : m::Side::Buy;
                            submit(engine,
                                   config.products[index * symbols + (i / 2) % symbols].symbol,
                                   seed + i + 1, side, emit);
                        }
                        engine.drain();
                        finished.arrive_and_wait();
                        completed = true;
                        measured.arrive_and_wait();
                        released = true;
                        b::require(result.accepted == commands && result.trade_legs == commands &&
                                       result.filled == commands,
                                   "Missing Accepted, Trade or Filled notifications");
                        result.order_syncs = order_syncs.load();
                        result.trade_syncs = trade_syncs.load();
                        verify_book(engine, seed);
                        b::require(engine.boundary().seq == seed + commands &&
                                       engine.boundary().trade_count == commands / 2,
                                   "Committed batch or trade count mismatch");
                        // Exercise the independently scheduled third queue as well.
                        engine.request_checkpoint();
                        engine.wait_checkpoint();
                    }
                    p::DurableEngine recovered(config, shard, options);
                    verify_book(recovered, seed);
                    b::require(recovered.boundary().seq == seed + commands &&
                                   recovered.boundary().trade_count == commands / 2,
                               "Reopened engine lost committed work");
                } catch (...) {
                    {
                        std::lock_guard lock(error_mutex);
                        if (!error) {
                            error = std::current_exception();
                        }
                    }
                    if (!started) {
                        ready.arrive_and_drop();
                    }
                    if (!timed) {
                        start.arrive_and_drop();
                    }
                    if (!completed) {
                        finished.arrive_and_drop();
                    }
                    if (!released) {
                        measured.arrive_and_drop();
                    }
                }
            });
        }
        // Seeding is excluded. Owners pause at start until the timer is running.
        ready.arrive_and_wait();
        const auto before = b::usage();
        const auto begin = b::Clock::now();
        start.arrive_and_wait();
        finished.arrive_and_wait();
        const auto seconds = std::chrono::duration<double>(b::Clock::now() - begin).count();
        const auto after = b::usage();
        measured.arrive_and_wait();
        for (auto &owner : owners) {
            owner.join();
        }
        if (error) {
            std::rethrow_exception(error);
        }
        std::vector<double> latency;
        std::uint64_t order_syncs = 0;
        std::uint64_t trade_syncs = 0;
        for (const auto &result : results) {
            latency.insert(latency.end(), result.latency_us.begin(), result.latency_us.end());
            order_syncs += result.order_syncs;
            trade_syncs += result.trade_syncs;
        }
        std::sort(latency.begin(), latency.end());
        std::cout << std::setprecision(9) << "RESULT," << MARKETLAB_BENCH_QUEUE_NAME << ','
                  << shards << ',' << total << ',' << active << ',' << seconds << ','
                  << total / seconds << ',' << (after.cpu_seconds - before.cpu_seconds) / seconds
                  << ',' << percentile(latency, 0.5) << ',' << percentile(latency, 0.99) << ','
                  << order_syncs << ',' << trade_syncs << ','
                  << after.context_switches - before.context_switches << '\n';
    } catch (const std::exception &error) {
        std::cerr << "FAIL " << error.what() << '\n';
        return 1;
    }
}
