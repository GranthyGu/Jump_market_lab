#include <barrier>
#include <future>
#include <iomanip>
#include <iostream>
#include <latch>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "common.hpp"
#include "matching_engine/persistence/queue.hpp"
#include "spsc_queue.hpp"

namespace b = marketlab::bench;
namespace p = marketlab::orders::persistence;
using namespace std::chrono_literals;

namespace {

struct Handle {
    std::uint64_t sequence = 0;
    std::uint64_t checksum = 0;
};

template <class Queue> void check_queue() {
    for (const std::size_t capacity : {1, 128}) {
        Queue queue(capacity);
        auto consumer = std::async(std::launch::async, [&] {
            for (std::uint64_t i = 0; i < 100000; ++i) {
                const auto value = queue.pop();
                b::require(value && value->sequence == i && value->checksum == ~i,
                           "FIFO, value integrity or delivery count failed");
                if (i % 1024 == 0) {
                    std::this_thread::yield();
                }
            }
            b::require(!queue.pop(), "Closed queue did not drain");
        });
        for (std::uint64_t i = 0; i < 100000; ++i) {
            b::require(queue.push({i, ~i}), "Unexpected closed queue");
        }
        queue.close();
        consumer.get();
    }

    for (int trial = 0; trial < 100; ++trial) {
        Queue empty(1);
        std::latch entering(1);
        auto consumer = std::async(std::launch::async, [&] {
            entering.count_down();
            return empty.pop();
        });
        entering.wait();
        std::this_thread::sleep_for(100us);
        empty.close();
        b::require(consumer.wait_for(2s) == std::future_status::ready,
                   "Closing empty queue did not wake consumer");
        b::require(!consumer.get(), "Unexpected value after close");

        Queue full(1);
        b::require(full.push({1, ~std::uint64_t{1}}), "Initial push failed");
        std::latch pushing(1);
        auto producer = std::async(std::launch::async, [&] {
            pushing.count_down();
            return full.push({2, ~std::uint64_t{2}});
        });
        pushing.wait();
        std::this_thread::sleep_for(100us);
        full.close();
        b::require(producer.wait_for(2s) == std::future_status::ready,
                   "Closing full queue did not wake producer");
        b::require(!producer.get(), "Push succeeded after close");
        const auto remaining = full.pop();
        b::require(remaining && remaining->sequence == 1, "Close lost queued value");
        b::require(!full.pop(), "Closed queue did not terminate");
        full.close();
    }

    // The engine's failure path can close a queue while its producer publishes
    // and its consumer drains. Every successful publication must still arrive.
    for (int trial = 0; trial < 100; ++trial) {
        Queue queue(128);
        std::latch active(1);
        auto producer = std::async(std::launch::async, [&] {
            std::uint64_t sent = 0;
            while (sent < 1000000 && queue.push({sent, ~sent})) {
                ++sent;
                if (sent == 64) {
                    active.count_down();
                }
            }
            return sent;
        });
        auto consumer = std::async(std::launch::async, [&] {
            std::uint64_t received = 0;
            while (const auto value = queue.pop()) {
                b::require(value->sequence == received && value->checksum == ~received,
                           "Concurrent close broke FIFO or payload integrity");
                ++received;
            }
            return received;
        });
        active.wait();
        queue.close();
        b::require(producer.get() == consumer.get(), "Concurrent close lost a published entry");
    }
}

template <class Queue>
void benchmark(const std::string &name, std::size_t pairs, std::uint64_t total,
               std::size_t capacity, int repeat) {
    std::vector<std::unique_ptr<Queue>> queues;
    std::vector<std::thread> workers;
    std::barrier start(static_cast<std::ptrdiff_t>(2 * pairs + 1));
    std::barrier finish(static_cast<std::ptrdiff_t>(2 * pairs + 1));
    std::atomic_bool valid{true};
    const auto count = total / pairs;
    for (std::size_t index = 0; index < pairs; ++index) {
        queues.push_back(std::make_unique<Queue>(capacity));
        auto *queue = queues.back().get();
        workers.emplace_back([&, queue] {
            start.arrive_and_wait();
            for (std::uint64_t i = 0; i < count; ++i) {
                if (!queue->push({i, ~i})) {
                    valid = false;
                }
            }
            queue->close();
            finish.arrive_and_wait();
        });
        workers.emplace_back([&, queue] {
            start.arrive_and_wait();
            std::uint64_t received = 0;
            while (const auto value = queue->pop()) {
                if (value->sequence != received || value->checksum != ~received) {
                    valid = false;
                }
                ++received;
            }
            if (received != count) {
                valid = false;
            }
            finish.arrive_and_wait();
        });
    }
    const auto before = b::usage();
    const auto begin = b::Clock::now();
    start.arrive_and_wait();
    finish.arrive_and_wait();
    const auto seconds = std::chrono::duration<double>(b::Clock::now() - begin).count();
    const auto after = b::usage();
    for (auto &thread : workers) {
        thread.join();
    }
    b::require(valid, "Queue benchmark validation failed");
    std::cout << name << ',' << repeat << ',' << pairs << ',' << capacity << ',' << count * pairs
              << ',' << seconds << ',' << count * pairs / seconds << ','
              << (after.cpu_seconds - before.cpu_seconds) / seconds << ','
              << after.context_switches - before.context_switches << '\n';
}

} // namespace

int main(int argc, char **argv) {
    try {
        if (argc == 2 && std::string(argv[1]) == "--check") {
            check_queue<p::Queue<Handle>>();
            check_queue<b::SpscQueue<Handle, true>>();
            check_queue<b::SpscQueue<Handle, false>>();
            std::cout << "PASS FIFO, integrity, bounded waiting, close and draining (3 queues)\n";
            return 0;
        }
        const auto total = argc > 1 ? std::stoull(argv[1]) : 2000000;
        const auto repeats = argc > 2 ? std::stoi(argv[2]) : 5;
        std::cout << std::setprecision(9);
        std::cout << "queue,repeat,pairs,capacity,items,seconds,items_per_second,cpu_cores,context_"
                     "switches\n";
        for (int repeat = 0; repeat < repeats; ++repeat) {
            for (const auto pairs : {1, 4, 8, 16}) {
                for (int offset = 0; offset < 3; ++offset) {
                    switch ((repeat + offset) % 3) {
                    case 0:
                        benchmark<p::Queue<Handle>>("mutex", pairs, total, 128, repeat);
                        break;
                    case 1:
                        benchmark<b::SpscQueue<Handle, true>>("spsc_park", pairs, total, 128,
                                                              repeat);
                        break;
                    case 2:
                        benchmark<b::SpscQueue<Handle, false>>("spsc_yield", pairs, total, 128,
                                                               repeat);
                        break;
                    }
                }
                std::cout.flush();
            }
        }
    } catch (const std::exception &error) {
        std::cerr << "FAIL " << error.what() << '\n';
        return 1;
    }
}
