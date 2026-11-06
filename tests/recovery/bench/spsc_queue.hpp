#pragma once

#include <atomic>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

namespace marketlab::bench {

// Experimental bounded SPSC ring: exactly one producer and one consumer.
// close() may be called by other threads. Existing entries drain after close.
// The closed bit lives in BOTH wait words so that close changes their values;
// notify_all alone cannot release an atomic::wait on an unchanged value.
// Park=false intentionally burns CPU on full/empty; it is a benchmark control.
template <class T, bool Park = true> class SpscQueue {
public:
    explicit SpscQueue(std::size_t capacity) : slots_(capacity), mask_(capacity - 1) {
        if (capacity == 0 || (capacity & (capacity - 1)) != 0) {
            throw std::invalid_argument("SPSC benchmark capacity must be a power of two");
        }
    }

    bool push(T value) {
        while (true) {
            const auto observed = read_.load(std::memory_order_acquire);
            auto write = write_.load(std::memory_order_relaxed);
            if ((write & closed_bit) != 0) {
                return false;
            }
            const auto read = observed & position_mask;
            if (((write - read) & position_mask) < slots_.size()) {
                slots_[write & mask_] = std::move(value);
                // Only close() can modify write_ concurrently with the producer.
                // Failed publication leaves the slot invisible to the consumer.
                if (!write_.compare_exchange_strong(write, write + 1, std::memory_order_release,
                                                    std::memory_order_relaxed)) {
                    return false;
                }
                notify(write_);
                return true;
            }
            wait(read_, observed);
        }
    }

    std::optional<T> pop() {
        while (true) {
            const auto observed = write_.load(std::memory_order_acquire);
            if (auto value = try_pop()) {
                return value;
            }
            if ((observed & closed_bit) != 0) {
                return {};
            }
            wait(write_, observed);
        }
    }

    std::optional<T> try_pop() {
        const auto read = read_.load(std::memory_order_relaxed) & position_mask;
        const auto write = write_.load(std::memory_order_acquire) & position_mask;
        if (read == write) {
            return {};
        }
        auto value = std::move(slots_[read & mask_]);
        // fetch_add preserves a concurrent close bit. Cursor wrap after 2^63
        // entries is outside this experimental implementation's lifetime.
        read_.fetch_add(1, std::memory_order_release);
        notify(read_);
        return value;
    }

    void close() {
        write_.fetch_or(closed_bit, std::memory_order_acq_rel);
        read_.fetch_or(closed_bit, std::memory_order_acq_rel);
        write_.notify_all();
        read_.notify_all();
    }

private:
    static_assert(std::atomic<std::uint64_t>::is_always_lock_free);
    static constexpr std::uint64_t closed_bit = std::uint64_t{1} << 63;
    static constexpr std::uint64_t position_mask = closed_bit - 1;

    static void wait(std::atomic<std::uint64_t> &cursor, std::uint64_t value) {
        if constexpr (Park) {
            cursor.wait(value, std::memory_order_acquire);
        } else {
            std::this_thread::yield();
        }
    }

    static void notify(std::atomic<std::uint64_t> &cursor) {
        if constexpr (Park) {
            cursor.notify_one();
        }
    }

    std::vector<T> slots_;
    const std::size_t mask_;
    alignas(128) std::atomic<std::uint64_t> write_{0};
    alignas(128) std::atomic<std::uint64_t> read_{0};
};

} // namespace marketlab::bench
