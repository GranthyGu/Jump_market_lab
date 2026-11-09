#pragma once
#include <cstdint>
#include <stdexcept>
#include <vector>

namespace marketlab::orders {
// Open addressing, <= 2/3 load, backward-shift deletion: no tombstones and no
// allocation per active order. Empty ID 0 is never an exchange order ID.
template <typename T>
class ActiveIndex {
 public:
  explicit ActiveIndex(std::size_t capacity = 1024) {
    std::size_t n = 8;
    while (n < capacity)
      n *= 2;
    entries_.resize(n);
  }

  T* find(std::uint64_t id) const noexcept {
    if (!id)
      return nullptr;
    auto slot = hash(id) & mask();
    while (entries_[slot].id) {
      if (entries_[slot].id == id)
        return entries_[slot].value;
      slot = (slot + 1) & mask();
    }
    return nullptr;
  }

  void insert(std::uint64_t id, T* value) {
    if (!id || !value || find(id))
      throw std::logic_error("Invalid/duplicate active order");
    if ((size_ + 1) * 3 > entries_.size() * 2) {
      auto old = std::move(entries_);
      entries_ = std::vector<Entry>(old.size() * 2);
      for (const auto& entry : old)
        if (entry.id)
          place(entry);
    }
    place({id, value});
    ++size_;
  }

  void erase(std::uint64_t id) noexcept {
    if (!id)
      return;
    auto hole = hash(id) & mask();
    while (entries_[hole].id && entries_[hole].id != id)
      hole = (hole + 1) & mask();
    if (!entries_[hole].id)
      return;
    auto next = (hole + 1) & mask();
    while (entries_[next].id) {
      const auto home = hash(entries_[next].id) & mask();
      if (((next - home) & mask()) >= ((next - hole) & mask())) {
        entries_[hole] = entries_[next];
        hole = next;
      }
      next = (next + 1) & mask();
    }
    entries_[hole] = {};
    --size_;
  }

  std::size_t size() const noexcept {
    return size_;
  }

 private:
  struct Entry {
    std::uint64_t id = 0;
    T* value = nullptr;
  };

  std::vector<Entry> entries_;
  std::size_t size_ = 0;

  std::size_t mask() const noexcept {
    return entries_.size() - 1;
  }

  static std::uint64_t hash(std::uint64_t x) noexcept {
    x ^= x >> 30;
    x *= 0xbf58476d1ce4e5b9ULL;
    x ^= x >> 27;
    x *= 0x94d049bb133111ebULL;
    return x ^ (x >> 31);
  }

  void place(Entry entry) noexcept {
    auto slot = hash(entry.id) & mask();
    while (entries_[slot].id)
      slot = (slot + 1) & mask();
    entries_[slot] = entry;
  }
};
}  // namespace marketlab::orders
