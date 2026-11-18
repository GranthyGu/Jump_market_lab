#include "matching_engine/cluster/router_spreads_publication.hpp"
#include <algorithm>

namespace marketlab::orders::spreads {
using persistence::Reader;

Publication::Publication(std::size_t bytes, Callbacks callbacks)
    : budget_(bytes),
      callbacks_(std::move(callbacks)) {
  if (!bytes || !callbacks_.event || !callbacks_.market)
    throw std::invalid_argument("Incomplete public result transport/budget");
}

void Publication::initialize(std::uint64_t epoch,
                             std::uint64_t sequence,
                             const std::array<persistence::Boundary, 4>& boundaries) {
  if (!epoch || !idle())
    throw std::runtime_error("Cannot reset unresolved public results");
  epoch_ = epoch;
  memory_ = published_ = dispatched_ = sequence;
  visible_ = boundaries;
  cursor_.fill(sequence);
  assembling_.fill(0);
  for (unsigned i = 0; i < 4; ++i)
    received_[i] = boundaries[i].seq;
}

void Publication::start(std::uint64_t sequence, std::uint8_t initiator) {
  if (!epoch_ || initiator >= 4 || !capacity() || dispatched_ == UINT64_MAX ||
      sequence != dispatched_ + 1 || memory_ != dispatched_)
    throw std::runtime_error("Group dispatch violates single memory command/window");
  rows_.push_back(Row{sequence, initiator});
  dispatched_ = sequence;
}

Publication::Row& Publication::row(std::uint64_t sequence) {
  if (rows_.empty() || sequence < rows_.front().seq || sequence > rows_.back().seq)
    throw std::runtime_error("Result outside outstanding command window");
  return rows_[sequence - rows_.front().seq];
}

void Publication::memory(const Header& h, std::span<const std::uint8_t> bytes) {
  if (h.kind != MessageKind::MemDone || h.epoch != epoch_ || h.flags || h.seq != memory_ + 1)
    throw std::runtime_error("Unexpected memory completion");
  auto& entry = row(h.seq);
  Reader reader{bytes};
  const auto participants = reader.number(1), market = reader.number(1);
  if (!participants || participants & ~15U || market & ~15U ||
      (participants & market) != participants || !(participants & (1U << entry.initiator)) ||
      entry.participants)
    throw std::runtime_error("Invalid completed participant/market mask");
  std::array<std::uint64_t, 4> batches{};
  for (unsigned i = 0; i < 4; ++i)
    if (participants & (1U << i)) {
      batches[i] = reader.number(8);
      if (!batches[i])
        throw std::runtime_error("Empty local batch identity");
    }
  reader.finish();
  entry.participants = participants;
  entry.market = market;
  entry.batches = batches;
  memory_ = h.seq;
  finish(entry);
}

void Publication::durable(std::uint8_t member,
                          const Header& h,
                          std::span<const std::uint8_t> bytes) {
  if (member >= 4 || h.kind != MessageKind::DurableResult || h.epoch != epoch_ || h.round ||
      (h.flags & ~group::final_part))
    throw std::runtime_error("Unexpected durable result identity");
  auto& entry = row(h.seq);
  if (entry.participants && !(entry.participants & (1U << member)))
    throw std::runtime_error("Durable result from view-only/non-participant shard");
  auto& result = entry.results[member];
  Reader reader{bytes};
  const auto part = reader.number(4);
  if (result.complete || part != result.next_part ||
      (part ? assembling_[member] != h.seq : assembling_[member] != 0))
    throw std::runtime_error("Duplicate/interleaved/incomplete durable result");
  auto boundary = result.boundary;
  if (!part) {
    boundary = read_boundary(reader);
    if (received_[member] == UINT64_MAX || boundary.seq != received_[member] + 1)
      throw std::runtime_error("Shard durable batches are out of order");
  }
  const auto count = reader.number(2);
  const auto offset = reader.position;
  for (unsigned i = 0; i < count; ++i)
    read_result_event(reader);
  reader.finish();
  if (!count && (!(h.flags & group::final_part) || part))
    throw std::runtime_error("Empty non-final durable result chunk");
  const auto added = bytes.size() - offset;
  const auto required = result.events.size() + added;
  if (required > result.events.capacity()) {
    const auto available = budget_ - std::min(budget_, bytes_);
    const auto maximum = result.events.capacity() + available;
    if (required > maximum)
      throw std::runtime_error("Router result byte budget exhausted");
    const auto old = result.events.capacity();
    result.events.reserve(std::min(maximum, std::max(required, old * 2)));
    bytes_ += result.events.capacity() - old;
  }
  result.events.insert(result.events.end(), bytes.begin() + offset, bytes.end());
  result.boundary = boundary;
  ++result.next_part;
  if (h.flags & group::final_part) {
    received_[member] = boundary.seq;
    assembling_[member] = 0;
    result.complete = true;
  } else
    assembling_[member] = h.seq;
  finish(entry);
}

void Publication::finish(Row& entry) {
  if (!entry.participants || entry.ready)
    return;
  for (unsigned i = 0; i < 4; ++i) {
    const auto& result = entry.results[i];
    if (!(entry.participants & (1U << i))) {
      if (result.next_part)
        throw std::runtime_error("MEM_DONE omitted a durable participant");
    } else {
      if (!result.complete)
        return;
      if (result.boundary.seq != entry.batches[i])
        throw std::runtime_error("Memory and durable local batch identities differ");
    }
  }
  entry.ready = true;
  const auto emit = [&](unsigned member) {
    auto& events = entry.results[member].events;
    Reader reader{events};
    while (reader.position < reader.data.size())
      callbacks_.event(member, read_result_event(reader));
    bytes_ -= events.capacity();
    Bytes{}.swap(events);
  };
  emit(entry.initiator);  // Accepted is necessarily ahead of its own/remote fills.
  for (unsigned i = 0; i < 4; ++i)
    if (i != entry.initiator && (entry.participants & (1U << i)))
      emit(i);
  advance();
}

void Publication::advance() {
  for (unsigned member = 0; member < 4; ++member)
    while (cursor_[member] < memory_) {
      const auto& entry = row(cursor_[member] + 1);
      if (entry.participants & (1U << member)) {
        if (!entry.ready)
          break;
        visible_[member] = entry.results[member].boundary;
      }
      ++cursor_[member];
    }
  const auto previous = published_;
  std::uint8_t targets = 0;
  while (published_ < memory_) {
    const auto& entry = row(published_ + 1);
    if (!entry.ready)
      break;
    targets |= entry.market;
    ++published_;
  }
  if (published_ != previous)
    callbacks_.market(published_, targets);
  while (!rows_.empty() && rows_.front().seq <= published_)
    rows_.pop_front();
}

}  // namespace marketlab::orders::spreads
