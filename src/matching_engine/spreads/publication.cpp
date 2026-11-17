#include "matching_engine/spreads/publication.hpp"
#include <algorithm>

namespace marketlab::orders::spreads {
using persistence::integer;
using persistence::Reader;
using persistence::Event;

namespace {
constexpr auto body_limit = group::maximum_frame - group::header_size;

std::size_t event_bytes(const Event& event) {
  return 5 + (event.context.token ? 16 : 0) + event.size;
}

void put_event(Bytes& bytes, const Event& event) {
  if (event.kind != LinkKind::Event || !event.context.user ||
      (!event.context.token && event.context.session) || event.size > event.payload.size())
    throw std::runtime_error("Invalid durable command event");
  integer(bytes, event.context.token ? 1 : 0, 1);
  integer(bytes, event.context.user, 4);
  if (event.context.token) {
    integer(bytes, event.context.token, 8);
    integer(bytes, event.context.session, 8);
  }
  bytes.insert(bytes.end(), event.payload.begin(), event.payload.begin() + event.size);
}

Event read_event(Reader& reader) {
  Event event;
  event.kind = event.context.kind = LinkKind::Event;
  const auto flags = reader.number(1);
  event.context.user = reader.number(4);
  if (flags > 1 || !event.context.user)
    throw std::runtime_error("Invalid durable event metadata");
  if (flags) {
    event.context.token = reader.number(8);
    event.context.session = reader.number(8);
    if (!event.context.token)
      throw std::runtime_error("Empty solicited event token");
  }
  if (reader.data.size() - reader.position < sizeof(marketlab::Header))
    throw std::runtime_error("Truncated durable client event");
  marketlab::Header header{};
  std::memcpy(&header, reader.data.data() + reader.position, sizeof(header));
  event.size = header.messageLength;
  if (event.size > event.payload.size() || event.size > reader.data.size() - reader.position ||
      (header.messageType != MessageType::OrderUpdate && header.messageType != MessageType::Trade &&
       header.messageType != MessageType::RequestRejected) ||
      (!event.context.token && header.requestId) ||
      !valid_client_record(reader.data.subspan(reader.position, event.size), false))
    throw std::runtime_error("Invalid durable client event");
  std::copy_n(reader.data.data() + reader.position, event.size, event.payload.data());
  reader.position += event.size;
  return event;
}
}  // namespace

void send_result(const Header& context,
                 const persistence::Boundary& boundary,
                 std::span<const Event> events,
                 Bytes& bytes,
                 const SendPart& send) {
  if (!context.epoch || !context.seq)
    throw std::runtime_error("Result requires group identity");
  std::uint32_t part = 0;
  std::size_t at = 0;
  do {
    bytes.clear();
    integer(bytes, part, 4);
    if (!part)
      put_boundary(bytes, boundary);
    const auto count_at = bytes.size();
    integer(bytes, 0, 2);
    unsigned count = 0;
    while (at < events.size() && bytes.size() + event_bytes(events[at]) <= body_limit) {
      put_event(bytes, events[at++]);
      ++count;
    }
    bytes[count_at] = count & 255;
    bytes[count_at + 1] = count >> 8;
    auto header = context;
    header.kind = MessageKind::DurableResult;
    header.round = 0;
    header.flags = at == events.size() ? group::final_part : 0;
    send(header, bytes);
    ++part;
  } while (at != events.size());
}

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
    read_event(reader);
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
      callbacks_.event(member, read_event(reader));
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

MarketPublication::MarketPublication(MatchingEngine& core,
                                     const OrderConfig& config,
                                     std::uint16_t shard)
    : core_(core),
      topology_(config),
      visible_(core.symbol_count()) {
  if (topology_.member(shard) >= 4)
    throw std::invalid_argument("Unknown group market owner");
  slots_.reserve(command_window + 1);
  for (unsigned i = 0; i < command_window + 1; ++i)
    slots_.emplace_back(core.symbol_count());
}

void MarketPublication::reset() {
  head_ = size_ = 0;
  permitted_ = 0;
}

void MarketPublication::capture(std::uint64_t seq, const Views& views) {
  std::size_t at;
  if (size_ && slots_[(head_ + size_ - 1) % slots_.size()].seq == seq) {
    at = (head_ + size_ - 1) % slots_.size();
  } else {
    if (size_ == slots_.size() || (seq && seq <= permitted_) ||
        (size_ && seq < slots_[(head_ + size_ - 1) % slots_.size()].seq))
      throw std::runtime_error("Market candidate window/order exhausted");
    at = (head_ + size_++) % slots_.size();
  }
  auto& candidate = slots_[at];
  candidate.seq = seq;
  core_.copy_market(candidate.snapshot);
  for (auto& book : candidate.snapshot.books) {
    const auto slot = topology_.slot(book.symbol_id);
    if (slot < 5)
      book = merged_market(core_, book.symbol_id, slot, views);
  }
}

void MarketPublication::permit(std::uint64_t seq) {
  if (seq < permitted_)
    throw std::runtime_error("Stale market publication permit");
  while (size_ && slots_[head_].seq <= seq) {
    const auto& snapshot = slots_[head_].snapshot;
    visible_.epoch = snapshot.epoch;
    std::copy(snapshot.books.begin(), snapshot.books.end(), visible_.books.begin());
    head_ = (head_ + 1) % slots_.size();
    --size_;
  }
  permitted_ = seq;
}

void MarketPublication::copy(MarketSnapshot& out) const noexcept {
  out.epoch = visible_.epoch;
  std::copy(visible_.books.begin(), visible_.books.end(), out.books.begin());
}
}  // namespace marketlab::orders::spreads
