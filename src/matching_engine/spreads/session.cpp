#include "matching_engine/spreads/session.hpp"
#ifdef MARKETLAB_LATENCY_PROBES
#include "spreads/latency_probe.hpp"
#endif
#include <algorithm>

namespace marketlab::orders::spreads {
using persistence::integer;
using persistence::Reader;

namespace {
constexpr auto body_limit = group::maximum_frame - group::header_size;

Side opposite(Side side) {
  return side == Side::Buy ? Side::Sell : Side::Buy;
}

SourcePiece piece(const MatchingEngine::FillPiece& p) {
  return {p.after.id, p.after.user, p.after.price, p.quantity};
}

void put_piece(Bytes& bytes, const SourcePiece& p) {
  integer(bytes, p.order, 8);
  integer(bytes, p.user, 4);
  integer(bytes, std::uint32_t(p.price), 4);
  integer(bytes, p.quantity, 4);
}

SourcePiece read_piece(Reader& reader) {
  return {reader.number(8),
          std::uint32_t(reader.number(4)),
          Price(reader.number(4)),
          Quantity(reader.number(4))};
}
}  // namespace

Session::Session(persistence::DurableEngine& durable,
                 const OrderConfig& config,
                 std::uint16_t shard,
                 Callbacks callbacks)
    : durable_(durable),
      core_(durable.working()),
      topology_(config),
      callbacks_(std::move(callbacks)),
      users_(config.accounts.size() + 1),
      budget_(durable.command_budget()),
      member_(topology_.member(shard)) {
  if (member_ >= 4 || !callbacks_.coordinator || !callbacks_.peer || !callbacks_.durable ||
      !callbacks_.capture)
    throw std::invalid_argument("Incomplete group session transport/owner");
  sources_ = real_sources();
}

void Session::epoch(std::uint64_t value) {
  if (!value || active())
    throw std::runtime_error("Cannot change group epoch with unresolved command");
  if (epoch_ != value) {
    epoch_ = value;
    closed_ = 0;
    view_sequence_ = view_round_ = 0;
    views_ = {};
  }
}

Header Session::message(MessageKind kind, std::uint8_t flags) const {
  return {kind, epoch_, current_.seq, current_.round, flags};
}

void Session::recover_sources() {
  if (active())
    throw std::runtime_error("Cannot rebuild sources across unresolved execution");
  sources_ = {};
  sources_ = real_sources();
}

std::size_t Session::buffer_bytes() const {
  auto bytes = scratch_.capacity() + direct_.capacity() * sizeof(FillPiece);
  for (const auto& v : local_)
    bytes += v.capacity() * sizeof(FillPiece);
  for (const auto& route : details_)
    for (const auto& d : route)
      bytes += d.pieces.capacity() * sizeof(SourcePiece);
  for (const auto& v : allocations_)
    bytes += v.capacity();
  return bytes;
}

void Session::check_budget() const {
  // Results and round workspace each have one explicit command-sized budget.
  // Retained capacity counts, so repeated rounds cannot grow memory without bound.
  if (buffer_bytes() > budget_ || (result_ && result_->bytes() > budget_))
    throw std::runtime_error("Group command exceeds bounded execution workspace");
}

bool Session::begin(const Header& h, std::uint8_t origin) {
  if (!epoch_ || h.epoch != epoch_ || !h.seq || h.seq <= closed_ || active())
    throw std::runtime_error("Stale/overlapping group command");
  const auto seq = h.seq;
  result_ = durable_.begin_group([this, seq](const auto& boundary, auto events) {
    callbacks_.durable(seq, boundary, events);
  });
  if (!result_)
    return false;
  current_ = h;
  origin_ = origin;
  initiator_ = origin == member_;
  participants_ = 1U << member_;
  finished_ = false;
  allocations_complete_ = true;
  remote_quantity_ = 0;
  fill_ = 0;
  reset_round();
  return true;
}

void Session::reset_round() {
  direct_.clear();
  for (auto& pieces : local_)
    pieces.clear();
  for (auto& route : details_)
    for (auto& details : route) {
      details.pieces.clear();
      details.quantity = 0;
      details.complete = false;
    }
  for (auto& bytes : allocations_)
    bytes.clear();
  allocation_at_.fill(0);
  round_members_ = executed_ = 0;
  round_ready_ = details_ready_ = false;
  plan_ = {};
}

Sources Session::real_sources() const {
  auto next = sources_;
  const auto own = member_sources(member_);
  for (unsigned i = 0; i < next.size(); ++i)
    if (own & (1U << i)) {
      next[i] = core_.source_depth(topology_.symbols[i / 2], i % 2 == Bid ? Side::Buy : Side::Sell);
      next[i].version = sources_[i].version;
    }
  return next;
}

bool Session::sources_changed() const {
  const auto next = real_sources();
  for (unsigned i = 0; i < next.size(); ++i)
    if (!next[i].same_levels(sources_[i]))
      return true;
  return false;
}

void Session::source_done() {
  auto next = real_sources();
  std::uint16_t changed = 0;
  for (unsigned i = 0; i < next.size(); ++i)
    if (!next[i].same_levels(sources_[i])) {
      if (next[i].version == UINT64_MAX)
        throw std::overflow_error("Source version exhausted");
      ++next[i].version;
      changed |= 1U << i;
    }
  scratch_.clear();
  put_sources(scratch_, changed, sources_, next, false);
  callbacks_.coordinator(message(MessageKind::SourceDone), scratch_);
  sources_ = next;
  callbacks_.capture(current_.seq);
}

void Session::declare_round(std::uint8_t members) {
  if (current_.round == UINT32_MAX)
    throw std::overflow_error("Group round exhausted");
  ++current_.round;
  round_members_ = members;
  participants_ |= members;
  round_ready_ = false;
  const std::array<std::uint8_t, 1> body{members};
  callbacks_.coordinator(message(MessageKind::RoundBegin), body);
}

void Session::take(std::uint16_t symbol,
                   Side side,
                   Quantity quantity,
                   std::vector<FillPiece>& pieces) {
  const auto used = buffer_bytes();
  if (used > budget_)
    throw std::runtime_error("Group workspace is full");
  const auto capacity = pieces.capacity() + (budget_ - used) / sizeof(FillPiece);
  core_.take_prefix(symbol, side, quantity, pieces, *result_, capacity);
  check_budget();
}

bool Session::run(const Header& h, const LinkHeader& context, std::span<const std::uint8_t> bytes) {
  if (h.kind != MessageKind::Run || h.round || h.flags ||
      (context.kind != LinkKind::NewOrder && context.kind != LinkKind::Cancel))
    throw std::runtime_error("Invalid group RUN");
  if (!begin(h, member_))
    return false;
  if (context.kind == LinkKind::NewOrder) {
    const auto request = copy_record<NewOrderRequest>(bytes);
#ifdef MARKETLAB_LATENCY_PROBES
    testing::latency::record(testing::latency::Kind::GroupStart, h.seq,
                            request.header.requestId, topology_.shards[member_]);
#endif
    const bool implied =
        std::any_of(topology_.symbols.begin(), topology_.symbols.end(), [&](auto symbol) {
          return std::memcmp(request.symbol, durable_.dictionary().symbol(symbol).data(), 32) == 0;
        });
    if (implied) {
      const auto order = core_.begin_group(context, request, *result_);
      if (order) {
        incoming_ = *order;
        select_round();
        return true;
      }
    } else {
      core_.execute(context, bytes, *result_);
    }
  } else {
    core_.execute(context, bytes, *result_);
  }
  finished_ = true;
  complete_local();
  return true;
}

void Session::select_round() {
  reset_round();
  pairing_ = incoming_;
  plan_ = select(core_, topology_.slot(incoming_.symbol), views_, incoming_);
  auto members = std::uint8_t(1U << member_);
  for (unsigned route = 0; route < routes.size(); ++route)
    if (plan_.prefixes[route]) {
      members |= 1U << topology_.owner(routes[route].first / 2);
      members |= 1U << topology_.owner(routes[route].second / 2);
    }
  if (members == (1U << member_)) {
    if (plan_.direct) {
      take(incoming_.symbol, opposite(incoming_.side), plan_.direct, direct_);
      for (auto& p : direct_)
        direct_fill(p, p.quantity);
    }
    incoming_ = pairing_;
    core_.finish_group(incoming_, *result_);
    finished_ = true;
    complete_local();
    return;
  }
  const auto previous = participants_;
  declare_round(members);
  for (std::uint8_t member = 0; member < 4; ++member) {
    if (member == member_ || !(members & (1U << member)))
      continue;
    scratch_.clear();
    const bool first = !(previous & (1U << member));
    integer(scratch_, first, 1);
    if (first) {
      integer(scratch_, incoming_.id, 8);
      integer(scratch_, incoming_.user, 4);
      integer(scratch_, incoming_.symbol, 2);
      integer(scratch_, static_cast<unsigned>(incoming_.side), 1);
      integer(scratch_, std::uint32_t(incoming_.price), 4);
      integer(scratch_, incoming_.original, 4);
    }
    const auto count_at = scratch_.size();
    integer(scratch_, 0, 1);
    for (std::uint8_t route = 0; route < routes.size(); ++route)
      if (plan_.prefixes[route] && (topology_.owner(routes[route].first / 2) == member ||
                                    topology_.owner(routes[route].second / 2) == member)) {
        integer(scratch_, route, 1);
        integer(scratch_, plan_.prefixes[route], 4);
        ++scratch_[count_at];
      }
    callbacks_.peer(member, message(MessageKind::ExecutePrefix), scratch_);
  }
  if (plan_.direct)
    take(incoming_.symbol, opposite(incoming_.side), plan_.direct, direct_);
}

void Session::complete_local() {
  details_ready_ = true;
  if (sources_changed()) {
    declare_round(1U << member_);
    source_done();
  } else if (!current_.round) {
    seal(MessageKind::LocalComplete);
  } else {
    close();
  }
}

void Session::seal(MessageKind kind) {
  check_budget();
  callbacks_.capture(current_.seq);
  const auto seq = durable_.seal_group();
  result_ = nullptr;
  closed_ = current_.seq;
  scratch_.clear();
  integer(scratch_, seq, 8);
  callbacks_.coordinator(message(kind), scratch_);
}

void Session::close() {
  for (std::uint8_t member = 0; member < 4; ++member)
    if (member != member_ && (participants_ & (1U << member)))
      callbacks_.peer(member, message(MessageKind::CommandEnd), {});
  seal(MessageKind::EndAck);
}

void Session::advance() {
  if (!round_ready_ || !details_ready_)
    return;
  if (finished_) {
    close();
    return;
  }
  if (plan_.refill && incoming_.remaining) {
    select_round();
    return;
  }
  core_.finish_group(incoming_, *result_);
  finished_ = true;
  complete_local();
}

void Session::fill_incoming(persistence::CompactOrder& incoming, Quantity quantity) {
  if (quantity <= 0 || quantity > incoming.remaining)
    throw std::runtime_error("Incoming allocation exceeds remaining quantity");
  incoming.remaining -= quantity;
  incoming.filled += quantity;
  incoming.status = incoming.remaining ? OrderStatus::PartiallyFilled : OrderStatus::Filled;
}

std::uint32_t Session::next_fill() {
  if (fill_ == UINT32_MAX)
    throw std::overflow_error("Execution fill number exhausted");
  return ++fill_;
}

void Session::direct_fill(FillPiece& p, Quantity quantity) {
  if (quantity <= 0 || quantity > p.quantity - p.allocated)
    throw std::runtime_error("Direct allocation exceeds FIFO piece");
  p.allocated += quantity;
  fill_incoming(pairing_, quantity);
  next_fill();
  const auto time = unix_ns();
  const auto local = allocated(p, p.allocated);
  const auto& buy = incoming_.side == Side::Buy ? pairing_ : local;
  const auto& sell = incoming_.side == Side::Sell ? pairing_ : local;
  const auto trade = core_.trade_record(
      incoming_.symbol, p.after.price, quantity, time, buy.id, buy.user, sell.id, sell.user);
  result_->add(result_->executions, persistence::ExecutionRecord(trade));
  core_.fill_events(pairing_, trade.id, trade.price, quantity, time, *result_);
  core_.fill_events(local, trade.id, trade.price, quantity, time, *result_);
}

void Session::validate_identity(OrderId order, std::uint32_t user, std::uint8_t owner) const {
  if (owner >= 4 || (order >> 48) != topology_.shards[owner] || !(order & ((1ULL << 48) - 1)) ||
      !user || user >= users_)
    throw std::runtime_error("Foreign/invalid group order identity");
}

bool Session::execute(std::uint8_t from, const Header& h, std::span<const std::uint8_t> bytes) {
  if (from >= 4 || from == member_ || h.flags || !h.round || h.epoch != epoch_)
    throw std::runtime_error("Invalid peer execution source/round");
  Reader reader{bytes};
  const auto first = reader.number(1);
  if (first > 1 || bool(first) == active())
    throw std::runtime_error("Peer execution command context mismatch");
  auto incoming = incoming_;
  if (first) {
    incoming = {};
    incoming.id = reader.number(8);
    incoming.user = reader.number(4);
    incoming.symbol = reader.number(2);
    incoming.side = static_cast<Side>(reader.number(1));
    incoming.price = Price(reader.number(4));
    incoming.original = incoming.remaining = Quantity(reader.number(4));
    const auto target = topology_.slot(incoming.symbol);
    validate_identity(incoming.id, incoming.user, from);
    if (target >= 5 || topology_.owner(target) != from || incoming.original <= 0 ||
        (incoming.side != Side::Buy && incoming.side != Side::Sell) ||
        incoming.price < topology_.limits[target].lower ||
        incoming.price > topology_.limits[target].upper)
      throw std::runtime_error("Invalid peer aggressor context");
  } else if (initiator_ || h.seq != current_.seq || h.round <= current_.round || origin_ != from ||
             !allocations_complete_) {
    throw std::runtime_error("Peer advanced before ordered allocations completed");
  }
  std::array<Quantity, 12> prefixes{};
  const auto count = reader.number(1);
  if (!count || count > 2)
    throw std::runtime_error("Invalid peer prefix count");
  std::uint64_t total = 0;
  for (unsigned i = 0; i < count; ++i) {
    const auto route = reader.number(1);
    const auto quantity = Quantity(reader.number(4));
    if (route >= routes.size() || quantity <= 0 || prefixes[route])
      throw std::runtime_error("Invalid/duplicate execution prefix");
    const auto& r = routes[route];
    if (r.target != topology_.slot(incoming.symbol) ||
        r.side != (incoming.side == Side::Buy ? Ask : Bid) ||
        (topology_.owner(r.first / 2) != member_ && topology_.owner(r.second / 2) != member_))
      throw std::runtime_error("Execution prefix does not belong to source");
    prefixes[route] = quantity;
    total += quantity;
  }
  reader.finish();
  if (total + (first ? 0 : remote_quantity_) > std::uint64_t(incoming.original))
    throw std::runtime_error("Peer execution exceeds aggressor original quantity");
  if (first && !begin(h, from))
    return false;
  if (!first)
    reset_round();
  current_ = h;
  incoming_ = incoming;
  remote_quantity_ += total;
  allocations_complete_ = false;
  plan_.prefixes = prefixes;
  for (std::uint8_t route = 0; route < routes.size(); ++route)
    if (prefixes[route]) {
      const auto& r = routes[route];
      const auto source = topology_.owner(r.first / 2) == member_ ? r.first : r.second;
      if (std::uint64_t(prefixes[route]) > sources_[source].total())
        throw std::runtime_error("Peer prefix exceeds advertised real source window");
      take(topology_.symbols[source / 2],
           source % 2 == Bid ? Side::Buy : Side::Sell,
           prefixes[route],
           local_[route]);
    }
  // Real state is complete now; identities and allocations proceed independently.
  source_done();
  callbacks_.peer(from, message(MessageKind::ExecutionDone), {});
  for (std::uint8_t route = 0; route < routes.size(); ++route)
    if (prefixes[route])
      send_details(route);
  return true;
}

void Session::send_details(std::uint8_t route) {
  const auto& pieces = local_[route];
  for (std::size_t offset = 0; offset < pieces.size();) {
    const auto count = std::min<std::size_t>((body_limit - 7) / 20, pieces.size() - offset);
    scratch_.clear();
    integer(scratch_, route, 1);
    integer(scratch_, offset, 4);
    integer(scratch_, count, 2);
    for (std::size_t i = offset; i < offset + count; ++i)
      put_piece(scratch_, piece(pieces[i]));
    offset += count;
    callbacks_.peer(
        origin_,
        message(MessageKind::ResultDetails, offset == pieces.size() ? group::final_part : 0),
        scratch_);
  }
  check_budget();
}

void Session::receive_details(std::uint8_t from,
                              const Header& h,
                              std::span<const std::uint8_t> bytes) {
  Reader reader{bytes};
  const auto route = reader.number(1);
  const auto offset = reader.number(4);
  const auto count = reader.number(2);
  if (route >= routes.size() || !plan_.prefixes[route] || !count ||
      bytes.size() != 7 + count * 20 || (h.flags & ~group::final_part))
    throw std::runtime_error("Invalid source detail chunk");
  const auto& r = routes[route];
  const unsigned side = topology_.owner(r.first / 2) == from ? 0 : 1;
  const auto source = side ? r.second : r.first;
  if (topology_.owner(source / 2) != from)
    throw std::runtime_error("Foreign source detail route");
  auto& details = details_[route][side];
  if (details.complete || offset != details.pieces.size())
    throw std::runtime_error("Duplicate/out-of-order source detail chunk");
  const std::size_t required = details.pieces.size() + count;
  if (required > details.pieces.capacity()) {
    const auto available = (budget_ - std::min(budget_, buffer_bytes())) / sizeof(SourcePiece);
    const auto maximum = details.pieces.capacity() + available;
    if (required > maximum)
      throw std::runtime_error("Source details exceed workspace budget");
    details.pieces.reserve(std::min(maximum, std::max(required, details.pieces.capacity() * 2)));
  }
  for (unsigned i = 0; i < count; ++i) {
    const auto p = read_piece(reader);
    validate_identity(p.order, p.user, from);
    const auto& limits = topology_.limits[source / 2];
    if (p.quantity <= 0 || p.price < limits.lower || p.price > limits.upper ||
        details.quantity + p.quantity > std::uint64_t(plan_.prefixes[route]))
      throw std::runtime_error("Source details exceed price/quantity limits");
    if (!details.pieces.empty()) {
      const auto& previous = details.pieces.back();
      if ((source % 2 == Bid ? p.price > previous.price : p.price < previous.price) ||
          (p.price == previous.price && p.order <= previous.order))
        throw std::runtime_error("Source details violate price/FIFO order");
    }
    details.pieces.push_back(p);
    details.quantity += p.quantity;
  }
  reader.finish();
  if (h.flags & group::final_part) {
    if (details.quantity != std::uint64_t(plan_.prefixes[route]))
      throw std::runtime_error("Incomplete final source detail chunk");
    details.complete = true;
  }
  pair_details();
}

void Session::send_allocations(std::uint8_t member, bool final) {
  auto& bytes = allocations_[member];
  if (bytes.empty())
    bytes.resize(2);
  const auto count = (bytes.size() - 2) / 37;
  bytes[0] = count & 255;
  bytes[1] = count >> 8;
  callbacks_.peer(member, message(MessageKind::Allocations, final ? group::final_part : 0), bytes);
  bytes.clear();
}

void Session::queue_allocation(std::uint8_t route,
                               unsigned side,
                               std::uint32_t index,
                               const Combination& fill,
                               const SourcePiece& other) {
  const auto& r = routes[route];
  const auto member = topology_.owner((side ? r.second : r.first) / 2);
  auto& bytes = allocations_[member];
  if (bytes.size() + 37 > body_limit)
    send_allocations(member, false);
  if (bytes.empty())
    bytes.resize(2);
  integer(bytes, route, 1);
  integer(bytes, index, 4);
  integer(bytes, fill.key.fill, 4);
  integer(bytes, fill.quantity, 4);
  integer(bytes, fill.timestamp, 8);
  integer(bytes, other.order, 8);
  integer(bytes, other.user, 4);
  integer(bytes, std::uint32_t(other.price), 4);
  check_budget();
}

void Session::pair_details() {
  if (details_ready_ || executed_ != (round_members_ & ~(1U << member_)))
    return;
  for (unsigned route = 0; route < routes.size(); ++route)
    if (plan_.prefixes[route] && (!details_[route][0].complete || !details_[route][1].complete))
      return;
  std::array<std::array<PieceCursor, 2>, 12> cursors{};
  for (unsigned route = 0; route < routes.size(); ++route)
    for (unsigned side = 0; side < 2; ++side)
      cursors[route][side] = PieceCursor(details_[route][side].pieces);
  std::size_t direct_at = 0;
  for (unsigned s = 0; s < plan_.count; ++s) {
    const auto& slice = plan_.slices[s];
    auto remaining = slice.quantity;
    while (remaining) {
      if (slice.route == no_slot) {
        auto& p = direct_.at(direct_at);
        const auto quantity = std::min(remaining, Quantity(p.quantity - p.allocated));
        direct_fill(p, quantity);
        if (p.allocated == p.quantity)
          ++direct_at;
        remaining -= quantity;
      } else {
        auto& first = cursors[slice.route][0];
        auto& second = cursors[slice.route][1];
        const auto quantity = std::min({remaining, first.remaining(), second.remaining()});
        const auto fill = pair(slice.route,
                               incoming_,
                               first.head(),
                               second.head(),
                               quantity,
                               {incoming_.id, next_fill()},
                               unix_ns());
        fill_incoming(pairing_, quantity);
        record(core_, *result_, topology_, fill, pairing_);
        queue_allocation(slice.route, 0, first.index(), fill, second.head());
        queue_allocation(slice.route, 1, second.index(), fill, first.head());
        first.consume(quantity);
        second.consume(quantity);
        remaining -= quantity;
      }
    }
  }
  for (std::uint8_t member = 0; member < 4; ++member)
    if (member != member_ && (round_members_ & (1U << member)))
      send_allocations(member, true);
  check_budget();
  if (pairing_.filled != incoming_.filled || pairing_.remaining != incoming_.remaining)
    throw std::logic_error("Paired details differ from confirmed real execution");
  details_ready_ = true;
  advance();
}

void Session::receive_allocations(std::uint8_t from,
                                  const Header& h,
                                  std::span<const std::uint8_t> bytes) {
  if (from != origin_ || allocations_complete_ || (h.flags & ~group::final_part))
    throw std::runtime_error("Unexpected peer allocation");
  Reader reader{bytes};
  const auto count = reader.number(2);
  if (bytes.size() != 2 + count * 37 || (!count && !(h.flags & group::final_part)))
    throw std::runtime_error("Invalid allocation chunk size");
  for (unsigned i = 0; i < count; ++i) {
    const auto route = reader.number(1);
    const auto index = reader.number(4);
    const auto fill = reader.number(4);
    const auto quantity = Quantity(reader.number(4));
    const auto time = reader.number(8);
    const SourcePiece other{
        reader.number(8), std::uint32_t(reader.number(4)), Price(reader.number(4)), quantity};
    if (route >= routes.size() || !plan_.prefixes[route] || index != allocation_at_[route] ||
        index >= local_[route].size() || !fill || fill <= fill_ || !time)
      throw std::runtime_error("Invalid/duplicate FIFO allocation");
    auto& p = local_[route][index];
    if (quantity <= 0 || quantity > p.quantity - p.allocated)
      throw std::runtime_error("Allocation exceeds local FIFO piece");
    const auto& r = routes[route];
    const bool first = topology_.owner(r.first / 2) == member_;
    const auto other_source = first ? r.second : r.first;
    validate_identity(other.order, other.user, topology_.owner(other_source / 2));
    const auto& limits = topology_.limits[other_source / 2];
    if (other.price < limits.lower || other.price > limits.upper)
      throw std::runtime_error("Remote allocation price is outside source limits");
    const auto own = piece(p);
    const auto combination = pair(route,
                                  incoming_,
                                  first ? own : other,
                                  first ? other : own,
                                  quantity,
                                  {incoming_.id, std::uint32_t(fill)},
                                  time);
    p.allocated += quantity;
    record(core_, *result_, topology_, combination, allocated(p, p.allocated));
    fill_ = fill;
    if (p.allocated == p.quantity)
      ++allocation_at_[route];
  }
  reader.finish();
  if (h.flags & group::final_part) {
    for (unsigned route = 0; route < routes.size(); ++route)
      if (allocation_at_[route] != local_[route].size())
        throw std::runtime_error("Final allocations left an unpaired real prefix");
    allocations_complete_ = true;
  }
  check_budget();
}

bool Session::peer(std::uint8_t from, const Header& h, std::span<const std::uint8_t> bytes) {
  if (h.kind == MessageKind::ExecutePrefix)
    return execute(from, h, bytes);
  if (!active() || from >= 4 || from == member_ || h.epoch != epoch_ || h.seq != current_.seq)
    throw std::runtime_error("Peer message outside active group command");
  if (h.kind == MessageKind::CommandEnd) {
    if (initiator_ || from != origin_ || h.flags || !bytes.empty() || h.round < current_.round ||
        !allocations_complete_)
      throw std::runtime_error("Command ended before ordered allocations completed");
    current_ = h;
    seal(MessageKind::EndAck);
    return true;
  }
  if (h.round != current_.round)
    throw std::runtime_error("Stale peer round");
  if (initiator_) {
    if (!(round_members_ & (1U << from)))
      throw std::runtime_error("Unexpected round peer");
    if (h.kind == MessageKind::ExecutionDone) {
      if (h.flags || !bytes.empty() || (executed_ & (1U << from)))
        throw std::runtime_error("Duplicate/invalid execution completion");
      executed_ |= 1U << from;
      if (executed_ == (round_members_ & ~(1U << member_))) {
        fill_incoming(incoming_, incoming_.remaining - plan_.remaining);
        source_done();
      }
      pair_details();
    } else if (h.kind == MessageKind::ResultDetails)
      receive_details(from, h, bytes);
    else
      throw std::runtime_error("Unexpected initiator peer message");
  } else if (h.kind == MessageKind::Allocations)
    receive_allocations(from, h, bytes);
  else
    throw std::runtime_error("Unexpected source peer message");
  return true;
}

void Session::control(const Header& h, std::span<const std::uint8_t> bytes) {
  if (!epoch_ || h.epoch != epoch_)
    throw std::runtime_error("Control epoch mismatch");
  if (h.kind == MessageKind::ImpliedReplace) {
    const bool initial = h.flags == group::initial_view;
    if ((h.flags && !initial) || (initial && (active() || h.seq || h.round)) ||
        (!initial &&
         (!h.seq || !h.round || h.seq <= closed_ || h.seq < view_sequence_ ||
          (h.seq == view_sequence_ && h.round <= view_round_) ||
          (active() && (h.seq != current_.seq || (initiator_ && h.round != current_.round))))))
      throw std::runtime_error("Stale/invalid implied replacement");
    auto next = views_;
    Reader reader{bytes};
    const auto targets = read_views(reader, next, member_targets(member_));
    reader.finish();
    views_ = next;
    view_sequence_ = h.seq;
    view_round_ = h.round;
    callbacks_.capture(h.seq);
    const std::array<std::uint8_t, 1> ack{targets};
    callbacks_.coordinator({MessageKind::ViewApplied, epoch_, h.seq, h.round, h.flags}, ack);
  } else if (h.kind == MessageKind::RoundReady) {
    if (!active() || !initiator_ || h.seq != current_.seq || h.round != current_.round || h.flags ||
        !bytes.empty() || round_ready_)
      throw std::runtime_error("Unexpected round-ready message");
    round_ready_ = true;
    advance();
  } else
    throw std::runtime_error("Unexpected shard control message");
}
}  // namespace marketlab::orders::spreads
