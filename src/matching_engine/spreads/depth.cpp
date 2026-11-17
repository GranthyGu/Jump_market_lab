#include "matching_engine/spreads/depth.hpp"
#include "matching_engine/persistence/codec.hpp"

#include <algorithm>
#include <bit>
#include <limits>
#include <set>

namespace marketlab::orders::spreads {
using persistence::Reader;
using persistence::integer;

bool DepthSide::same_levels(const DepthSide& other) const {
  if (count != other.count || more != other.more)
    return false;
  for (std::size_t i = 0; i < count; ++i)
    if (price[i] != other.price[i] || quantity[i] != other.quantity[i])
      return false;
  return true;
}

std::uint64_t DepthSide::total() const {
  std::uint64_t sum = 0;
  for (std::size_t i = 0; i < count; ++i) {
    if (quantity[i] > std::numeric_limits<std::uint64_t>::max() - sum)
      throw std::overflow_error("Source depth quantity overflow");
    sum += quantity[i];
  }
  return sum;
}

Topology::Topology(const OrderConfig& c) {
  if (!c.spreads || c.direct)
    throw std::invalid_argument("Spread group requires router routing");
  for (std::size_t slot = 0; slot < symbols.size(); ++slot) {
    const auto it = std::find_if(c.products.begin(), c.products.end(), [&](const auto& p) {
      return p.symbol == c.spread_symbols[slot];
    });
    if (it == c.products.end())
      throw std::invalid_argument("Missing spread product: " + c.spread_symbols[slot]);
    const auto index = static_cast<std::size_t>(it - c.products.begin());
    symbols[slot] = static_cast<std::uint16_t>(index + 1);
    limits[slot] = product_limits(c, index);
    if (slot == AC && shards[0] != it->shard)
      throw std::invalid_argument("Spread books must share their owner shard");
    shards[owner(slot)] = it->shard;
  }
  if (std::set(symbols.begin(), symbols.end()).size() != 5 ||
      std::set(shards.begin(), shards.end()).size() != 4)
    throw std::invalid_argument("Spread group requires five distinct products on four shards");
  auto hash = [&](std::uint64_t value) {
    for (unsigned n = 0; n < 8; ++n) {
      fingerprint ^= (value >> (8 * n)) & 255;
      fingerprint *= 1099511628211ULL;
    }
  };
  hash(1);  // Group topology/wire schema version.
  hash(c.accounts.size());
  for (const auto& account : c.accounts) {
    hash(account.name.size());
    for (unsigned char ch : account.name)
      hash(ch);
  }
  for (std::size_t i = 0; i < symbols.size(); ++i) {
    hash(c.spread_symbols[i].size());
    for (unsigned char ch : c.spread_symbols[i])
      hash(ch);
    hash(symbols[i]);
    hash(shards[owner(i)]);
    hash(limits[i].lower);
    hash(limits[i].upper);
  }
}

std::uint8_t Topology::slot(std::uint16_t symbol) const {
  for (std::uint8_t i = 0; i < symbols.size(); ++i)
    if (symbols[i] == symbol)
      return i;
  return no_slot;
}

std::uint8_t Topology::member(std::uint16_t shard) const {
  for (std::uint8_t i = 0; i < shards.size(); ++i)
    if (shards[i] == shard)
      return i;
  return no_slot;
}

void validate(const DepthSide& side, BookSide direction) {
  if (side.count > source_depth || (side.more && side.count != source_depth))
    throw std::runtime_error("Invalid source depth count/more");
  for (std::size_t i = 0; i < side.count; ++i) {
    if (!side.quantity[i] || (i && (direction == Bid ? side.price[i - 1] <= side.price[i]
                                                     : side.price[i - 1] >= side.price[i])))
      throw std::runtime_error("Invalid source depth ordering/quantity");
  }
  (void)side.total();
}

Implied derive(const Route& route, const Sources& sources) {
  const auto& a = sources[route.first];
  const auto& b = sources[route.second];
  const auto ta = a.total(), tb = b.total(), q = std::min(ta, tb);
  Implied out;
  out.more = (ta > q || a.more) && (tb > q || b.more);
  std::size_t i = 0, j = 0;
  std::uint64_t qa = a.count ? a.quantity[0] : 0, qb = b.count ? b.quantity[0] : 0;
  while (i < a.count && j < b.count) {
    const auto amount = std::min(qa, qb);
    const auto value = std::int64_t(a.price[i]) +
                       (route.add ? std::int64_t(b.price[j]) : -std::int64_t(b.price[j]));
    if (value < INT32_MIN || value > INT32_MAX)
      throw std::overflow_error("Implied price outside int32");
    if (out.count && out.price[out.count - 1] == value)
      out.quantity[out.count - 1] += amount;
    else {
      out.price[out.count] = static_cast<std::int32_t>(value);
      out.quantity[out.count++] = amount;
    }
    qa -= amount;
    qb -= amount;
    if (!qa && ++i < a.count)
      qa = a.quantity[i];
    if (!qb && ++j < b.count)
      qb = b.quantity[j];
  }
  return out;
}

std::uint8_t recompute(const Sources& sources, std::uint16_t dirty, Views& views) {
  std::uint16_t affected = 0;
  for (unsigned i = 0; i < sources.size(); ++i)
    if (dirty & (1U << i))
      affected |= dependencies[i];
  std::uint8_t changed = 0;
  for (unsigned i = 0; i < routes.size(); ++i) {
    if (!(affected & (1U << i)))
      continue;
    auto next = derive(routes[i], sources);
    if (next != views[i]) {
      views[i] = next;
      changed |= std::uint8_t(1U << routes[i].target);
    }
  }
  return changed;
}

void encode_depth(Bytes& out,
                  std::uint8_t source,
                  const DepthSide& before,
                  const DepthSide& after,
                  bool initial) {
  if (source >= 10)
    throw std::invalid_argument("Invalid depth source");
  validate(after, static_cast<BookSide>(source % 2));
  if (!initial && (before.version == UINT64_MAX || after.version != before.version + 1))
    throw std::invalid_argument("Source version must advance once");
  std::array<std::uint8_t, 3> removed{}, changed{};
  std::size_t nr = 0, ns = 0;
  auto find = [](const DepthSide& w, std::int32_t price) {
    for (std::uint8_t i = 0; i < w.count; ++i)
      if (w.price[i] == price)
        return i;
    return no_slot;
  };
  for (std::uint8_t i = 0; i < before.count; ++i)
    if (find(after, before.price[i]) == no_slot)
      removed[nr++] = i;
  for (std::uint8_t i = 0; i < after.count; ++i) {
    const auto old = find(before, after.price[i]);
    if (old == no_slot || before.quantity[old] != after.quantity[i])
      changed[ns++] = i;
  }
  const bool snapshot = initial || after.count * 12 < 8 + nr * 5 + ns * 13;
  integer(out, static_cast<unsigned>(snapshot ? DepthEncoding::Snapshot : DepthEncoding::Delta), 1);
  integer(out, source, 1);
  if (!snapshot)
    integer(out, before.version, 8);
  integer(out, after.version, 8);
  integer(out, after.more, 1);
  integer(out, snapshot ? after.count : nr + ns, 1);
  if (snapshot) {
    for (std::uint8_t i = 0; i < after.count; ++i) {
      integer(out, static_cast<std::uint32_t>(after.price[i]), 4);
      integer(out, after.quantity[i], 8);
    }
  } else {
    for (std::size_t i = 0; i < nr; ++i) {
      integer(out, 0, 1);
      integer(out, static_cast<std::uint32_t>(before.price[removed[i]]), 4);
    }
    for (std::size_t i = 0; i < ns; ++i) {
      integer(out, 1, 1);
      integer(out, static_cast<std::uint32_t>(after.price[changed[i]]), 4);
      integer(out, after.quantity[changed[i]], 8);
    }
  }
}

std::uint8_t apply_depth(std::span<const std::uint8_t> bytes, Sources& sources, bool initial) {
  Reader r{bytes};
  const auto kind = static_cast<DepthEncoding>(r.number(1));
  const auto source = r.number(1);
  if (source >= sources.size() ||
      (kind != DepthEncoding::Snapshot && kind != DepthEncoding::Delta) ||
      (initial && kind != DepthEncoding::Snapshot))
    throw std::runtime_error("Invalid depth record");
  const auto& before = sources[source];
  DepthSide after = kind == DepthEncoding::Snapshot ? DepthSide{} : before;
  if (kind == DepthEncoding::Delta && r.number(8) != before.version)
    throw std::runtime_error("Depth base version mismatch");
  after.version = r.number(8);
  const auto more = r.number(1), count = r.number(1);
  if (more > 1 || count > (kind == DepthEncoding::Snapshot ? 3 : 6) ||
      (!initial && (before.version == UINT64_MAX || after.version != before.version + 1)))
    throw std::runtime_error("Invalid depth version/length");
  after.more = more;
  std::array<std::int32_t, 6> touched{};
  for (unsigned i = 0; i < count; ++i) {
    const auto set = kind == DepthEncoding::Snapshot ? 1 : r.number(1);
    const auto price = std::bit_cast<std::int32_t>(static_cast<std::uint32_t>(r.number(4)));
    if (set > 1)
      throw std::runtime_error("Invalid depth operation");
    if (std::find(touched.begin(), touched.begin() + i, price) != touched.begin() + i)
      throw std::runtime_error("Repeated depth price");
    touched[i] = price;
    std::uint8_t at = 0;
    while (at < after.count && after.price[at] != price)
      ++at;
    if (!set) {
      if (at == after.count)
        throw std::runtime_error("Remove missing depth level");
      for (auto j = at; j + 1 < after.count; ++j) {
        after.price[j] = after.price[j + 1];
        after.quantity[j] = after.quantity[j + 1];
      }
      --after.count;
    } else {
      const auto quantity = r.number(8);
      if (!quantity || (at == after.count && after.count == 3))
        throw std::runtime_error("Invalid depth quantity/capacity");
      if (at == after.count)
        ++after.count;
      after.price[at] = price;
      after.quantity[at] = quantity;
    }
  }
  r.finish();
  for (unsigned i = 1; i < after.count; ++i)
    for (unsigned j = i; j && (source % 2 == Ask ? after.price[j] < after.price[j - 1]
                                                 : after.price[j] > after.price[j - 1]);
         --j) {
      std::swap(after.price[j], after.price[j - 1]);
      std::swap(after.quantity[j], after.quantity[j - 1]);
    }
  for (unsigned i = after.count; i < 3; ++i) {
    after.price[i] = 0;
    after.quantity[i] = 0;
  }
  validate(after, static_cast<BookSide>(source % 2));
  sources[source] = after;
  return static_cast<std::uint8_t>(source);
}
}  // namespace marketlab::orders::spreads
