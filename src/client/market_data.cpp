#include "client/market_data.hpp"
#include "client/recovery.hpp"
#include "shared_data/market_data/availability.hpp"

#include <arpa/inet.h>
#include <fcntl.h>
#include <poll.h>
#include <sys/socket.h>

#include <cerrno>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <algorithm>

namespace marketlab::md {
namespace {
void observe_datagrams(Fd fd, std::size_t count, const std::atomic_bool& stop) {
  std::size_t received = 0;
  AvailabilityNotice availability("MARKET_DATA");
  auto last_received = Clock::now();
  while (!stop.load() && (!count || received < count)) {
    if (Clock::now() - last_received >= std::chrono::seconds(3))
      availability.unavailable("No market-data packets for 3s; displayed prices may be stale");
    pollfd p{fd.get(), POLLIN, 0};
    if (poll(&p, 1, 100) <= 0)
      continue;
    std::array<std::uint8_t, 65536> data{};
    const auto n = recv(fd.get(), data.data(), data.size(), 0);
    if (n < 0 && errno == EINTR)
      continue;
    if (n < 0)
      throw std::runtime_error(std::string("UDP recv: ") + std::strerror(errno));
    try {
      const auto message = decode(std::span(data.data(), static_cast<std::size_t>(n)));
      last_received = Clock::now();
      availability.recovered();
      print_message(message);
      ++received;
    } catch (const std::exception& e) {
      availability.unavailable(std::string("Invalid market-data packet: ") + e.what());
    }
  }
}
}  // namespace

void observe_udp(const Endpoint& endpoint, std::size_t count, const std::atomic_bool& stop) {
  auto fd = bind_udp(endpoint.host, endpoint.port);
  std::cout << "Listening UDP " << endpoint.host << ':' << endpoint.port << std::endl;
  observe_datagrams(std::move(fd), count, stop);
}

void observe_multicast(const MulticastConfig& config,
                       std::size_t count,
                       const std::atomic_bool& stop) {
  auto fd = join_multicast(config);
  std::cout << "Joined UDP multicast " << config.group.host << ':' << config.group.port
            << " interface=" << config.interface << std::endl;
  observe_datagrams(std::move(fd), count, stop);
}

Message query_snapshot(const Endpoint& endpoint, std::uint16_t symbol, int timeout_ms) {
  const auto deadline = Clock::now() + std::chrono::milliseconds(timeout_ms);
  auto fd = connect_tcp(endpoint.host, endpoint.port, deadline);
  Bytes request;
  append_uint(request, symbol, 2);
  send_all(fd.get(), request, deadline);
  const auto prefix = receive_exact(fd.get(), 2, deadline);
  std::size_t offset = 0;
  const auto length = read_uint(prefix, offset, 2);
  if (length < header_size || length > max_snapshot_size)
    throw std::runtime_error("Invalid snapshot length");
  auto message = decode(receive_exact(fd.get(), length, deadline));
  if (message.kind != Kind::Snapshot || message.symbol_id != symbol)
    throw std::runtime_error("Unexpected snapshot");
  return message;
}

namespace {
struct Query {
  Fd socket;
  SnapshotRequest request;
  Deadline deadline;
  bool connecting = false, done = false;
  std::array<std::uint8_t, 2> output;
  std::size_t sent = 0, expected = 0;
  Bytes input;

  Query(const Endpoint& endpoint, SnapshotRequest ticket, Deadline until)
      : socket(::socket(AF_INET, SOCK_STREAM, 0)),
        request(ticket),
        deadline(until),
        output{static_cast<std::uint8_t>(ticket.symbol),
               static_cast<std::uint8_t>(ticket.symbol >> 8)} {
    if (!socket)
      throw std::runtime_error("Recovery socket failed");
    const auto flags = fcntl(socket.get(), F_GETFL, 0);
    if (flags < 0 || fcntl(socket.get(), F_SETFL, flags | O_NONBLOCK) < 0 ||
        fcntl(socket.get(), F_SETFD, FD_CLOEXEC) < 0)
      throw std::runtime_error("Recovery fcntl failed");
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(endpoint.port);
    if (inet_pton(AF_INET, endpoint.host.c_str(), &address.sin_addr) != 1)
      throw std::runtime_error("Recovery endpoint must be numeric IPv4");
    if (connect(socket.get(), reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0) {
      if (errno != EINPROGRESS)
        throw std::runtime_error(std::string("Recovery connect: ") + std::strerror(errno));
      connecting = true;
    }
  }

  short events() const {
    return connecting || sent < output.size() ? POLLOUT : POLLIN;
  }

  std::optional<Message> ready(short events) {
    if (events & POLLNVAL)
      throw std::runtime_error("Recovery socket invalid");
    if (connecting) {
      int error = 0;
      socklen_t size = sizeof(error);
      if (getsockopt(socket.get(), SOL_SOCKET, SO_ERROR, &error, &size) < 0 || error)
        throw std::runtime_error("Recovery connect failed");
      connecting = false;
    }
    while (sent < output.size()) {
      const auto n = send(socket.get(), output.data() + sent, output.size() - sent, 0);
      if (n < 0 && errno == EINTR)
        continue;
      if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
        return {};
      if (n <= 0)
        throw std::runtime_error("Recovery send failed");
      sent += static_cast<std::size_t>(n);
    }
    for (;;) {
      std::array<std::uint8_t, 2 + max_snapshot_size> buffer{};
      const auto n = recv(socket.get(), buffer.data(), buffer.size(), 0);
      if (n < 0 && errno == EINTR)
        continue;
      if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
        return {};
      if (n <= 0)
        throw std::runtime_error("Recovery response closed");
      input.insert(input.end(), buffer.begin(), buffer.begin() + n);
      if (input.size() >= 2 && !expected) {
        std::size_t offset = 0;
        const auto length = read_uint(input, offset, 2);
        if (length < header_size || length > max_snapshot_size)
          throw std::runtime_error("Invalid recovery length");
        expected = 2 + length;
      }
      if (input.size() > 2 + max_snapshot_size || (expected && input.size() > expected))
        throw std::runtime_error("Extra recovery bytes");
      if (expected && input.size() == expected)
        return decode(std::span(input).subspan(2));
    }
  }
};

void print_view(std::string_view event, std::uint16_t id, const MarketView& view) {
  if (event == "BOOK" && view.snapshot) {
    const auto& m = *view.snapshot;
    std::cout << console::cyan << "BOOK symbol=" << id << " index=" << m.index
              << " time=" << console::time(m.timestamp) << " epoch=" << epoch_text(m.epoch)
              << " state=" << status_name(view.status) << " entries=" << m.levels.size()
              << console::reset << '\n';
    for (const auto& level : m.levels)
      std::cout << "  "
                << console::Label{level.side == Side::Buy ? "BUY " : "SELL",
                                  level.side == Side::Buy ? console::green : console::yellow}
                << " price=" << level.price << " quantity=" << level.quantity
                << " numOfOrders=" << level.num_orders << '\n';
  }
  std::cout << std::flush;
}

class MarketConsole {
 public:
  void event(std::string_view event, std::uint16_t id, const MarketView& view) {
    if (view.status == SyncStatus::Late || view.status == SyncStatus::Stale ||
        event == "TCP_FAILED" || event == "INVALID_MESSAGE" || event == "INVALID_RESULT")
      unavailable();
    // Per-symbol STATE/TCP_START/TCP_FAILED events otherwise flood the terminal
    // during a single source outage. Keep the recovery engine itself unchanged.
    if (event == "BOOK" && view.status == SyncStatus::Synced && !availability_.active())
      print_view(event, id, view);
  }

  void unavailable() {
    availability_.unavailable(
        "Feed or snapshot recovery unavailable; displayed prices may be stale");
  }

  void refresh(const Config& config, const MarketRecovery& recovery) {
    if (!availability_.active())
      return;
    for (const auto& [id, name] : config.symbols) {
      (void)name;
      if (recovery.view(id).status != SyncStatus::Synced)
        return;
    }
    availability_.recovered();
    // Publish one coherent restored view after every configured symbol is ready.
    for (const auto& [id, name] : config.symbols) {
      (void)name;
      print_view("BOOK", id, recovery.view(id));
    }
  }

 private:
  AvailabilityNotice availability_{"MARKET_DATA"};
};
}

void observe_market(const Config& config, std::size_t count, const std::atomic_bool& stop) {
  auto udp = config.multicast
                 ? join_multicast(*config.multicast)
                 : bind_udp(config.receivers.front().host, config.receivers.front().port);
  if (config.multicast)
    std::cout << "Joined UDP multicast " << config.multicast->group.host << ':'
              << config.multicast->group.port
              << " interface=" << config.multicast->interface << '\n';
  else
    std::cout << "Listening UDP " << config.receivers.front().host << ':'
              << config.receivers.front().port << '\n';
  std::cout << "Automatic snapshot recovery TCP " << config.query.host << ':' << config.query.port
            << " reorder_wait_ms=" << config.recovery.reorder_wait_ms << '\n'
            << std::flush;
  MarketConsole console;
  MarketRecovery recovery(
      config.symbols, config.recovery, Clock::now(), [&](auto event, auto id, const auto& view) {
        console.event(event, id, view);
      });
  std::vector<Query> queries;
  std::size_t received = 0;
  while (!stop.load() && (!count || received < count)) {
    auto now = Clock::now();
    recovery.tick(now);
    // Obsolete tasks are closed, so their partial responses cannot enter a later query.
    std::erase_if(queries, [&](Query& query) {
      if (!recovery.valid(query.request))
        return true;
      if (now >= query.deadline) {
        recovery.failed(query.request, now);
        return true;
      }
      return false;
    });
    for (std::size_t attempt = 0;
         queries.size() < config.recovery.max_queries && attempt < config.recovery.max_queries;
         ++attempt) {
      const auto request = recovery.next_request(now);
      if (!request)
        break;
      try {
        queries.emplace_back(
            config.query, *request, now + std::chrono::milliseconds(config.recovery.timeout_ms));
      } catch (const std::exception&) {
        console.unavailable();
        recovery.failed(*request, now);
      }
    }
    std::vector<pollfd> descriptors{{udp.get(), POLLIN, 0}};
    for (const auto& query : queries)
      descriptors.push_back({query.socket.get(), query.events(), 0});
    const auto ready = poll(descriptors.data(), descriptors.size(), 20);
    if (ready < 0 && errno == EINTR)
      continue;
    if (ready < 0)
      throw std::runtime_error("Market client poll failed");
    if (descriptors[0].revents & POLLIN) {
      // Bound each UDP drain so busy feeds cannot starve TCP or timer handling.
      for (unsigned budget = 0; budget < 256 && (!count || received < count); ++budget) {
        std::array<std::uint8_t, max_delta_size + 1> bytes{};
        iovec vector{bytes.data(), bytes.size()};
        msghdr packet{};
        packet.msg_iov = &vector;
        packet.msg_iovlen = 1;
        const auto length = recvmsg(udp.get(), &packet, MSG_DONTWAIT);
        if (length < 0 && errno == EINTR)
          continue;
        if (length < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
          break;
        if (length < 0)
          throw std::runtime_error("Market UDP receive failed");
        const auto size = std::min(bytes.size(), static_cast<std::size_t>(length));
        try {
          if (packet.msg_flags & MSG_TRUNC)
            throw std::runtime_error("Truncated UDP message");
          const auto message = decode(std::span(bytes).first(size));
          if (!config.symbols.contains(message.symbol_id))
            throw std::runtime_error("Unknown UDP symbol ID");
          recovery.udp(message, Clock::now());
          ++received;
        } catch (const std::exception&) {
          console.unavailable();
          std::optional<std::uint16_t> symbol;
          if (size >= 19)
            symbol = static_cast<std::uint16_t>(bytes[17] | (std::uint16_t(bytes[18]) << 8));
          recovery.invalid(symbol, Clock::now());
        }
      }
    }
    for (std::size_t i = 0; i < queries.size(); ++i) {
      auto& query = queries[i];
      if (!recovery.valid(query.request)) {
        query.done = true;
        continue;
      }
      if (!descriptors[i + 1].revents)
        continue;
      try {
        if (const auto response = query.ready(descriptors[i + 1].revents)) {
          recovery.complete(query.request, *response, Clock::now());
          query.done = true;
        }
      } catch (const std::exception&) {
        console.unavailable();
        recovery.failed(query.request, Clock::now());
        query.done = true;
      }
    }
    std::erase_if(queries, [](const Query& query) {
      return query.done;
    });
    console.refresh(config, recovery);
  }
}
}  // namespace marketlab::md
