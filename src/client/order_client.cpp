#include "shared_data/orders/engine_link.hpp"
#include "shared_data/orders/config.hpp"
#include "shared_data/market_data/transport.hpp"

#include <arpa/inet.h>
#include <poll.h>
#include <sys/socket.h>
#include <termios.h>
#include <unistd.h>
#include <cerrno>
#include <charconv>
#include <csignal>
#include <deque>
#include <iostream>
#include <limits>
#include <map>
#include <sstream>
#include <utility>

namespace {
using namespace marketlab;
using namespace marketlab::orders;
namespace md = marketlab::md;

template <typename T>
T number(const std::string& text) {
  T value{};
  const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), value);
  if (text.empty() || error != std::errc{} || end != text.data() + text.size())
    throw std::runtime_error("Invalid integer: " + text);
  return value;
}

const char* status(OrderStatus value) {
  switch (value) {
    case OrderStatus::Accepted:
      return "Accepted";
    case OrderStatus::PartiallyFilled:
      return "PartiallyFilled";
    case OrderStatus::Filled:
      return "Filled";
    case OrderStatus::Canceled:
      return "Canceled";
  }
  return "Unknown";
}

void print_order(const OrderState& order) {
  std::cout << " orderId=" << order.orderId << " symbol=" << fixed_string(order.symbol)
            << " side=" << (order.side == Side::Buy ? "BUY" : "SELL") << " price=" << order.price
            << " original=" << order.originalQuantity << " filled=" << order.filledQuantity
            << " remaining=" << order.remainingQuantity << " status=" << status(order.status);
}

void help() {
  std::cout
      << "\nCommands (one command per line):\n"
      << "  buy SYMBOL PRICE QUANTITY  : place a buy limit order, e.g. buy AAPL 100 10\n"
      << "  sell SYMBOL PRICE QUANTITY : place a sell limit order; prices/quantities are integers\n"
      << "  cancel ORDER_ID            : cancel your remaining open quantity using an ORDER_UPDATE/SNAPSHOT ID\n"
      << "  cancel-last                : cancel the last order accepted during this client session\n"
      << "  trades                     : read ALL your committed trades across all shards; pages are fetched automatically\n"
      << "                               HISTORY_TRADE prints original IDs/times, symbol, price, quantity and both parties\n"
      << "                               HISTORY_COMPLETE confirms the full result; live TRADE notifications remain separate\n"
      << "  relogin                    : authenticate again and refresh your open-order snapshot\n"
      << "  help                       : display these instructions\n"
      << "  quit                       : disconnect; open orders remain on the exchange\n"
      << "History is fixed at each shard's committed boundary when the query starts. Run trades again for newer fills.\n\n";
}

struct Options {
  std::string host = "127.0.0.1", user, password;
  std::uint16_t port = default_gateway_port;
};

class Console {
 public:
  explicit Console(Options options)
      : options_(std::move(options)),
        fd_(md::connect_tcp(
            options_.host, options_.port, md::Clock::now() + std::chrono::seconds(3))) {
    sockaddr_in local{};
    socklen_t size = sizeof(local);
    if (getsockname(fd_.get(), reinterpret_cast<sockaddr*>(&local), &size) < 0)
      throw std::runtime_error("getsockname failed");
    char address[INET_ADDRSTRLEN]{};
    if (!inet_ntop(AF_INET, &local.sin_addr, address, sizeof(address)))
      throw std::runtime_error("inet_ntop failed");
    std::cout << "CONNECTED local=" << address << ':' << ntohs(local.sin_port)
              << " gateway=" << options_.host << ':' << options_.port << " user=" << options_.user
              << '\n';
    login();
  }

  int run() {
    while (!interrupted_) {
      if (!waiting_) {
        const auto newline = stdin_buffer_.find('\n');
        if (newline != std::string::npos) {
          const auto line = stdin_buffer_.substr(0, newline);
          stdin_buffer_.erase(0, newline + 1);
          if (!command(line))
            return 0;
          continue;
        }
        if (stdin_closed_) {
          if (!stdin_buffer_.empty()) {
            auto line = std::exchange(stdin_buffer_, {});
            if (!command(line))
              return 0;
          } else
            return 0;
          continue;
        }
      }
      if (waiting_ && md::Clock::now() > deadline_)
        throw std::runtime_error("Response timeout");
      pollfd ready[]{{fd_.get(), POLLIN, 0},
                     {STDIN_FILENO, static_cast<short>(waiting_ ? 0 : POLLIN), 0}};
      // Disable stdin entirely while waiting; a closed pipe otherwise reports
      // POLLHUP continuously even when no input events were requested.
      if (waiting_)
        ready[1].fd = -1;
      const int count = poll(ready, 2, 100);
      if (count < 0 && errno == EINTR)
        continue;
      if (count < 0)
        throw std::runtime_error("poll failed");
      if (ready[0].revents & (POLLIN | POLLHUP | POLLERR)) {
        if (!receive()) {
          std::cout
              << "DISCONNECTED: gateway closed TCP (another login may have replaced this session).\n";
          if (history_) {
            std::cout << "HISTORY_FAILED partial_count=" << history_total_
                      << " (query incomplete)\n";
            return 1;
          }
          return 0;
        }
      }
      if (!waiting_ && ready[1].revents & (POLLIN | POLLHUP)) {
        char buffer[4096];
        const auto n = ::read(STDIN_FILENO, buffer, sizeof(buffer));
        if (n > 0)
          stdin_buffer_.append(buffer, static_cast<std::size_t>(n));
        else if (n == 0)
          stdin_closed_ = true;
        else if (errno != EINTR && errno != EAGAIN)
          throw std::runtime_error("stdin read failed");
        if (stdin_buffer_.size() > 64 * 1024)
          throw std::runtime_error("Command input exceeds 64 KiB");
      }
    }
    return 0;
  }

  static volatile std::sig_atomic_t interrupted_;

 private:
  Options options_;
  md::Fd fd_;
  Bytes input_;
  RequestId next_ = 1, waiting_ = 0;
  OrderId last_order_ = 0;
  bool snapshot_ = false;
  bool history_ = false, history_fanout_ = false, stdin_closed_ = false;
  std::string stdin_buffer_;
  std::uint64_t history_total_ = 0;
  std::deque<TradeHistoryEnd> history_pages_;
  std::map<std::uint16_t, std::uint32_t> history_page_counts_;
  std::map<std::uint16_t, TradeId> history_last_ids_;
  md::Clock::time_point deadline_;

  template <typename T>
  void send(T value, MessageType type) {
    if (next_ == std::numeric_limits<RequestId>::max())
      throw std::runtime_error("Request IDs exhausted");
    value.header = {type, sizeof(T), next_++};
    waiting_ = value.header.requestId;
    deadline_ = md::Clock::now() + std::chrono::seconds(10);
    md::send_all(fd_.get(), record_bytes(value), deadline_);
    std::cout << "SENT requestId=" << waiting_ << " sentAtNs=" << unix_ns() << '\n';
  }

  void login() {
    LoginRequest value{};
    put_string(value.username, options_.user);
    put_string(value.password, options_.password);
    snapshot_ = true;
    send(value, MessageType::LoginRequest);
  }

  bool command(const std::string& line) {
    std::istringstream in(line);
    std::string op;
    in >> op;
    if (op.empty())
      return true;
    try {
      std::vector<std::string> args;
      for (std::string arg; in >> arg;)
        args.push_back(std::move(arg));
      if (op == "quit" && args.empty())
        return false;
      if (op == "help" && args.empty()) {
        help();
        return true;
      }
      if (op == "relogin" && args.empty()) {
        login();
        return true;
      }
      if (op == "trades" && args.empty()) {
        history_ = history_fanout_ = true;
        history_total_ = 0;
        history_pages_.clear();
        history_page_counts_.clear();
        history_last_ids_.clear();
        TradeHistoryRequest query{};
        query.limit = trade_history_page_limit;
        std::cout << "HISTORY_BEGIN user=" << options_.user << " page_limit=" << query.limit
                  << '\n';
        send(query, MessageType::TradeHistoryRequest);
        return true;
      }
      if ((op == "buy" || op == "sell") && args.size() == 3) {
        NewOrderRequest value{};
        put_string(value.symbol, args[0]);
        value.side = op == "buy" ? Side::Buy : Side::Sell;
        value.price = number<Price>(args[1]);
        value.quantity = number<Quantity>(args[2]);
        send(value, MessageType::NewOrderRequest);
        return true;
      }
      if ((op == "cancel" && args.size() == 1) || (op == "cancel-last" && args.empty())) {
        const auto id = op == "cancel" ? number<OrderId>(args[0]) : last_order_;
        if (!id)
          throw std::runtime_error("No order ID; copy one from an order update or login snapshot");
        CancelRequest value{};
        value.orderId = id;
        send(value, MessageType::CancelRequest);
        return true;
      }
      throw std::runtime_error("Unknown command or argument count; type help");
    } catch (const std::exception& e) {
      // Parse errors leave the connection usable; transport errors must stop it.
      if (waiting_)
        throw;
      std::cout << "INPUT_ERROR " << e.what() << '\n';
      return true;
    }
  }

  bool receive() {
    std::array<std::uint8_t, 4096> buffer{};
    const auto n = recv(fd_.get(), buffer.data(), buffer.size(), 0);
    if (!n)
      return false;
    if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR))
      return true;
    if (n < 0)
      throw std::runtime_error("recv failed");
    input_.insert(input_.end(), buffer.begin(), buffer.begin() + n);
    std::size_t used = 0;
    while (input_.size() - used >= sizeof(Header)) {
      Header h{};
      std::memcpy(&h, input_.data() + used, sizeof(h));
      if (h.messageLength < sizeof(Header) || h.messageLength > max_client_frame)
        throw std::runtime_error("Invalid server length");
      if (input_.size() - used < h.messageLength)
        break;
      const auto frame = std::span<const std::uint8_t>(input_).subspan(used, h.messageLength);
      if (!valid_client_record(frame, false))
        throw std::runtime_error("Invalid server record");
      message(h, frame);
      used += h.messageLength;
    }
    input_.erase(input_.begin(), input_.begin() + static_cast<std::ptrdiff_t>(used));
    return true;
  }

  void message(const Header& h, std::span<const std::uint8_t> bytes) {
    bool complete = false;
    Timestamp exchange_time = 0;
    std::memcpy(&exchange_time, bytes.data() + sizeof(Header), sizeof(exchange_time));
    const auto received_at = unix_ns();
    switch (h.messageType) {
      case MessageType::LoginAccepted:
        std::cout << "LOGIN_ACCEPTED requestId=" << h.requestId
                  << " exchangeTimeNs=" << exchange_time << " receivedAtNs=" << received_at << '\n';
        break;
      case MessageType::ActiveOrderSnapshot: {
        const auto value = copy_record<ActiveOrderSnapshot>(bytes);
        if (value.isEnd) {
          std::cout << "SNAPSHOT_END requestId=" << h.requestId
                    << " exchangeTimeNs=" << exchange_time << " receivedAtNs=" << received_at
                    << '\n';
          complete = true;
        } else {
          std::cout << "SNAPSHOT requestId=" << h.requestId;
          print_order(value.order);
          std::cout << " exchangeTimeNs=" << exchange_time << " receivedAtNs=" << received_at
                    << '\n';
        }
        break;
      }
      case MessageType::OrderUpdate: {
        const auto value = copy_record<OrderUpdate>(bytes);
        std::cout << "ORDER_UPDATE requestId=" << h.requestId;
        print_order(value.order);
        std::cout << " exchangeTimeNs=" << exchange_time << " receivedAtNs=" << received_at << '\n';
        if (value.order.status == OrderStatus::Accepted)
          last_order_ = value.order.orderId;
        complete = h.requestId != 0;
        break;
      }
      case MessageType::Trade: {
        const auto value = copy_record<Trade>(bytes);
        std::cout << "TRADE requestId=" << h.requestId << " tradeId=" << value.tradeId
                  << " orderId=" << value.orderId << " symbol=" << fixed_string(value.symbol)
                  << " side=" << (value.side == Side::Buy ? "BUY" : "SELL")
                  << " price=" << value.price << " quantity=" << value.quantity
                  << " exchangeTimeNs=" << exchange_time << " receivedAtNs=" << received_at << '\n';
        break;
      }
      case MessageType::RequestRejected: {
        const auto reason = copy_record<RequestRejected>(bytes).reason;
        std::cout << "REJECTED requestId=" << h.requestId
                  << " reason=" << static_cast<unsigned>(reason)
                  << " exchangeTimeNs=" << exchange_time << " receivedAtNs=" << received_at << '\n';
        if (snapshot_)
          throw std::runtime_error("Login rejected");
        if (history_ && h.requestId == waiting_) {
          std::cout << "HISTORY_FAILED partial_count=" << history_total_
                    << " (query incomplete; reason 4=invalid cursor, 5=unavailable, 6=busy)\n";
          history_ = false;
          history_pages_.clear();
        }
        complete = true;
        break;
      }
      case MessageType::TradeHistoryRecord: {
        if (!history_ || h.requestId != waiting_)
          throw std::runtime_error("Unexpected history record");
        const auto value = copy_record<TradeHistoryRecord>(bytes);
        const auto shard = static_cast<std::uint16_t>(value.tradeId >> 48);
        if (value.tradeId <= history_last_ids_[shard])
          throw std::runtime_error("History trade IDs repeated or out of order within a shard");
        history_last_ids_[shard] = value.tradeId;
        ++history_page_counts_[shard];
        ++history_total_;
        std::cout << "HISTORY_TRADE requestId=" << h.requestId << " shard=" << shard
                  << " tradeId=" << value.tradeId << " symbol=" << fixed_string(value.symbol)
                  << " price=" << value.price << " quantity=" << value.quantity
                  << " buyOrderId=" << value.buyOrderId << " sellOrderId=" << value.sellOrderId
                  << " buyer=" << fixed_string(value.buyer)
                  << " seller=" << fixed_string(value.seller)
                  << " exchangeTimeNs=" << value.timestamp << " receivedAtNs=" << received_at
                  << '\n';
        break;
      }
      case MessageType::TradeHistoryEnd: {
        if (!history_ || h.requestId != waiting_)
          throw std::runtime_error("Unexpected history marker");
        const auto end = copy_record<TradeHistoryEnd>(bytes);
        const std::uint16_t shard_id = end.shard;
        if (end.shard == 0) {
          if (!history_fanout_ || end.flags != history_all_shards)
            throw std::runtime_error("Invalid history all-shards marker");
          complete = true;
        } else {
          if (end.count != history_page_counts_[shard_id] || end.flags & ~history_more)
            throw std::runtime_error("History page count/flags mismatch");
          history_page_counts_[shard_id] = 0;
          if (end.flags & history_more)
            history_pages_.push_back(end);
          std::cout << "HISTORY_PAGE_END shard=" << end.shard << " count=" << end.count
                    << " more=" << ((end.flags & history_more) != 0)
                    << " nextOffset=" << end.nextOffset << " throughOffset=" << end.throughOffset
                    << '\n';
          complete = !history_fanout_;
        }
        break;
      }
      default:
        throw std::runtime_error("Unexpected server message");
    }
    if (complete && h.requestId == waiting_) {
      waiting_ = 0;
      if (snapshot_) {
        snapshot_ = false;
        std::cout << "READY (type help for commands)\n";
        help();
      }
      if (history_) {
        history_fanout_ = false;
        if (history_pages_.empty()) {
          history_ = false;
          std::cout << "HISTORY_COMPLETE total=" << history_total_ << " user=" << options_.user
                    << '\n';
        } else {
          const auto page = history_pages_.front();
          history_pages_.pop_front();
          TradeHistoryRequest query{};
          query.shard = page.shard;
          query.limit = trade_history_page_limit;
          query.afterOffset = page.nextOffset;
          query.throughOffset = page.throughOffset;
          send(query, MessageType::TradeHistoryRequest);
        }
      }
    }
  }
};

volatile std::sig_atomic_t Console::interrupted_ = 0;

void interrupted(int) {
  Console::interrupted_ = 1;
}

std::string login_input(const char* prompt, bool hidden) {
  struct EchoGuard {
    termios saved{};
    bool changed = false;

    explicit EchoGuard(bool hide) {
      if (!hide)
        return;
      if (tcgetattr(STDIN_FILENO, &saved) < 0)
        throw std::runtime_error("Cannot read terminal settings");
      auto quiet = saved;
      quiet.c_lflag &= static_cast<tcflag_t>(~(ECHO | ECHONL));
      if (tcsetattr(STDIN_FILENO, TCSANOW, &quiet) < 0)
        throw std::runtime_error("Cannot hide password input");
      changed = true;
    }

    ~EchoGuard() {
      if (changed) {
        (void)tcsetattr(STDIN_FILENO, TCSANOW, &saved);
        std::cout << '\n';
      }
    }
  } echo(hidden);

  std::cout << prompt;
  std::string value;
  for (;;) {
    if (Console::interrupted_)
      throw std::runtime_error("Login interrupted");
    pollfd input{STDIN_FILENO, POLLIN, 0};
    const auto ready = poll(&input, 1, 100);
    if (ready < 0 && errno == EINTR)
      continue;
    if (ready < 0)
      throw std::runtime_error("Login terminal poll failed");
    if (!ready)
      continue;
    char byte{};
    const auto n = read(STDIN_FILENO, &byte, 1);
    if (n < 0 && errno == EINTR)
      continue;
    if (n <= 0)
      throw std::runtime_error("Login input closed");
    if (byte == '\n')
      break;
    // Keep memory bounded while draining an overlong terminal line.
    if (value.size() < 32)
      value.push_back(byte);
  }
  if (value.empty() || value.size() > 31)
    throw std::runtime_error("Username and password must contain 1..31 bytes");
  return value;
}

void login_screen(Options& options) {
  if (!options.user.empty() && !options.password.empty())
    return;
  if (!isatty(STDIN_FILENO))
    throw std::runtime_error(
        "Interactive login requires a terminal; automation can supply --user and --password");
  std::cout << "\n========== MarketLab Trading Login ==========\n"
            << "Gateway: " << options.host << ':' << options.port << '\n';
  if (options.user.empty())
    options.user = login_input("Username: ", false);
  else
    std::cout << "Username: " << options.user << '\n';
  if (options.password.empty())
    options.password = login_input("Password: ", true);
  std::cout << "Connecting and logging in...\n";
}
}  // namespace

int main(int argc, char** argv) {
  std::signal(SIGINT, interrupted);
  std::signal(SIGTERM, interrupted);
  std::signal(SIGPIPE, SIG_IGN);
  std::cout << std::unitbuf;
  try {
    Options options;
    for (int i = 1; i < argc; ++i) {
      const std::string flag = argv[i];
      if (flag == "--help") {
        std::cout
            << "gateway_client [--user NAME] [--password PASSWORD] [--host 127.0.0.1] [--port "
            << default_gateway_port << "]\n";
        std::cout << "Missing credentials are prompted in a terminal; password input is hidden.\n";
        help();
        return 0;
      }
      if (++i == argc)
        throw std::runtime_error("Missing option value");
      const std::string value = argv[i];
      if (flag == "--host")
        options.host = value;
      else if (flag == "--port")
        options.port = number<std::uint16_t>(value);
      else if (flag == "--user")
        options.user = value;
      else if (flag == "--password")
        options.password = value;
      else
        throw std::runtime_error("Unknown option: " + flag);
    }
    if (!options.port)
      throw std::runtime_error("Port must be in 1..65535");
    login_screen(options);
    Console client(std::move(options));
    return client.run();
  } catch (const std::exception& e) {
    std::cerr << "ERROR " << e.what() << '\n';
    return 1;
  }
}
