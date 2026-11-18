#include "shared_data/runtime/io.hpp"
#include "shared_data/orders/engine_link.hpp"
#include "shared_data/orders/config.hpp"
#include "shared_data/console.hpp"

#include <arpa/inet.h>
#include <poll.h>
#include <sys/socket.h>
#include <termios.h>
#include <unistd.h>
#include <algorithm>
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

template <typename T>
T number(const std::string& text) {
  T value{};
  const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), value);
  if (text.empty() || error != std::errc{} || end != text.data() + text.size())
    throw std::runtime_error("Invalid integer: " + text);
  return value;
}

console::Label status(OrderStatus value) {
  switch (value) {
    case OrderStatus::Accepted:
      return {"ACCEPTED", console::green};
    case OrderStatus::PartiallyFilled:
      return {"PARTIAL", console::yellow};
    case OrderStatus::Filled:
      return {"FILLED", console::green};
    case OrderStatus::Canceled:
      return {"CANCELED", console::yellow};
  }
  return {"UNKNOWN", console::red};
}

void print_order(const OrderState& order) {
  std::cout << "  " << (order.side == Side::Buy ? "BUY " : "SELL ") << fixed_string(order.symbol)
            << "  qty=" << order.originalQuantity << "  price=" << order.price
            << "  filled=" << order.filledQuantity << "  remaining=" << order.remainingQuantity
            << "  order=" << order.orderId;
}

void print_time(Timestamp timestamp) {
  std::cout << console::dim << '[' << console::time(timestamp) << "] " << console::reset;
}

const char* rejection(RejectReason reason) {
  switch (reason) {
    case RejectReason::InvalidCredentials:
      return "Invalid username or password";
    case RejectReason::InvalidOrder:
      return "Invalid order";
    case RejectReason::CancelFailed:
      return "Order could not be canceled";
    case RejectReason::InvalidHistoryQuery:
      return "Trade history query is invalid; run trades again";
    case RejectReason::HistoryUnavailable:
      return "Trade history is temporarily unavailable; try again later";
    case RejectReason::HistoryBusy:
      return "Trade history is busy; try again later";
  }
  return "Request rejected";
}

void help() {
  std::cout
      << "\nCommands (one command per line):\n"
      << "  buy SYMBOL PRICE QUANTITY  : place a buy limit order, e.g. buy AAPL 100 10\n"
      << "  sell SYMBOL PRICE QUANTITY : place a sell limit order; prices/quantities are integers\n"
      << "  cancel ORDER_ID            : cancel your remaining open quantity using the displayed order ID\n"
      << "  cancel-last                : cancel the last order accepted during this client session\n"
      << "  trades                     : list all your trades: time, side, symbol, quantity, price and trade ID\n"
      << "  relogin                    : authenticate again and refresh your open-order snapshot\n"
      << "  help                       : display these instructions\n"
      << "  quit                       : disconnect; open orders remain on the exchange\n"
      << "Trade times use your local timezone. Run trades again to include newer fills.\n"
      << "Colors: " << console::Label{"ACCEPTED / READY", console::green} << "  "
      << console::Label{"TRADE", console::magenta} << "  "
      << console::Label{"CANCELED / NOTICE", console::yellow} << "  "
      << console::Label{"ERROR / DISCONNECTED", console::red} << "\n\n";
}

struct Options {
  std::string host = "127.0.0.1", user, password;
  std::uint16_t port = default_gateway_port;
};

class ConnectionFailure : public std::runtime_error {
 public:
  using std::runtime_error::runtime_error;
};

class Console {
 public:
  explicit Console(Options options)
      : options_(std::move(options)) {
  }

  int run() {
    while (!interrupted_) {
      if (recovering_ && runtime::Clock::now() >= reconnect_deadline_)
        throw std::runtime_error("RECONNECT_TIMEOUT: session unavailable for 20s; exiting");
      try {
        if (!fd_ && runtime::Clock::now() >= retry_at_)
          connect();
        if (waiting_ && runtime::Clock::now() >= deadline_)
          throw ConnectionFailure("Response timeout");
        if (!step())
          return 0;
      } catch (const ConnectionFailure& error) {
        disconnected(error.what());
      }
    }
    return 0;
  }

  static volatile std::sig_atomic_t interrupted_;

 private:
  bool step() {
    if (!waiting_ || recovering_) {
      const auto newline = stdin_buffer_.find('\n');
      if (newline != std::string::npos) {
        const auto line = stdin_buffer_.substr(0, newline);
        stdin_buffer_.erase(0, newline + 1);
        if (!command(line))
          return false;
        return true;
      }
      if (stdin_closed_ && session_ready_) {
        if (!stdin_buffer_.empty()) {
          auto line = std::exchange(stdin_buffer_, {});
          if (!command(line))
            return false;
        } else
          return false;
        return true;
      }
    }
    pollfd ready[]{{fd_.get(), POLLIN, 0}, {STDIN_FILENO, POLLIN, 0}};
    // Disable stdin entirely while waiting; a closed pipe otherwise reports
    // POLLHUP continuously even when no input events were requested.
    if (stdin_closed_ || (waiting_ && !recovering_))
      ready[1].fd = -1;
    const int count = poll(ready, 2, 100);
    if (count < 0 && errno == EINTR)
      return true;
    if (count < 0)
      throw std::runtime_error("poll failed");
    if (ready[0].revents & (POLLIN | POLLHUP | POLLERR)) {
      if (!receive()) {
        throw ConnectionFailure("Gateway closed TCP");
      }
    }
    if (ready[1].fd >= 0 && ready[1].revents & (POLLIN | POLLHUP)) {
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
    return true;
  }

  Options options_;
  runtime::Fd fd_;
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
  runtime::Clock::time_point deadline_;
  static constexpr auto tolerance = std::chrono::seconds(20);
  static constexpr auto retry_interval = std::chrono::milliseconds(200);
  bool recovering_ = false, session_ready_ = false, recovered_last_order_ = false;
  std::size_t recovered_orders_ = 0;
  runtime::Clock::time_point reconnect_deadline_, retry_at_{};

  void disconnected(const std::string& reason) {
    if (!recovering_) {
      recovering_ = true;
      reconnect_deadline_ = runtime::Clock::now() + tolerance;
      std::cout << console::red << "DISCONNECTED: " << reason
                << "; retrying every 200ms for up to 20s";
      if (waiting_ && !snapshot_) {
        if (history_)
          std::cout << "; history incomplete, partial_count=" << history_total_;
        else
          std::cout << "; requestId=" << waiting_ << " outcome unknown, not replayed";
      }
      std::cout << console::reset << '\n';
    }
    fd_.reset();
    input_.clear();
    waiting_ = 0;
    session_ready_ = snapshot_ = history_ = history_fanout_ = false;
    history_pages_.clear();
    history_page_counts_.clear();
    history_last_ids_.clear();
    retry_at_ = runtime::Clock::now() + retry_interval;
  }

  void connect() {
    auto until = runtime::Clock::now() + std::chrono::milliseconds(500);
    if (recovering_)
      until = std::min(until, reconnect_deadline_);
    try {
      fd_ = runtime::connect_tcp(options_.host, options_.port, until);
    } catch (const std::exception& error) {
      throw ConnectionFailure(error.what());
    }
    if (!recovering_) {
      std::cout << console::Label{"CONNECTED", console::cyan} << "  " << options_.user << " @ "
                << options_.host << ':' << options_.port << '\n';
    }
    recovered_orders_ = 0;
    recovered_last_order_ = false;
    login();
  }

  template <typename T>
  void send(T value, MessageType type) {
    if (next_ == std::numeric_limits<RequestId>::max())
      throw std::runtime_error("Request IDs exhausted");
    value.header = {type, sizeof(T), next_++};
    waiting_ = value.header.requestId;
    deadline_ = runtime::Clock::now() + std::chrono::seconds(35);
    if (recovering_)
      deadline_ = std::min(reconnect_deadline_, runtime::Clock::now() + std::chrono::seconds(3));
    try {
      runtime::send_all(fd_.get(), record_bytes(value), deadline_);
    } catch (const std::exception& error) {
      throw ConnectionFailure(error.what());
    }
  }

  void login() {
    LoginRequest value{};
    put_string(value.username, options_.user);
    put_string(value.password, options_.password);
    snapshot_ = true;
    session_ready_ = false;
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
      if (!session_ready_) {
        std::cout << console::Label{"NOT_READY", console::yellow}
                  << ": reconnecting; no request sent. Use help or quit.\n";
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
        std::cout
            << '\n'
            << console::Label{"TRADE HISTORY", console::cyan} << " — " << options_.user << '\n'
            << console::dim
            << "        No.  Time (local)              Side  Symbol             Qty       Price  Trade ID\n"
            << console::reset;
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
      std::cout << console::Label{"INPUT_ERROR", console::red} << " " << e.what() << '\n';
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
      throw ConnectionFailure("TCP receive failed");
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
    switch (h.messageType) {
      case MessageType::LoginAccepted:
        break;
      case MessageType::ActiveOrderSnapshot: {
        const auto value = copy_record<ActiveOrderSnapshot>(bytes);
        if (value.isEnd) {
          complete = true;
        } else if (recovering_) {
          ++recovered_orders_;
          recovered_last_order_ = recovered_last_order_ || value.order.orderId == last_order_;
        } else {
          std::cout << console::Label{"OPEN ORDER", console::cyan};
          print_order(value.order);
          std::cout << '\n';
        }
        break;
      }
      case MessageType::OrderUpdate: {
        const auto value = copy_record<OrderUpdate>(bytes);
        print_time(exchange_time);
        std::cout << status(value.order.status);
        print_order(value.order);
        std::cout << '\n';
        if (value.order.status == OrderStatus::Accepted)
          last_order_ = value.order.orderId;
        complete = h.requestId != 0;
        break;
      }
      case MessageType::Trade: {
        const auto value = copy_record<Trade>(bytes);
        print_time(exchange_time);
        std::cout << console::Label{"TRADE", console::magenta} << "  "
                  << (value.side == Side::Buy ? "BUY " : "SELL ") << fixed_string(value.symbol)
                  << "  qty=" << value.quantity << "  price=" << value.price
                  << "  trade=" << value.tradeId << "  order=" << value.orderId << '\n';
        break;
      }
      case MessageType::RequestRejected: {
        const auto reason = copy_record<RequestRejected>(bytes).reason;
        print_time(exchange_time);
        std::cout << console::Label{"REJECTED", console::red} << "  " << rejection(reason) << '\n';
        if (snapshot_)
          throw std::runtime_error("Login rejected");
        if (history_ && h.requestId == waiting_) {
          std::cout << console::Label{"INCOMPLETE", console::red} << "  " << history_total_
                    << " trades shown; query failed. Run trades again.\n\n";
          history_ = false;
          history_pages_.clear();
        }
        complete = true;
        break;
      }
      case MessageType::ImpliedLegHistory:
      case MessageType::TradeHistoryRecord: {
        if (!history_ || h.requestId != waiting_)
          throw std::runtime_error("Unexpected history record");
        const bool implied = h.messageType == MessageType::ImpliedLegHistory;
        const auto value = implied ? copy_record<ImpliedLegHistory>(bytes).trade
                                   : copy_record<TradeHistoryRecord>(bytes);
        const auto shard = static_cast<std::uint16_t>(value.tradeId >> 48);
        if (value.tradeId <= history_last_ids_[shard])
          throw std::runtime_error("History trade IDs repeated or out of order within a shard");
        history_last_ids_[shard] = value.tradeId;
        ++history_page_counts_[shard];
        ++history_total_;
        const bool buying = fixed_string(value.buyer) == options_.user;
        const bool selling = fixed_string(value.seller) == options_.user;
        if (!buying && !selling)
          throw std::runtime_error("Unexpected trade history owner");
        std::ostringstream row;
        row << std::right << std::setw(5) << history_total_ << "  "
            << console::time(value.timestamp, true) << "  " << std::left << std::setw(4)
            << (buying && selling ? "SELF"
                : buying          ? "BUY"
                                  : "SELL")
            << "  " << std::setw(12) << fixed_string(value.symbol) << "  " << std::right
            << std::setw(8) << value.quantity << "  " << std::setw(10) << value.price << "  "
            << value.tradeId;
        std::cout << console::Label{implied ? "LEG" : "TRADE", console::magenta} << ' '
                  << row.str();
        if (implied) {
          const auto leg = copy_record<ImpliedLegHistory>(bytes);
          std::cout << "  execution=" << leg.aggressorOrderId << ':' << leg.fillNo;
        }
        std::cout << '\n';
        break;
      }
      case MessageType::SpreadExecutionHistory:
      case MessageType::ButterflyExecutionHistory: {
        if (!history_ || h.requestId != waiting_)
          throw std::runtime_error("Unexpected spread history record");
        const bool butterfly = h.messageType == MessageType::ButterflyExecutionHistory;
        const auto value = butterfly ? copy_record<ButterflyExecutionHistory>(bytes).spread
                                     : copy_record<SpreadExecutionHistory>(bytes);
        const auto shard = static_cast<std::uint16_t>(value.tradeId >> 48);
        if (value.tradeId <= history_last_ids_[shard] || fixed_string(value.user) != options_.user)
          throw std::runtime_error("Spread history identity/order mismatch");
        history_last_ids_[shard] = value.tradeId;
        ++history_page_counts_[shard];
        ++history_total_;
        std::cout << console::Label{butterfly ? "BUTTERFLY" : "SPREAD", console::magenta} << ' '
                  << history_total_ << "  " << console::time(value.timestamp, true) << "  "
                  << (value.side == Side::Buy ? "BUY " : "SELL ") << fixed_string(value.symbol)
                  << "  qty=" << value.quantity << "  price=" << value.price
                  << "  trade=" << value.tradeId << "  execution=" << value.aggressorOrderId << ':'
                  << value.fillNo << "  A=" << fixed_string(value.a.user) << '/' << value.a.orderId
                  << '@' << value.a.price << (butterfly ? "  B1=" : "  X=")
                  << fixed_string(value.x.user) << '/' << value.x.orderId << '@' << value.x.price;
        if (butterfly) {
          const auto full = copy_record<ButterflyExecutionHistory>(bytes);
          std::cout << "  B2=" << fixed_string(full.bSecond.user) << '/' << full.bSecond.orderId
                    << '@' << full.bSecond.price << "  C=" << fixed_string(full.c.user) << '/'
                    << full.c.orderId << '@' << full.c.price;
        }
        std::cout << '\n';
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
        const bool recovered = recovering_;
        snapshot_ = false;
        session_ready_ = true;
        if (recovering_) {
          // Only a complete authenticated snapshot ends the outage window.
          // A successful TCP connect alone must never reset its deadline.
          if (runtime::Clock::now() >= reconnect_deadline_)
            throw std::runtime_error("RECONNECT_TIMEOUT: session unavailable for 20s; exiting");
          recovering_ = false;
          if (!recovered_last_order_)
            last_order_ = 0;
          std::cout << console::green
                    << "RECONNECTED: login and snapshot complete; open_orders=" << recovered_orders_
                    << ". Use relogin to list orders or trades to check fills." << console::reset
                    << '\n';
        }
        std::cout << console::green << "READY (type help for commands)" << console::reset << '\n';
        if (!recovered)
          std::cout
              << "Commands: buy | sell | cancel | cancel-last | trades | relogin | help | quit\n";
      }
      if (history_) {
        history_fanout_ = false;
        if (history_pages_.empty()) {
          history_ = false;
          std::cout << console::Label{"COMPLETE", console::green} << "  " << history_total_
                    << (history_total_ == 1 ? " trade" : " trades") << '\n';
          if (!history_total_)
            std::cout << "No trades yet.\n";
          std::cout << '\n';
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
        std::cout
            << "On disconnect, retry every 200ms for up to 20s, then exit if recovery fails.\n"
               "Recovery requires login and a complete snapshot. Sent orders are never replayed.\n";
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
    in_addr address{};
    if (inet_pton(AF_INET, options.host.c_str(), &address) != 1)
      throw std::runtime_error("Host must be a numeric IPv4 address");
    login_screen(options);
    Console client(std::move(options));
    return client.run();
  } catch (const std::exception& e) {
    std::cerr << console::red << "ERROR " << e.what() << console::reset << '\n';
    return 1;
  }
}
