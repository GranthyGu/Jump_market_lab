#include "gateway/protocol/engine_link.hpp"
#include "market_publisher/net/transport.hpp"

#include <arpa/inet.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>
#include <cerrno>
#include <charconv>
#include <csignal>
#include <iostream>
#include <limits>
#include <sstream>

namespace {
using namespace marketlab;
using namespace marketlab::orders;
namespace md = marketlab::md;

template <typename T> T number(const std::string& text) {
  T value{};
  const auto [end,error] = std::from_chars(text.data(),text.data() + text.size(),value);
  if (text.empty() || error != std::errc{} || end != text.data() + text.size())
    throw std::runtime_error("Invalid integer: " + text);
  return value;
}
const char* status(OrderStatus value) {
  switch (value) {
    case OrderStatus::Accepted: return "Accepted";
    case OrderStatus::PartiallyFilled: return "PartiallyFilled";
    case OrderStatus::Filled: return "Filled";
    case OrderStatus::Canceled: return "Canceled";
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
  std::cout << "Commands: buy SYMBOL PRICE QUANTITY | sell SYMBOL PRICE QUANTITY\n"
      << "          cancel ORDER_ID | cancel-last | relogin | help | quit\n";
}
struct Options {
  std::string host = "127.0.0.1", user, password;
  std::uint16_t port = 7000;
};
class Console {
 public:
  explicit Console(Options options) : options_(std::move(options)),
      fd_(md::connect_tcp(options_.host,options_.port,md::Clock::now() + std::chrono::seconds(3))) {
    sockaddr_in local{}; socklen_t size = sizeof(local);
    if (getsockname(fd_.get(),reinterpret_cast<sockaddr*>(&local),&size) < 0)
      throw std::runtime_error("getsockname failed");
    char address[INET_ADDRSTRLEN]{};
    if (!inet_ntop(AF_INET,&local.sin_addr,address,sizeof(address))) throw std::runtime_error("inet_ntop failed");
    std::cout << "CONNECTED local=" << address << ':' << ntohs(local.sin_port)
        << " gateway=" << options_.host << ':' << options_.port << " user=" << options_.user << '\n';
    login();
  }
  int run() {
    while (!interrupted_) {
      if (waiting_ && md::Clock::now() > deadline_) throw std::runtime_error("Response timeout");
      pollfd ready[]{{fd_.get(),POLLIN,0},{STDIN_FILENO,static_cast<short>(waiting_ ? 0 : POLLIN),0}};
      // Disable stdin entirely while waiting; a closed pipe otherwise reports
      // POLLHUP continuously even when no input events were requested.
      if (waiting_) ready[1].fd = -1;
      const int count = poll(ready,2,100);
      if (count < 0 && errno == EINTR) continue;
      if (count < 0) throw std::runtime_error("poll failed");
      if (ready[0].revents & (POLLIN | POLLHUP | POLLERR)) {
        if (!receive()) { std::cout << "DISCONNECTED: gateway closed TCP (another login may have replaced this session).\n"; return 0; }
      }
      if (!waiting_ && ready[1].revents & (POLLIN | POLLHUP)) {
        std::string line;
        if (!std::getline(std::cin,line)) return 0;
        if (!command(line)) return 0;
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
  md::Clock::time_point deadline_;

  template <typename T> void send(T value,MessageType type) {
    if (next_ == std::numeric_limits<RequestId>::max()) throw std::runtime_error("Request IDs exhausted");
    value.header = {type,sizeof(T),next_++};
    waiting_ = value.header.requestId;
    deadline_ = md::Clock::now() + std::chrono::seconds(10);
    md::send_all(fd_.get(),record_bytes(value),deadline_);
    std::cout << "SENT requestId=" << waiting_ << '\n';
  }
  void login() {
    LoginRequest value{};
    put_string(value.username,options_.user); put_string(value.password,options_.password);
    snapshot_ = true; send(value,MessageType::LoginRequest);
  }
  bool command(const std::string& line) {
    std::istringstream in(line);
    std::string op; in >> op;
    if (op.empty()) return true;
    try {
      std::vector<std::string> args;
      for (std::string arg; in >> arg;) args.push_back(std::move(arg));
      if (op == "quit" && args.empty()) return false;
      if (op == "help" && args.empty()) { help(); return true; }
      if (op == "relogin" && args.empty()) { login(); return true; }
      if ((op == "buy" || op == "sell") && args.size() == 3) {
        NewOrderRequest value{}; put_string(value.symbol,args[0]);
        value.side = op == "buy" ? Side::Buy : Side::Sell;
        value.price = number<Price>(args[1]); value.quantity = number<Quantity>(args[2]);
        send(value,MessageType::NewOrderRequest); return true;
      }
      if ((op == "cancel" && args.size() == 1) || (op == "cancel-last" && args.empty())) {
        const auto id = op == "cancel" ? number<OrderId>(args[0]) : last_order_;
        if (!id) throw std::runtime_error("No order ID; copy one from an order update or login snapshot");
        CancelRequest value{}; value.orderId = id; send(value,MessageType::CancelRequest); return true;
      }
      throw std::runtime_error("Unknown command or argument count; type help");
    } catch (const std::exception& e) {
      // Parse errors leave the connection usable; transport errors must stop it.
      if (waiting_) throw;
      std::cout << "INPUT_ERROR " << e.what() << '\n';
      return true;
    }
  }
  bool receive() {
    std::array<std::uint8_t,4096> buffer{};
    const auto n = recv(fd_.get(),buffer.data(),buffer.size(),0);
    if (!n) return false;
    if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) return true;
    if (n < 0) throw std::runtime_error("recv failed");
    input_.insert(input_.end(),buffer.begin(),buffer.begin() + n);
    std::size_t used = 0;
    while (input_.size() - used >= sizeof(Header)) {
      Header h{}; std::memcpy(&h,input_.data() + used,sizeof(h));
      if (h.messageLength < sizeof(Header) || h.messageLength > max_client_frame)
        throw std::runtime_error("Invalid server length");
      if (input_.size() - used < h.messageLength) break;
      const auto frame = std::span<const std::uint8_t>(input_).subspan(used,h.messageLength);
      if (!valid_client_record(frame,false)) throw std::runtime_error("Invalid server record");
      message(h,frame); used += h.messageLength;
    }
    input_.erase(input_.begin(),input_.begin() + static_cast<std::ptrdiff_t>(used));
    return true;
  }
  void message(const Header& h,std::span<const std::uint8_t> bytes) {
    bool complete = false;
    switch (h.messageType) {
      case MessageType::LoginAccepted:
        std::cout << "LOGIN_ACCEPTED requestId=" << h.requestId << '\n'; break;
      case MessageType::ActiveOrderSnapshot: {
        const auto value = copy_record<ActiveOrderSnapshot>(bytes);
        if (value.isEnd) { std::cout << "SNAPSHOT_END requestId=" << h.requestId << '\n'; complete = true; }
        else { std::cout << "SNAPSHOT requestId=" << h.requestId; print_order(value.order); std::cout << '\n'; }
        break;
      }
      case MessageType::OrderUpdate: {
        const auto value = copy_record<OrderUpdate>(bytes);
        std::cout << "ORDER_UPDATE requestId=" << h.requestId; print_order(value.order); std::cout << '\n';
        if (value.order.status == OrderStatus::Accepted) last_order_ = value.order.orderId;
        complete = h.requestId != 0; break;
      }
      case MessageType::Trade: {
        const auto value = copy_record<Trade>(bytes);
        std::cout << "TRADE requestId=" << h.requestId << " tradeId=" << value.tradeId
            << " orderId=" << value.orderId << " symbol=" << fixed_string(value.symbol)
            << " side=" << (value.side == Side::Buy ? "BUY" : "SELL")
            << " price=" << value.price << " quantity=" << value.quantity << '\n'; break;
      }
      case MessageType::RequestRejected: {
        const auto reason = copy_record<RequestRejected>(bytes).reason;
        std::cout << "REJECTED requestId=" << h.requestId << " reason=" << static_cast<unsigned>(reason) << '\n';
        if (snapshot_) throw std::runtime_error("Login rejected");
        complete = true; break;
      }
      default: throw std::runtime_error("Unexpected server message");
    }
    if (complete && h.requestId == waiting_) {
      waiting_ = 0;
      if (snapshot_) { snapshot_ = false; std::cout << "READY (type help for commands)\n"; }
    }
  }
};
volatile std::sig_atomic_t Console::interrupted_ = 0;
void interrupted(int) { Console::interrupted_ = 1; }
}  // namespace

int main(int argc,char** argv) {
  std::signal(SIGINT,interrupted); std::signal(SIGTERM,interrupted); std::signal(SIGPIPE,SIG_IGN);
  std::cout << std::unitbuf;
  try {
    Options options;
    for (int i = 1; i < argc; ++i) {
      const std::string flag = argv[i];
      if (flag == "--help") {
        std::cout << "gateway_client --user NAME --password PASSWORD [--host 127.0.0.1] [--port 7000]\n";
        help(); return 0;
      }
      if (++i == argc) throw std::runtime_error("Missing option value");
      const std::string value = argv[i];
      if (flag == "--host") options.host = value;
      else if (flag == "--port") options.port = number<std::uint16_t>(value);
      else if (flag == "--user") options.user = value;
      else if (flag == "--password") options.password = value;
      else throw std::runtime_error("Unknown option: " + flag);
    }
    if (options.user.empty() || options.password.empty() || !options.port)
      throw std::runtime_error("Supply --user and --password; use --help for commands");
    Console client(std::move(options)); return client.run();
  } catch (const std::exception& e) { std::cerr << "ERROR " << e.what() << '\n'; return 1; }
}
