#include "deployment.hpp"
#include "latency_probe.hpp"
#include <limits>

using namespace spread_test;
namespace latency = m::testing::latency;

namespace {
struct Product {
  const char* symbol;
  m::Price price;
  unsigned shard;
  m::Quantity quantity = 1;
};

const std::array<Product, 4> ordinary{
    {{"ITEM1", 100, 1}, {"ITEM2", 100, 2}, {"ITEM3", 100, 3}, {"ITEM4", 100, 4}}};
const std::array<Product, 4> direct{
    {{"SP_AB", 10, 5}, {"A", 120, 6}, {"B", 111, 7}, {"C", 106, 8}}};
const std::array<Product, 2> implied{{{"SP_AB", 10, 5}, {"SP_AC", 15, 5}}};

Product product(const std::string& scenario, unsigned index) {
  if (scenario == "ordinary-hot")
    return ordinary[0];
  if (scenario == "ordinary-balanced")
    return ordinary[index % ordinary.size()];
  if (scenario == "group-direct")
    return direct[index % direct.size()];
  if (scenario == "group-implied")
    return implied[index % implied.size()];
  if (scenario == "group-butterfly-in")
    return {"BF_ABC", 5, 5};
  if (scenario == "group-butterfly-out" || scenario == "group-shared")
    return {"A", 120, 6};
  if (scenario == "group-butterfly-b")
    return {"B", 111, 7, 2};
  throw std::invalid_argument("Unknown latency scenario: " + scenario);
}

void seed(Deployment& deployment, const std::string& scenario, unsigned quantity) {
  demo::Trader maker(deployment.gateway);
  require(maker.login("Bob").empty(), "Maker must start empty");
  unsigned request = 10;
  if (scenario == "group-implied") {
    maker.add(++request, "A", m::Side::Sell, 120, quantity);
    maker.add(++request, "B", m::Side::Buy, 110, quantity);
    maker.add(++request, "C", m::Side::Buy, 105, quantity);
  } else if (scenario == "group-butterfly-in") {
    maker.add(++request, "A", m::Side::Sell, 120, quantity);
    maker.add(++request, "B", m::Side::Buy, 110, 2 * quantity);
    maker.add(++request, "C", m::Side::Sell, 105, quantity);
  } else if (scenario == "group-butterfly-out" || scenario == "group-shared") {
    maker.add(++request, "B", m::Side::Sell, 110, 2 * quantity);
    maker.add(++request, "C", m::Side::Buy, 105, quantity);
    maker.add(++request, "BF_ABC", m::Side::Sell, 5, quantity);
    if (scenario == "group-shared")
      maker.add(++request, "SP_AB", m::Side::Sell, 10, quantity / 2);
  } else if (scenario == "group-butterfly-b") {
    maker.add(++request, "A", m::Side::Sell, 120, quantity);
    maker.add(++request, "C", m::Side::Sell, 105, quantity);
    maker.add(++request, "BF_ABC", m::Side::Buy, 4, quantity);
  } else {
    const unsigned products = scenario == "ordinary-hot" ? 1 : 4;
    for (unsigned i = 0; i < products; ++i) {
      const auto p = product(scenario, i);
      maker.add(++request, p.symbol, m::Side::Sell, p.price, quantity);
    }
  }
  // Bob disconnects with resting liquidity; only Alice's complete results are timed.
}

struct Sample {
  m::OrderId order = 0;
  std::uint64_t sent = 0, done = 0;
  m::Quantity traded = 0, updated = 0;
  bool filled = false;
};

std::vector<Sample> measure(demo::Trader& client,
                            const std::string& scenario,
                            unsigned count,
                            unsigned window,
                            unsigned base,
                            unsigned order_capacity) {
  std::vector<Sample> samples(count);
  std::vector<m::NewOrderRequest> requests;
  requests.reserve(count);
  for (unsigned i = 0; i < count; ++i) {
    const auto p = product(scenario, i);
    requests.push_back(demo::order_record(base + i, p.symbol, m::Side::Buy, p.price, p.quantity));
  }
  constexpr auto missing = std::numeric_limits<unsigned>::max();
  std::array<std::vector<unsigned>, 9> orders;
  for (auto& index : orders)
    index.assign(order_capacity, missing);
  auto lookup = [&](m::OrderId id) -> unsigned& {
    const auto shard = id >> 48;
    const auto counter = id & ((1ULL << 48) - 1);
    require(shard > 0 && shard < orders.size() && counter < order_capacity,
            "Order ID exceeds benchmark lookup bounds");
    return orders[shard][counter];
  };
  unsigned sent = 0, completed = 0;
  while (completed < count) {
    while (sent < count && sent - completed < window) {
      samples[sent].sent = latency::now_ns();
      client.send(requests[sent]);
      ++sent;
    }
    const auto bytes = client.receive();
    const auto received = latency::now_ns();
    m::Header header{};
    std::memcpy(&header, bytes.data(), sizeof(header));
    unsigned index = missing;
    if (header.messageType == m::MessageType::OrderUpdate) {
      const auto update = o::copy_record<m::OrderUpdate>(bytes);
      const auto& state = update.order;
      if (state.status == m::OrderStatus::Accepted) {
        require(header.requestId >= base && header.requestId < base + sent,
                "Acceptance request is not in flight");
        index = unsigned(header.requestId - base);
        auto& slot = lookup(state.orderId);
        require(slot == missing && !samples[index].order &&
                    state.orderId >> 48 == product(scenario, index).shard &&
                    state.originalQuantity == product(scenario, index).quantity &&
                    state.remainingQuantity == product(scenario, index).quantity &&
                    state.filledQuantity == 0 && state.side == m::Side::Buy &&
                    o::fixed_string(state.symbol) == product(scenario, index).symbol,
                "Invalid or duplicate acceptance");
        samples[index].order = state.orderId;
        slot = index;
      } else {
        index = lookup(state.orderId);
        require(index < sent && !samples[index].filled && header.requestId == 0,
                "Unknown or already completed order update");
        const auto total = product(scenario, index).quantity;
        require(state.filledQuantity > samples[index].updated && state.filledQuantity <= total &&
                    state.filledQuantity + state.remainingQuantity == total &&
                    state.originalQuantity == total &&
                    state.status == (state.remainingQuantity ? m::OrderStatus::PartiallyFilled
                                                             : m::OrderStatus::Filled),
                "Order update lost quantity or completion state");
        samples[index].updated = state.filledQuantity;
        samples[index].filled = state.remainingQuantity == 0;
      }
    } else if (header.messageType == m::MessageType::Trade) {
      const auto trade = o::copy_record<m::Trade>(bytes);
      index = lookup(trade.orderId);
      require(index < sent && header.requestId == 0, "Uncorrelated trade");
      const auto p = product(scenario, index);
      const auto expected_price =
          scenario == "group-butterfly-b" ? 110 + samples[index].traded : p.price;
      require(samples[index].traded < p.quantity && trade.quantity == 1 &&
                  trade.price == expected_price && o::fixed_string(trade.symbol) == p.symbol,
              "Wrong trade identity, price, quantity or duplicate trade");
      samples[index].traded += trade.quantity;
    } else {
      throw std::runtime_error("Unexpected response/rejection during latency load: " +
                               std::to_string(unsigned(header.messageType)));
    }
    auto& sample = samples[index];
    if (sample.traded == product(scenario, index).quantity && sample.filled) {
      require(!sample.done, "Order completion counted twice");
      sample.done = received;
      ++completed;
    }
  }
  return samples;
}

unsigned number(const char* text) {
  std::size_t used = 0;
  const auto value = std::stoul(text, &used);
  require(used == std::strlen(text) && value > 0 && value <= 30000, "Counts must be in [1, 30000]");
  return unsigned(value);
}
}  // namespace

int main(int argc, char** argv) {
  try {
    require(argc == 7, "Usage: spread_latency_tests BIN OUTPUT SCENARIO COUNT WINDOW WARMUP");
    const auto root = std::filesystem::absolute(argv[2]);
    const std::string scenario = argv[3];
    const unsigned count = number(argv[4]), window = number(argv[5]), warmup = number(argv[6]);
    timespec resolution{};
    require(clock_getres(CLOCK_MONOTONIC_RAW, &resolution) == 0, "Cannot read clock resolution");
    require(window <= count && count + warmup <= 60000, "Invalid load window/count");
    (void)product(scenario, 0);
    require(!std::filesystem::exists(root / "probes") &&
                !std::filesystem::exists(root / "exchange.conf"),
            "Use a fresh output directory for each deployment");
    std::filesystem::create_directories(root / "probes");
    require(setenv("MARKETLAB_LATENCY_PROBE_DIR", (root / "probes").c_str(), 1) == 0,
            "Cannot set latency probe output");
    std::vector<Sample> samples;
    constexpr unsigned base = 100000;
    {
      Deployment deployment(argv[1], root, false);
      seed(deployment, scenario, count + warmup + 1);
      const auto capacity = count + warmup + 16;
      (void)measure(*deployment.client, scenario, warmup, std::min(window, warmup), 1000, capacity);
      samples = measure(*deployment.client, scenario, count, window, base, capacity);
    }  // Graceful shutdown flushes test probes after the timed interval.
    std::ofstream raw(root / "client.csv");
    raw << "request,order,shard,send_ns,done_ns\n";
    std::uint64_t last = 0;
    for (unsigned i = 0; i < count; ++i) {
      const auto& s = samples[i];
      require(s.order && s.traded == product(scenario, i).quantity && s.filled && s.done > s.sent,
              "Incomplete client sample");
      last = std::max(last, s.done);
      raw << base + i << ',' << s.order << ',' << (s.order >> 48) << ',' << s.sent << ',' << s.done
          << '\n';
    }
    raw.close();
    require(bool(raw), "Cannot write client samples");
    const double seconds = (last - samples.front().sent) / 1e9;
    std::ofstream metadata(root / "run.json");
    metadata << std::setprecision(12) << "{\"scenario\":" << demo::json(scenario)
             << ",\"count\":" << count << ",\"window\":" << window << ",\"warmup\":" << warmup
             << ",\"order_quantity\":" << product(scenario, 0).quantity
             << ",\"calendar_orders\":" << (count + warmup + 1) / 2 << ",\"seconds\":" << seconds
             << ",\"clock\":\"CLOCK_MONOTONIC_RAW\",\"clock_resolution_ns\":"
             << std::uint64_t(resolution.tv_sec) * 1'000'000'000ULL + resolution.tv_nsec << "}\n";
    metadata.close();
    require(bool(metadata), "Cannot write run metadata");
    std::cout << "PASS " << scenario << " orders=" << count << " window=" << window
              << " seconds=" << seconds << " orders/s=" << count / seconds << '\n';
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "FAIL latency benchmark: " << error.what() << '\n';
    return 1;
  }
}
