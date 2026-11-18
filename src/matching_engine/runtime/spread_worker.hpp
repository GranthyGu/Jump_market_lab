#pragma once
#include "matching_engine/spreads/transport/join.hpp"
#include "matching_engine/spreads/transport/links.hpp"
#include "matching_engine/market_data/spread_market.hpp"
#include "matching_engine/spreads/session.hpp"

namespace marketlab::orders::spreads {
class Worker final : public EngineBackend {
 public:
  using Business = std::function<bool(
      const LinkHeader&, std::span<const std::uint8_t>, std::span<const std::uint8_t>)>;
  Worker(persistence::DurableEngine&,
         const OrderConfig&,
         std::uint16_t,
         TcpReactor&,
         Business,
         Emit,
         std::function<bool()> active);

  bool contains(ConnectionId id) const {
    return links_.contains(id);
  }

  void incoming(ConnectionId id) {
    links_.incoming(id);
  }

  void connected(ConnectionId id) {
    links_.connected(id);
  }

  void lost(ConnectionId id) {
    links_.lost(id);
  }

  void receive(ConnectionId id, std::span<const std::uint8_t> bytes) {
    links_.receive(id, bytes);
  }

  void business(const LinkHeader&, std::span<const std::uint8_t>);
  void business_lost();
  void poll() override;

  void tick() {
    links_.tick();
  }

  void fence();

  bool unresolved() const {
    return session_.active();
  }

  bool market_ready() const {
    return market_ready_.load();
  }

  void command(const LinkHeader& h, std::span<const std::uint8_t> bytes, const Emit&) override {
    business(h, bytes);
  }

  std::size_t symbol_count() const noexcept override {
    return durable_.symbol_count();
  }

  void copy_market(MarketSnapshot& out) noexcept override {
    market_.copy(out);
  }

 private:
  struct Deferred {
    std::uint8_t peer = no_slot;
    Header header;
    LinkHeader context{};
    Bytes body;
  };

  persistence::DurableEngine& durable_;
  const OrderConfig& config_;
  Topology topology_;
  std::uint16_t shard_;
  std::uint8_t member_;
  TcpReactor& io_;
  Business business_;
  Emit emit_;
  std::function<bool()> active_;
  const std::uint64_t instance_;
  Links links_;
  MarketPublication market_;
  Session session_;
  Directory directory_{};
  ConnectionId coordinator_ = 0;
  std::array<ConnectionId, 4> peers_{};
  std::array<Clock::time_point, 4> reconnect_{};
  std::uint64_t epoch_ = 0, join_ = 0;
  bool topology_ready_ = false, reported_peers_ = false, ready_ = false, view_ready_ = false,
       source_sent_ = false, failed_ = false, changed_ = false;
  std::atomic_bool market_ready_{false};
  std::optional<Header> control_recovery_, business_recovery_, initial_permit_;
  std::optional<Deferred> deferred_;
  Clock::time_point progress_ = Clock::now();
  Bytes scratch_, results_;
  void pause(std::string_view, bool notify = true);
  void control(const FrameView&);
  void peer(std::uint8_t, const FrameView&);
  void send_control(const Header&, std::span<const std::uint8_t> = {});
  void send_peer(std::uint8_t, const Header&, std::span<const std::uint8_t>);
  void send_business(const Header&, std::span<const std::uint8_t>, const LinkHeader& context = {});
  bool apply(std::uint8_t, const Header&, const LinkHeader&, std::span<const std::uint8_t>);
  void defer(std::uint8_t, const Header&, const LinkHeader&, std::span<const std::uint8_t>);
  void join_progress();
  void recover();
};
}  // namespace marketlab::orders::spreads
