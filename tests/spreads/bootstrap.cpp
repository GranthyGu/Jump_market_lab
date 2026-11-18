#include "fixtures.hpp"
#include "network.hpp"
#include "matching_engine/cluster/router_spreads.hpp"
#include "matching_engine/runtime/spread_worker.hpp"

using namespace spread_test;
namespace s = o::spreads;

namespace {
template <class F>
void pump(o::TcpReactor& a, o::TcpReactor& b, F complete) {
  const auto deadline = demo::Clock::now() + std::chrono::seconds(3);
  while (!complete()) {
    require(demo::Clock::now() < deadline, "Bootstrap exchange timed out");
    a.step(1);
    b.step(1);
  }
}

void worker_pause(const std::filesystem::path& root) {
  auto c = config();
  demo::Ports ports;
  for (unsigned shard = 5; shard <= 8; ++shard)
    c.spread_peers.push_back({std::uint16_t(shard), {"127.0.0.1", ports.take()}});
  ports.release();
  p::DurableEngine durable(c, 5, options(root));
  o::TcpReactor worker_io, control_io;
  s::Worker worker(
      durable,
      c,
      5,
      worker_io,
      [](const auto&, auto, auto) {
        return true;
      },
      [](auto, const auto&, auto) {
      },
      [] {
        return true;
      });
  constexpr std::uint64_t epoch = 123;
  s::Links control(
      control_io, c, {o::group::Role::Coordinator, 0, epoch, s::Topology(c).fingerprint});
  worker_io.on_accept = [&](auto id) {
    worker.incoming(id);
  };
  worker_io.on_frame = [&](auto id, auto bytes) {
    worker.receive(id, bytes);
  };
  worker_io.on_close = [&](auto id) {
    worker.lost(id);
  };
  control_io.on_connect = [&](auto id) {
    control.connected(id);
  };
  control_io.on_frame = [&](auto id, auto bytes) {
    control.receive(id, bytes);
  };
  control_io.on_close = [&](auto id) {
    control.lost(id);
  };
  bool recovered = false;
  control.on_message = [&](auto, const auto& message) {
    require(message.header.kind == s::MessageKind::Recovered && message.header.epoch == epoch &&
                message.header.seq == 1,
            "Worker emitted an unbound or unexpected message");
    p::Reader reader{message.body};
    require(s::read_recovery(reader).boundary.seq == 0, "Fresh worker boundary changed");
    reader.finish();
    recovered = true;
  };
  const auto id = control_io.connect(c.spread_peers[0].address, o::Framing::Group);
  control.outgoing(id, o::group::Role::Peer, 5);
  pump(worker_io, control_io, [&] {
    return control.ready(id);
  });
  worker.business_lost();
  require(control.send(id, {s::MessageKind::Pause, epoch}) &&
              control.send(id, {s::MessageKind::Recovered, epoch, 1}),
          "Cannot send deterministic pre-join pause/recovery");
  pump(worker_io, control_io, [&] {
    return recovered;
  });
  require(control.send(id, {s::MessageKind::Pause, epoch + 1}), "Cannot send stale epoch");
  pump(worker_io, control_io, [&] {
    return !control.ready(id);
  });
}

void router_pause() {
  auto c = config();
  demo::Ports ports;
  c.coordinator = {"127.0.0.1", ports.take()};
  ports.release();
  o::TcpReactor router_io, control_io;
  constexpr std::uint64_t epoch = 456;
  s::Links control(
      control_io, c, {o::group::Role::Coordinator, 0, epoch, s::Topology(c).fingerprint});
  unsigned recovered = 0, premature = 0;
  s::RouterGroup router(c,
                        router_io,
                        {[&](auto, const auto&, auto header, auto body) {
                           const auto message = s::parse_frame(header);
                           require(body.empty() &&
                                       message.header.kind == s::MessageKind::Recovered &&
                                       message.header.epoch == epoch && message.header.seq == 1,
                                   "Router recovery request changed");
                           ++recovered;
                           return true;
                         },
                         [](auto, const auto&) {
                         },
                         [](auto, auto) {
                         }});
  router_io.on_connect = [&](auto id) {
    router.connected(id);
  };
  router_io.on_frame = [&](auto id, auto bytes) {
    router.receive(id, bytes);
  };
  router_io.on_close = [&](auto id) {
    router.lost(id);
  };
  o::ConnectionId connection = 0;
  control_io.on_accept = [&](auto id) {
    control.incoming(id);
  };
  control_io.on_frame = [&](auto id, auto bytes) {
    control.receive(id, bytes);
  };
  control_io.on_close = [&](auto id) {
    control.lost(id);
  };
  control.on_ready = [&](auto id, const auto&) {
    connection = id;
  };
  control.on_message = [&](auto, const auto& message) {
    if (!message.header.epoch)
      ++premature;
  };
  control_io.listen(c.coordinator, o::Framing::Group, 4);
  router.poll();
  pump(router_io, control_io, [&] {
    return control.ready(connection);
  });
  // Deliver Welcome before simulating a business failure on the new link.
  control_io.step(1);
  router_io.step(1);
  router.worker_lost();
  require(control.send(connection, {s::MessageKind::Pause, epoch}) &&
              control.send(connection, {s::MessageKind::Recovered, epoch, 1}),
          "Cannot send Router pre-join pause/recovery");
  pump(router_io, control_io, [&] {
    return recovered == 4;
  });
  require(!premature, "Router emitted epoch zero before initial recovery");
  require(control.send(connection, {s::MessageKind::Pause, epoch + 1}), "Cannot send stale epoch");
  pump(router_io, control_io, [&] {
    return !control.ready(connection);
  });
}
}  // namespace

int main(int argc, char** argv) {
  demo::Suite suite(argc > 1 ? argv[1] : "bootstrap-results");
  suite.test("worker-pre-join-pause-then-recovery-retains-epoch-fencing", [&] {
    worker_pause(suite.output / std::to_string(o::unix_ns()));
  });
  suite.test("router-pre-join-pause-and-no-zero-epoch-send", router_pause);
  return suite.finish();
}
