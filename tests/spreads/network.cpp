#include "network.hpp"
#include "fixtures.hpp"
#include "matching_engine/spreads/transport/join.hpp"
#include "matching_engine/spreads/transport/links.hpp"
#include "shared_data/orders/engine_connections.hpp"

using namespace spread_test;
namespace s = o::spreads;

namespace {
void codec() {
  const auto c = config();
  const s::Topology topology(c);
  for (const auto& route : s::routes) {
    const auto owner = topology.owner(route.target);
    require(!(s::peer_mask(owner) & (1U << owner)), "Peer graph includes self");
    for (unsigned leg = 0; leg < route.legs(); ++leg) {
      const auto remote = topology.owner(route.source(leg) / 2);
      if (remote != owner)
        require((s::peer_mask(owner) & (1U << remote)) && (s::peer_mask(remote) & (1U << owner)),
                "Route cannot reach a real source owner");
    }
  }
  require(!s::peer_mask(4), "Invalid group member has peers");
  const s::ConnectionIdentity peer{o::group::Role::Peer, 5, 123, topology.fingerprint};
  auto bytes = s::identity_bytes(peer);
  p::Reader identity_reader{bytes};
  require(s::read_identity(identity_reader).shard == 5, "Group identity roundtrip failed");
  identity_reader.finish();
  bytes[0] ^= 1;
  rejects([&] {
    p::Reader reader{bytes};
    s::read_identity(reader);
  });
  s::Sources sources{};
  sources[1] = {4, {3, 4, 5}, {120, 121, 122}, 3, true};
  sources[6] = {7, {5, 6, 7}, {2, 1, 0}, 3, false};
  o::Bytes body;
  s::put_sources(body, (1U << 1) | (1U << 6), {}, sources, true);
  s::Sources received{};
  p::Reader reader{body};
  require(s::read_sources(reader, received, s::all_sources, true) == ((1U << 1) | (1U << 6)),
          "Source mask roundtrip");
  reader.finish();
  require(received[1].same_levels(sources[1]) && received[6].version == 7,
          "Source contents differ");
  auto unchanged = received;
  rejects([&] {
    p::Reader foreign{body};
    s::read_sources(foreign, received, 1U << 1, true);
  });
  require(received[6].version == unchanged[6].version,
          "Foreign multi-source packet installed partially");
  s::Views views{};
  s::recompute(sources, s::all_sources, views);
  body.clear();
  s::put_views(body, s::all_targets, views);
  s::Views installed{};
  p::Reader view_reader{body};
  require(
      s::read_views(view_reader, installed, s::all_targets) == s::all_targets && installed == views,
      "View replacement mismatch");
  view_reader.finish();
  auto packet = s::frame({s::MessageKind::ImpliedReplace, 12, 15, 2}, body);
  const auto parsed = s::parse_frame(packet);
  require(parsed.header.epoch == 12 && parsed.header.seq == 15 && parsed.header.round == 2 &&
              std::equal(body.begin(), body.end(), parsed.body.begin()),
          "Group correlation header changed");
  const auto enveloped = o::link_frame(o::LinkKind::Group, 0, 0, 0, packet);
  require(o::valid_link_record(enveloped), "Business envelope rejected valid group message");
  packet[5] = 128;
  require(!o::group::valid_frame(packet), "Unknown header flags accepted");
  rejects([&] {
    s::frame({s::MessageKind::ResultDetails}, o::Bytes(o::group::maximum_frame));
  });
}

template <class F>
void pump(o::TcpReactor& a, o::TcpReactor& b, F done) {
  const auto deadline = demo::Clock::now() + std::chrono::seconds(3);
  while (!done()) {
    require(demo::Clock::now() < deadline, "Reactor exchange timed out");
    a.step(1);
    b.step(1);
  }
}

void buffered_parts() {
  demo::Ports ports;
  const o::Address address{"127.0.0.1", ports.take()};
  ports.release();
  o::TcpReactor server, client(32);
  server.listen(address, o::Framing::Group, 1);
  const auto id = client.connect(address, o::Framing::Group);
  const auto peer = client.peer(id);
  o::Bytes prefix{1, 2, 3}, body{4, 5, 6, 7};
  const o::Bytes expected{1, 2, 3, 4, 5, 6, 7};
  require(client.send_parts(id, {prefix, {}, body}), "Multipart enqueue failed");
  std::fill(prefix.begin(), prefix.end(), 0);
  std::fill(body.begin(), body.end(), 0);
  require(peer->output == expected, "Queued frame borrowed reusable input buffers");
  const auto storage = peer->output.data();
  peer->output.clear();
  require(client.send_parts(id, {std::span(expected).first(3), std::span(expected).subspan(3)}) &&
              peer->output == expected && peer->output.data() == storage,
          "Cleared output capacity was not reused");
  // Model a partially flushed frame without relying on OS socket buffer sizes.
  peer->sent = 3;
  const o::Bytes tail(27, 0x5a);
  require(client.send_parts(id, {tail}), "Valid pending-byte limit rejected");
  o::Bytes pending(expected.begin() + 3, expected.end());
  pending.insert(pending.end(), tail.begin(), tail.end());
  require(peer->sent == 0 && peer->output == pending, "Compaction changed unsent byte order");
  const std::array<std::uint8_t, 1> byte{8};
  require(!client.send_parts(id, {byte, byte}) && !client.contains(id) && peer->output == pending,
          "Multipart overflow appended a partial message before rejecting it");
  rejects([&] {
    s::wire_header({s::MessageKind::SourceDone, 1, 1, 1, 128}, 0);
  });
  rejects([&] {
    s::wire_header({s::MessageKind::SourceDone}, o::group::maximum_frame);
  });
}

void roles_and_framing() {
  auto c = config();
  const s::Topology topology(c);
  demo::Ports ports;
  const o::Address business{"127.0.0.1", ports.take()}, group{"127.0.0.1", ports.take()};
  ports.release();
  o::TcpReactor server, client;
  o::EngineLinks server_business(server, c), client_business(client, c);
  s::Links server_group(server, c, {o::group::Role::Peer, 6, 600, topology.fingerprint});
  s::Links client_group(client, c, {o::group::Role::Peer, 5, 500, topology.fingerprint});
  server_group.allow = [](auto, const auto& remote) {
    return remote.role == o::group::Role::Peer && remote.shard == 5 && remote.instance == 500;
  };
  unsigned accepted_business = 0, accepted_group = 0, messages = 0;
  o::ConnectionId server_peer = 0;
  server_business.on_ready = [&](auto) {
    ++accepted_business;
  };
  server_group.on_ready = [&](auto id, const auto&) {
    ++accepted_group;
    server_peer = id;
  };
  server.on_accept = [&](auto id) {
    if (server.peer(id)->framing == o::Framing::Group)
      server_group.incoming(id);
    else
      server_business.incoming(id);
  };
  server.on_frame = [&](auto id, auto bytes) {
    if (server_group.contains(id))
      server_group.receive(id, bytes);
    else
      server_business.receive(id, bytes);
  };
  server.on_close = [&](auto id) {
    server_group.lost(id);
    server_business.lost(id);
  };
  client.on_connect = [&](auto id) {
    if (client_group.contains(id))
      client_group.connected(id);
    else
      client_business.connected(id);
  };
  client.on_frame = [&](auto id, auto bytes) {
    if (client_group.contains(id))
      client_group.receive(id, bytes);
    else
      client_business.receive(id, bytes);
  };
  client.on_close = [&](auto id) {
    client_group.lost(id);
    client_business.lost(id);
  };
  server.listen(business, o::Framing::Internal, 4);
  server.listen(group, o::Framing::Group, 8);
  const auto regular = client.connect(business, o::Framing::Internal);
  client_business.outgoing(regular);
  const auto peer = client.connect(group, o::Framing::Group);
  client_group.outgoing(peer, o::group::Role::Peer, 6, 600);
  pump(server, client, [&] {
    return client_business.ready(regular) && client_group.ready(peer);
  });
  require(accepted_business == 1 && accepted_group == 1,
          "Separate listeners mixed connection roles");

  o::Bytes payload(50000, 0x5a);
  const auto expected_payload = payload;
  server_group.on_message = [&](auto id, const auto& message) {
    require(id == server_peer && message.header.kind == s::MessageKind::ResultDetails &&
                message.header.epoch == 1 && message.header.seq == 2 && message.header.round == 3 &&
                message.body.size() == expected_payload.size() &&
                std::equal(expected_payload.begin(), expected_payload.end(), message.body.begin()),
            "Split/coalesced large group frame corrupted");
    ++messages;
  };
  const auto packet = s::frame({s::MessageKind::ResultDetails, 1, 2, 3}, payload);
  require(client.send(peer, std::span(packet).first(13)), "Split send failed");
  for (unsigned i = 0; i < 3; ++i) {
    client.step(1);
    server.step(1);
  }
  require(!messages, "Incomplete header emitted a message");
  require(client.send(peer, std::span(packet).subspan(13)) && client.send(peer, packet),
          "Coalesced send failed");
  pump(server, client, [&] {
    return messages == 2;
  });

  require(client_group.send(peer, {s::MessageKind::ResultDetails, 1, 2, 3}, payload),
          "Direct group-header/body enqueue failed");
  std::fill(payload.begin(), payload.end(), 0);
  pump(server, client, [&] {
    return messages == 3;
  });
  bool business_received = false;
  server_business.on_message = [&](auto, const auto& h, auto bytes) {
    require(h.kind == o::LinkKind::Group && bytes.size() == packet.size() &&
                std::equal(bytes.begin(), bytes.end(), packet.begin()),
            "Business envelope changed group header/body bytes");
    business_received = true;
  };
  const auto wire =
      s::wire_header({s::MessageKind::ResultDetails, 1, 2, 3}, expected_payload.size());
  require(client_business.send(regular,
                               o::LinkKind::Group,
                               {},
                               {reinterpret_cast<const std::uint8_t*>(&wire), sizeof(wire)},
                               expected_payload),
          "Business multipart enqueue failed");
  pump(server, client, [&] {
    return business_received;
  });

  o::TcpReactor bad_io;
  s::Links wrong_role(bad_io, c, {o::group::Role::Router, 0, 900, topology.fingerprint});
  bad_io.on_connect = [&](auto id) {
    wrong_role.connected(id);
  };
  bad_io.on_frame = [&](auto id, auto bytes) {
    wrong_role.receive(id, bytes);
  };
  bad_io.on_close = [&](auto id) {
    wrong_role.lost(id);
  };
  const auto bad = bad_io.connect(group, o::Framing::Group);
  wrong_role.outgoing(bad, o::group::Role::Peer, 6, 600);
  pump(server, bad_io, [&] {
    return !bad_io.contains(bad);
  });
  require(accepted_business == 1 && accepted_group == 1,
          "Wrong group role replaced a business/peer connection");
  auto oversized = packet;
  const auto size = std::uint32_t(o::group::maximum_frame + 1);
  std::memcpy(oversized.data(), &size, sizeof(size));
  require(client.send(peer, std::span(oversized).first(o::group::header_size)),
          "Oversized test send failed");
  pump(server, client, [&] {
    return !client.contains(peer);
  });
  require(client_business.ready(regular), "Malformed peer frame disconnected business listener");
}
}  // namespace

int main(int argc, char** argv) {
  demo::Suite suite(argc > 1 ? argv[1] : "spread-network-results");
  suite.test("group-headers-identities-and-atomic-source-view-codecs", codec);
  suite.test("owned-send-parts-capacity-reuse-compaction-and-atomic-limit", buffered_parts);
  suite.test(
      "independent-business-peer-listeners-split-coalesced-frames-and-"
      "role-fencing",
      roles_and_framing);
  return suite.finish();
}
