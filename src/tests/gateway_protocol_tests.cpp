#include "tests/gateway_support.hpp"
#include "gateway/net/tcp_reactor.hpp"
#include <limits>

using namespace marketlab;
using namespace marketlab::orders;
using namespace marketlab::orders::test;
int main(int argc,char** argv) {
  try {
    check(argc == 2,"Expected artifact directory");
    std::filesystem::create_directories(argv[1]);
    Cases cases(std::filesystem::path(argv[1]) / "protocol.csv");
    cases.run("wire_layout_and_roundtrip",[] {
      check(sizeof(Header) == 14 && sizeof(OrderState) == 58 && link_header_size == 26,"Packed sizes");
      auto request = Client::new_request(0x0102030405060708ULL,"AAPL",Side::Buy,100,7);
      const auto wire = record_bytes(request);
      check(wire.size() == 55 && wire[0] == 3 && wire[2] == 55 && wire[6] == 8 && wire[13] == 1,"Little endian offsets");
      const auto frame = link_frame(LinkKind::NewOrder,123,2,456,wire);
      check(valid_link_record(frame) && frame.size() == 81 && frame[0] == 81 && frame[4] == 10,"Internal framing");
      const auto decoded = copy_record<NewOrderRequest>(std::span(frame).subspan(link_header_size));
      check(std::memcmp(&request,&decoded,sizeof(request)) == 0,"Lossless engine request");
      auto update = server_record<OrderUpdate>(MessageType::OrderUpdate,request.header.requestId);
      update.order.orderId = std::numeric_limits<std::uint64_t>::max();
      update.order.originalQuantity = std::numeric_limits<std::int32_t>::max();
      const auto event = link_record(LinkKind::Event,{0,LinkKind::Event,123,2,456},update);
      check(valid_link_record(event),"Maximum integer event roundtrip");
      check(copy_record<OrderUpdate>(std::span(event).subspan(link_header_size)).order.orderId == update.order.orderId,"uint64 precision");
    });
    cases.run("invalid_frames_and_strings",[] {
      auto request = Client::new_request(1,"AAPL",Side::Buy,100,1);
      auto wire = record_bytes(request);
      for (std::size_t i = 0; i < wire.size(); ++i) check(!valid_client_record(std::span(wire).first(i),true),"Truncated client frame accepted");
      request.header.requestId = 0; check(!valid_client_record(record_bytes(request),true),"Zero request ID accepted");
      request.header.requestId = 1; request.header.messageType = static_cast<MessageType>(999);
      check(!valid_client_record(record_bytes(request),true),"Unknown type accepted");
      auto frame = link_frame(LinkKind::NewOrder,1,1,1,wire);
      for (std::size_t n = 0; n < frame.size(); ++n) check(!valid_link_record(std::span(frame).first(n)),"Truncated link frame accepted");
      frame[4] = 255; check(!valid_link_record(frame),"Unknown engine kind accepted");
      std::array<char,32> chars{}; chars.fill('a');
      bool rejected = false; try { (void)fixed_string(chars); } catch (...) { rejected = true; }
      check(rejected,"Unterminated string accepted");
      chars.fill(0); chars[0] = 'a'; chars[2] = 'b'; rejected = false;
      try { (void)fixed_string(chars); } catch (...) { rejected = true; }
      check(rejected,"Nonzero padding accepted");
    });
    cases.run("tcp_fragmentation_and_coalescing",[] {
      TcpReactor io;
      const Address address{"127.0.0.1",free_port()}; io.listen(address,Framing::Client,16);
      std::vector<Bytes> received;
      io.on_frame = [&](auto id,auto bytes) {
        received.emplace_back(bytes.begin(),bytes.end());
        Header h{}; std::memcpy(&h,bytes.data(),sizeof(h));
        io.send(id,record_bytes(server_record<LoginAccepted>(MessageType::LoginAccepted,h.requestId)));
      };
      {
        md::StopThread thread([&](const auto& stop) { while (!stop.load()) io.step(5); });
        Client client(address);
        auto one = record_bytes(Client::new_request(1,"AAPL",Side::Buy,100,1));
        for (auto byte : one) { client.send_bytes(std::span(&byte,1)); std::this_thread::sleep_for(1ms); }
        check(client.receive_as<LoginAccepted>(MessageType::LoginAccepted).header.requestId == 1,"Fragment correlation");
        const auto two = record_bytes(Client::new_request(2,"MSFT",Side::Sell,200,5));
        const auto three = record_bytes(CancelRequest{{MessageType::CancelRequest,sizeof(CancelRequest),3},999});
        auto combined = two; combined.insert(combined.end(),three.begin(),three.end()); client.send_bytes(combined);
        check(client.receive_as<LoginAccepted>(MessageType::LoginAccepted).header.requestId == 2,"Coalesced first");
        check(client.receive_as<LoginAccepted>(MessageType::LoginAccepted).header.requestId == 3,"Coalesced second");
      }
      check(received.size() == 3 && received[0].size() == 55 && received[2].size() == 22,"Frame boundaries");
    });
    cases.run("partial_writes_and_slow_reader",[] {
      TcpReactor io(1024 * 1024,3000);
      const Address address{"127.0.0.1",free_port()}; io.listen(address,Framing::Client,16);
      const auto record = record_bytes(server_record<LoginAccepted>(MessageType::LoginAccepted,7));
      Bytes expected; for (unsigned i = 0; i < 8000; ++i) expected.insert(expected.end(),record.begin(),record.end());
      io.on_accept = [&](auto id) {
        int buffer = 1024; setsockopt(io.peer(id)->fd,SOL_SOCKET,SO_SNDBUF,&buffer,sizeof(buffer));
        io.send(id,expected);
      };
      {
        md::StopThread thread([&](const auto& stop) { while (!stop.load()) io.step(5); });
        Client client(address); std::this_thread::sleep_for(150ms);
        check(md::receive_exact(client.fd.get(),expected.size(),deadline(10000)) == expected,"Partial writes lost bytes");
      }
      check(io.bytes_out == expected.size(),"Byte accounting");
    });
    cases.run("buffer_limits_and_partial_deadline",[] {
      TcpReactor io(4096,80); const Address address{"127.0.0.1",free_port()}; io.listen(address,Framing::Client,16);
      io.on_frame = [&](auto id,auto) { io.send(id,Bytes(4097,0)); };
      {
        md::StopThread thread([&](const auto& stop) { while (!stop.load()) { io.step(5); io.expire(); } });
        Client partial(address); const std::uint8_t byte = 1; partial.send_bytes(std::span(&byte,1)); partial.expect_closed();
        Client overflow(address); overflow.send(Client::new_request(1,"AAPL",Side::Buy,100,1)); overflow.expect_closed();
      }
      check(io.disconnected >= 2,"Bounded cleanup");
    });
    std::cout << "gateway_protocol_cases=" << cases.count << " PASS\n";
    return 0;
  } catch (const std::exception& e) { std::cerr << "FAIL " << e.what() << '\n'; return 1; }
}
