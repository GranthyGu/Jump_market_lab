#pragma once
#include "network.hpp"
namespace demo {
// Deterministic synthetic input; the Publisher executable remains unmodified.
inline md::Book source_book(std::uint16_t id,std::uint64_t version) {
  md::Book book{id,{}};
  const auto shape=id%6;
  // Empty, static, replaced prices, all quantities, one quantity, empty/full.
  if(shape==0 || (shape==5 && version%2==1)) return book;
  const int shift=shape==2?10*int(version%2):0;
  for(auto side:{md::Side::Buy,md::Side::Sell}) for(unsigned level=0;level<5;++level) {
    int price=(side==md::Side::Buy?100-int(level):110+int(level))+shift;
    const auto change=(shape==3 || (shape==4 && side==md::Side::Buy && level==0))?version:0;
    book.levels.push_back({side,price,1000+10ULL*id+change,shape==3?1+version%3:1});
  }
  return book;
}
struct Sources {
  md::Config config; fs::path directory;
  std::vector<std::unique_ptr<md::StopThread>> workers;
  std::atomic_bool fail{false}; std::atomic<unsigned> epoch_number{1};
  std::vector<std::shared_ptr<std::atomic_uint64_t>> calls;
  ~Sources() { workers.clear(); } // Join before the atomics used by providers die.
  Sources(fs::path dir,unsigned products,unsigned shards):directory(std::move(dir)) {
    require(products<=2000 && shards<=32 && shards<=products,"Synthetic input beyond demo bounds"); fs::create_directories(directory);
    Ports reserved; config.query={"127.0.0.1",reserved.take()};
    config.multicast=md::MulticastConfig{{"239.10.20.31",reserved.take()},"127.0.0.1",1,true};
    for(unsigned s=0;s<shards;++s) {
      auto listener=md::listen_tcp("127.0.0.1",0);
      config.shards.push_back({"synthetic"+std::to_string(s),"tcp","127.0.0.1",bound_port(listener.get()),(directory/("unused-"+std::to_string(s))).string(),{}});
      calls.push_back(std::make_shared<std::atomic_uint64_t>(0));
      for(unsigned p=s;p<products;p+=shards) config.shards.back().symbols.push_back(p+1);
      auto ids=config.shards.back().symbols; auto counter=calls.back();
      workers.push_back(std::make_unique<md::StopThread>([this,s,ids,counter,fd=std::move(listener)](const auto& stop) mutable {
        md::serve_shard(std::move(fd),[this,s,ids,counter] {
          if(s==0 && fail.load()) throw std::runtime_error("Intentional source outage");
          md::Epoch epoch{}; epoch[0]=static_cast<std::uint8_t>(epoch_number.load()); epoch[1]=static_cast<std::uint8_t>(s);
          const auto step=counter->fetch_add(1); md::ShardView view{epoch,{}};
          for(auto id:ids) view.books.emplace(id,source_book(id,step)); return view;
        },stop);
      }));
    }
    for(unsigned p=0;p<products;++p) config.symbols.emplace(p+1,symbol(p));
  }
};
}
