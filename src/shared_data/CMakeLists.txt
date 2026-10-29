add_library(marketlab_order_link STATIC
  orders/config.cpp orders/engine_link.cpp orders/tcp_reactor.cpp)
add_library(marketlab_market_data_link STATIC
  market_data/config.cpp market_data/market_data.cpp market_data/transport.cpp)
foreach(target marketlab_order_link marketlab_market_data_link)
  target_include_directories(${target} PUBLIC "${MARKETLAB_SOURCE_DIR}")
  target_compile_definitions(${target} PUBLIC MARKETLAB_SOURCE_DIR="${MARKETLAB_SOURCE_DIR}")
  target_link_libraries(${target} PUBLIC Threads::Threads)
  target_compile_options(${target} PRIVATE -Wall -Wextra -Wpedantic)
endforeach()
