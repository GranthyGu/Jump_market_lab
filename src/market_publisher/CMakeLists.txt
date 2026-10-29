# Public market-data formats and transport adapters, shared with engines/clients.
add_library(marketlab_market_data_link STATIC
  protocol/market_data.cpp config.cpp net/transport.cpp)
target_include_directories(marketlab_market_data_link PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/..)
target_link_libraries(marketlab_market_data_link PUBLIC Threads::Threads)

# Publishing state and scheduling depend on the interface library, not vice versa.
add_library(marketlab_publisher STATIC publisher.cpp)
target_link_libraries(marketlab_publisher PUBLIC marketlab_market_data_link)

# Synthetic market-data generator for the existing CLI demo, never used for orders.
add_library(marketlab_demo_source STATIC demo_source.cpp)
target_link_libraries(marketlab_demo_source PUBLIC marketlab_market_data_link)

foreach(target marketlab_market_data_link marketlab_publisher marketlab_demo_source)
  target_compile_options(${target} PRIVATE -Wall -Wextra -Wpedantic)
endforeach()
