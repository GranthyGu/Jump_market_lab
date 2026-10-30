cmake_minimum_required(VERSION 3.20)
if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
  project(marketlab_client LANGUAGES CXX)
endif()
include(../CMakeLists.txt)

# One home for both client transports; session unification is a separate change.
add_library(marketlab_client STATIC market_data.cpp)
target_link_libraries(marketlab_client PUBLIC marketlab_market_data_link)
target_compile_options(marketlab_client PRIVATE -Wall -Wextra -Wpedantic)
add_executable(gateway_client order_client.cpp)
target_link_libraries(gateway_client PRIVATE marketlab_order_link marketlab_market_data_link)
target_compile_options(gateway_client PRIVATE -Wall -Wextra -Wpedantic)
