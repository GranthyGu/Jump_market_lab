cmake_minimum_required(VERSION 3.20)
project(marketlab_exchange LANGUAGES CXX)

include(cmake/MarketLab.cmake)
add_subdirectory(client)
add_subdirectory(gateway)
add_subdirectory(matching_engine)
add_subdirectory(publisher)

# Normal builds contain runtime code only; old tests remain opt-in.
option(MARKETLAB_BUILD_TESTS "Build the grouped legacy tests and benchmarks" OFF)
if(MARKETLAB_BUILD_TESTS)
  enable_testing()
  add_subdirectory("${MARKETLAB_SOURCE_DIR}/../tests" "${CMAKE_BINARY_DIR}/tests")
endif()
