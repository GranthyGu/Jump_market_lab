cmake_minimum_required(VERSION 3.20)
project(marketlab_broadcast LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
find_package(Threads REQUIRED)

add_subdirectory(market_publisher)

add_library(marketlab_client STATIC client/client.cpp)
target_link_libraries(marketlab_client PUBLIC marketlab_market_data_link)

foreach(target marketlab_client)
  target_compile_options(${target} PRIVATE -Wall -Wextra -Wpedantic)
endforeach()

# One executable keeps the existing publish/mock/observe/query command interface.
add_executable(market_broadcast main.cpp)
target_link_libraries(market_broadcast PRIVATE marketlab_publisher marketlab_demo_source marketlab_client)
target_compile_options(market_broadcast PRIVATE -Wall -Wextra -Wpedantic)

enable_testing()
add_executable(broadcast_tests tests/broadcast_tests.cpp)
target_link_libraries(broadcast_tests PRIVATE marketlab_publisher marketlab_client)
target_compile_options(broadcast_tests PRIVATE -Wall -Wextra -Wpedantic)
add_test(NAME broadcast_tests COMMAND broadcast_tests)
add_test(NAME broadcast_demo COMMAND sh ${CMAKE_CURRENT_SOURCE_DIR}/tests/smoke_test.sh
    $<TARGET_FILE:market_broadcast> ${CMAKE_CURRENT_SOURCE_DIR}/market_publisher/example.conf)
set_tests_properties(broadcast_tests broadcast_demo PROPERTIES TIMEOUT 30)

# Build explicitly when measuring the local algorithm; timing is not a test threshold.
add_executable(difference_benchmark EXCLUDE_FROM_ALL tests/difference_benchmark.cpp)
target_link_libraries(difference_benchmark PRIVATE marketlab_market_data_link)
target_compile_options(difference_benchmark PRIVATE -Wall -Wextra -Wpedantic)

include(tests/extended_tests.cmake)

add_subdirectory(gateway)
add_subdirectory(matching_engine)
