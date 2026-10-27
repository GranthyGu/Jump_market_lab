add_library(marketlab_order_link STATIC
  protocol/engine_link.cpp config.cpp net/tcp_reactor.cpp)
target_include_directories(marketlab_order_link PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/..)
target_link_libraries(marketlab_order_link PUBLIC Threads::Threads)

add_library(marketlab_gateway STATIC gateway.cpp)
target_link_libraries(marketlab_gateway PUBLIC marketlab_order_link)

add_library(marketlab_mock_orders STATIC
  ../mock_matching_engine/order_router.cpp
  ../mock_matching_engine/order_shard.cpp
  ../mock_matching_engine/order_book.cpp)
target_link_libraries(marketlab_mock_orders PUBLIC marketlab_order_link marketlab_common)

add_executable(market_gateway main.cpp)
set_target_properties(market_gateway PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})
target_link_libraries(market_gateway PRIVATE marketlab_gateway marketlab_mock_orders)
foreach(target marketlab_order_link marketlab_gateway marketlab_mock_orders market_gateway)
  target_compile_options(${target} PRIVATE -Wall -Wextra -Wpedantic)
endforeach()

# Keep every test source under src/tests while preserving the gateway targets.
set(gateway_test_sources ${CMAKE_CURRENT_SOURCE_DIR}/../tests)
add_executable(gateway_protocol_tests ${gateway_test_sources}/gateway_protocol_tests.cpp)
target_link_libraries(gateway_protocol_tests PRIVATE marketlab_order_link marketlab_common)
add_executable(gateway_integration_tests ${gateway_test_sources}/gateway_integration_tests.cpp)
target_link_libraries(gateway_integration_tests PRIVATE marketlab_order_link marketlab_common)
foreach(target gateway_protocol_tests gateway_integration_tests)
  target_compile_options(${target} PRIVATE -Wall -Wextra -Wpedantic)
endforeach()
set(gateway_results ${CMAKE_BINARY_DIR}/gateway/results)
add_test(NAME gateway_protocol COMMAND gateway_protocol_tests ${gateway_results}/protocol)
foreach(topology direct router)
  add_test(NAME gateway_integration_${topology} COMMAND gateway_integration_tests
    $<TARGET_FILE:market_gateway> ${gateway_results}/integration_${topology} ${topology})
  set_tests_properties(gateway_integration_${topology} PROPERTIES TIMEOUT 60 RUN_SERIAL TRUE LABELS "gateway;network;correctness")
endforeach()
set_tests_properties(gateway_protocol PROPERTIES TIMEOUT 30 RUN_SERIAL TRUE LABELS "gateway;network;correctness")

add_executable(gateway_benchmark ${gateway_test_sources}/gateway_benchmark.cpp)
target_link_libraries(gateway_benchmark PRIVATE marketlab_order_link marketlab_common)
target_compile_options(gateway_benchmark PRIVATE -Wall -Wextra -Wpedantic)
add_test(NAME gateway_performance COMMAND gateway_benchmark $<TARGET_FILE:market_gateway> ${gateway_results}/performance)
set_tests_properties(gateway_performance PROPERTIES TIMEOUT 120 RUN_SERIAL TRUE LABELS "gateway;network;performance")

add_executable(gateway_failure_tests ${gateway_test_sources}/gateway_failure_tests.cpp)
target_link_libraries(gateway_failure_tests PRIVATE marketlab_order_link marketlab_common)
target_compile_options(gateway_failure_tests PRIVATE -Wall -Wextra -Wpedantic)
add_test(NAME gateway_failures COMMAND gateway_failure_tests $<TARGET_FILE:market_gateway> ${gateway_results}/failures)
set_tests_properties(gateway_failures PROPERTIES TIMEOUT 30 RUN_SERIAL TRUE LABELS "gateway;network;correctness")
