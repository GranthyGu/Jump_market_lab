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
target_link_libraries(marketlab_mock_orders PUBLIC marketlab_order_link marketlab_market_data_link)

add_executable(market_gateway main.cpp)
set_target_properties(market_gateway PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})
target_link_libraries(market_gateway PRIVATE marketlab_gateway marketlab_mock_orders)
add_executable(gateway_client ../client/order_client.cpp)
set_target_properties(gateway_client PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})
target_link_libraries(gateway_client PRIVATE marketlab_order_link marketlab_market_data_link)
target_compile_options(gateway_client PRIVATE -Wall -Wextra -Wpedantic)
foreach(target marketlab_order_link marketlab_gateway marketlab_mock_orders market_gateway)
  target_compile_options(${target} PRIVATE -Wall -Wextra -Wpedantic)
endforeach()

# Keep every test source under src/tests while preserving the gateway targets.
set(gateway_test_sources ${CMAKE_CURRENT_SOURCE_DIR}/../tests)
add_executable(gateway_protocol_tests ${gateway_test_sources}/gateway_protocol_tests.cpp)
target_link_libraries(gateway_protocol_tests PRIVATE marketlab_order_link marketlab_market_data_link)
add_executable(gateway_integration_tests ${gateway_test_sources}/gateway_integration_tests.cpp)
target_link_libraries(gateway_integration_tests PRIVATE marketlab_order_link marketlab_market_data_link)
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
target_link_libraries(gateway_benchmark PRIVATE marketlab_order_link marketlab_market_data_link)
target_compile_options(gateway_benchmark PRIVATE -Wall -Wextra -Wpedantic)
add_test(NAME gateway_performance COMMAND gateway_benchmark $<TARGET_FILE:market_gateway> ${gateway_results}/performance)
set_tests_properties(gateway_performance PROPERTIES TIMEOUT 120 RUN_SERIAL TRUE LABELS "gateway;network;performance")

add_executable(gateway_failure_tests ${gateway_test_sources}/gateway_failure_tests.cpp)
target_link_libraries(gateway_failure_tests PRIVATE marketlab_order_link marketlab_market_data_link)
target_compile_options(gateway_failure_tests PRIVATE -Wall -Wextra -Wpedantic)
add_test(NAME gateway_failures COMMAND gateway_failure_tests $<TARGET_FILE:market_gateway> ${gateway_results}/failures)
set_tests_properties(gateway_failures PROPERTIES TIMEOUT 30 RUN_SERIAL TRUE LABELS "gateway;network;correctness")

add_executable(manual_cli_tests ${gateway_test_sources}/manual_cli_tests.cpp)
target_link_libraries(manual_cli_tests PRIVATE marketlab_order_link marketlab_market_data_link)
target_compile_options(manual_cli_tests PRIVATE -Wall -Wextra -Wpedantic)
add_test(NAME gateway_manual_cli COMMAND manual_cli_tests $<TARGET_FILE:market_gateway>
  $<TARGET_FILE:gateway_client> $<TARGET_FILE:market_broadcast> ${gateway_results}/manual_cli)
set_tests_properties(gateway_manual_cli PROPERTIES TIMEOUT 45 RUN_SERIAL TRUE LABELS "gateway;network;correctness;manual-cli")
