add_library(marketlab_matching_core STATIC engine.cpp)
target_link_libraries(marketlab_matching_core PUBLIC marketlab_order_link marketlab_market_data_link)
add_library(marketlab_matching_runtime STATIC server.cpp)
target_link_libraries(marketlab_matching_runtime PUBLIC marketlab_matching_core)

# One component executable; launch it once per configured shard, in separate processes.
add_executable(market_matching_engine main.cpp)
set_target_properties(market_matching_engine PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})
target_link_libraries(market_matching_engine PRIVATE marketlab_matching_runtime)
foreach(target marketlab_matching_core marketlab_matching_runtime market_matching_engine)
  target_compile_options(${target} PRIVATE -Wall -Wextra -Wpedantic)
endforeach()

add_executable(matching_engine_tests ../tests/matching_engine_tests.cpp)
target_link_libraries(matching_engine_tests PRIVATE marketlab_matching_core)
target_compile_options(matching_engine_tests PRIVATE -Wall -Wextra -Wpedantic)
add_test(NAME matching_engine_correctness COMMAND matching_engine_tests ${CMAKE_BINARY_DIR}/matching_engine/results)
set_tests_properties(matching_engine_correctness PROPERTIES TIMEOUT 120 LABELS "engine;correctness;concurrency")

add_executable(matching_engine_benchmark ../tests/matching_engine_benchmark.cpp)
target_link_libraries(matching_engine_benchmark PRIVATE marketlab_matching_core)
target_compile_options(matching_engine_benchmark PRIVATE -Wall -Wextra -Wpedantic)
add_test(NAME matching_engine_performance COMMAND matching_engine_benchmark ${CMAKE_BINARY_DIR}/matching_engine/results)
set_tests_properties(matching_engine_performance PROPERTIES TIMEOUT 60 RUN_SERIAL TRUE LABELS "engine;performance")
