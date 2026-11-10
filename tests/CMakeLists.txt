# Four freshly implemented presentation suites; no legacy test targets.
find_package(Python3 REQUIRED COMPONENTS Interpreter)
foreach(component engine gateway publisher integration)
  add_executable(demo_${component}_tests ${component}/suite.cpp)
  target_include_directories(demo_${component}_tests PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}")
  target_compile_options(demo_${component}_tests PRIVATE -Wall -Wextra -Wpedantic)
  if(component STREQUAL "engine")
    target_link_libraries(demo_${component}_tests PRIVATE marketlab_matching_core)
  elseif(component STREQUAL "integration")
    target_link_libraries(demo_${component}_tests PRIVATE marketlab_order_link marketlab_client)
  else()
    target_link_libraries(demo_${component}_tests PRIVATE marketlab_order_link marketlab_market_data_link)
  endif()
  add_dependencies(demo_${component}_tests market_gateway market_matching_engine market_engine_router market_broadcast)
  add_test(NAME demo_${component}
    COMMAND "${Python3_EXECUTABLE}" "${CMAKE_CURRENT_SOURCE_DIR}/run.py" ${component}
      --build-dir "$<TARGET_FILE_DIR:demo_${component}_tests>"
      --output "${CMAKE_BINARY_DIR}/demo-results")
  set_tests_properties(demo_${component} PROPERTIES TIMEOUT 180 RUN_SERIAL TRUE LABELS "demo;${component};correctness;performance")
endforeach()
add_custom_target(demo_tests DEPENDS demo_engine_tests demo_gateway_tests demo_publisher_tests demo_integration_tests)
add_executable(snapshot_contention_benchmark EXCLUDE_FROM_ALL engine/snapshot_contention.cpp)
target_include_directories(snapshot_contention_benchmark PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}")
target_link_libraries(snapshot_contention_benchmark PRIVATE marketlab_matching_core)
target_compile_options(snapshot_contention_benchmark PRIVATE -Wall -Wextra -Wpedantic)
add_executable(snapshot_mailbox_tests engine/snapshot_mailbox.cpp)
target_include_directories(snapshot_mailbox_tests PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}")
target_link_libraries(snapshot_mailbox_tests PRIVATE marketlab_matching_core)
target_compile_options(snapshot_mailbox_tests PRIVATE -Wall -Wextra -Wpedantic)
add_dependencies(demo_tests snapshot_mailbox_tests)
add_test(NAME snapshot_mailbox COMMAND snapshot_mailbox_tests "${CMAKE_BINARY_DIR}/mailbox-results")
set_tests_properties(snapshot_mailbox PROPERTIES TIMEOUT 30 LABELS "engine;concurrency;correctness")
file(GENERATE OUTPUT "${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/demo-build.json"
  CONTENT "{\"compiler\":\"${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}\",\"build_type\":\"${CMAKE_BUILD_TYPE}\",\"cxx_standard\":20}\n")

add_executable(persistence_tests recovery/suite.cpp)
target_link_libraries(persistence_tests PRIVATE marketlab_matching_core)
target_compile_options(persistence_tests PRIVATE -Wall -Wextra -Wpedantic)
add_test(NAME persistence_correctness COMMAND persistence_tests "${CMAKE_BINARY_DIR}/recovery-results")
set_tests_properties(persistence_correctness PROPERTIES TIMEOUT 60 LABELS "engine;recovery;correctness")
add_dependencies(demo_tests persistence_tests)

add_executable(persistence_optimization_tests recovery/optimizations.cpp)
target_link_libraries(persistence_optimization_tests PRIVATE marketlab_matching_core)
target_compile_options(persistence_optimization_tests PRIVATE -Wall -Wextra -Wpedantic)
add_test(NAME persistence_optimizations
  COMMAND persistence_optimization_tests "${CMAKE_BINARY_DIR}/optimization-results")
set_tests_properties(persistence_optimizations PROPERTIES TIMEOUT 60 LABELS "engine;recovery;correctness")
add_dependencies(demo_tests persistence_optimization_tests)

add_executable(recovery_process_test recovery/process.cpp)
target_include_directories(recovery_process_test PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}")
target_link_libraries(recovery_process_test PRIVATE marketlab_order_link marketlab_market_data_link)
target_compile_options(recovery_process_test PRIVATE -Wall -Wextra -Wpedantic)
add_dependencies(recovery_process_test market_matching_engine market_gateway market_broadcast)
add_dependencies(demo_tests recovery_process_test)
add_test(NAME recovery_process COMMAND recovery_process_test "$<TARGET_FILE_DIR:market_matching_engine>" "${CMAKE_BINARY_DIR}/recovery-results")
set_tests_properties(recovery_process PROPERTIES TIMEOUT 60 RUN_SERIAL TRUE LABELS "engine;recovery;integration;correctness")

add_subdirectory(recovery/bench)
add_executable(bpt_tests bpt/suite.cpp)
target_link_libraries(bpt_tests PRIVATE marketlab_bpt marketlab_order_link)
target_compile_options(bpt_tests PRIVATE -Wall -Wextra -Wpedantic)
add_test(NAME bpt_correctness_concurrency_stress COMMAND bpt_tests "${CMAKE_BINARY_DIR}/bpt-results" 300000)
set_tests_properties(bpt_correctness_concurrency_stress PROPERTIES TIMEOUT 180 LABELS "bpt;concurrency;correctness;performance")
add_executable(bpt_cache_concurrency_benchmark bpt/cache_concurrency_benchmark.cpp)
target_link_libraries(bpt_cache_concurrency_benchmark PRIVATE marketlab_bpt marketlab_order_link)
target_compile_options(bpt_cache_concurrency_benchmark PRIVATE -Wall -Wextra -Wpedantic)
add_test(NAME bpt_cache_control_correctness
  COMMAND bpt_cache_concurrency_benchmark "${CMAKE_BINARY_DIR}/bpt-cache-control-results" verify)
set_tests_properties(bpt_cache_control_correctness PROPERTIES TIMEOUT 180 LABELS "bpt;concurrency;correctness")
add_executable(trade_history_tests bpt/history.cpp)
target_link_libraries(trade_history_tests PRIVATE marketlab_matching_core)
add_test(NAME trade_history_correctness COMMAND trade_history_tests "${CMAKE_BINARY_DIR}/history-results")
set_tests_properties(trade_history_correctness PROPERTIES TIMEOUT 90 LABELS "bpt;recovery;correctness")
add_test(NAME trade_history_process
  COMMAND "${Python3_EXECUTABLE}" "${CMAKE_CURRENT_SOURCE_DIR}/bpt/process.py"
    "$<TARGET_FILE_DIR:market_matching_engine>" "${CMAKE_BINARY_DIR}/history-process-results")
set_tests_properties(trade_history_process PROPERTIES TIMEOUT 180 RUN_SERIAL TRUE LABELS "bpt;recovery;integration")
add_test(NAME planned_restart_process
  COMMAND "${Python3_EXECUTABLE}" "${CMAKE_CURRENT_SOURCE_DIR}/restart/process.py"
    "$<TARGET_FILE_DIR:market_matching_engine>" "${CMAKE_BINARY_DIR}/restart-results")
set_tests_properties(planned_restart_process PROPERTIES TIMEOUT 240 RUN_SERIAL TRUE
  LABELS "engine;router;recovery;integration;performance")
add_executable(replication_tests restart/replication.cpp)
target_link_libraries(replication_tests PRIVATE marketlab_matching_core)
target_compile_options(replication_tests PRIVATE -Wall -Wextra -Wpedantic)
add_test(NAME replication_correctness COMMAND replication_tests "${CMAKE_BINARY_DIR}/replication-results")
set_tests_properties(replication_correctness PROPERTIES TIMEOUT 30 LABELS "engine;recovery;correctness")
add_dependencies(demo_tests replication_tests market_engine_router)
add_executable(trade_history_benchmark EXCLUDE_FROM_ALL bpt/history_benchmark.cpp)
target_link_libraries(trade_history_benchmark PRIVATE marketlab_matching_core)

# Explicit audit for loss/corruption after an acknowledged commit. It reports
# known protection gaps with a nonzero exit code, rather than hiding them in a
# passing test. This is separate from the process-crash regression suite.
add_executable(persistence_recovery_audit EXCLUDE_FROM_ALL recovery/recovery_audit.cpp)
target_link_libraries(persistence_recovery_audit PRIVATE marketlab_matching_core)
target_compile_options(persistence_recovery_audit PRIVATE -Wall -Wextra -Wpedantic)
