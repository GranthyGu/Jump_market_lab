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
  add_dependencies(demo_${component}_tests market_gateway market_matching_engine market_broadcast)
  add_test(NAME demo_${component}
    COMMAND "${Python3_EXECUTABLE}" "${CMAKE_CURRENT_SOURCE_DIR}/run.py" ${component}
      --build-dir "$<TARGET_FILE_DIR:demo_${component}_tests>"
      --output "${CMAKE_BINARY_DIR}/demo-results")
  set_tests_properties(demo_${component} PROPERTIES TIMEOUT 180 RUN_SERIAL TRUE LABELS "demo;${component};correctness;performance")
endforeach()
add_custom_target(demo_tests DEPENDS demo_engine_tests demo_gateway_tests demo_publisher_tests demo_integration_tests)
file(GENERATE OUTPUT "${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/demo-build.json"
  CONTENT "{\"compiler\":\"${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}\",\"build_type\":\"${CMAKE_BUILD_TYPE}\",\"cxx_standard\":20}\n")
