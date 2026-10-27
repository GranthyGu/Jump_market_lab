add_executable(difference_stress_tests tests/difference_stress_tests.cpp)
target_link_libraries(difference_stress_tests PRIVATE marketlab_common)
target_compile_options(difference_stress_tests PRIVATE -Wall -Wextra -Wpedantic)
add_test(NAME difference_stress COMMAND difference_stress_tests)
set_tests_properties(difference_stress PROPERTIES TIMEOUT 120 LABELS "stress;correctness")

add_executable(transport_corner_tests tests/transport_corner_tests.cpp)
target_link_libraries(transport_corner_tests PRIVATE marketlab_common)
target_compile_options(transport_corner_tests PRIVATE -Wall -Wextra -Wpedantic)
add_test(NAME transport_corners COMMAND transport_corner_tests)
set_tests_properties(transport_corners PROPERTIES TIMEOUT 30 RUN_SERIAL TRUE LABELS "stress;correctness;network")

# Generate a test-only previous implementation without adding baseline branches to production.
set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/common/protocol.cpp")
file(READ "${CMAKE_CURRENT_SOURCE_DIR}/common/protocol.cpp" baseline_source)
function(replace_test_function input begin_marker end_marker replacement output)
  string(FIND "${input}" "${begin_marker}" first)
  string(FIND "${input}" "${end_marker}" last)
  if(first LESS 0 OR last LESS_EQUAL first)
    message(FATAL_ERROR "Cannot locate baseline protocol function markers")
  endif()
  string(SUBSTRING "${input}" 0 ${first} head)
  string(SUBSTRING "${input}" ${last} -1 tail)
  set(${output} "${head}\n${replacement}\n${tail}" PARENT_SCOPE)
endfunction()
replace_test_function("${baseline_source}" "\nvoid validate_levels(" "\nvoid validate_book("
  "void validate_levels(const std::vector<Level>& levels, bool delta) { test::reference_validate_levels(levels, delta); }"
  baseline_source)
replace_test_function("${baseline_source}" "\nstd::vector<Level> difference(" "\nBytes encode("
  "std::vector<Level> difference(const Book& before, const Book& after) { return test::reference_difference(before, after); }"
  baseline_source)
file(CONFIGURE OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/generated/baseline_protocol.cpp"
  CONTENT "#include \"tests/reference_difference.hpp\"\n${baseline_source}" @ONLY)

add_library(marketlab_common_baseline STATIC "${CMAKE_CURRENT_BINARY_DIR}/generated/baseline_protocol.cpp" common/io.cpp)
target_include_directories(marketlab_common_baseline PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}")
target_link_libraries(marketlab_common_baseline PUBLIC Threads::Threads)
add_library(marketlab_md_baseline STATIC market_publisher/service.cpp)
target_link_libraries(marketlab_md_baseline PUBLIC marketlab_common_baseline)

foreach(variant baseline optimized)
  add_executable(network_stress_${variant} tests/network_stress.cpp)
  target_compile_definitions(network_stress_${variant} PRIVATE STRESS_IMPLEMENTATION="${variant}")
  target_compile_options(network_stress_${variant} PRIVATE -Wall -Wextra -Wpedantic)
  if(variant STREQUAL "baseline")
    target_link_libraries(network_stress_${variant} PRIVATE marketlab_md_baseline)
  else()
    target_link_libraries(network_stress_${variant} PRIVATE marketlab_md)
  endif()
endforeach()

foreach(scenario tcp_1000_16 tcp_10000_64 tcp_1000_128 shm_1000_16)
  string(REPLACE "_" ";" dimensions "${scenario}")
  list(GET dimensions 0 transport)
  list(GET dimensions 1 products)
  list(GET dimensions 2 shards)
  foreach(variant baseline optimized)
    add_test(NAME stress_${scenario}_${variant}
      COMMAND network_stress_${variant} ${transport} ${products} ${shards} 4)
    set_tests_properties(stress_${scenario}_${variant} PROPERTIES TIMEOUT 40
      RUN_SERIAL TRUE LABELS "stress;network;comparison")
  endforeach()
endforeach()
