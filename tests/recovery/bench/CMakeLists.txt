# These targets are explicit opt-ins; production Queue and engine sources stay unchanged.
add_executable(persistence_queue_benchmark EXCLUDE_FROM_ALL queue_benchmark.cpp)
target_link_libraries(persistence_queue_benchmark PRIVATE marketlab_matching_core)
target_compile_options(persistence_queue_benchmark PRIVATE -Wall -Wextra -Wpedantic)

foreach(policy mutex spsc_park spsc_yield)
    set(target persistence_engine_benchmark_${policy})
    add_executable(${target} EXCLUDE_FROM_ALL engine_benchmark.cpp)
    target_link_libraries(${target} PRIVATE marketlab_matching_core)
    target_compile_options(${target} PRIVATE -Wall -Wextra -Wpedantic)
    target_compile_definitions(${target} PRIVATE MARKETLAB_BENCH_QUEUE_NAME="${policy}")

    if(NOT policy STREQUAL "mutex")
        if(policy STREQUAL "spsc_park")
            set(park true)
        else()
            set(park false)
        endif()
        set(overlay "${CMAKE_CURRENT_BINARY_DIR}/${policy}")
        file(GENERATE OUTPUT "${overlay}/matching_engine/persistence/queue.hpp"
            CONTENT "#pragma once\n#include <type_traits>\n#include \"${CMAKE_CURRENT_SOURCE_DIR}/spsc_queue.hpp\"\nnamespace marketlab::orders::persistence {\n// Snapshot* is rare work: always park, including the yielding log-queue variant.\ntemplate <class T> using Queue = marketlab::bench::SpscQueue<T, ${park} || std::is_pointer_v<T>>;\n}\n")

        # Compile the same engine with an include overlay. The archive supplies
        # its other objects; its original durable_engine.o is not linked twice.
        target_sources(${target} PRIVATE
            "${MARKETLAB_SOURCE_DIR}/matching_engine/persistence/durable_engine.cpp")
        target_include_directories(${target} BEFORE PRIVATE "${overlay}")

        set(check persistence_queue_recovery_${policy})
        add_executable(${check} EXCLUDE_FROM_ALL "${CMAKE_CURRENT_SOURCE_DIR}/../suite.cpp"
            "${MARKETLAB_SOURCE_DIR}/matching_engine/persistence/durable_engine.cpp")
        target_include_directories(${check} BEFORE PRIVATE "${overlay}")
        target_link_libraries(${check} PRIVATE marketlab_matching_core)
        target_compile_options(${check} PRIVATE -Wall -Wextra -Wpedantic)
    endif()
endforeach()

add_custom_target(persistence_benchmarks DEPENDS
    persistence_queue_benchmark
    persistence_engine_benchmark_mutex
    persistence_engine_benchmark_spsc_park
    persistence_engine_benchmark_spsc_yield
    persistence_queue_recovery_spsc_park
    persistence_queue_recovery_spsc_yield)
