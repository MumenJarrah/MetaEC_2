add_executable(test_server test_server.cc)
add_executable(test_multi_client test_multi_client.cc)
add_executable(test_client_cache test_client_cache.cc)
add_executable(test_memory_consumption test_memory_consumption.cc)
add_executable(test_clientfr_latency test_clientfr_latency.cc)
add_executable(test_serverfr test_serverfr.cc)
add_executable(test_multi_clientfr test_multi_clientfr.cc)
add_executable(test_race test_race.cc)

# Common sanitizer flags for all micro tests (disabled for test_server due to huge page issues)
set(SANITIZER_COMPILE_FLAGS
    "-O0"
    "-g"
    "-fsanitize=address,undefined"
    "-fno-omit-frame-pointer"
    "-D_DEBUG"
)

set(SANITIZER_LINK_FLAGS
    "-fsanitize=address,undefined"
    "-fno-omit-frame-pointer"
)

set(SANITIZED_TESTS
    test_multi_client
    test_client_cache
    test_memory_consumption
    test_clientfr_latency
    test_serverfr
    test_multi_clientfr
    test_race
)

foreach(tgt ${SANITIZED_TESTS})
    target_compile_options(${tgt} PRIVATE ${SANITIZER_COMPILE_FLAGS})
    target_link_options(${tgt} PRIVATE ${SANITIZER_LINK_FLAGS})
endforeach()

target_link_libraries(test_server
    libddckv
    pthread
    ibverbs
)

target_link_libraries(test_client_cache
    libddckv
    pthread
    ibverbs
)

target_link_libraries(test_multi_client
    libddckv
    pthread
    ibverbs
)

target_link_libraries(test_client_cache
    libddckv
    pthread
    ibverbs
)

target_link_libraries(test_memory_consumption
    libddckv
    pthread
    ibverbs
)

target_link_libraries(test_clientfr_latency
    libddckv
    pthread
    ibverbs
)

target_link_libraries(test_race
    libddckv
    pthread
    ibverbs
)

target_link_libraries(test_serverfr
    libddckv
    pthread
    ibverbs
)

target_link_libraries(test_multi_clientfr
    libddckv
    pthread
    ibverbs
)
