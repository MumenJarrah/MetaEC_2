add_executable(test_server test_server.cc)
add_executable(test_multi_client test_multi_client.cc)
add_executable(test_client_cache test_client_cache.cc)
add_executable(test_memory_consumption test_memory_consumption.cc)
add_executable(test_clientfr_latency test_clientfr_latency.cc)
add_executable(test_serverfr test_serverfr.cc)
add_executable(test_multi_clientfr test_multi_clientfr.cc)
add_executable(test_race test_race.cc)

target_compile_options(
    test_multi_client
    PRIVATE
    "-O0"
    # ${CMAKE_CXX_FLAGS_DEBUG}
    "-g"
    "-fsanitize=address,undefined"
    -fno-omit-frame-pointer
    "-D_DEBUG"
)

target_link_options(test_multi_client PRIVATE
    -fsanitize=address,undefined
    -fno-omit-frame-pointer
)

target_link_options(test_server PRIVATE
    -fsanitize=address,undefined
    -fno-omit-frame-pointer
)

target_link_options(test_memory_consumption PRIVATE
    -fsanitize=address,undefined
    -fno-omit-frame-pointer
)

target_link_options(test_clientfr_latency PRIVATE
    -fsanitize=address,undefined
    -fno-omit-frame-pointer
)



target_link_libraries(test_client_cache
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