add_library(ycsb_test ycsb_test.cc)
add_executable(ycsb_test_multi_client ycsb_test_multi_client.cc)

# Common sanitizer flags for YCSB tests
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

# Optional: sanitize the helper library as well
target_compile_options(ycsb_test
    PRIVATE
    ${SANITIZER_COMPILE_FLAGS}
)

target_compile_options(ycsb_test_multi_client
    PRIVATE
    ${SANITIZER_COMPILE_FLAGS}
)

target_link_options(ycsb_test_multi_client
    PRIVATE
    ${SANITIZER_LINK_FLAGS}
)

target_link_libraries(ycsb_test
    libddckv
    pthread
    ibverbs
)

target_link_libraries(ycsb_test_multi_client
    ycsb_test
    libddckv
    pthread
    ibverbs
)
