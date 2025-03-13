add_library(ycsb_test ycsb_test.cc)
add_executable(ycsb_test_multi_client ycsb_test_multi_client.cc)

target_compile_options(ycsb_test_multi_client
    PRIVATE
    # "-O2"
    ${CMAKE_CXX_FLAGS_DEBUG}
    # "-g"
    # "-D_DEBUG"
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