add_library(twitter_test twitter_test.cc)
add_executable(twitter_test_multi_client twitter_test_multi_clients.cc)

target_compile_options(twitter_test_multi_client
    PRIVATE
    # "-O2"
    ${CMAKE_CXX_FLAGS_DEBUG}
    # "-g"
    # "-D_DEBUG"
)

target_link_libraries(twitter_test
    libddckv
    pthread
    ibverbs
)

target_link_libraries(twitter_test_multi_client
    twitter_test
    libddckv
    pthread
    ibverbs
)