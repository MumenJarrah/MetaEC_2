add_library(twitter_test twitter_test.cc)
add_executable(twitter_test_multi_client twitter_test_multi_clients.cc)

# Common sanitizer flags for Twitter tests
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

target_compile_options(twitter_test
    PRIVATE
    ${SANITIZER_COMPILE_FLAGS}
)

target_compile_options(twitter_test_multi_client
    PRIVATE
    ${SANITIZER_COMPILE_FLAGS}
)

target_link_options(twitter_test_multi_client
    PRIVATE
    ${SANITIZER_LINK_FLAGS}
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
