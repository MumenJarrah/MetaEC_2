add_library(mncrash_test mncrash_test.cc)
add_executable(mncrash_test_multi_client mncrash_test_multi_clients.cc)
add_executable(cncrash_test cncrash_test.cc)

add_library(mncrash_test_fr mncrash_test_fr.cc)
add_executable(mncrash_test_fr_multi_client mncrash_test_fr_multi_clients.cc)

# Common sanitizer flags for crash tests
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

foreach(tgt mncrash_test_multi_client cncrash_test mncrash_test_fr_multi_client)
    target_compile_options(${tgt}
        PRIVATE
        ${SANITIZER_COMPILE_FLAGS}
    )
    target_link_options(${tgt}
        PRIVATE
        ${SANITIZER_LINK_FLAGS}
    )
endforeach()

target_link_libraries(mncrash_test
    libddckv
    pthread
    ibverbs
)

target_link_libraries(cncrash_test
    libddckv
    pthread
    ibverbs
)

target_link_libraries(mncrash_test_multi_client
    mncrash_test
    libddckv
    pthread
    ibverbs
)

target_link_libraries(mncrash_test_fr
    libddckv
    pthread
    ibverbs
)

target_link_libraries(mncrash_test_fr_multi_client
    mncrash_test_fr
    libddckv
    pthread
    ibverbs
)
