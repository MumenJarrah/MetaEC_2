add_library(mncrash_test mncrash_test.cc)
add_executable(mncrash_test_multi_client mncrash_test_multi_clients.cc)
add_executable(cncrash_test cncrash_test.cc)

add_library(mncrash_test_fr mncrash_test_fr.cc)
add_executable(mncrash_test_fr_multi_client mncrash_test_fr_multi_clients.cc)

target_compile_options(mncrash_test_multi_client
    PRIVATE
    # "-O2"
    ${CMAKE_CXX_FLAGS_DEBUG}
    # "-g"
    # "-D_DEBUG"
)

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