list(APPEND source_ddckv 
    core/nm.cc 
    core/ib.cc 
    core/kv_utils.cc
    core/config.cc
    memory_pool/server.cc
    compute_pool/client.cc
    memory_pool/server_mm.cc
    compute_pool/client_mm.cc
    core/hashtable.cc
    ec_encoding/hashring.cc
    ec_encoding/echashtable.cc
    ec_encoding/rscoding.cc
    ec_encoding/city_hash.cc
    core/logging.hpp
    core/super_queue.cc
    ec_cache/lrucache.hpp
    ec_cache/ecmeta_cache.cc
    core/circular_queue.h
    ec_encoding/buffer.cc
    ec_log/ec_log.cc
    ec_encoding/hashringtable.cc
    fragec/clientfr.cc
    fragec/clientfr_mm.cc
    fragec/serverfr.cc
    fragec/serverfr_mm.cc
    )

add_library(libddckv STATIC ${source_ddckv})

# Propagate RDMA log level compile definition from top-level CMake option
if(DEFINED RDMA_LOG_LEVEL_DEF)
    target_compile_definitions(libddckv PRIVATE RDMA_LOG_LEVEL=${RDMA_LOG_LEVEL_DEF})
else()
    # default to DBG if not defined
    target_compile_definitions(libddckv PRIVATE RDMA_LOG_LEVEL=1)
endif()

target_compile_options(
    libddckv
    PRIVATE
    "-O0"
    # ${CMAKE_CXX_FLAGS_DEBUG}
    "-g"
    "-D_DEBUG"
)

target_link_libraries(libddckv /usr/local/lib/libboost_context.a ${Boost_LIBRARIES} boost_context boost_fiber tbb isal boost_coroutine)
# target_link_libraries(libddckv ${Boost_LIBRARIES} boost_context boost_fiber tbb isal boost_coroutine profiler)

add_subdirectory(compute_pool)
add_subdirectory(core)
add_subdirectory(ec_encoding)
add_subdirectory(memory_pool)
add_subdirectory(ec_cache)
add_subdirectory(ec_log)
add_subdirectory(fragec)

# file(GLOB SOURCES "*.hpp")
# add_library(rlib STATIC ${SOURCES})
# set_target_properties(rlib PROPERTIES LINKER_LANGUAGE CXX)
# target_link_libraries(rlib ibverbs pthread libddckv)
