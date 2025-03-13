cmake_minimum_required(VERSION 3.16 FATAL_ERROR)

project(DDCKV LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 11)
SET(CMAKE_C_COMPILER /home/hadoop/lt/gccenv/gcc-7/bin)

# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall")
# set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall")

# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2")
# set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O2")

# set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -pg")
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pg")

find_package(Boost REQUIRED)
set(CMAKE_BUILD_TYPE Release)

include_directories(src)
include_directories(micro-test)
include_directories(module-test)
include_directories(ycsb-test)
include_directories(twitter-test)
include_directories(crash-test)

add_subdirectory(src)
add_subdirectory(micro-test)
add_subdirectory(module-test)
add_subdirectory(ycsb-test)
add_subdirectory(twitter-test)
add_subdirectory(crash-test)

enable_testing()

