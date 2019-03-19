cmake_minimum_required(VERSION 3.1)

project(tarantool-cpp)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED on)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Winline")

include_directories(.)

add_executable(bench bench/bench.cpp)
target_link_libraries(bench tarantool)

add_executable(tests test.cpp)
target_link_libraries(tests tarantool)
