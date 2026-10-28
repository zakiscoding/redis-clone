cmake_minimum_required(VERSION 3.16)

project(redis-clone C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

add_executable(redis
    src/main.c
    src/commands.c
    src/store.c
    src/client.c
    src/aof.c
    src/loop.c
)

add_executable(redis-bench src/bench.c)
add_executable(repro-old-bugs tests/repro_old_bugs.c)

if(NOT MSVC)
    foreach(target redis redis-bench repro-old-bugs)
        target_compile_options(${target} PRIVATE -Wall -Wextra -Wno-unused-parameter)
    endforeach()
endif()

if(WIN32)
    target_link_libraries(redis PRIVATE ws2_32)
    target_link_libraries(redis-bench PRIVATE ws2_32 psapi)
    target_link_libraries(repro-old-bugs PRIVATE ws2_32)
else()
    find_package(Threads REQUIRED)
    target_link_libraries(redis PRIVATE Threads::Threads)
    target_link_libraries(redis-bench PRIVATE Threads::Threads)
endif()

option(ENABLE_ASAN "Build with AddressSanitizer" OFF)
if(ENABLE_ASAN)
    foreach(target redis redis-bench repro-old-bugs)
        target_compile_options(${target} PRIVATE -fsanitize=address -fno-omit-frame-pointer -g -O1)
        target_link_options(${target} PRIVATE -fsanitize=address)
    endforeach()
endif()
