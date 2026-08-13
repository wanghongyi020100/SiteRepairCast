cmake_minimum_required(VERSION 3.16)
project(SiteRepairCast LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(OpenSSL REQUIRED)
find_package(Threads REQUIRED)

add_library(srcast_common src/checksum.cpp)
target_include_directories(srcast_common PUBLIC include)
target_link_libraries(srcast_common PUBLIC OpenSSL::Crypto)
target_compile_options(srcast_common PRIVATE -Wall -Wextra -Wpedantic -Wconversion)

add_executable(site_proxy_sender src/sender.cpp)
target_link_libraries(site_proxy_sender PRIVATE srcast_common Threads::Threads)
target_compile_options(site_proxy_sender PRIVATE -Wall -Wextra -Wpedantic -Wconversion)

add_executable(receiver_agent src/receiver.cpp)
target_link_libraries(receiver_agent PRIVATE srcast_common Threads::Threads)
target_compile_options(receiver_agent PRIVATE -Wall -Wextra -Wpedantic -Wconversion)

add_executable(central_sender src/central_sender.cpp)
target_link_libraries(central_sender PRIVATE srcast_common Threads::Threads)
target_compile_options(central_sender PRIVATE -Wall -Wextra -Wpedantic -Wconversion)

find_program(BASH_EXECUTABLE bash)
if(BASH_EXECUTABLE)
    set(SMOKE_TEST_BUILD_DIRS
        ${CMAKE_SOURCE_DIR}/build-local-repair-smoke
        ${CMAKE_SOURCE_DIR}/build-central-cache-smoke
        ${CMAKE_SOURCE_DIR}/build-section-flow-smoke
        ${CMAKE_SOURCE_DIR}/build-central-resume-smoke
        ${CMAKE_SOURCE_DIR}/build-tcp-backfill-smoke
        ${CMAKE_SOURCE_DIR}/build-fault-recovery-smoke
        ${CMAKE_SOURCE_DIR}/build-benchmark-smoke
        ${CMAKE_SOURCE_DIR}/build-multi-proxy-smoke
    )

    add_custom_target(test
        COMMAND ${BASH_EXECUTABLE} ${CMAKE_SOURCE_DIR}/scripts/all_smoke_test.sh
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        USES_TERMINAL
    )

    add_custom_target(cleantest
        COMMAND ${CMAKE_COMMAND} -E rm -rf
            ${SMOKE_TEST_BUILD_DIRS}
            ${CMAKE_SOURCE_DIR}/artifacts/benchmark
        COMMAND ${BASH_EXECUTABLE} ${CMAKE_SOURCE_DIR}/scripts/all_smoke_test.sh
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        USES_TERMINAL
    )
endif()
