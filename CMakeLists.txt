cmake_minimum_required(VERSION 3.16)
project(SiteRepairCast LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(OpenSSL REQUIRED)

add_library(srcast_common src/checksum.cpp)
target_include_directories(srcast_common PUBLIC include)
target_link_libraries(srcast_common PUBLIC OpenSSL::Crypto)
target_compile_options(srcast_common PRIVATE -Wall -Wextra -Wpedantic -Wconversion)

add_executable(site_proxy_sender src/sender.cpp)
target_link_libraries(site_proxy_sender PRIVATE srcast_common)
target_compile_options(site_proxy_sender PRIVATE -Wall -Wextra -Wpedantic -Wconversion)

add_executable(receiver_agent src/receiver.cpp)
target_link_libraries(receiver_agent PRIVATE srcast_common)
target_compile_options(receiver_agent PRIVATE -Wall -Wextra -Wpedantic -Wconversion)
