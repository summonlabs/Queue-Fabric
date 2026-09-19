cmake_minimum_required(VERSION 3.25)

# Independent downstream consumer. This project is deliberately NOT part of the
# Queue Fabric build: it validates that the installed package can be found,
# linked and used by an unrelated CMake project through find_package(qf).
project(qf_consumer LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(qf 1.0 CONFIG REQUIRED)

add_executable(qf_consumer consumer_main.cpp)
target_link_libraries(qf_consumer PRIVATE qf::queue_fabric)

if(MSVC)
  target_compile_options(qf_consumer PRIVATE /W4 /WX)
else()
  target_compile_options(qf_consumer PRIVATE -Wall -Wextra -Werror)
endif()

enable_testing()
add_test(NAME qf_consumer COMMAND qf_consumer)
