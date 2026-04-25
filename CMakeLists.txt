cmake_minimum_required(VERSION 3.20)
project(borink_db LANGUAGES CXX VERSION 0.1.0)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(Threads REQUIRED)
find_package(libpqxx CONFIG REQUIRED)
find_package(llfio CONFIG REQUIRED)
find_package(mimalloc CONFIG REQUIRED)

add_executable(borink_db src/main.cpp src/mimalloc.cpp)
target_link_libraries(borink_db PRIVATE libpqxx::pqxx llfio::sl mimalloc)

install(TARGETS borink_db RUNTIME DESTINATION bin)
