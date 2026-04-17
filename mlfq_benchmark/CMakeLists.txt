cmake_minimum_required(VERSION 3.14)
project(MLFQBenchmark)

# C++17 is required for standard DuckDB builds
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Point to your compiled DuckDB source headers and library
set(DUCKDB_DIR "../")
include_directories(${DUCKDB_DIR}/src/include)

# Windows: built with cmake --build build --config Release
link_directories(${DUCKDB_DIR}/build/src/Release)

add_executable(mlfq_bench main.cpp)

# Link the DuckDB C++ library
target_link_libraries(mlfq_bench duckdb)