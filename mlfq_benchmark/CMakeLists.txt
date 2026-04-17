cmake_minimum_required(VERSION 3.14)
project(MLFQBenchmark)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 1. Define the DuckDB root directory portably
set(DUCKDB_DIR "${CMAKE_CURRENT_SOURCE_DIR}/..")

# 2. Include the headers
include_directories(${DUCKDB_DIR}/src/include)

# 3. Add the executable
add_executable(mlfq_bench main.cpp)

# 4. Portably search for the DuckDB compiled library.
# find_library automatically looks for libduckdb.dylib (Mac), libduckdb.so (Linux), or duckdb.lib (Windows)
find_library(DUCKDB_LIB
    NAMES duckdb
    HINTS
        "${DUCKDB_DIR}/build/release/src"     # Standard Unix Makefiles
        "${DUCKDB_DIR}/build/release"         # Alternative standard layout
        "${DUCKDB_DIR}/build/src/Release"     # Xcode / Ninja generators
    REQUIRED
)

# 5. Link the library we just found
target_link_libraries(mlfq_bench ${DUCKDB_LIB})