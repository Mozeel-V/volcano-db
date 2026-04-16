cmake_minimum_required(VERSION 3.16)
project(SimpleQueryProcessor LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Find Flex and Bison
find_package(FLEX REQUIRED)
find_package(BISON REQUIRED)

# Generate lexer and parser
BISON_TARGET(SQLParser
    ${CMAKE_SOURCE_DIR}/src/parser/sql_parser.y
    ${CMAKE_BINARY_DIR}/sql_parser.tab.cpp
    DEFINES_FILE ${CMAKE_BINARY_DIR}/sql_parser.tab.h
    COMPILE_FLAGS "-d -v"
)

FLEX_TARGET(SQLLexer
    ${CMAKE_SOURCE_DIR}/src/parser/sql_lexer.l
    ${CMAKE_BINARY_DIR}/sql_lexer.cpp
)

ADD_FLEX_BISON_DEPENDENCY(SQLLexer SQLParser)

# Include directories
include_directories(
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_BINARY_DIR}
)

# Core library sources (everything except main.cpp)
set(LIB_SOURCES
    src/ast/ast.cpp
    src/storage/table.cpp
    src/storage/catalog.cpp
    src/storage/index.cpp
    src/storage/transaction.cpp
    src/storage/lock_manager.cpp
    src/storage/wal.cpp
    src/planner/logical_plan.cpp
    src/planner/physical_plan.cpp
    src/optimizer/rule_optimizer.cpp
    src/optimizer/cost_optimizer.cpp
    src/executor/executor.cpp
    src/executor/view_support.cpp
    src/executor/operators.cpp
    src/benchmark/benchmark.cpp
    src/benchmark/data_generator.cpp
    ${BISON_SQLParser_OUTPUTS}
    ${FLEX_SQLLexer_OUTPUTS}
)

# Main executable
add_executable(vdb src/main.cpp ${LIB_SOURCES})
target_compile_options(vdb PRIVATE -O2 -Wall)

# ----- Tests (Catch2) -----
# Try system-installed Catch2 first; fall back to downloading if not found
find_package(Catch2 3 QUIET)
if(NOT Catch2_FOUND)
    message(STATUS "Catch2 not found locally, fetching from GitHub...")
    Include(FetchContent)
    FetchContent_Declare(
        Catch2
        GIT_REPOSITORY https://github.com/catchorg/Catch2.git
        GIT_TAG        v3.5.2
    )
    FetchContent_MakeAvailable(Catch2)
endif()

add_executable(vdb_tests
    tests/test_main.cpp
    tests/test_commands.cpp
    ${LIB_SOURCES}
)
target_link_libraries(vdb_tests PRIVATE Catch2::Catch2WithMain)
target_compile_options(vdb_tests PRIVATE -O2 -Wall)

include(CTest)
include(Catch)
catch_discover_tests(vdb_tests)
