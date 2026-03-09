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

# Source files
set(SOURCES
    src/main.cpp
    src/ast/ast.cpp
    src/storage/table.cpp
    src/storage/catalog.cpp
    src/storage/index.cpp
    src/planner/logical_plan.cpp
    src/planner/physical_plan.cpp
    src/optimizer/rule_optimizer.cpp
    src/optimizer/cost_optimizer.cpp
    src/executor/executor.cpp
    src/executor/operators.cpp
    src/benchmark/benchmark.cpp
    src/benchmark/data_generator.cpp
    ${BISON_SQLParser_OUTPUTS}
    ${FLEX_SQLLexer_OUTPUTS}
)

add_executable(sqp ${SOURCES})
target_compile_options(sqp PRIVATE -O2 -Wall)
