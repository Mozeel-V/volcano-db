# Simple Query Processor & Optimizer (SQP)

A SQL query processor and optimizer built from scratch in C++17 using Flex and Bison. It parses SQL queries into an AST, converts them to logical plans, applies rule-based and cost-based optimizations, and executes them against an in-memory storage engine.

## Prerequisites

- **CMake** >= 3.16
- **C++17** compatible compiler (GCC 7+, Clang 5+, AppleClang 10+)
- **Flex** (lexer generator)
- **Bison** (parser generator)

### Installing dependencies

**macOS (Homebrew):**
```bash
brew install cmake flex bison
```

**Ubuntu / Debian:**
```bash
sudo apt-get install cmake g++ flex bison
```

## Building

```bash
mkdir build && cd build
cmake ..
make
```

This produces the `sqp` executable in the `build/` directory.

## Running

```bash
./sqp
```

This starts an interactive REPL. Type `.help` for a list of commands.

### Quick start

```sql
-- Generate sample data (employees, departments, orders)
.generate 10000

-- Run a query
SELECT name, salary FROM employees WHERE salary > 100000 ORDER BY salary DESC LIMIT 10;

-- Join with aliases
SELECT e.name, d.budget FROM employees e JOIN departments d ON e.dept = d.dept_name WHERE d.budget > 500000;

-- Aggregation
SELECT dept, COUNT(*), AVG(salary) FROM employees GROUP BY dept ORDER BY dept;

-- View the query plan
EXPLAIN SELECT name, salary FROM employees WHERE salary > 100000;

-- Create a logical view (recomputed on each query)
CREATE VIEW high_earners AS
SELECT name, salary FROM employees WHERE salary > 100000;

-- Create a materialized view (snapshot at creation time)
CREATE MATERIALIZED VIEW top_departments AS
SELECT dept, COUNT(*) FROM employees GROUP BY dept;

-- Run the built-in benchmark suite
.benchmark
```

### REPL commands

| Command | Description |
|---|---|
| `.help` | Show help |
| `.tables` | List loaded tables |
| `.schema <table>` | Show table schema |
| `.generate <n>` | Generate sample data with n employee rows |
| `.benchmark` | Run benchmark suite (optimized vs unoptimized) |
| `.quit` / `.exit` | Exit |

## Architecture

The query processing pipeline follows a classical design:

```
SQL string ──► Parser ──► AST ──► Logical Plan ──► Optimizer ──► Physical Plan ──► Executor ──► Result
```

### Project structure

```
src/
├── ast/              # Abstract Syntax Tree
│   ├── ast.h         #   Expr, TableRef, SelectStmt, Statement types
│   └── ast.cpp       #   Factory methods and pretty-printing
├── parser/           # Flex/Bison SQL parser
│   ├── sql_lexer.l   #   Lexer — tokenizes SQL keywords, operators, literals
│   ├── sql_parser.y  #   Grammar — full SELECT, JOIN, GROUP BY, subqueries, etc.
│   └── parser_types.h#   Shared types between parser and lexer
├── storage/          # In-memory storage engine
│   ├── storage.h     #   Table, HashIndex, Catalog, Value type definitions
│   ├── table.cpp     #   Table operations (insert, load CSV, distinct values)
│   ├── catalog.cpp   #   Catalog + value helper functions (comparison, arithmetic)
│   └── index.cpp     #   Hash index build and lookup
├── planner/          # Query plan generation
│   ├── planner.h     #   LogicalNode type, build_logical_plan() declaration
│   ├── logical_plan.cpp  # AST → logical plan tree conversion
│   └── physical_plan.cpp # Physical plan annotation (join algorithm selection)
├── optimizer/        # Query optimization
│   ├── optimizer.h       # optimize_rules(), optimize_cost(), optimize()
│   ├── rule_optimizer.cpp# Selection pushdown, projection pushdown
│   └── cost_optimizer.cpp# Selectivity estimation, cost annotation, hash join selection
├── executor/         # Query execution engine
│   ├── executor.h    #   ExecStats, ExecResult, execute()
│   ├── executor.cpp  #   Expression evaluator + all operator implementations
│   └── operators.cpp #   (Operator stubs)
├── benchmark/        # Performance benchmarking
│   ├── benchmark.h   #   Benchmark and data generation declarations
│   ├── data_generator.cpp # Synthetic data generation (employees, departments, orders)
│   └── benchmark.cpp #   Benchmark suite comparing optimized vs unoptimized plans
└── main.cpp          # REPL driver, parse_sql() bridge to Flex/Bison
```

### Key components

**Parser** — Flex tokenizes SQL into keywords, operators, and literals. Bison parses tokens into an AST using a precedence-climbing expression grammar. Supports `SELECT` (with `DISTINCT`, `JOIN`, `WHERE`, `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`/`OFFSET`), `CREATE TABLE`, `CREATE INDEX`, `CREATE VIEW`, `CREATE MATERIALIZED VIEW`, `INSERT`, `LOAD`, `EXPLAIN`, and `BENCHMARK`.

**AST** (`ast::Expr`, `ast::SelectStmt`, `ast::Statement`) — Tree representation of parsed SQL. Expressions cover column refs, literals, binary/unary ops, function calls (aggregates), subqueries, `IN`, `BETWEEN`, `LIKE`, `CASE`.

**Storage** (`storage::Table`, `storage::Catalog`, `storage::HashIndex`) — In-memory row store. `Value` is a `std::variant<std::monostate, int64_t, double, std::string>`. The catalog manages tables and provides statistics (row counts, distinct values) for the cost optimizer.

**Planner** (`planner::LogicalNode`) — Converts the AST into a tree of logical operators: `TABLE_SCAN`, `FILTER`, `PROJECTION`, `JOIN`, `AGGREGATION`, `SORT`, `LIMIT`, `DISTINCT`.

**Optimizer** — Two-phase optimization:
1. **Rule-based** (`optimize_rules`): Selection pushdown (push filters below joins/projections), projection pushdown.
2. **Cost-based** (`optimize_cost`): Estimates selectivity and row counts, annotates cost on each node, selects hash join over nested-loop join when estimated comparisons exceed a threshold.

**Executor** — Volcano-style pull-based execution. Implements sequential scan, filter, projection, nested-loop join, hash join, aggregation, sort, limit, and distinct operators. The expression evaluator handles all `BinOp`/`UnaryOp` types, NULL propagation, and string `LIKE` matching.

## Supported SQL

```sql
-- Queries
SELECT [DISTINCT] <columns> FROM <tables>
  [JOIN <table> ON <condition>]
  [WHERE <condition>]
  [GROUP BY <columns>]
  [HAVING <condition>]
  [ORDER BY <columns> [ASC|DESC]]
  [LIMIT n [OFFSET m]];

-- DDL / DML
CREATE TABLE <name> (<col> <type>, ...);
CREATE INDEX <name> ON <table> (<col>) [USING HASH|BTREE];
CREATE VIEW <name> AS <query>;
CREATE MATERIALIZED VIEW <name> AS <query>;
INSERT INTO <table> VALUES (...);
LOAD <table> FROM '<file.csv>';

-- Analysis
EXPLAIN <query>;
EXPLAIN ANALYZE <query>;
```

## Extending the project

- **Add new operators**: Define a new `LogicalNodeType` in [src/planner/planner.h](src/planner/planner.h), handle it in the logical plan builder, optimizer, and executor.
- **Add optimization rules**: Add new transformation functions in [src/optimizer/rule_optimizer.cpp](src/optimizer/rule_optimizer.cpp) and call them from `optimize_rules()`.
- **Add new data types**: Extend the `Value` variant in [src/storage/storage.h](src/storage/storage.h) and update the comparison/arithmetic helpers in [src/storage/catalog.cpp](src/storage/catalog.cpp).
- **Add persistent storage**: Replace the in-memory `Table::rows` with a disk-backed page structure.
- **Add new SQL syntax**: Add tokens in [src/parser/sql_lexer.l](src/parser/sql_lexer.l), grammar rules in [src/parser/sql_parser.y](src/parser/sql_parser.y), and corresponding AST nodes in [src/ast/ast.h](src/ast/ast.h).

> **Note on Bison 2.3**: macOS ships with Bison 2.3 which uses `#define` macros for tokens (e.g., `#define AND 262`). All C++ enum values are prefixed (`OP_AND`, `JT_INNER`, `ST_SELECT`, etc.) to avoid collisions. If upgrading to Bison 3.x+, these prefixes are still safe but the collision risk goes away.