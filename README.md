# Simple Query Processor & Optimizer (SQP)

A SQL query processor and optimizer built from scratch in C++17 using Flex and Bison. It parses SQL queries into an AST, converts them to logical plans, applies rule-based and cost-based optimizations, and executes them against an in-memory storage engine.

<p align="center">
    <a href="#prerequisites">Prerequisites</a> •
    <a href="#building">Building</a> •
    <a href="#running">Running</a> •
    <a href="#architecture">Architecture</a> •
    <a href="#supported-sql">Supported SQL</a> •
    <a href="#sqp-test-suite-documentation">Testing</a>
</p>

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

You can also execute commands from a SQL file:

```bash
./sqp path/to/script.sql
```

or

```bash
./sqp --file path/to/script.sql
```

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

-- Transactional writes (MVP)
BEGIN;
INSERT INTO employees VALUES (10001, 'Temp User', 'Engineering', 120000, 30);
ROLLBACK;

-- Save all current tables to a formatted text dump
.save sqp_dump.txt
```

### REPL commands

| Command | Description |
|---|---|
| `.help` | Show help |
| `.tables` | List loaded tables |
| `.schema <table>` | Show table schema |
| `.generate <n>` | Generate sample data with n employee rows |
| `.save <file>` | Save all current tables to a formatted text file (creates or overwrites) |
| `.source <file>` | Execute SQL commands from a file |
| `.plan` | Show last EXPLAIN plan (tree format) |
| `.plan dot` | Show last EXPLAIN plan (Graphviz DOT format) |
| `.triggers` | List all defined triggers |
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
│   ├── index.cpp     #   Hash/B-tree index build and lookup
│   ├── transaction.h #   Undo-log transaction manager (BEGIN/COMMIT/ROLLBACK)
│   ├── transaction.cpp
│   ├── lock_manager.h#   Table lock manager (shared/exclusive locks)
│   ├── lock_manager.cpp
│   ├── wal.h         #   Write-ahead log and recovery interfaces
│   └── wal.cpp
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

**Parser** — Flex tokenizes SQL into keywords, operators, and literals. Bison parses tokens into an AST using a precedence-climbing expression grammar. Supports `SELECT` (with `DISTINCT`, `JOIN`, `WHERE`, `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`/`OFFSET`), predicate operators including `IN`, `NOT IN`, `EXISTS`, `NOT EXISTS`, and quantified subquery predicates (`SOME`/`ANY`, `ALL`), plus expression forms like `CASE WHEN ... THEN ... ELSE ... END`, and `CREATE TABLE`, `CREATE INDEX`, `CREATE VIEW`, `CREATE MATERIALIZED VIEW`, `INSERT`, `UPDATE`, `DELETE`, `ALTER TABLE`, `DROP TABLE/INDEX/VIEW`, `TRUNCATE`, `LOAD`, `EXPLAIN`, `BENCHMARK`, and transaction statements `BEGIN [TRANSACTION]`, `COMMIT`, `ROLLBACK`.

**AST** (`ast::Expr`, `ast::SelectStmt`, `ast::Statement`) — Tree representation of parsed SQL. Expressions cover column refs, literals, binary/unary ops, function calls (aggregates), subqueries, `IN`/`NOT IN`, `EXISTS`/`NOT EXISTS`, quantified predicates (`SOME`/`ANY`, `ALL`), `BETWEEN`, `LIKE`, `CASE`.

**Storage** (`storage::Table`, `storage::Catalog`, `storage::HashIndex`) — In-memory row store. `Value` is a `std::variant<std::monostate, int64_t, double, std::string>`. The catalog manages tables and provides statistics (row counts, distinct values) for the cost optimizer.

**Planner** (`planner::LogicalNode`) — Converts the AST into a tree of logical operators: `TABLE_SCAN`, `FILTER`, `PROJECTION`, `JOIN`, `AGGREGATION`, `SORT`, `LIMIT`, `DISTINCT`.

**Optimizer** — Two-phase optimization:
1. **Rule-based** (`optimize_rules`): Selection pushdown (push filters below joins/projections), projection pushdown.
2. **Cost-based** (`optimize_cost`): Estimates selectivity and row counts, annotates cost on each node, selects hash join over nested-loop join when estimated comparisons exceed a threshold.

**Transactions** — `TransactionManager` maintains per-transaction undo records for row-level `INSERT`/`UPDATE`/`DELETE`/`MERGE` changes. `ROLLBACK` replays undo in reverse and rebuilds affected indexes. `COMMIT` clears undo records.

**Isolation** — `LockManager` provides table-level shared/exclusive locks for explicit transactions. Write locks are held until `COMMIT`/`ROLLBACK`; read locks are statement-scoped.

**Durability** — `WalManager` appends transactional row-level WAL records (`BEGIN`/`INSERT`/`UPDATE`/`DELETE`/`COMMIT`/`ROLLBACK`), flushes WAL on `COMMIT` before acknowledgement, checkpoints catalog state, and performs startup recovery by redoing committed transactions after the last checkpoint.

**Executor** — Volcano-style pull-based execution. Implements sequential scan, filter, projection, nested-loop join, hash join, aggregation, sort, limit, and distinct operators. The expression evaluator handles all `BinOp`/`UnaryOp` types, NULL propagation, and string `LIKE` matching.

## Supported SQL

```sql
-- Queries
SELECT [DISTINCT] <columns> FROM <tables>
  [JOIN <table> ON <condition>]
    [WHERE <condition>]   -- includes IN/NOT IN, EXISTS/NOT EXISTS, SOME/ANY/ALL subquery predicates, CASE expressions
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
UPDATE <table> SET <col> = <expr> [WHERE <condition>];
DELETE FROM <table> [WHERE <condition>];
ALTER TABLE <table> ADD COLUMN <col_name> <type>;
ALTER TABLE <table> DROP COLUMN <col_name>;
ALTER TABLE <table> RENAME COLUMN <old> TO <new>;
ALTER TABLE <table> RENAME TO <new_table_name>;
RENAME TABLE <old_name> TO <new_name>;  -- alias for ALTER TABLE ... RENAME TO
DROP TABLE <table_name>;
DROP INDEX <index_name>;
DROP VIEW <view_name>;
TRUNCATE TABLE <table_name>;
TRUNCATE <table_name>;                  -- shorthand (without TABLE keyword)
LOAD <table> '<file.csv>';

-- Transactions (MVP)
BEGIN;
BEGIN TRANSACTION;
COMMIT;
ROLLBACK;

-- Analysis
EXPLAIN <query>;
EXPLAIN ANALYZE <query>;
```

### Transaction notes (current behavior)

- In explicit transactions, SQP currently allows: `SELECT`, `EXPLAIN`, `BENCHMARK`, `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `COMMIT`, `ROLLBACK`.
- DDL statements inside an active transaction are currently rejected.
- WAL-based durability and startup recovery are enabled for explicit transaction writes.

## SQP Test Suite Documentation

Exhaustive test suite for the Simple Query Processor & Optimizer (SQP) using **Catch2 v3**.

### Building & Running

```bash
cd build
cmake ..
make sqp_tests
./sqp_tests               # Run all tests
./sqp_tests "[parser]"    # Run parser tests only
./sqp_tests "[e2e]"       # Run end-to-end tests only
./sqp_tests --list-tests  # List all test names
```

### Test Organization

Tests are organized across `tests/test_main.cpp` (core SQL logic) and `tests/test_commands.cpp` (CLI commands):

| File | Primary focus | Representative tags |
|------|---------------|---------------------|
| `tests/test_main.cpp` | Parser, storage, planner/optimizer, executor internals, benchmark generators, WAL recovery | `[parser]`, `[storage]`, `[planner]`, `[optimizer]`, `[executor]`, `[benchmark]`, `[lock]`, `[wal]`, `[durability]` |
| `tests/test_commands.cpp` | REPL/CLI behavior and end-to-end SQL workflows | `[e2e]`, `[commands]`, `[dml]`, `[ddl]`, `[alter]`, `[merge]`, `[constraint]`, `[trigger]`, `[transaction]`, `[durability]` |

Tag snapshot from `./sqp_tests --list-tags` (counts are per-tag and overlap across tests):

| Area | Tag(s) | Cases |
|------|--------|------:|
| **Total suite** | `all` | **405** |
| Parsing and grammar | `[parser]` | 104 |
| End-to-end SQL | `[e2e]` | 229 |
| CLI and scripts | `[commands]` | 23 |
| Storage core | `[storage]` | 32 |
| Indexing | `[index]` | 19 |
| Transactions | `[transaction]` | 8 |
| Locking | `[lock]` | 5 |
| Durability and WAL | `[durability]`, `[wal]` | 3, 2 |
| Constraints and foreign keys | `[constraint]`, `[fk]` | 25, 11 |
| Planner, optimizer, executor | `[planner]`, `[optimizer]`, `[executor]` | 11, 8, 8 |
| DML and DDL families | `[dml]`, `[ddl]`, `[alter]`, `[merge]` | 15, 28, 16, 5 |

### Features Tested

#### SQL Commands
- `SELECT` (with `*`, column list, expressions, aliases, `CASE WHEN ... THEN ... ELSE ... END`)
- `SELECT DISTINCT`
- `UNION`, `UNION ALL`, `INTERSECT`, `EXCEPT`
- `CREATE TABLE` (INT, FLOAT, VARCHAR, VARCHAR(n), INTEGER, DOUBLE, TEXT)
- `CREATE INDEX` (basic B-Tree, USING HASH, USING BTREE)
- `INSERT INTO ... VALUES` (single row, multi-row, with column validation)
- `UPDATE ... SET ... WHERE` (single/multi column SET, optional WHERE)
- `DELETE FROM ... WHERE` (with or without WHERE clause)
- `ALTER TABLE ... ADD COLUMN` (with NULL backfill for existing rows)
- `ALTER TABLE ... DROP COLUMN` (validates not last column)
- `ALTER TABLE ... RENAME COLUMN ... TO` (updates indexes)
- `ALTER TABLE ... RENAME TO` (updates indexes and views)
- `DROP TABLE <name>` (cascades index removal)
- `DROP INDEX <name>`
- `DROP VIEW <name>`
- `TRUNCATE TABLE <name>` (clears rows, preserves table structure)
- `TRUNCATE <name>` (shorthand without TABLE keyword)
- `MERGE INTO ... USING ... ON ... WHEN MATCHED THEN UPDATE SET ... WHEN NOT MATCHED THEN INSERT VALUES ...` (upsert)
- `CREATE TRIGGER name BEFORE|AFTER INSERT|UPDATE|DELETE ON table FOR EACH ROW EXECUTE 'action_sql'`
- `DROP TRIGGER name`

### Triggers

Event-driven actions that run automatically when `INSERT`, `UPDATE`, or `DELETE` occur. Supports `BEFORE` or `AFTER` timing and multi-statement bodies.

```sql
-- Single statement
CREATE TRIGGER audit_log AFTER INSERT ON users
FOR EACH ROW EXECUTE 'INSERT INTO logs VALUES (1)';

-- Multi-statement block
CREATE TRIGGER update_stats AFTER DELETE ON orders
FOR EACH ROW EXECUTE BEGIN
    'UPDATE summary SET count = count - 1';
    'INSERT INTO log VALUES (2)';
END;

DROP TRIGGER audit_log;
```

**Column constraints** (inline with CREATE TABLE):
- `NOT NULL` — rejects null values on INSERT/UPDATE
- `DEFAULT <value>` — provides fallback when column value is omitted
- `PRIMARY KEY` — implies NOT NULL + UNIQUE, auto-creates BTree index
- `UNIQUE` — rejects duplicate values (multiple NULLs allowed per SQL standard)
- `CHECK (<expr>)` — enforces arbitrary boolean expression on INSERT/UPDATE
- `REFERENCES table(column)` — FOREIGN KEY referential integrity
- `REFERENCES table(column) ON DELETE RESTRICT` — blocks parent DELETE when referenced (default)
- `REFERENCES table(column) ON DELETE CASCADE` — deletes referencing child rows automatically

```sql
CREATE TABLE users (
    id INT PRIMARY KEY,
    email VARCHAR UNIQUE NOT NULL,
    age INT CHECK (age > 0),
    role VARCHAR DEFAULT 'user',
    dept_id INT REFERENCES departments(id)
);
```
- `LOAD table 'file'`
- `.save <file>` (Save current tables to a formatted text file)
- `.source <file>` (Execute SQL commands from file — stops on error)
- `.plan` (Show last EXPLAIN plan in tree format)
- `.plan dot` (Show last EXPLAIN plan in Graphviz DOT format)
- `--file <file>` (Command line script execution — stops on error)
- `EXPLAIN SELECT` (tree-connector plan visualization)
- `EXPLAIN ANALYZE SELECT` (plan + per-node actual execution stats)
- `EXPLAIN FORMAT DOT SELECT` (Graphviz DOT format plan export)
- `BENCHMARK SELECT`

#### SQL Clauses
- `FROM` (single table, multiple tables, aliases, subqueries)
- `WHERE` (all comparison operators, LIKE, BETWEEN, IN, IS NULL, IS NOT NULL, AND, OR, NOT)
- `GROUP BY` (single/multiple columns)
- `HAVING` (with aggregate conditions)
- `ORDER BY` (ASC, DESC, multiple keys)
- `LIMIT` (basic, with OFFSET, edge values like 0)
- `JOIN` (INNER, LEFT, LEFT OUTER, RIGHT, FULL OUTER, CROSS)

#### Expressions
- Integer, float, string, NULL literals
- Column references (unqualified and qualified `table.col`)
- Arithmetic: `+`, `-`, `*`, `/`, `%`
- Comparison: `=`, `!=`, `<>`, `<`, `>`, `<=`, `>=`
- Logical: `AND`, `OR`, `NOT`
- Pattern: `LIKE` (prefix%, %suffix, %substring%, exact)
- Range: `BETWEEN low AND high`
- Set: `IN (list)`, `IN (subquery)`, `EXISTS (subquery)`
- Unary: `-` negation, `IS NULL`, `IS NOT NULL`
- Aggregate: `COUNT(*)`, `COUNT(col)`, `COUNT(DISTINCT col)`, `SUM`, `AVG`, `MIN`, `MAX`
- Parenthesized expressions

#### Case Insensitivity
- All SQL keywords (SELECT, FROM, WHERE, etc.) work in lowercase, uppercase, and mIxEd case
- Aggregate function names (count, COUNT, Count)
- JOIN types, ASC/DESC, EXPLAIN ANALYZE, DISTINCT
- BETWEEN, LIKE, IN, IS NULL, IS NOT NULL
- Data values remain case-sensitive (e.g., LIKE matching)

#### Punctuation & Grammar
- Semicolon statement terminator
- Commas (SELECT list, column definitions, IN lists, ORDER BY)
- Parentheses (expressions, function calls, CREATE TABLE, IN list)
- Dot notation (table.column)
- Single-quoted string literals
- SQL comments (`--` line and `/* */` block)
- Operator precedence (`*` before `+`, `AND` before `OR`)

#### NULL Semantics (SQL Standard)
- `NULL = NULL` → false
- `NULL = x` → false
- `NULL + x` → NULL (arithmetic propagation)
- Division by zero → NULL
- `COUNT(*)` includes NULL rows, `COUNT(col)` excludes
- `SUM`/`AVG` skip NULL values
- `IS NULL` / `IS NOT NULL` work correctly

#### Storage Layer
- Value type operations (comparison, arithmetic, display)
- Table insert, cardinality, distinct_values
- Catalog table management
- Hash index build and lookup (int and string keys)

#### Query Planner
- Correct node type generation for each clause
- TABLE_SCAN, FILTER, PROJECTION, JOIN, AGGREGATION, SORT, LIMIT, DISTINCT nodes

#### Optimizer
- Rule-based: selection pushdown below joins
- Cost-based: estimates populated, hash join selection
- Optimization does not change query results

#### Edge Cases
- Empty tables (scan, COUNT)
- Single-row tables
- WHERE matching zero rows
- LIMIT 0, LIMIT > total rows, OFFSET > total rows
- Very long strings (10,000 chars)
- Negative integers, zero comparisons
- All-NULL columns
- Self-join
- Multiple identical rows for DISTINCT
