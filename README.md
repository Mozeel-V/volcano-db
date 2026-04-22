# VolcanoDB

[![C++17](https://img.shields.io/badge/C%2B%2B-17-00599C?logo=c%2B%2B)](#building)
[![CMake 3.16+](https://img.shields.io/badge/CMake-3.16%2B-064F8C?logo=cmake)](#building)
[![Flex/Bison](https://img.shields.io/badge/Flex%2FBison-Parser%20Toolchain-6A4C93)](#architecture)
[![License](https://img.shields.io/badge/License-Apache%202.0-1B5E20)](LICENSE)

VolcanoDB is a relational SQL engine built in C++17 with Flex and Bison. It parses SQL into an AST, builds logical and physical plans, applies rule-based and cost-based optimization, and executes queries with a Volcano-style engine over an in-memory storage layer with indexes, constraints, views, and triggers. It also provides ACID transaction support for DML using table-level locking, WAL-backed durability, checkpointing, and startup recovery.

<p align="center">
    <a href="#prerequisites">Prerequisites</a> •
    <a href="#building">Building</a> •
    <a href="#running">Running</a> •
    <a href="#architecture">Architecture</a> •
    <a href="#supported-sql">Supported SQL</a> •
    <a href="#test-suite-documentation">Testing</a>
</p>

## Prerequisites

- **CMake** >= 3.16
- **C++17** compatible compiler (GCC 7+, Clang 5+, AppleClang 10+)
- **Flex** (lexer generator)
- **Bison** (parser generator)

### Installing dependencies

**Windows (MSYS2 + MinGW-w64, recommended):**

1. Install MSYS2 from https://www.msys2.org/
2. Open the **MSYS2 MinGW x64** shell and run:

```bash
pacman -Syu
pacman -S --needed mingw-w64-x86_64-toolchain mingw-w64-x86_64-cmake mingw-w64-x86_64-flex mingw-w64-x86_64-bison make
```

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

This produces the `vdb` executable (VolcanoDB CLI) in the `build/` directory.

## Running

```bash
./vdb
```

This starts an interactive REPL. Type `.help` for a list of commands.

You can also execute commands from a SQL file:

```bash
./vdb path/to/script.sql
```

or

```bash
./vdb --file path/to/script.sql
```

### Server mode

Run VolcanoDB as a TCP server:

```bash
./vdb --server --host 127.0.0.1 --port 54330
```

Enable password mode:

```bash
./vdb --server --host 127.0.0.1 --port 54330 --auth-mode password
```

In password mode, the server bootstraps a superuser `admin` with password `admin` if missing. Change it immediately:

```sql
ALTER USER admin IDENTIFIED BY 'your_strong_password';
```

The server currently uses a text-based native protocol documented in [docs/protocol.md](docs/protocol.md).

#### What the server does

1. Opens a TCP listener on the configured host and port.
2. Accepts multiple concurrent client connections.
3. Assigns each connection an endpoint session identity (`IP:port`) and sends it during handshake.
4. Supports protocol control commands:
  - `PING` -> `PONG`
  - `QUIT` / `.quit` / `.exit` -> `BYE`
5. Executes SQL sent over the socket and returns response envelopes:
  - `CONTINUE` for incomplete statements (no trailing `;` yet)
  - `OK` or `ERROR` once statement execution finishes
  - `END` to terminate each response body
6. Reuses the existing SQL engine pipeline (parser/planner/optimizer/executor).

#### Authentication and authorization (current behavior)

When running with `--auth-mode password`:

1. Server-side auth handshake is required before SQL execution:
  - `AUTH_START <username>`
  - `AUTH_CHALLENGE <salt_hex> <nonce> sha256`
  - `AUTH_PROOF <proof_hex>`
  - `AUTH_OK <username>` or `AUTH_ERROR <reason>`
2. Unauthenticated SQL requests are rejected with `auth_required`.
3. SQL identity is principal-based (`username`) and is distinct from endpoint identity (`IP:port`).
4. User and privilege SQL is supported:
  - `CREATE USER ... IDENTIFIED BY ...`
  - `ALTER USER ... IDENTIFIED BY ...`
  - `DROP USER ...`
  - `GRANT ... ON TABLE|VIEW|FUNCTION ... TO ...`
  - `REVOKE ... ON TABLE|VIEW|FUNCTION ... FROM ...`
5. In password mode, a bootstrap superuser `admin` is auto-created if missing.

Current authorization enforcement:

1. Enforced for `SELECT`/`EXPLAIN` table reads and for key DML/DDL paths (`INSERT`, `UPDATE`, `DELETE`, `CREATE INDEX`, `ALTER TABLE`, `DROP TABLE`, `DROP VIEW`, `DROP FUNCTION`).
2. Superusers bypass privilege checks.
3. Object owners are treated as authorized for their owned object.

Protocol handshake example:

```text
HELLO VDB
SESSION 127.0.0.1:60344
```

SQL response example:

```text
OK
<engine output lines>
END
```

Session identity model:

1. One active session per connected client endpoint (`IP:port`).
2. Different server endpoints (`host:port`) represent separate server instances.

Python native client smoke test:

```bash
python clients/python/smoke_test.py --host 127.0.0.1 --port 54330
```

Node native client smoke test:

```bash
node clients/node/smoke_test.js --host 127.0.0.1 --port 54330
```

Node client integration tests:

```bash
npm --prefix clients/node test
```

#### Current constraints

1. Protocol is text, newline-delimited, not a fully structured binary wire protocol yet.
2. Server bind host currently supports IPv4 values.
3. SQL execution in server mode is serialized through a shared engine mutex.
4. Runtime is memory-first: restart clears live state unless data is explicitly exported and reloaded.
5. `--auth-mode password` is implemented; request-size caps, idle timeout controls, and connection-limit flags are still planned.
6. Non-QUIT dot commands are not part of the TCP SQL protocol surface.

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
.save volcanodb_dump.txt
```

### REPL commands

| Command | Description |
|---|---|
| `.help` | Show help |
| `.functions [builtins\|udf]` | List built-in and/or user-defined SQL functions |
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
│   ├── rule_optimizer.cpp# Constant folding + selection/projection pushdown
│   └── cost_optimizer.cpp# Selectivity estimation, cost annotation, hash join selection
├── executor/         # Query execution engine
│   ├── executor.h    #   ExecStats, ExecResult, execute()
│   ├── functions.h   #   Scalar built-in function registry helpers
│   ├── functions.cpp
│   ├── executor.cpp  #   Expression evaluator + all operator implementations
│   └── operators.cpp #   (Operator stubs)
├── benchmark/        # Performance benchmarking
│   ├── benchmark.h   #   Benchmark and data generation declarations
│   ├── data_generator.cpp # Synthetic data generation (employees, departments, orders)
│   └── benchmark.cpp #   Benchmark suite comparing optimized vs unoptimized plans
└── main.cpp          # REPL driver, parse_sql() bridge to Flex/Bison
```

### Key components

**Parser** — Flex tokenizes SQL into keywords, operators, and literals. Bison parses tokens into an AST using a precedence-climbing expression grammar. Supports `SELECT` (with `DISTINCT`, `JOIN`, `WHERE`, `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`/`OFFSET`), predicate operators including `IN`, `NOT IN`, `EXISTS`, `NOT EXISTS`, and quantified subquery predicates (`SOME`/`ANY`, `ALL`), plus expression forms like `CASE WHEN ... THEN ... ELSE ... END`, scalar and aggregate function calls, and window `OVER (PARTITION BY ... ORDER BY ...)` clauses. DDL/DML support includes `CREATE TABLE`, `CREATE INDEX`, `CREATE VIEW`, `CREATE MATERIALIZED VIEW`, `CREATE FUNCTION`, `INSERT`, `UPDATE`, `DELETE`, `ALTER TABLE`, `DROP TABLE/INDEX/VIEW/FUNCTION`, `TRUNCATE`, `LOAD`, `EXPLAIN`, `BENCHMARK`, and transaction statements `BEGIN [TRANSACTION]`, `COMMIT`, `ROLLBACK`.

**AST** (`ast::Expr`, `ast::SelectStmt`, `ast::Statement`) — Tree representation of parsed SQL. Expressions cover column refs, literals, binary/unary ops, function calls (scalar, aggregate, window, and UDF invocations), subqueries, `IN`/`NOT IN`, `EXISTS`/`NOT EXISTS`, quantified predicates (`SOME`/`ANY`, `ALL`), `BETWEEN`, `LIKE`, `CASE`.

**Storage** (`storage::Table`, `storage::Catalog`, `storage::HashIndex`) — In-memory row store. `Value` is a `std::variant<std::monostate, int64_t, double, std::string>`. The catalog manages tables, views, SQL UDF definitions, and provides statistics (row counts, distinct values) for the cost optimizer.

**Planner** (`planner::LogicalNode`) — Converts the AST into a tree of logical operators: `TABLE_SCAN`, `FILTER`, `PROJECTION`, `JOIN`, `AGGREGATION`, `SORT`, `LIMIT`, `DISTINCT`.

**Optimizer** — Two-phase optimization:
1. **Rule-based** (`optimize_rules`): Constant folding for deterministic literal/scalar subexpressions, selection pushdown (push filters below joins/projections), projection pushdown.
2. **Cost-based** (`optimize_cost`): Estimates selectivity and row counts, annotates cost on each node, selects hash join over nested-loop join when estimated comparisons exceed a threshold.

**Transactions** — `TransactionManager` maintains per-transaction undo records for row-level `INSERT`/`UPDATE`/`DELETE`/`MERGE` changes. `ROLLBACK` replays undo in reverse and rebuilds affected indexes. `COMMIT` clears undo records.

**Isolation** — `LockManager` provides table-level shared/exclusive locks for explicit transactions. Write locks are held until `COMMIT`/`ROLLBACK`; read locks are statement-scoped. Conflicts currently use deterministic immediate-abort behavior (no wait).

**Durability** — `WalManager` appends transactional row-level WAL records (`BEGIN`/`INSERT`/`UPDATE`/`DELETE`/`COMMIT`/`ROLLBACK`), flushes WAL on `COMMIT` before acknowledgement, checkpoints catalog state, and performs startup recovery by redoing committed transactions after the last checkpoint.

**Consistency hardening** — Catalog-level index integrity checks validate table/index synchronization after rollback and during/after recovery replay.

**Executor** — Volcano-style pull-based execution. Implements sequential scan, filter, projection, nested-loop join, hash join, aggregation, sort, limit, and distinct operators. The expression evaluator handles all `BinOp`/`UnaryOp` types, NULL propagation, scalar built-ins, SQL UDF invocation, and SQL-style `LIKE` matching with `%`, `_`, and escaped wildcards. Projection also supports window functions `ROW_NUMBER`, `RANK`, and `DENSE_RANK` with `PARTITION BY` and `ORDER BY`.

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

-- Window expressions in SELECT
SELECT ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) FROM employees;
SELECT RANK() OVER (PARTITION BY dept ORDER BY salary DESC) FROM employees;
SELECT DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary DESC) FROM employees;

-- DDL / DML
CREATE TABLE <name> (<col> <type>, ...);
CREATE INDEX <name> ON <table> (<col>) [USING HASH|BTREE];
CREATE VIEW <name> AS <query>;
CREATE MATERIALIZED VIEW <name> AS <query>;
CREATE FUNCTION <name>(<param> <type>, ...) RETURNS <type> AS '<expr>';
CREATE USER <name> IDENTIFIED BY '<password>';
ALTER USER <name> IDENTIFIED BY '<password>';
DROP USER <name>;
GRANT <privileges> ON TABLE|VIEW|FUNCTION <object_name> TO <user>;
REVOKE <privileges> ON TABLE|VIEW|FUNCTION <object_name> FROM <user>;
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
DROP FUNCTION <function_name>;
TRUNCATE TABLE <table_name>;
TRUNCATE <table_name>;                  -- shorthand (without TABLE keyword)
LOAD <table> '<file.csv>';

-- Transactions
BEGIN;
BEGIN TRANSACTION;
COMMIT;
ROLLBACK;

-- Analysis
EXPLAIN <query>;
EXPLAIN ANALYZE <query>;

-- Expressions
LOWER(name), UPPER(name), LENGTH(name), TRIM(name), SUBSTR(name, 1, 3),
ABS(x), ROUND(x), CEIL(x), FLOOR(x), COALESCE(a, b), NULLIF(a, b),
custom_udf(col1, col2);
```

### Functions, Pattern Matching, and Window Analytics

- Scalar built-ins are supported in expression contexts (`SELECT`, `WHERE`, `ORDER BY`, `GROUP BY`): `LOWER`, `UPPER`, `LENGTH`, `TRIM`, `SUBSTR`, `ABS`, `ROUND`, `CEIL`/`CEILING`, `FLOOR`, `COALESCE`, `NULLIF`.
- SQL UDF lifecycle is supported via `CREATE FUNCTION ... RETURNS ... AS ...` and `DROP FUNCTION`.
- UDF resolution is name-based and currently expression-body focused (no statement-body UDFs).
- `LIKE` supports SQL wildcards `%` and `_`, plus escaped literals (for example `LIKE 'a\_b'` and `LIKE 'a\%b'`).
- Window functions are supported in projection via `ROW_NUMBER`, `RANK`, and `DENSE_RANK` with `OVER (PARTITION BY ... ORDER BY ...)`.
- Current window limitation: mixing window expressions with `GROUP BY`/aggregate queries in the same select block is rejected.

### Transaction notes (current behavior)

- In explicit transactions, VolcanoDB currently allows: `SELECT`, `EXPLAIN`, `BENCHMARK`, `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `COMMIT`, `ROLLBACK`.
- Non-transactional mutating statements (for example DDL and `TRUNCATE`) are rejected inside active transactions.
- WAL-based durability and startup recovery are enabled for explicit transaction writes.
- Rollback and recovery paths include index consistency validation.

### ACID Compliance

VolcanoDB is **ACID compliant** for the implemented architecture and transaction model, based on the ACID evidence matrix tests (`[acid]`, `[acid-a]`, `[acid-c]`, `[acid-i]`, `[acid-d]`).

### Assumptions and Limitations

The ACID claim above assumes the following boundaries:

1. Single-node, single-process deployment (no distributed transactions or replication).
2. Isolation model is table-level locking with deterministic immediate-abort conflict policy.
3. Explicit transactions are supported for DML (`INSERT`, `UPDATE`, `DELETE`, `MERGE`); DDL remains blocked inside active transactions.
4. Durability is based on local WAL + checkpoint files and startup recovery for committed transactional writes.
5. Full ANSI SERIALIZABLE/predicate-lock semantics are out of scope.

### ACID Proof Suite

Run the consolidated ACID evidence matrix:

```bash
./vdb_tests "[acid]"
./vdb_tests "[acid-a]"
./vdb_tests "[acid-c]"
./vdb_tests "[acid-i]"
./vdb_tests "[acid-d]"
```

For repeatability checks, run the matrix multiple times (for example 10 consecutive runs):

```bash
for i in {1..10}; do ./vdb_tests "[acid]" || break; done
```

## Test Suite Documentation

Exhaustive test suite for VolcanoDB using **Catch2 v3**.

### Building & Running

```bash
cd build
cmake ..
make vdb_tests
./vdb_tests               # Run all tests
./vdb_tests "[parser]"    # Run parser tests only
./vdb_tests "[e2e]"       # Run end-to-end tests only
./vdb_tests --list-tests  # List all test names
```

### Test Organization

Tests are organized across `tests/test_main.cpp` (core SQL logic), `tests/test_commands.cpp` (CLI commands), and `tests/test_server_integration.cpp` (server protocol/integration):

| File | Primary focus | Representative tags |
|------|---------------|---------------------|
| `tests/test_main.cpp` | Parser, storage, planner/optimizer, executor internals, benchmark generators, WAL recovery, consistency and lock-isolation checks | `[parser]`, `[storage]`, `[planner]`, `[optimizer]`, `[executor]`, `[benchmark]`, `[lock]`, `[isolation]`, `[wal]`, `[durability]`, `[consistency]`, `[recovery]`, `[acid]` |
| `tests/test_commands.cpp` | REPL/CLI behavior and end-to-end SQL workflows | `[e2e]`, `[commands]`, `[dml]`, `[ddl]`, `[alter]`, `[merge]`, `[constraint]`, `[trigger]`, `[transaction]`, `[durability]`, `[acid]` |
| `tests/test_server_integration.cpp` | TCP server handshake/protocol behavior, multi-client flows, auth gating | `[server]`, `[integration]`, `[auth]` |

Tag snapshot from `./vdb_tests --list-tags` (counts are per-tag and overlap across tests):

| Area | Tag(s) | Cases |
|------|--------|------:|
| **Total suite** | `all` | **457** |
| Parsing and grammar | `[parser]` | 112 |
| End-to-end SQL | `[e2e]` | 251 |
| CLI and scripts | `[commands]` | 29 |
| Storage core | `[storage]` | 38 |
| Indexing | `[index]` | 21 |
| Server/integration/auth | `[server]`, `[integration]`, `[auth]` | 8, 8, 6 |
| ACID evidence matrix | `[acid]`, `[acid-a]`, `[acid-c]`, `[acid-i]`, `[acid-d]` | 19, 1, 3, 5, 11 |
| Transactions | `[transaction]` | 19 |
| Locking | `[lock]` | 10 |
| Isolation conflict policy | `[isolation]` | 5 |
| Durability and WAL | `[durability]`, `[wal]` | 11, 6 |
| Recovery hardening | `[recovery]` | 3 |
| Consistency hardening | `[consistency]` | 2 |
| Constraints and foreign keys | `[constraint]`, `[fk]` | 26, 11 |
| Planner, optimizer, executor | `[planner]`, `[optimizer]`, `[executor]` | 12, 11, 8 |
| DML and DDL families | `[dml]`, `[ddl]`, `[alter]`, `[merge]` | 15, 30, 16, 6 |

### Features Tested

#### SQL Commands
- `SELECT` (with `*`, column list, expressions, aliases, `CASE WHEN ... THEN ... ELSE ... END`)
- `SELECT DISTINCT`
- `UNION`, `UNION ALL`, `INTERSECT`, `EXCEPT`
- `CREATE TABLE` (INT, FLOAT, VARCHAR, VARCHAR(n), INTEGER, DOUBLE, TEXT)
- `CREATE INDEX` (basic B-Tree, USING HASH, USING BTREE)
- `CREATE FUNCTION name(param type, ...) RETURNS type AS 'expr'`
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
- `DROP FUNCTION <name>`
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
- Pattern: `LIKE` (supports `%`, `_`, escaped wildcard literals, exact)
- Range: `BETWEEN low AND high`
- Set: `IN (list)`, `IN (subquery)`, `EXISTS (subquery)`
- Unary: `-` negation, `IS NULL`, `IS NOT NULL`
- Aggregate: `COUNT(*)`, `COUNT(col)`, `COUNT(DISTINCT col)`, `SUM`, `AVG`, `MIN`, `MAX`
- Scalar function calls: `LOWER`, `UPPER`, `LENGTH`, `TRIM`, `SUBSTR`, `ABS`, `ROUND`, `CEIL`/`CEILING`, `FLOOR`, `COALESCE`, `NULLIF`, and user-defined SQL functions
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
