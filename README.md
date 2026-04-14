# Simple Query Processor & Optimizer (SQP)

A SQL query processor and optimizer built from scratch in C++17 using Flex and Bison. It parses SQL queries into an AST, converts them to logical plans, applies rule-based and cost-based optimizations, and executes them against an in-memory storage engine.

<p align="center">
    <a href="#prerequisites">Prerequisites</a> •
    <a href="#building">Building</a> •
    <a href="#running">Running</a> •
    <a href="#architecture">Architecture</a> •
    <a href="#supported-sql">Supported SQL</a> •
    <a href="#extending-the-project">Extending the project</a> •
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

**Parser** — Flex tokenizes SQL into keywords, operators, and literals. Bison parses tokens into an AST using a precedence-climbing expression grammar. Supports `SELECT` (with `DISTINCT`, `JOIN`, `WHERE`, `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`/`OFFSET`), `CREATE TABLE`, `CREATE INDEX`, `CREATE VIEW`, `CREATE MATERIALIZED VIEW`, `INSERT`, `UPDATE`, `DELETE`, `ALTER TABLE`, `DROP TABLE/INDEX/VIEW`, `TRUNCATE`, `LOAD`, `EXPLAIN`, and `BENCHMARK`.

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

## SQP Test Suite Documentation

Exhaustive test suite for the Simple Query Processor & Optimizer (SQP) using **Catch2 v3**.

### Building & Running

```bash
cd build
cmake ..
make sqp_tests
./sqp_tests               # Run all tests
./sqp_tests -t "[parser]" # Run parser tests only
./sqp_tests -t "[e2e]"    # Run end-to-end tests only
./sqp_tests --list-tests  # List all test names
```

### Test Organization

Tests are organized across `tests/test_main.cpp` (core SQL logic) and `tests/test_commands.cpp` (CLI commands):

| # | Section | Tag(s) | Count | Description |
|---|---------|--------|-------|-------------|
| 1 | Parser: DDL Statements | `[parser][ddl]` | 11 | CREATE TABLE (INT, FLOAT, VARCHAR, VARCHAR(n), INTEGER, DOUBLE, TEXT), CREATE INDEX (basic, USING HASH), INSERT, LOAD
| 2 | Parser: SELECT Basics | `[parser][select]` | 6 | SELECT *, specific columns, alias (AS / implicit), DISTINCT, table alias
| 3 | Parser: Expressions | `[parser][expr]` | 19 | Literals (int, float, string, NULL), arithmetic ops (+,-,*,/,%), comparisons (=,!=,<>,<,>,<=,>=), logical (AND, OR, NOT), IS NULL / IS NOT NULL, LIKE, BETWEEN, IN (list & subquery), EXISTS, negation, parenthesized, qualified columns
| 4 | Parser: Aggregate Functions | `[parser][aggregate]` | 4 | COUNT(*), COUNT(column), COUNT(DISTINCT col), SUM/AVG/MIN/MAX
| 5 | Parser: Clauses | `[parser][clause]` | 8 | WHERE, GROUP BY, HAVING, ORDER BY (ASC default, DESC, multiple keys), LIMIT, LIMIT+OFFSET
| 6 | Parser: JOIN Syntax | `[parser][join]` | 8 | INNER (implicit/explicit), LEFT, LEFT OUTER, RIGHT, FULL OUTER, CROSS, subquery in FROM
| 7 | Parser: EXPLAIN / BENCHMARK | `[parser][explain]` | 3 | EXPLAIN SELECT, EXPLAIN ANALYZE SELECT, BENCHMARK SELECT
| 8 | Case Insensitivity | `[parser][case]` | 12 | All lowercase, all uppercase, mixed case keywords, DDL keywords, aggregate names (count/COUNT/Count), JOIN keywords, ASC/DESC, EXPLAIN/ANALYZE, DISTINCT, IS NULL/IS NOT NULL, BETWEEN/LIKE/IN keywords, identifier case preservation
| 9 | Punctuation & Grammar | `[parser][grammar]` | 11 | Semicolons, commas in SELECT/DDL, parenthesized expressions, dot notation, single-quoted strings, multiple FROM tables, precedence (*before+, AND before OR), parse failure returns nullptr, incomplete query error, single-line `--` comments, block `/* */` comments
| 10 | Storage: Value Helpers | `[storage][value]` | 11 | value_is_null, value_to_int (truncation, null→0), value_to_double, value_to_string, value_equal (normal, NULL=NULL→false, NULL=x→false), value_less (normal, null→false, mixed int/float), value_add/sub/mul/div (int+int=int, int+float=float), arithmetic null propagation, division-by-zero→NULL, value_display formatting
| 11 | Storage: Table Operations | `[storage][table]` | 4 | Table creation/schema, insert_row, distinct_values, cardinality
| 12 | Storage: Catalog | `[storage][catalog]` | 3 | add/get table, cardinality/distinct stats
| 13 | Storage: Hash Index | `[storage][index]` | 3 | Build+lookup int, build+lookup string, catalog create_index
| 14 | E2E: SELECT * | `[e2e][select]` | 3 | SELECT *, specific columns, expression in select list
| 15 | E2E: WHERE Clause | `[e2e][where]` | 15 | Equality, string comparison, inequality (>), <=, AND, OR, NOT, !=, <>, BETWEEN, IN list, LIKE prefix%, LIKE %suffix, LIKE %substring%, LIKE exact
| 16 | E2E: ORDER BY | `[e2e][orderby]` | 4 | ASC default, DESC, string column, multiple keys
| 17 | E2E: LIMIT/OFFSET | `[e2e][limit]` | 5 | Basic LIMIT, LIMIT+OFFSET, LIMIT > row count, OFFSET beyond rows, LIMIT 0
| 18 | E2E: DISTINCT | `[e2e][distinct]` | 2 | DISTINCT on duplicates, DISTINCT on unique column
| 19 | E2E: Aggregation | `[e2e][aggregation]` | 12 | COUNT(*), COUNT(col), SUM(int), AVG(float), MIN/MAX int, MIN/MAX string, GROUP BY+COUNT, GROUP BY+SUM, GROUP BY+multiple aggs, HAVING
| 20 | E2E: Joins | `[e2e][join]` | 9 | INNER JOIN, JOIN with alias, JOIN+WHERE, CROSS JOIN, implicit cross join (multi-FROM), JOIN+aggregation
| 21 | E2E: Arithmetic | `[e2e][arithmetic]` | 3 | Integer arithmetic in SELECT, float arithmetic, modulo
| 22 | E2E: NULL Handling | `[e2e][null]` | 2 | IS NULL/IS NOT NULL filter, COUNT(*) vs COUNT(col) with NULL, SUM/AVG skip nulls, NULL=NULL→false
| 23 | E2E: Combined Queries | `[e2e][combined]` | 5 | WHERE+ORDER+LIMIT, DISTINCT+ORDER, GROUP+HAVING+ORDER, JOIN+WHERE+ORDER+LIMIT, aggregate without GROUP BY
| 24 | Planner: Structure | `[planner]` | 8 | Scan+projection, filter node, sort node, limit node, distinct node, join node, aggregation node
| 25 | Optimizer: Rule-Based | `[optimizer][rules]` | 1 | Selection pushdown below join, optimization preserves results
| 26 | Optimizer: Cost-Based | `[optimizer][cost]` | 2 | Cost estimates populated, hash join selection for large tables
| 27 | Executor: Stats | `[executor][stats]` | 3 | rows_scanned/produced, filter stats, join comparisons
| 28 | E2E: DDL+DML Pipeline | `[e2e][ddl]` | 2 | CREATE TABLE then query, CREATE INDEX then verify
| 29 | E2E: Generated Data | `[e2e][benchmark]` | 1 | Scan employees, filter salary, group by dept, join emp+dept, complex compound query
| 30 | Edge Cases | `[e2e][edge]` | 14 | Empty table, COUNT on empty, WHERE no match, single row, LIMIT 1, OFFSET not reaching LIMIT, very long strings, negative integers, zero comparisons, all-NULL column, single GROUP, identical rows DISTINCT, self-join, case-sensitive LIKE data
| 31 | AST Factory Methods | `[ast]` | 9 | make_int/float/string/column/star/binary/unary/func, to_string
| 32 | Planner: to_string | `[planner]` | 8 | Plan to_string, optimized plan to_string
| 33 | String Literals | `[e2e][strings]` | 3 | Strings with spaces, empty string comparison, LIKE empty pattern
| 34 | Subqueries | `[e2e][subquery]`, `[executor]` | 5 | IN subquery parse, FROM subquery parse, scalar evaluation, IN correlated nested loops, EXISTS (uncorrelated/correlated), scalar correlated execution, correlation memoization, IN uncorrelated Hash Join |
| 35 | EXPLAIN E2E | `[e2e][explain]` | 2 | EXPLAIN builds plan, EXPLAIN ANALYZE executes
| 36 | Complex Join Patterns | `[e2e][join]` | 9 | JOIN+ORDER BY joined col, JOIN+LIMIT, JOIN+GROUP+HAVING
| 37 | Float/Int Mixed Types | `[e2e][types]` | 2 | Float comparison in WHERE, int/float mixed arithmetic
| 38 | Multiple Aggregates | `[e2e][aggregation]` | 12 | All aggregates in one query, GROUP BY+AVG
| 39 | Benchmark Data | `[benchmark]` | 3 | generate_employees, generate_departments, generate_orders
| 40 | Regression & Stress | `[e2e][regression]` `[e2e][stress]` | 9 | Nested AND/OR, aggregate+WHERE, ORDER+DISTINCT, BETWEEN boundaries, IN single, expression alias, JOIN+agg+order+limit, many columns, many WHERE conditions
| 41 | Additional Tests | `[e2e][view]` | 2 | Generated | 
| 42 | Additional Tests | `[optimizer]` | 1 | Generated | 
| 43 | Additional Tests | `[parser][error]` | 2 | Generated | 
| 44 | Additional Tests | `[planner][optimizer]` | 1 | Generated |
| 45 | E2E: CLI Commands | `[e2e][commands]` | 22 | `.help`, `.tables` (empty/generated/created), `.schema` (valid/invalid), `.generate` (with/without arg), `.save` (create/overwrite/missing arg), `.benchmark` (empty/loaded), `.quit`, `.exit`, `.source` (valid/missing file/no arg), `--file` (valid/missing/no arg), bare arg as file, unknown command |
| 46 | Index Integration | `[storage][index]` `[optimizer][index]` `[executor][index]` `[planner][index]` | 17 | BTreeIndex build/lookup/range/insert/string-keys, Catalog hash vs btree routing, index maintenance on insert, optimizer INDEX_SCAN rewrite (equality/range/no-index), executor correctness (hash eq, btree eq/range/lt), index vs full scan equivalence, planner to_string |
| 47 | DML Operations | `[e2e][dml]` | 15 | INSERT (single/multi-row/mismatch/nonexistent/data-verify), UPDATE (single col/multi col/no WHERE/nonexistent), DELETE (WHERE/no WHERE/compound/no match/nonexistent), full DML sequence |
| 48 | ALTER TABLE | `[e2e][alter]` | 16 | ADD COLUMN (NULL backfill + new insert), DROP COLUMN (schema+data), RENAME COLUMN (SELECT with new name), RENAME TABLE (success + old name gone), RENAME TABLE standalone (success + old name gone + error cases), error: duplicate column, nonexistent column, last column, existing table, nonexistent table, rename to existing name, index compatibility |
| 49 | DROP TABLE/INDEX/VIEW | `[e2e][ddl]` | 6 | DROP TABLE (success + nonexistent error + index cascade), DROP INDEX (success + nonexistent), DROP VIEW (success + create+drop+verify gone + nonexistent) |
| 50 | TRUNCATE | `[e2e][ddl]` | 5 | TRUNCATE TABLE (clears rows, table persists, insert after), TRUNCATE shorthand, TRUNCATE empty table, TRUNCATE nonexistent error, case insensitivity |
| 51 | Script Error-Stop | `[e2e][error-stop]` | 3 | `--file` stops on syntax error (earlier commands execute, later don't), interactive REPL continues after error, `--file` stops on runtime error (nonexistent table) |
| 52 | MERGE | `[e2e][merge]` | 5 | MERGE basic upsert (matched update + unmatched insert), update-only (all matched), insert-only (all unmatched), nonexistent source table error, case insensitivity |
| 53 | Query Plan Visualization | `[e2e][explain]` `[e2e][commands]` | 4 | EXPLAIN tree connectors, EXPLAIN ANALYZE per-node actual stats, EXPLAIN FORMAT DOT (Graphviz), `.plan`/`.plan dot` commands |
| 54 | Triggers | `[e2e][trigger]` | 6 | AFTER INSERT trigger fires, BEFORE DELETE trigger fires, DROP TRIGGER, nonexistent table error, case insensitivity, `.triggers` command |
| 55 | Constraints | `[e2e][constraint]` | 14 | NOT NULL reject/allow, PK reject duplicate/null/auto-index, UNIQUE reject/allow-null, UPDATE NOT NULL/UNIQUE, CHECK reject/allow/update, case insensitivity, multiple constraints |
| 56 | Foreign Keys | `[e2e][constraint][fk]` | 6 | FK rejects invalid INSERT, FK allows valid INSERT, FK rejects DELETE of referenced parent, FK rejects UPDATE to invalid, FK allows NULL, FK case insensitivity |

### Test Categories Summary

| Category | Tests | Coverage |
|----------|------:|----------|
| Parsing & Grammar | 81 | SQL parsing, case insensitivity, punctuation, operator precedence, comments |
| Storage & Indexes | 36 | Value ops, Table CRUD, Catalog, Hash/BTree indexes, integration |
| Query Pipeline | 15 | Planner nodes, optimizer rules, executor stats |
| End-to-End & Regression | 101 | Full pipeline, edge cases, regression, benchmarks |
| CLI & Script Execution | 25 | Dot commands, `--file`, `.source`, error-stop behavior |
| DML Operations | 20 | INSERT/UPDATE/DELETE, MERGE (upsert, update-only, insert-only) |
| DDL Operations | 27 | ALTER TABLE, DROP TABLE/INDEX/VIEW, TRUNCATE |
| Query Plan Visualization | 4 | EXPLAIN tree connectors, per-node stats, DOT export, `.plan` |
| Triggers | 9 | CREATE/DROP TRIGGER, BEFORE/AFTER firing, multi-statement BEGIN...END, `.triggers` |
| Constraints | 20 | NOT NULL, PRIMARY KEY, UNIQUE, CHECK, REFERENCES (FK) auto-index, UPDATE enforcement |
| **Total** | **357** | **1060 assertions — all passing** |

### Features Tested

#### SQL Commands
- `SELECT` (with `*`, column list, expressions, aliases)
- `SELECT DISTINCT`
- `CREATE TABLE` (INT, FLOAT, VARCHAR, VARCHAR(n), INTEGER, DOUBLE, TEXT)
- `CREATE INDEX` (basic B-Tree, USING HASH)
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
- `REFERENCES table(column)` — FOREIGN KEY referential integrity (rejects invalid references, blocks parent DELETE)

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
