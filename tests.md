# SQP Test Suite Documentation

Exhaustive test suite for the Simple Query Processor & Optimizer (SQP) using **Catch2 v3**.

## Building & Running

```bash
cd build
cmake ..
make sqp_tests
./sqp_tests               # Run all tests
./sqp_tests -t "[parser]" # Run parser tests only
./sqp_tests -t "[e2e]"    # Run end-to-end tests only
./sqp_tests --list-tests  # List all test names
```

## Test Organization

All tests are in `tests/test_main.cpp`, organized by section:

| # | Section | Tag(s) | Count | Description |
|---|---------|--------|-------|-------------|
| 1 | Parser: DDL Statements | `[parser][ddl]` | 8 | CREATE TABLE (INT, FLOAT, VARCHAR, VARCHAR(n), INTEGER, DOUBLE, TEXT), CREATE INDEX (basic, USING HASH), INSERT, LOAD |
| 2 | Parser: SELECT Basics | `[parser][select]` | 6 | SELECT *, specific columns, alias (AS / implicit), DISTINCT, table alias |
| 3 | Parser: Expressions | `[parser][expr]` | 14 | Literals (int, float, string, NULL), arithmetic ops (+,-,*,/,%), comparisons (=,!=,<>,<,>,<=,>=), logical (AND, OR, NOT), IS NULL / IS NOT NULL, LIKE, BETWEEN, IN (list & subquery), EXISTS, negation, parenthesized, qualified columns |
| 4 | Parser: Aggregate Functions | `[parser][aggregate]` | 4 | COUNT(*), COUNT(column), COUNT(DISTINCT col), SUM/AVG/MIN/MAX |
| 5 | Parser: Clauses | `[parser][clause]` | 7 | WHERE, GROUP BY, HAVING, ORDER BY (ASC default, DESC, multiple keys), LIMIT, LIMIT+OFFSET |
| 6 | Parser: JOIN Syntax | `[parser][join]` | 8 | INNER (implicit/explicit), LEFT, LEFT OUTER, RIGHT, FULL OUTER, CROSS, subquery in FROM |
| 7 | Parser: EXPLAIN / BENCHMARK | `[parser][explain]` | 3 | EXPLAIN SELECT, EXPLAIN ANALYZE SELECT, BENCHMARK SELECT |
| 8 | Case Insensitivity | `[parser][case]` | 11 | All lowercase, all uppercase, mixed case keywords, DDL keywords, aggregate names (count/COUNT/Count), JOIN keywords, ASC/DESC, EXPLAIN/ANALYZE, DISTINCT, IS NULL/IS NOT NULL, BETWEEN/LIKE/IN keywords, identifier case preservation |
| 9 | Punctuation & Grammar | `[parser][grammar]` | 10 | Semicolons, commas in SELECT/DDL, parenthesized expressions, dot notation, single-quoted strings, multiple FROM tables, precedence (*before+, AND before OR), parse failure returns nullptr, incomplete query error, single-line `--` comments, block `/* */` comments |
| 10 | Storage: Value Helpers | `[storage][value]` | 10 | value_is_null, value_to_int (truncation, null→0), value_to_double, value_to_string, value_equal (normal, NULL=NULL→false, NULL=x→false), value_less (normal, null→false, mixed int/float), value_add/sub/mul/div (int+int=int, int+float=float), arithmetic null propagation, division-by-zero→NULL, value_display formatting |
| 11 | Storage: Table Operations | `[storage][table]` | 4 | Table creation/schema, insert_row, distinct_values, cardinality |
| 12 | Storage: Catalog | `[storage][catalog]` | 2 | add/get table, cardinality/distinct stats |
| 13 | Storage: Hash Index | `[storage][index]` | 3 | Build+lookup int, build+lookup string, catalog create_index |
| 14 | E2E: SELECT * | `[e2e][select]` | 3 | SELECT *, specific columns, expression in select list |
| 15 | E2E: WHERE Clause | `[e2e][where]` | 14 | Equality, string comparison, inequality (>), <=, AND, OR, NOT, !=, <>, BETWEEN, IN list, LIKE prefix%, LIKE %suffix, LIKE %substring%, LIKE exact |
| 16 | E2E: ORDER BY | `[e2e][orderby]` | 4 | ASC default, DESC, string column, multiple keys |
| 17 | E2E: LIMIT/OFFSET | `[e2e][limit]` | 5 | Basic LIMIT, LIMIT+OFFSET, LIMIT > row count, OFFSET beyond rows, LIMIT 0 |
| 18 | E2E: DISTINCT | `[e2e][distinct]` | 2 | DISTINCT on duplicates, DISTINCT on unique column |
| 19 | E2E: Aggregation | `[e2e][aggregation]` | 8 | COUNT(*), COUNT(col), SUM(int), AVG(float), MIN/MAX int, MIN/MAX string, GROUP BY+COUNT, GROUP BY+SUM, GROUP BY+multiple aggs, HAVING |
| 20 | E2E: Joins | `[e2e][join]` | 6 | INNER JOIN, JOIN with alias, JOIN+WHERE, CROSS JOIN, implicit cross join (multi-FROM), JOIN+aggregation |
| 21 | E2E: Arithmetic | `[e2e][arithmetic]` | 3 | Integer arithmetic in SELECT, float arithmetic, modulo |
| 22 | E2E: NULL Handling | `[e2e][null]` | 2 | IS NULL/IS NOT NULL filter, COUNT(*) vs COUNT(col) with NULL, SUM/AVG skip nulls, NULL=NULL→false |
| 23 | E2E: Combined Queries | `[e2e][combined]` | 5 | WHERE+ORDER+LIMIT, DISTINCT+ORDER, GROUP+HAVING+ORDER, JOIN+WHERE+ORDER+LIMIT, aggregate without GROUP BY |
| 24 | Planner: Structure | `[planner]` | 6 | Scan+projection, filter node, sort node, limit node, distinct node, join node, aggregation node |
| 25 | Optimizer: Rule-Based | `[optimizer][rules]` | 2 | Selection pushdown below join, optimization preserves results |
| 26 | Optimizer: Cost-Based | `[optimizer][cost]` | 2 | Cost estimates populated, hash join selection for large tables |
| 27 | Executor: Stats | `[executor][stats]` | 3 | rows_scanned/produced, filter stats, join comparisons |
| 28 | E2E: DDL+DML Pipeline | `[e2e][ddl]` | 2 | CREATE TABLE then query, CREATE INDEX then verify |
| 29 | E2E: Generated Data | `[e2e][benchmark]` | 5 (sections) | Scan employees, filter salary, group by dept, join emp+dept, complex compound query |
| 30 | Edge Cases | `[e2e][edge]` | 13 | Empty table, COUNT on empty, WHERE no match, single row, LIMIT 1, OFFSET not reaching LIMIT, very long strings, negative integers, zero comparisons, all-NULL column, single GROUP, identical rows DISTINCT, self-join, case-sensitive LIKE data |
| 31 | AST Factory Methods | `[ast]` | 8 | make_int/float/string/column/star/binary/unary/func, to_string |
| 32 | Planner: to_string | `[planner]` | 2 | Plan to_string, optimized plan to_string |
| 33 | String Literals | `[e2e][strings]` | 3 | Strings with spaces, empty string comparison, LIKE empty pattern |
| 34 | Subqueries | `[e2e][subquery]` | 2 | IN subquery parse, FROM subquery parse |
| 35 | EXPLAIN E2E | `[e2e][explain]` | 2 | EXPLAIN builds plan, EXPLAIN ANALYZE executes |
| 36 | Complex Join Patterns | `[e2e][join]` | 3 | JOIN+ORDER BY joined col, JOIN+LIMIT, JOIN+GROUP+HAVING |
| 37 | Float/Int Mixed Types | `[e2e][types]` | 2 | Float comparison in WHERE, int/float mixed arithmetic |
| 38 | Multiple Aggregates | `[e2e][aggregation]` | 2 | All aggregates in one query, GROUP BY+AVG |
| 39 | Benchmark Data | `[benchmark]` | 3 | generate_employees, generate_departments, generate_orders |
| 40 | Regression & Stress | `[e2e][regression]` `[e2e][stress]` | 8 | Nested AND/OR, aggregate+WHERE, ORDER+DISTINCT, BETWEEN boundaries, IN single, expression alias, JOIN+agg+order+limit, many columns, many WHERE conditions |

## Test Categories Summary

| Category | Total Tests | What it covers |
|----------|-------------|----------------|
| **Parser** | ~60 | All DDL/DML parsing, expression types, clauses, join syntax, EXPLAIN/BENCHMARK |
| **Case Insensitivity** | 11 | Keywords, aggregate names, JOIN types, ASC/DESC, EXPLAIN, DISTINCT, IS NULL, BETWEEN/LIKE/IN — all mixed case |
| **Grammar & Punctuation** | 10 | Semicolons, commas, parentheses, dot notation, quotes, operator precedence, comments |
| **Storage** | ~19 | Value operations (null, arithmetic, comparison), Table CRUD, Catalog, Hash Index |
| **Planner** | ~8 | Logical plan structure for each node type, to_string |
| **Optimizer** | ~4 | Rule-based pushdown, cost estimation, hash join selection, result preservation |
| **Executor** | ~3 | Execution stats tracking |
| **End-to-End** | ~80+ | Full pipeline (parse→plan→optimize→execute) for SELECT, WHERE, ORDER BY, LIMIT, DISTINCT, aggregation, JOINs, NULLs, subqueries, EXPLAIN, combined patterns |
| **Edge Cases** | ~13 | Empty tables, single rows, long strings, negative values, NULL-heavy, self-joins, boundary conditions |
| **Regression** | ~8 | Complex nested logic, boundary BETWEEN, single IN, expression aliases, compound queries |
| **Benchmarks** | ~8 | Data generation, query correctness on generated data |

## Features Tested

### SQL Commands
- `SELECT` (with `*`, column list, expressions, aliases)
- `SELECT DISTINCT`
- `CREATE TABLE` (INT, FLOAT, VARCHAR, VARCHAR(n), INTEGER, DOUBLE, TEXT)
- `CREATE INDEX` (basic, USING HASH)
- `INSERT INTO ... VALUES`
- `LOAD table 'file'`
- `EXPLAIN SELECT`
- `EXPLAIN ANALYZE SELECT`
- `BENCHMARK SELECT`

### SQL Clauses
- `FROM` (single table, multiple tables, aliases, subqueries)
- `WHERE` (all comparison operators, LIKE, BETWEEN, IN, IS NULL, IS NOT NULL, AND, OR, NOT)
- `GROUP BY` (single/multiple columns)
- `HAVING` (with aggregate conditions)
- `ORDER BY` (ASC, DESC, multiple keys)
- `LIMIT` (basic, with OFFSET, edge values like 0)
- `JOIN` (INNER, LEFT, LEFT OUTER, RIGHT, FULL OUTER, CROSS)

### Expressions
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

### Case Insensitivity
- All SQL keywords (SELECT, FROM, WHERE, etc.) work in lowercase, uppercase, and mIxEd case
- Aggregate function names (count, COUNT, Count)
- JOIN types, ASC/DESC, EXPLAIN ANALYZE, DISTINCT
- BETWEEN, LIKE, IN, IS NULL, IS NOT NULL
- Data values remain case-sensitive (e.g., LIKE matching)

### Punctuation & Grammar
- Semicolon statement terminator
- Commas (SELECT list, column definitions, IN lists, ORDER BY)
- Parentheses (expressions, function calls, CREATE TABLE, IN list)
- Dot notation (table.column)
- Single-quoted string literals
- SQL comments (`--` line and `/* */` block)
- Operator precedence (`*` before `+`, `AND` before `OR`)

### NULL Semantics (SQL Standard)
- `NULL = NULL` → false
- `NULL = x` → false
- `NULL + x` → NULL (arithmetic propagation)
- Division by zero → NULL
- `COUNT(*)` includes NULL rows, `COUNT(col)` excludes
- `SUM`/`AVG` skip NULL values
- `IS NULL` / `IS NOT NULL` work correctly

### Storage Layer
- Value type operations (comparison, arithmetic, display)
- Table insert, cardinality, distinct_values
- Catalog table management
- Hash index build and lookup (int and string keys)

### Query Planner
- Correct node type generation for each clause
- TABLE_SCAN, FILTER, PROJECTION, JOIN, AGGREGATION, SORT, LIMIT, DISTINCT nodes

### Optimizer
- Rule-based: selection pushdown below joins
- Cost-based: estimates populated, hash join selection
- Optimization does not change query results

### Edge Cases
- Empty tables (scan, COUNT)
- Single-row tables
- WHERE matching zero rows
- LIMIT 0, LIMIT > total rows, OFFSET > total rows
- Very long strings (10,000 chars)
- Negative integers, zero comparisons
- All-NULL columns
- Self-join
- Multiple identical rows for DISTINCT

## Test Results

**225 test cases — 745 assertions — all passing**
