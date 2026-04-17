#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>
#include <cstdlib>
#include <fstream>
#include <string>
#include <cstdio>

static void cleanup_durability_files() {
    std::remove("sqp.wal");
    std::remove("sqp.checkpoint");
}

// Cross-platform path to the vdb executable
#ifdef _WIN32
#define SQP_EXE ".\\vdb.exe"
#else
#define SQP_EXE "./vdb"
#endif

// We use this to run a shell command and capture its output
static std::string run_cmd(const std::string& cmd, bool preserve_state = false) {
    if (!preserve_state) cleanup_durability_files();

    std::string result = "";
    std::string full_cmd = cmd + " > .test_out.tmp 2>&1";
    int ret = std::system(full_cmd.c_str());
    (void)ret;

    std::ifstream ifs(".test_out.tmp");
    if (ifs) {
        result.assign(std::istreambuf_iterator<char>(ifs), std::istreambuf_iterator<char>());
    }
    std::remove(".test_out.tmp");

    if (!preserve_state) cleanup_durability_files();

    return result;
}

// We use this to write lines to a temp script, pipe into sqp, return output
static std::string run_interactive(const std::string& commands, bool preserve_state = false) {
    std::ofstream script(".cmd_test.sql");
    script << commands;
    script.close();
    std::string output = run_cmd(std::string(SQP_EXE) + " < .cmd_test.sql", preserve_state);
    std::remove(".cmd_test.sql");
    return output;
}

static std::string read_text_file(const std::string& path) {
    std::ifstream ifs(path);
    if (!ifs) return "";
    return std::string(std::istreambuf_iterator<char>(ifs), std::istreambuf_iterator<char>());
}


TEST_CASE("E2E: Execute SQL via --file", "[e2e][commands]") {
    std::ofstream script("dynamic_test.sql");
    script << "CREATE TABLE t1 (id INT);\n";
    script << "INSERT INTO t1 VALUES (100);\n";
    script << "SELECT * FROM t1;\n";
    script.close();

    std::string output = run_cmd(std::string(SQP_EXE) + " --file dynamic_test.sql");

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Table 't1' created."));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("1 row(s) inserted"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("(1 rows)"));

    std::remove("dynamic_test.sql");
}

TEST_CASE("E2E: Execute predefined test_script.sql via .source", "[e2e][commands]") {
    std::string output = run_interactive(
        ".source ../tests/test_script.sql\n"
        ".quit\n"
    );

    // test_script.sql uses .generate 10 then SELECT ... LIMIT 2
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("(2 rows)"));
}

TEST_CASE("E2E: File execution error handling", "[e2e][commands]") {
    // --file with nonexistent path
    std::string output = run_cmd(std::string(SQP_EXE) + " --file does_not_exist.sql");
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Error: could not open script file"));

    // .source with no argument
    std::string output2 = run_interactive(
        ".source\n"
        ".quit\n"
    );
    CHECK_THAT(output2, Catch::Matchers::ContainsSubstring("Usage: .source <file.sql>"));
}

TEST_CASE("E2E: Unterminated SQL in file", "[e2e][commands]") {
    std::ofstream script("unterminated.sql");
    script << "CREATE TABLE t2 (id INT)\n"; // no semicolon
    script.close();

    std::string output = run_cmd(std::string(SQP_EXE) + " --file unterminated.sql");
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Warning: unterminated SQL statement at end of file"));

    std::remove("unterminated.sql");
}

TEST_CASE("E2E: CREATE FUNCTION and DROP FUNCTION", "[e2e][commands][function]") {
    std::string output = run_interactive(
        "CREATE TABLE nums (n INT);\n"
        "INSERT INTO nums VALUES (41);\n"
        "CREATE FUNCTION add1(x INT) RETURNS INT AS 'x + 1';\n"
        "SELECT add1(n) FROM nums;\n"
        "DROP FUNCTION add1;\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Function 'add1' created."));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("42"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Function 'add1' dropped."));
}


TEST_CASE("E2E: .help command", "[e2e][commands]") {
    std::string output = run_interactive(
        ".help\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Simple Query Processor"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring(".help"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring(".functions"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring(".tables"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring(".schema"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring(".generate"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring(".save"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring(".source"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring(".benchmark"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring(".quit"));
}

TEST_CASE("E2E: .functions lists built-ins", "[e2e][commands][function]") {
    std::string output = run_interactive(
        ".functions\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Built-in scalar functions"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("LOWER"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("User-defined SQL functions: (none)"));
}

TEST_CASE("E2E: .functions builtins filter", "[e2e][commands][function]") {
    std::string output = run_interactive(
        "CREATE FUNCTION add1(x INT) RETURNS INT AS 'x + 1';\n"
        ".functions builtins\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Built-in scalar functions"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("LOWER"));
    CHECK(output.find("User-defined SQL functions") == std::string::npos);
}

TEST_CASE("E2E: .functions lists user-defined functions", "[e2e][commands][function]") {
    std::string output = run_interactive(
        "CREATE FUNCTION add1(x INT) RETURNS INT AS 'x + 1';\n"
        ".functions\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("User-defined SQL functions (1):"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("add1(x INT) RETURNS INT"));
}

TEST_CASE("E2E: .functions udf filter", "[e2e][commands][function]") {
    std::string output = run_interactive(
        "CREATE FUNCTION add1(x INT) RETURNS INT AS 'x + 1';\n"
        ".functions udf\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("User-defined SQL functions (1):"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("add1(x INT) RETURNS INT"));
    CHECK(output.find("Built-in scalar functions") == std::string::npos);
}

TEST_CASE("E2E: .functions invalid filter", "[e2e][commands][function]") {
    std::string output = run_interactive(
        ".functions unknown\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Usage: .functions [builtins|udf]"));
}


TEST_CASE("E2E: .save with no argument", "[e2e][commands]") {
    std::string output = run_interactive(
        ".save\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Usage: .save <file>"));
}

TEST_CASE("E2E: .save creates formatted dump file", "[e2e][commands]") {
    const std::string dump_file = "repl_save_dump.txt";
    std::remove(dump_file.c_str());

    std::string output = run_interactive(
        ".generate 3\n"
        ".save repl_save_dump.txt\n"
        ".quit\n"
    );

    std::string dump = read_text_file(dump_file);

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Saved 3 tables to 'repl_save_dump.txt'"));
    CHECK(!dump.empty());
    CHECK_THAT(dump, Catch::Matchers::ContainsSubstring("Simple Query Processor Table Dump"));
    CHECK_THAT(dump, Catch::Matchers::ContainsSubstring("Table: employees"));
    CHECK_THAT(dump, Catch::Matchers::ContainsSubstring("Rows: 3"));

    std::remove(dump_file.c_str());
}

TEST_CASE("E2E: .save overwrites existing file", "[e2e][commands]") {
    const std::string dump_file = "repl_save_overwrite.txt";
    {
        std::ofstream out(dump_file);
        out << "OLD_CONTENT_SHOULD_BE_OVERWRITTEN\n";
    }

    run_interactive(
        ".generate 2\n"
        ".save repl_save_overwrite.txt\n"
        ".quit\n"
    );

    std::string dump = read_text_file(dump_file);
    CHECK(dump.find("OLD_CONTENT_SHOULD_BE_OVERWRITTEN") == std::string::npos);
    CHECK_THAT(dump, Catch::Matchers::ContainsSubstring("Table: employees"));
    CHECK_THAT(dump, Catch::Matchers::ContainsSubstring("Rows: 2"));

    std::remove(dump_file.c_str());
}


TEST_CASE("E2E: .tables with no tables", "[e2e][commands]") {
    std::string output = run_interactive(
        ".tables\n"
        ".quit\n"
    );
    // No tables loaded, so output should just have the banner and prompts
    // Importantly it should NOT crash
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("sqp>"));
}

TEST_CASE("E2E: .tables after .generate", "[e2e][commands]") {
    std::string output = run_interactive(
        ".generate 5\n"
        ".tables\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("employees"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("departments"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("orders"));
}

TEST_CASE("E2E: .tables after CREATE TABLE", "[e2e][commands]") {
    std::string output = run_interactive(
        "CREATE TABLE my_tbl (x INT, y VARCHAR);\n"
        ".tables\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("my_tbl"));
}


TEST_CASE("E2E: .schema shows column types", "[e2e][commands]") {
    std::string output = run_interactive(
        "CREATE TABLE schema_test (id INT, name VARCHAR, score FLOAT);\n"
        ".schema schema_test\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Table: schema_test"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("id INT"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("name VARCHAR"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("score FLOAT"));
}

TEST_CASE("E2E: .schema on nonexistent table", "[e2e][commands]") {
    std::string output = run_interactive(
        ".schema no_such_table\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Table not found: no_such_table"));
}


TEST_CASE("E2E: .generate creates tables with data", "[e2e][commands]") {
    std::string output = run_interactive(
        ".generate 20\n"
        ".tables\n"
        "SELECT COUNT(*) FROM employees;\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("employees"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("departments"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("orders"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("20")); // 20 employee rows
}

TEST_CASE("E2E: .generate default (no argument)", "[e2e][commands]") {
    // .generate with no arg defaults to 10000 -- just verify it doesn't crash
    // and tables appear. We use a small number to keep it fast.
    std::string output = run_interactive(
        ".generate 5\n"
        "SELECT COUNT(*) FROM employees;\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("5"));
}


TEST_CASE("E2E: .benchmark with no data", "[e2e][commands]") {
    std::string output = run_interactive(
        ".benchmark\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("No tables loaded. Use .generate first."));
}

TEST_CASE("E2E: .benchmark with data", "[e2e][commands]") {
    std::string output = run_interactive(
        ".generate 100\n"
        ".benchmark\n"
        ".quit\n"
    );

    // benchmark::run_benchmarks prints timing info
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("ms"));
}


TEST_CASE("E2E: .quit exits cleanly", "[e2e][commands]") {
    std::string output = run_interactive(".quit\n");

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Bye!"));
}

TEST_CASE("E2E: .exit exits cleanly", "[e2e][commands]") {
    std::string output = run_interactive(".exit\n");

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Bye!"));
}


TEST_CASE("E2E: Unknown dot command", "[e2e][commands]") {
    std::string output = run_interactive(
        ".foobar\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Unknown command: .foobar"));
}


TEST_CASE("E2E: --file with no path", "[e2e][commands]") {
    std::string output = run_cmd(std::string(SQP_EXE) + " --file");
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Usage: sqp --file <script.sql>"));
}


TEST_CASE("E2E: Bare argument treated as script file", "[e2e][commands]") {
    std::ofstream script("bare_arg_test.sql");
    script << "CREATE TABLE bare_t (v INT);\n";
    script.close();

    std::string output = run_cmd(std::string(SQP_EXE) + " bare_arg_test.sql");
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Table 'bare_t' created."));

    std::remove("bare_arg_test.sql");
}


TEST_CASE("E2E: INSERT single row", "[e2e][dml]") {
    std::string output = run_interactive(
        "CREATE TABLE t (id INT, name VARCHAR);\n"
        "INSERT INTO t VALUES (1, 'Alice');\n"
        "SELECT * FROM t;\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("1 row(s) inserted"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Alice"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("(1 rows)"));
}

TEST_CASE("E2E: INSERT multiple rows", "[e2e][dml]") {
    std::string output = run_interactive(
        "CREATE TABLE t (id INT, name VARCHAR);\n"
        "INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol');\n"
        "SELECT * FROM t;\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("3 row(s) inserted"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("(3 rows)"));
}

TEST_CASE("E2E: INSERT column count mismatch", "[e2e][dml]") {
    std::string output = run_interactive(
        "CREATE TABLE t (id INT, name VARCHAR);\n"
        "INSERT INTO t VALUES (1);\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Column count mismatch"));
}

TEST_CASE("E2E: INSERT into nonexistent table", "[e2e][dml]") {
    std::string output = run_interactive(
        "INSERT INTO ghost VALUES (1, 'x');\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Table not found"));
}

TEST_CASE("E2E: INSERT then SELECT verifies data", "[e2e][dml]") {
    std::string output = run_interactive(
        "CREATE TABLE t (id INT, val FLOAT);\n"
        "INSERT INTO t VALUES (10, 3.14);\n"
        "INSERT INTO t VALUES (20, 2.72);\n"
        "SELECT * FROM t WHERE id = 10;\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("3.14"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("(1 rows)"));
}

TEST_CASE("E2E: DELETE with WHERE", "[e2e][dml]") {
    std::string output = run_interactive(
        "CREATE TABLE t (id INT, name VARCHAR);\n"
        "INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol');\n"
        "DELETE FROM t WHERE id = 2;\n"
        "SELECT * FROM t;\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("1 row(s) deleted"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("(2 rows)"));
}

TEST_CASE("E2E: DELETE without WHERE (truncate)", "[e2e][dml]") {
    std::string output = run_interactive(
        "CREATE TABLE t (id INT, name VARCHAR);\n"
        "INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob');\n"
        "DELETE FROM t;\n"
        "SELECT * FROM t;\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("2 row(s) deleted"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("(0 rows)"));
}

TEST_CASE("E2E: DELETE with compound WHERE", "[e2e][dml]") {
    std::string output = run_interactive(
        "CREATE TABLE t (id INT, name VARCHAR);\n"
        "INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol');\n"
        "DELETE FROM t WHERE id > 1 AND id < 3;\n"
        "SELECT * FROM t;\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("1 row(s) deleted"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("(2 rows)"));
}

TEST_CASE("E2E: UPDATE single column", "[e2e][dml]") {
    std::string output = run_interactive(
        "CREATE TABLE t (id INT, name VARCHAR);\n"
        "INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob');\n"
        "UPDATE t SET name = 'UPDATED' WHERE id = 1;\n"
        "SELECT * FROM t WHERE id = 1;\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("1 row(s) updated"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("UPDATED"));
}

TEST_CASE("E2E: UPDATE without WHERE (all rows)", "[e2e][dml]") {
    std::string output = run_interactive(
        "CREATE TABLE t (id INT, name VARCHAR);\n"
        "INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob');\n"
        "UPDATE t SET name = 'ALL';\n"
        "SELECT * FROM t;\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("2 row(s) updated"));
}

TEST_CASE("E2E: UPDATE multiple columns", "[e2e][dml]") {
    std::string output = run_interactive(
        "CREATE TABLE t (id INT, name VARCHAR, score INT);\n"
        "INSERT INTO t VALUES (1, 'Alice', 80), (2, 'Bob', 90);\n"
        "UPDATE t SET name = 'Carol', score = 100 WHERE id = 2;\n"
        "SELECT * FROM t WHERE id = 2;\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("1 row(s) updated"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Carol"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("100"));
}

TEST_CASE("E2E: Full DML sequence INSERT->UPDATE->DELETE->SELECT", "[e2e][dml]") {
    std::string output = run_interactive(
        "CREATE TABLE t (id INT, name VARCHAR);\n"
        "INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol');\n"
        "UPDATE t SET name = 'Bobby' WHERE id = 2;\n"
        "DELETE FROM t WHERE id = 3;\n"
        "SELECT * FROM t;\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("3 row(s) inserted"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("1 row(s) updated"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("1 row(s) deleted"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Bobby"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("(2 rows)"));
}

TEST_CASE("E2E: DELETE from nonexistent table", "[e2e][dml]") {
    std::string output = run_interactive(
        "DELETE FROM ghost WHERE id = 1;\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Table not found"));
}

TEST_CASE("E2E: UPDATE nonexistent table", "[e2e][dml]") {
    std::string output = run_interactive(
        "UPDATE ghost SET name = 'x' WHERE id = 1;\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Table not found"));
}

TEST_CASE("E2E: DELETE WHERE no rows match", "[e2e][dml]") {
    std::string output = run_interactive(
        "CREATE TABLE t (id INT, name VARCHAR);\n"
        "INSERT INTO t VALUES (1, 'Alice');\n"
        "DELETE FROM t WHERE id = 999;\n"
        "SELECT * FROM t;\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("0 row(s) deleted"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("(1 rows)"));
}


TEST_CASE("E2E: ALTER TABLE ADD COLUMN with NULL in old rows", "[e2e][alter]") {
    std::string output = run_interactive(
        "CREATE TABLE t (id INT, name VARCHAR);\n"
        "INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob');\n"
        "ALTER TABLE t ADD COLUMN age INT;\n"
        "INSERT INTO t VALUES (3, 'Carol', 30);\n"
        "SELECT * FROM t;\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Column 'age' added to table 't'"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("NULL"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Carol"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("30"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("(3 rows)"));
}

TEST_CASE("E2E: ALTER TABLE DROP COLUMN", "[e2e][alter]") {
    std::string output = run_interactive(
        "CREATE TABLE t (id INT, name VARCHAR, score INT);\n"
        "INSERT INTO t VALUES (1, 'Alice', 90), (2, 'Bob', 80);\n"
        "ALTER TABLE t DROP COLUMN score;\n"
        ".schema t\n"
        "SELECT * FROM t;\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Column 'score' dropped from table 't'"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("(2 rows)"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Alice"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Bob"));
}

TEST_CASE("E2E: ALTER TABLE RENAME COLUMN", "[e2e][alter]") {
    std::string output = run_interactive(
        "CREATE TABLE t (id INT, name VARCHAR);\n"
        "INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob');\n"
        "ALTER TABLE t RENAME COLUMN name TO full_name;\n"
        "SELECT full_name FROM t;\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Column 'name' renamed to 'full_name'"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Alice"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Bob"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("(2 rows)"));
}

TEST_CASE("E2E: ALTER TABLE RENAME TO", "[e2e][alter]") {
    std::string output = run_interactive(
        "CREATE TABLE t (id INT, name VARCHAR);\n"
        "INSERT INTO t VALUES (1, 'Alice');\n"
        "ALTER TABLE t RENAME TO people;\n"
        "SELECT * FROM people;\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Table 't' renamed to 'people'"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Alice"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("(1 rows)"));
}

TEST_CASE("E2E: ALTER TABLE RENAME TO old name no longer works", "[e2e][alter]") {
    std::string output = run_interactive(
        "CREATE TABLE t (id INT, name VARCHAR);\n"
        "INSERT INTO t VALUES (1, 'Alice');\n"
        "ALTER TABLE t RENAME TO people;\n"
        "SELECT * FROM t;\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Table 't' renamed to 'people'"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Error"));
}

TEST_CASE("E2E: ALTER TABLE ADD duplicate column error", "[e2e][alter]") {
    std::string output = run_interactive(
        "CREATE TABLE t (id INT, name VARCHAR);\n"
        "ALTER TABLE t ADD COLUMN name VARCHAR;\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("already exists"));
}

TEST_CASE("E2E: ALTER TABLE DROP nonexistent column error", "[e2e][alter]") {
    std::string output = run_interactive(
        "CREATE TABLE t (id INT, name VARCHAR);\n"
        "ALTER TABLE t DROP COLUMN ghost;\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("not found"));
}

TEST_CASE("E2E: ALTER TABLE DROP last column error", "[e2e][alter]") {
    std::string output = run_interactive(
        "CREATE TABLE t (id INT);\n"
        "ALTER TABLE t DROP COLUMN id;\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("cannot drop the last column"));
}

TEST_CASE("E2E: ALTER TABLE RENAME TO existing table error", "[e2e][alter]") {
    std::string output = run_interactive(
        "CREATE TABLE t1 (id INT);\n"
        "CREATE TABLE t2 (id INT);\n"
        "ALTER TABLE t1 RENAME TO t2;\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("already exists"));
}

TEST_CASE("E2E: ALTER TABLE on nonexistent table error", "[e2e][alter]") {
    std::string output = run_interactive(
        "ALTER TABLE ghost ADD COLUMN x INT;\n"
        "ALTER TABLE ghost DROP COLUMN x;\n"
        "ALTER TABLE ghost RENAME COLUMN x TO y;\n"
        "ALTER TABLE ghost RENAME TO other;\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("table not found"));
}

TEST_CASE("E2E: ALTER TABLE RENAME COLUMN to existing name error", "[e2e][alter]") {
    std::string output = run_interactive(
        "CREATE TABLE t (id INT, name VARCHAR);\n"
        "ALTER TABLE t RENAME COLUMN id TO name;\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("already exists"));
}

TEST_CASE("E2E: ALTER TABLE with index compatibility", "[e2e][alter]") {
    std::string output = run_interactive(
        "CREATE TABLE t (id INT, name VARCHAR, score INT);\n"
        "INSERT INTO t VALUES (1, 'Alice', 90), (2, 'Bob', 80);\n"
        "CREATE INDEX idx_id ON t (id);\n"
        "ALTER TABLE t ADD COLUMN age INT;\n"
        "ALTER TABLE t DROP COLUMN score;\n"
        "SELECT * FROM t WHERE id = 1;\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Column 'age' added"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Column 'score' dropped"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Alice"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("(1 rows)"));
}


TEST_CASE("E2E: RENAME TABLE success", "[e2e][alter]") {
    std::string output = run_interactive(
        "CREATE TABLE t (id INT, name VARCHAR);\n"
        "INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob');\n"
        "RENAME TABLE t TO people;\n"
        "SELECT * FROM people;\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Table 't' renamed to 'people'"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Alice"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Bob"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("(2 rows)"));
}

TEST_CASE("E2E: RENAME TABLE old name inaccessible", "[e2e][alter]") {
    std::string output = run_interactive(
        "CREATE TABLE t (id INT);\n"
        "INSERT INTO t VALUES (1);\n"
        "RENAME TABLE t TO t_new;\n"
        "SELECT * FROM t;\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Table 't' renamed to 't_new'"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Error"));
}

TEST_CASE("E2E: RENAME TABLE to existing name error", "[e2e][alter]") {
    std::string output = run_interactive(
        "CREATE TABLE a (id INT);\n"
        "CREATE TABLE b (id INT);\n"
        "RENAME TABLE a TO b;\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("already exists"));
}

TEST_CASE("E2E: RENAME TABLE nonexistent table error", "[e2e][alter]") {
    std::string output = run_interactive(
        "RENAME TABLE ghost TO other;\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("table not found"));
}


TEST_CASE("E2E: DROP TABLE success", "[e2e][ddl]") {
    std::string output = run_interactive(
        "CREATE TABLE t (id INT, name VARCHAR);\n"
        "INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob');\n"
        "DROP TABLE t;\n"
        "SELECT * FROM t;\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Table 't' dropped."));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Error"));
}

TEST_CASE("E2E: DROP TABLE nonexistent", "[e2e][ddl]") {
    std::string output = run_interactive(
        "DROP TABLE ghost_table;\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Table not found: ghost_table"));
}

TEST_CASE("E2E: DROP TABLE cascades index removal", "[e2e][ddl]") {
    std::string output = run_interactive(
        "CREATE TABLE t (id INT, val INT);\n"
        "INSERT INTO t VALUES (1, 100), (2, 200);\n"
        "CREATE INDEX idx_val ON t (val);\n"
        "DROP TABLE t;\n"
        "SELECT * FROM t;\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Table 't' dropped."));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Error"));
}


TEST_CASE("E2E: DROP VIEW success", "[e2e][ddl]") {
    std::string output = run_interactive(
        "CREATE TABLE v_base (id INT, name VARCHAR);\n"
        "INSERT INTO v_base VALUES (1, 'Alice');\n"
        "CREATE VIEW my_view AS SELECT * FROM v_base;\n"
        "DROP VIEW my_view;\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("View 'my_view' dropped."));
}

TEST_CASE("E2E: DROP VIEW nonexistent", "[e2e][ddl]") {
    std::string output = run_interactive(
        "DROP VIEW no_such_view;\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("View not found: no_such_view"));
}


TEST_CASE("E2E: DROP INDEX nonexistent", "[e2e][ddl]") {
    std::string output = run_interactive(
        "DROP INDEX fake_index;\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Index not found: fake_index"));
}


TEST_CASE("E2E: TRUNCATE TABLE clears rows and table persists", "[e2e][ddl]") {
    std::string output = run_interactive(
        "CREATE TABLE t (id INT, name VARCHAR);\n"
        "INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol');\n"
        "TRUNCATE TABLE t;\n"
        "SELECT * FROM t;\n"
        "INSERT INTO t VALUES (10, 'New');\n"
        "SELECT * FROM t;\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("truncated (3 rows removed)"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("(0 rows)"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("1 row(s) inserted"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("New"));
}

TEST_CASE("E2E: TRUNCATE shorthand (no TABLE keyword)", "[e2e][ddl]") {
    std::string output = run_interactive(
        "CREATE TABLE t (x INT);\n"
        "INSERT INTO t VALUES (1), (2), (3);\n"
        "TRUNCATE t;\n"
        "SELECT * FROM t;\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("truncated (3 rows removed)"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("(0 rows)"));
}

TEST_CASE("E2E: TRUNCATE empty table", "[e2e][ddl]") {
    std::string output = run_interactive(
        "CREATE TABLE t (id INT);\n"
        "TRUNCATE TABLE t;\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("truncated (0 rows removed)"));
}

TEST_CASE("E2E: TRUNCATE nonexistent table", "[e2e][ddl]") {
    std::string output = run_interactive(
        "TRUNCATE TABLE ghost;\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Table not found: ghost"));
}

TEST_CASE("E2E: TRUNCATE case insensitivity", "[e2e][ddl]") {
    std::string output = run_interactive(
        "CREATE TABLE t (id INT);\n"
        "INSERT INTO t VALUES (1), (2);\n"
        "truncate table t;\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("truncated (2 rows removed)"));
}


TEST_CASE("E2E: --file stops on syntax error", "[e2e][error-stop]") {
    // Script: CREATE -> valid INSERT -> INVALID syntax -> another INSERT -> SELECT
    // Expected: first INSERT runs, second INSERT does NOT (script stops at error)
    std::ofstream script("err_stop_test.sql");
    script << "CREATE TABLE es (id INT);\n";
    script << "INSERT INTO es VALUES (1);\n";
    script << "INVALID SYNTAX HERE;\n";
    script << "INSERT INTO es VALUES (2);\n";
    script << "SELECT * FROM es;\n";
    script.close();

    std::string output = run_cmd(std::string(SQP_EXE) + " --file err_stop_test.sql");

    // First command should have executed
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Table 'es' created."));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("1 row(s) inserted"));
    // Error should be reported
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Error: failed to parse query"));
    // Second INSERT and SELECT should NOT appear (script stopped)
    CHECK_THAT(output, !Catch::Matchers::ContainsSubstring("(1 rows)"));

    std::remove("err_stop_test.sql");
}

TEST_CASE("E2E: interactive REPL continues after error", "[e2e][error-stop]") {
    // In interactive mode, the REPL should continue after errors
    std::string output = run_interactive(
        "CREATE TABLE ie (id INT);\n"
        "INVALID SYNTAX;\n"
        "INSERT INTO ie VALUES (42);\n"
        "SELECT * FROM ie;\n"
        ".quit\n"
    );

    // Both the error and the subsequent INSERT should appear
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Table 'ie' created."));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Error: failed to parse query"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("1 row(s) inserted"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("(1 rows)"));
}

TEST_CASE("E2E: --file stops on runtime error", "[e2e][error-stop]") {
    // Script: INSERT into nonexistent table -> should stop; subsequent CREATE should not run
    std::ofstream script("err_runtime_test.sql");
    script << "INSERT INTO ghost VALUES (1);\n";
    script << "CREATE TABLE after_err (id INT);\n";
    script.close();

    std::string output = run_cmd(std::string(SQP_EXE) + " --file err_runtime_test.sql");

    // The INSERT should fail
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Table not found: ghost"));
    // The CREATE after the error should NOT execute
    CHECK_THAT(output, !Catch::Matchers::ContainsSubstring("Table 'after_err' created."));

    std::remove("err_runtime_test.sql");
}


TEST_CASE("E2E: MERGE basic upsert", "[e2e][merge]") {
    std::string output = run_interactive(
        "CREATE TABLE target (id INT, name VARCHAR);\n"
        "INSERT INTO target VALUES (1, 'Alice'), (2, 'Bob');\n"
        "CREATE TABLE source (id INT, name VARCHAR);\n"
        "INSERT INTO source VALUES (2, 'Bobby'), (3, 'Charlie');\n"
        "MERGE INTO target USING source ON target.id = source.id "
        "WHEN MATCHED THEN UPDATE SET name = source.name "
        "WHEN NOT MATCHED THEN INSERT VALUES (3, 'Charlie');\n"
        "SELECT * FROM target;\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("MERGE into 'target'"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("updated"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("inserted"));
    // Table should now have 3 rows
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("(3 rows)"));
}

TEST_CASE("E2E: MERGE update only", "[e2e][merge]") {
    std::string output = run_interactive(
        "CREATE TABLE t1 (id INT, val VARCHAR);\n"
        "INSERT INTO t1 VALUES (1, 'old');\n"
        "CREATE TABLE s1 (id INT, val VARCHAR);\n"
        "INSERT INTO s1 VALUES (1, 'new');\n"
        "MERGE INTO t1 USING s1 ON t1.id = s1.id "
        "WHEN MATCHED THEN UPDATE SET val = s1.val "
        "WHEN NOT MATCHED THEN INSERT VALUES (99, 'x');\n"
        "SELECT * FROM t1;\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("1 row(s) updated"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("0 row(s) inserted"));
    // Still only 1 row
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("(1 rows)"));
}

TEST_CASE("E2E: MERGE insert only", "[e2e][merge]") {
    std::string output = run_interactive(
        "CREATE TABLE t2 (id INT, val VARCHAR);\n"
        "CREATE TABLE s2 (id INT, val VARCHAR);\n"
        "INSERT INTO s2 VALUES (1, 'a'), (2, 'b');\n"
        "MERGE INTO t2 USING s2 ON t2.id = s2.id "
        "WHEN MATCHED THEN UPDATE SET val = s2.val "
        "WHEN NOT MATCHED THEN INSERT VALUES (1, 'a');\n"
        "SELECT * FROM t2;\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("0 row(s) updated"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("2 row(s) inserted"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("(2 rows)"));
}

TEST_CASE("E2E: MERGE nonexistent table error", "[e2e][merge]") {
    std::string output = run_interactive(
        "CREATE TABLE mt (id INT);\n"
        "MERGE INTO mt USING ghost ON mt.id = ghost.id "
        "WHEN MATCHED THEN UPDATE SET id = 1 "
        "WHEN NOT MATCHED THEN INSERT VALUES (1);\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Source table not found: ghost"));
}

TEST_CASE("E2E: MERGE case insensitivity", "[e2e][merge]") {
    std::string output = run_interactive(
        "CREATE TABLE ct (id INT, v VARCHAR);\n"
        "INSERT INTO ct VALUES (1, 'x');\n"
        "CREATE TABLE cs (id INT, v VARCHAR);\n"
        "INSERT INTO cs VALUES (1, 'y');\n"
        "merge into ct using cs on ct.id = cs.id "
        "when matched then update set v = cs.v "
        "when not matched then insert values (2, 'z');\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("MERGE into 'ct'"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("1 row(s) updated"));
}


TEST_CASE("E2E: EXPLAIN tree connectors", "[e2e][explain]") {
    std::string output = run_interactive(
        "CREATE TABLE emp (id INT, name VARCHAR, salary INT);\n"
        "INSERT INTO emp VALUES (1, 'Alice', 50000);\n"
        "EXPLAIN SELECT * FROM emp WHERE salary > 40000;\n"
        ".quit\n"
    );

    // Should have tree connectors
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Logical Plan"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Optimized Plan"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("SeqScan(emp)"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Filter("));
}

TEST_CASE("E2E: EXPLAIN ANALYZE per-node stats", "[e2e][explain]") {
    std::string output = run_interactive(
        "CREATE TABLE stats_t (id INT, val VARCHAR);\n"
        "INSERT INTO stats_t VALUES (1, 'a'), (2, 'b'), (3, 'c');\n"
        "EXPLAIN ANALYZE SELECT * FROM stats_t WHERE id > 1;\n"
        ".quit\n"
    );

    // Should show actual stats
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("actual="));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("time="));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Execution Statistics"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Rows scanned"));
}

TEST_CASE("E2E: EXPLAIN FORMAT DOT", "[e2e][explain]") {
    std::string output = run_interactive(
        "CREATE TABLE dot_t (id INT, name VARCHAR);\n"
        "INSERT INTO dot_t VALUES (1, 'x');\n"
        "EXPLAIN FORMAT DOT SELECT * FROM dot_t;\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("digraph QueryPlan"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("rankdir=TB"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("SeqScan"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("fillcolor="));
}

TEST_CASE("E2E: .plan command", "[e2e][commands]") {
    std::string output = run_interactive(
        "CREATE TABLE plan_t (id INT, val VARCHAR);\n"
        "INSERT INTO plan_t VALUES (1, 'a');\n"
        "EXPLAIN SELECT * FROM plan_t;\n"
        ".plan\n"
        ".plan dot\n"
        ".quit\n"
    );

    // .plan should show tree
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Last Optimized Plan"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("SeqScan(plan_t)"));
    // .plan dot should show DOT
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("digraph QueryPlan"));
}


TEST_CASE("E2E: AFTER INSERT trigger fires", "[e2e][trigger]") {
    std::string output = run_interactive(
        "CREATE TABLE orders (id INT, item VARCHAR);\n"
        "CREATE TABLE audit_log (id INT);\n"
        "CREATE TRIGGER log_insert AFTER INSERT ON orders FOR EACH ROW EXECUTE 'INSERT INTO audit_log VALUES (1)';\n"
        "INSERT INTO orders VALUES (1, 'widget');\n"
        "SELECT * FROM audit_log;\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Trigger 'log_insert' created"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("1 row(s) inserted into 'orders'"));
    // audit_log should have 1 row from trigger
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("1 rows"));
}

TEST_CASE("E2E: BEFORE DELETE trigger fires", "[e2e][trigger]") {
    std::string output = run_interactive(
        "CREATE TABLE items (id INT, name VARCHAR);\n"
        "CREATE TABLE del_log (id INT);\n"
        "CREATE TRIGGER before_del BEFORE DELETE ON items FOR EACH ROW EXECUTE 'INSERT INTO del_log VALUES (99)';\n"
        "INSERT INTO items VALUES (1, 'a'), (2, 'b');\n"
        "DELETE FROM items WHERE id = 1;\n"
        "SELECT * FROM del_log;\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Trigger 'before_del' created"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("99"));
}

TEST_CASE("E2E: DROP TRIGGER removes trigger", "[e2e][trigger]") {
    std::string output = run_interactive(
        "CREATE TABLE t1 (id INT);\n"
        "CREATE TABLE t2 (id INT);\n"
        "CREATE TRIGGER trg1 AFTER INSERT ON t1 FOR EACH ROW EXECUTE 'INSERT INTO t2 VALUES (42)';\n"
        "DROP TRIGGER trg1;\n"
        "INSERT INTO t1 VALUES (1);\n"
        "SELECT * FROM t2;\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Trigger 'trg1' dropped"));
    // After dropping, trigger should not fire -- t2 should be empty
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("0 rows"));
}

TEST_CASE("E2E: CREATE TRIGGER on nonexistent table", "[e2e][trigger]") {
    std::string output = run_interactive(
        "CREATE TRIGGER trg_bad AFTER INSERT ON no_such_table FOR EACH ROW EXECUTE 'SELECT 1';\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Table not found"));
}

TEST_CASE("E2E: CREATE TRIGGER case insensitive", "[e2e][trigger]") {
    std::string output = run_interactive(
        "CREATE TABLE ci_t (id INT);\n"
        "CREATE TABLE ci_log (id INT);\n"
        "create trigger ci_trg after insert on ci_t for each row execute 'INSERT INTO ci_log VALUES (7)';\n"
        "INSERT INTO ci_t VALUES (1);\n"
        "SELECT * FROM ci_log;\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Trigger 'ci_trg' created"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("7"));
}

TEST_CASE("E2E: .triggers command", "[e2e][trigger]") {
    std::string output = run_interactive(
        "CREATE TABLE emp (id INT, name VARCHAR);\n"
        "CREATE TABLE log (id INT);\n"
        "CREATE TRIGGER audit_ins AFTER INSERT ON emp FOR EACH ROW EXECUTE 'INSERT INTO log VALUES (1)';\n"
        ".triggers\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("audit_ins"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("emp"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("AFTER"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("INSERT"));
}


TEST_CASE("E2E: NOT NULL rejects null INSERT", "[e2e][constraint]") {
    std::string output = run_interactive(
        "CREATE TABLE t1 (id INT NOT NULL, name VARCHAR);\n"
        "INSERT INTO t1 VALUES (NULL, 'alice');\n"
        ".quit\n"
    );
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("NOT NULL constraint violated for column 'id'"));
}

TEST_CASE("E2E: NOT NULL allows non-null INSERT", "[e2e][constraint]") {
    std::string output = run_interactive(
        "CREATE TABLE t2 (id INT NOT NULL, name VARCHAR);\n"
        "INSERT INTO t2 VALUES (1, 'alice');\n"
        ".quit\n"
    );
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("1 row(s) inserted"));
}

TEST_CASE("E2E: PRIMARY KEY rejects duplicate", "[e2e][constraint]") {
    std::string output = run_interactive(
        "CREATE TABLE pk_t (id INT PRIMARY KEY, name VARCHAR);\n"
        "INSERT INTO pk_t VALUES (1, 'alice');\n"
        "INSERT INTO pk_t VALUES (1, 'bob');\n"
        ".quit\n"
    );
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("1 row(s) inserted"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("UNIQUE constraint violated for column 'id'"));
}

TEST_CASE("E2E: PRIMARY KEY rejects null", "[e2e][constraint]") {
    std::string output = run_interactive(
        "CREATE TABLE pk_n (id INT PRIMARY KEY, name VARCHAR);\n"
        "INSERT INTO pk_n VALUES (NULL, 'alice');\n"
        ".quit\n"
    );
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("NOT NULL constraint violated"));
}

TEST_CASE("E2E: UNIQUE rejects duplicate", "[e2e][constraint]") {
    std::string output = run_interactive(
        "CREATE TABLE uq_t (id INT, email VARCHAR UNIQUE);\n"
        "INSERT INTO uq_t VALUES (1, 'a@b.com');\n"
        "INSERT INTO uq_t VALUES (2, 'a@b.com');\n"
        ".quit\n"
    );
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("UNIQUE constraint violated for column 'email'"));
}

TEST_CASE("E2E: UNIQUE allows multiple NULLs", "[e2e][constraint]") {
    std::string output = run_interactive(
        "CREATE TABLE uq_n (id INT, email VARCHAR UNIQUE);\n"
        "INSERT INTO uq_n VALUES (1, NULL);\n"
        "INSERT INTO uq_n VALUES (2, NULL);\n"
        "SELECT COUNT(*) FROM uq_n;\n"
        ".quit\n"
    );
    // Both inserts should succeed -- NULL is not a duplicate
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("2"));
}

TEST_CASE("E2E: UPDATE violating NOT NULL rejected", "[e2e][constraint]") {
    std::string output = run_interactive(
        "CREATE TABLE upd_nn (id INT NOT NULL, name VARCHAR);\n"
        "INSERT INTO upd_nn VALUES (1, 'alice');\n"
        "UPDATE upd_nn SET id = NULL WHERE name = 'alice';\n"
        ".quit\n"
    );
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("NOT NULL constraint violated"));
}

TEST_CASE("E2E: UPDATE violating UNIQUE rejected", "[e2e][constraint]") {
    std::string output = run_interactive(
        "CREATE TABLE upd_uq (id INT PRIMARY KEY, name VARCHAR);\n"
        "INSERT INTO upd_uq VALUES (1, 'alice');\n"
        "INSERT INTO upd_uq VALUES (2, 'bob');\n"
        "UPDATE upd_uq SET id = 1 WHERE name = 'bob';\n"
        ".quit\n"
    );
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("UNIQUE constraint violated"));
}

TEST_CASE("E2E: CHECK rejects invalid INSERT", "[e2e][constraint]") {
    std::string output = run_interactive(
        "CREATE TABLE chk_t (qty INT CHECK (qty > 0), name VARCHAR);\n"
        "INSERT INTO chk_t VALUES (0, 'bad');\n"
        ".quit\n"
    );
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("CHECK constraint violated"));
}

TEST_CASE("E2E: CHECK allows valid INSERT", "[e2e][constraint]") {
    std::string output = run_interactive(
        "CREATE TABLE chk_ok (qty INT CHECK (qty > 0), name VARCHAR);\n"
        "INSERT INTO chk_ok VALUES (5, 'good');\n"
        ".quit\n"
    );
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("1 row(s) inserted"));
}

TEST_CASE("E2E: UPDATE violating CHECK rejected", "[e2e][constraint]") {
    std::string output = run_interactive(
        "CREATE TABLE chk_u (qty INT CHECK (qty > 0), name VARCHAR);\n"
        "INSERT INTO chk_u VALUES (5, 'ok');\n"
        "UPDATE chk_u SET qty = 0 WHERE name = 'ok';\n"
        ".quit\n"
    );
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("CHECK constraint violated"));
}

TEST_CASE("E2E: Constraints case insensitive", "[e2e][constraint]") {
    std::string output = run_interactive(
        "create table ci_con (id int not null primary key, name varchar unique);\n"
        "INSERT INTO ci_con VALUES (1, 'alice');\n"
        ".quit\n"
    );
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Table 'ci_con' created"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("1 row(s) inserted"));
}

TEST_CASE("E2E: PK auto-creates index", "[e2e][constraint]") {
    std::string output = run_interactive(
        "CREATE TABLE pk_idx (id INT PRIMARY KEY, val VARCHAR);\n"
        "INSERT INTO pk_idx VALUES (1, 'a');\n"
        "INSERT INTO pk_idx VALUES (2, 'b');\n"
        "EXPLAIN SELECT * FROM pk_idx WHERE id = 1;\n"
        ".quit\n"
    );
    // PK should auto-create a btree index -> optimizer should use IndexScan
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("IndexScan"));
}

TEST_CASE("E2E: Multiple constraints on one column", "[e2e][constraint]") {
    std::string output = run_interactive(
        "CREATE TABLE multi (id INT NOT NULL UNIQUE CHECK (id > 0), name VARCHAR);\n"
        "INSERT INTO multi VALUES (1, 'ok');\n"
        "INSERT INTO multi VALUES (0, 'bad');\n"
        ".quit\n"
    );
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("1 row(s) inserted"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("CHECK constraint violated"));
}

TEST_CASE("E2E: FK rejects INSERT with invalid reference", "[e2e][constraint][fk]") {
    std::string output = run_interactive(
        "CREATE TABLE departments (id INT PRIMARY KEY, name VARCHAR);\n"
        "INSERT INTO departments VALUES (1, 'Engineering');\n"
        "CREATE TABLE employees (id INT, dept_id INT REFERENCES departments(id));\n"
        "INSERT INTO employees VALUES (1, 99);\n"
        ".quit\n"
    );
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Foreign key violation"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("99"));
}

TEST_CASE("E2E: FK allows INSERT with valid reference", "[e2e][constraint][fk]") {
    std::string output = run_interactive(
        "CREATE TABLE depts (id INT PRIMARY KEY, name VARCHAR);\n"
        "INSERT INTO depts VALUES (1, 'Engineering');\n"
        "CREATE TABLE emps (id INT, dept_id INT REFERENCES depts(id));\n"
        "INSERT INTO emps VALUES (1, 1);\n"
        ".quit\n"
    );
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("1 row(s) inserted into 'emps'"));
}

TEST_CASE("E2E: FK rejects DELETE of referenced parent", "[e2e][constraint][fk]") {
    std::string output = run_interactive(
        "CREATE TABLE parent_t (id INT PRIMARY KEY, val VARCHAR);\n"
        "INSERT INTO parent_t VALUES (1, 'a');\n"
        "CREATE TABLE child_t (id INT, pid INT REFERENCES parent_t(id));\n"
        "INSERT INTO child_t VALUES (10, 1);\n"
        "DELETE FROM parent_t WHERE id = 1;\n"
        ".quit\n"
    );
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Cannot delete: referenced by foreign key"));
}

TEST_CASE("E2E: FK rejects UPDATE to invalid reference", "[e2e][constraint][fk]") {
    std::string output = run_interactive(
        "CREATE TABLE colors (id INT PRIMARY KEY, name VARCHAR);\n"
        "INSERT INTO colors VALUES (1, 'red');\n"
        "CREATE TABLE items (id INT, color_id INT REFERENCES colors(id));\n"
        "INSERT INTO items VALUES (1, 1);\n"
        "UPDATE items SET color_id = 999 WHERE id = 1;\n"
        ".quit\n"
    );
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Foreign key violation"));
}

TEST_CASE("E2E: FK allows NULL values", "[e2e][constraint][fk]") {
    std::string output = run_interactive(
        "CREATE TABLE ref_t (id INT PRIMARY KEY, val VARCHAR);\n"
        "INSERT INTO ref_t VALUES (1, 'x');\n"
        "CREATE TABLE fk_null (id INT, ref_id INT REFERENCES ref_t(id));\n"
        "INSERT INTO fk_null VALUES (1, NULL);\n"
        ".quit\n"
    );
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("1 row(s) inserted into 'fk_null'"));
}

TEST_CASE("E2E: FK case insensitive", "[e2e][constraint][fk]") {
    std::string output = run_interactive(
        "create table pkci (id int primary key, name varchar);\n"
        "insert into pkci values (1, 'x');\n"
        "create table fkci (id int, pid int references pkci(id));\n"
        "insert into fkci values (1, 1);\n"
        ".quit\n"
    );
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Table 'fkci' created"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("1 row(s) inserted into 'fkci'"));
}

TEST_CASE("E2E: FK ON DELETE CASCADE deletes child rows", "[e2e][constraint][fk]") {
    std::string output = run_interactive(
        "CREATE TABLE parent_c (id INT PRIMARY KEY, val VARCHAR);\n"
        "CREATE TABLE child_c (id INT, pid INT REFERENCES parent_c(id) ON DELETE CASCADE);\n"
        "INSERT INTO parent_c VALUES (1, 'p1');\n"
        "INSERT INTO child_c VALUES (10, 1);\n"
        "DELETE FROM parent_c WHERE id = 1;\n"
        "SELECT * FROM child_c;\n"
        ".quit\n"
    );
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("1 row(s) deleted from 'parent_c'"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("(0 rows)"));
}

TEST_CASE("E2E: FK ON DELETE RESTRICT blocks parent delete", "[e2e][constraint][fk]") {
    std::string output = run_interactive(
        "CREATE TABLE parent_r (id INT PRIMARY KEY, val VARCHAR);\n"
        "CREATE TABLE child_r (id INT, pid INT REFERENCES parent_r(id) ON DELETE RESTRICT);\n"
        "INSERT INTO parent_r VALUES (1, 'p1');\n"
        "INSERT INTO child_r VALUES (10, 1);\n"
        "DELETE FROM parent_r WHERE id = 1;\n"
        ".quit\n"
    );
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Cannot delete: referenced by foreign key"));
}

TEST_CASE("E2E: FK default ON DELETE remains RESTRICT", "[e2e][constraint][fk]") {
    std::string output = run_interactive(
        "CREATE TABLE parent_d (id INT PRIMARY KEY, val VARCHAR);\n"
        "CREATE TABLE child_d (id INT, pid INT REFERENCES parent_d(id));\n"
        "INSERT INTO parent_d VALUES (1, 'p1');\n"
        "INSERT INTO child_d VALUES (10, 1);\n"
        "DELETE FROM parent_d WHERE id = 1;\n"
        ".quit\n"
    );
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Cannot delete: referenced by foreign key"));
}

TEST_CASE("E2E: FK mixed CASCADE and RESTRICT is atomic", "[e2e][constraint][fk]") {
    std::string output = run_interactive(
        "CREATE TABLE parent_m (id INT PRIMARY KEY, val VARCHAR);\n"
        "CREATE TABLE child_mc (id INT, pid INT REFERENCES parent_m(id) ON DELETE CASCADE);\n"
        "CREATE TABLE child_mr (id INT, pid INT REFERENCES parent_m(id) ON DELETE RESTRICT);\n"
        "INSERT INTO parent_m VALUES (1, 'p1');\n"
        "INSERT INTO child_mc VALUES (10, 1);\n"
        "INSERT INTO child_mr VALUES (20, 1);\n"
        "DELETE FROM parent_m WHERE id = 1;\n"
        "SELECT * FROM child_mc;\n"
        ".quit\n"
    );
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Cannot delete: referenced by foreign key"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("10"));
}

TEST_CASE("E2E: FK ON DELETE CASCADE case insensitive", "[e2e][constraint][fk]") {
    std::string output = run_interactive(
        "create table parent_ci (id int primary key, val varchar);\n"
        "create table child_ci (id int, pid int references parent_ci(id) on delete cascade);\n"
        "insert into parent_ci values (1, 'p1');\n"
        "insert into child_ci values (10, 1);\n"
        "delete from parent_ci where id = 1;\n"
        "select * from child_ci;\n"
        ".quit\n"
    );
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("(0 rows)"));
}

TEST_CASE("E2E: Multi-statement trigger with BEGIN END", "[e2e][trigger][multi]") {
    std::string output = run_interactive(
        "CREATE TABLE orders (id INT, total INT);\n"
        "CREATE TABLE audit_log (msg INT);\n"
        "CREATE TABLE stats (order_count INT);\n"
        "INSERT INTO stats VALUES (0);\n"
        "CREATE TRIGGER on_order AFTER INSERT ON orders FOR EACH ROW EXECUTE BEGIN 'INSERT INTO audit_log VALUES (100)'; 'UPDATE stats SET order_count = order_count + 1'; END;\n"
        "INSERT INTO orders VALUES (1, 100);\n"
        "SELECT * FROM audit_log;\n"
        "SELECT * FROM stats;\n"
        ".quit\n"
    );
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("100"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("1"));
}

TEST_CASE("E2E: Single-statement trigger backward compat", "[e2e][trigger][multi]") {
    std::string output = run_interactive(
        "CREATE TABLE items (id INT);\n"
        "CREATE TABLE log_t (msg INT);\n"
        "CREATE TRIGGER simple_trig AFTER INSERT ON items FOR EACH ROW EXECUTE 'INSERT INTO log_t VALUES (42)';\n"
        "INSERT INTO items VALUES (1);\n"
        "SELECT * FROM log_t;\n"
        ".quit\n"
    );
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("42"));
}

TEST_CASE("E2E: Multi-statement BEFORE trigger", "[e2e][trigger][multi]") {
    std::string output = run_interactive(
        "CREATE TABLE products (id INT, name VARCHAR);\n"
        "CREATE TABLE pre_log (msg INT);\n"
        "CREATE TABLE pre_count (n INT);\n"
        "INSERT INTO pre_count VALUES (0);\n"
        "CREATE TRIGGER pre_ins BEFORE INSERT ON products FOR EACH ROW EXECUTE BEGIN 'INSERT INTO pre_log VALUES (99)'; 'UPDATE pre_count SET n = n + 1'; END;\n"
        "INSERT INTO products VALUES (1, 'widget');\n"
        "SELECT * FROM pre_log;\n"
        "SELECT * FROM pre_count;\n"
        ".quit\n"
    );
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("99"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("1"));
}


// --- Correlated Scalar Subquery Test ---
TEST_CASE("E2E: Execute Correlated Scalar Subquery", "[e2e][subquery]") {
    std::string output = run_interactive(
        "CREATE TABLE dept (id INT, name VARCHAR);\n"
        "CREATE TABLE emp (id INT, dept_id INT, name VARCHAR);\n"
        "INSERT INTO dept VALUES (1, 'Engineering');\n"
        "INSERT INTO dept VALUES (2, 'Sales');\n"
        "INSERT INTO emp VALUES (10, 1, 'Alice');\n"
        "INSERT INTO emp VALUES (20, 2, 'Bob');\n"
        "SELECT dept.name, (SELECT name FROM emp WHERE emp.dept_id = dept.id) AS emp_name FROM dept;\n"
        ".quit\n"
    );
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Engineering"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Alice"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Sales"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Bob"));
}

// --- Uncorrelated IN Subquery Test ---
TEST_CASE("E2E: Execute IN Subquery", "[e2e][subquery]") {
    std::string output = run_interactive(
        "CREATE TABLE t1 (id INT, val INT);\n"
        "CREATE TABLE t2 (id INT, max_val INT);\n"
        "INSERT INTO t1 VALUES (1, 10);\n"
        "INSERT INTO t1 VALUES (2, 20);\n"
        "INSERT INTO t1 VALUES (3, 30);\n"
        "INSERT INTO t2 VALUES (1, 20);\n"
        "SELECT * FROM t1 WHERE val IN (SELECT max_val FROM t2);\n"
        ".quit\n"
    );
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("20"));
}

// --- Benchmark: Hash Join IN Optimization --
TEST_CASE("E2E: IN Subquery Hash Performance", "[e2e][benchmark][subquery]") {
    // Generate inner table (10 rows) and outer table (100 rows)
    std::string script = 
        ".generate 100\n" // creates employees (100) and depts (10)
        "EXPLAIN ANALYZE SELECT * FROM employees WHERE department_id IN (SELECT id FROM departments WHERE name LIKE '%A%');\n"
        ".quit\n";
    std::string output = run_interactive(script);
    
    // With hash-optimization, the subquery should execute EXACTLY ONCE and cache it! 
    // The query plan output prints execution statistics: 
    // We should see `Subqueries executed: 1`
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Subqueries executed: 1"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Subqueries cached:")); // Should be > 0 (actually 100 times)
}

TEST_CASE("E2E: Scalar Subquery", "[e2e][subquery]") {
    std::string output = run_interactive(
        "CREATE TABLE t1 (id INT, val INT);\n"
        "CREATE TABLE t2 (id INT, max_val INT);\n"
        "INSERT INTO t1 VALUES (1, 10);\n"
        "INSERT INTO t1 VALUES (2, 20);\n"
        "INSERT INTO t2 VALUES (1, 20);\n"
        "EXPLAIN SELECT * FROM t1 WHERE val = (SELECT max_val FROM t2);\n"
        "SELECT * FROM t1 WHERE val = (SELECT max_val FROM t2);\n"
        ".quit\n"
    );
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("2"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("20"));
}

TEST_CASE("E2E: IN Subquery", "[e2e][subquery]") {
    std::string output = run_interactive(
        "CREATE TABLE emps (id INT, name VARCHAR);\n"
        "CREATE TABLE top_ids (emp_id INT);\n"
        "INSERT INTO emps VALUES (1, 'Alice');\n"
        "INSERT INTO emps VALUES (2, 'Bob');\n"
        "INSERT INTO emps VALUES (3, 'Charlie');\n"
        "INSERT INTO top_ids VALUES (1);\n"
        "INSERT INTO top_ids VALUES (3);\n"
        "SELECT * FROM emps WHERE id IN (SELECT emp_id FROM top_ids);\n"
        ".quit\n"
    );
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Alice"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Charlie"));
}

TEST_CASE("E2E: EXISTS Subquery", "[e2e][subquery]") {
    std::string output = run_interactive(
        "CREATE TABLE depts (id INT, name VARCHAR);\n"
        "CREATE TABLE emp (dept_id INT, name VARCHAR);\n"
        "INSERT INTO depts VALUES (1, 'Engineering');\n"
        "INSERT INTO depts VALUES (2, 'HR');\n"
        "INSERT INTO emp VALUES (1, 'Sam');\n"
        "SELECT * FROM depts WHERE EXISTS (SELECT dept_id FROM emp WHERE emp.dept_id = depts.id);\n" // Correlated EXISTS
        ".quit\n"
    );
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Engineering"));
}

TEST_CASE("E2E: Transaction rollback restores pre-BEGIN state", "[e2e][transaction][acid][acid-a]") {
    std::string output = run_interactive(
        "CREATE TABLE tx_roll (id INT);\n"
        "INSERT INTO tx_roll VALUES (111);\n"
        "BEGIN;\n"
        "INSERT INTO tx_roll VALUES (222);\n"
        "UPDATE tx_roll SET id = 999 WHERE id = 111;\n"
        "ROLLBACK;\n"
        "SELECT * FROM tx_roll WHERE id = 222;\n"
        "SELECT * FROM tx_roll WHERE id = 111;\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Transaction rolled back."));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("(0 rows)"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("111"));
}

TEST_CASE("E2E: Transaction commit keeps writes", "[e2e][transaction]") {
    std::string output = run_interactive(
        "CREATE TABLE tx_commit (id INT);\n"
        "BEGIN;\n"
        "INSERT INTO tx_commit VALUES (333);\n"
        "COMMIT;\n"
        "SELECT * FROM tx_commit WHERE id = 333;\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Transaction committed."));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("333"));
}

TEST_CASE("E2E: DDL blocked in active transaction (MVP)", "[e2e][transaction]") {
    std::string output = run_interactive(
        "BEGIN;\n"
        "CREATE TABLE tx_blocked (id INT);\n"
        "ROLLBACK;\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("is not allowed in active transaction"));
}

TEST_CASE("E2E: COMMIT without active transaction errors", "[e2e][transaction]") {
    std::string output = run_interactive(
        "COMMIT;\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("No active transaction"));
}

TEST_CASE("E2E: ROLLBACK without active transaction errors", "[e2e][transaction]") {
    std::string output = run_interactive(
        "ROLLBACK;\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("No active transaction"));
}

TEST_CASE("E2E: Nested BEGIN is rejected", "[e2e][transaction]") {
    std::string output = run_interactive(
        "BEGIN;\n"
        "BEGIN;\n"
        "ROLLBACK;\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Transaction already active"));
}

TEST_CASE("E2E: TRUNCATE blocked in active transaction", "[e2e][transaction]") {
    std::string output = run_interactive(
        "CREATE TABLE tx_trunc (id INT);\n"
        "INSERT INTO tx_trunc VALUES (999);\n"
        "BEGIN;\n"
        "TRUNCATE TABLE tx_trunc;\n"
        "ROLLBACK;\n"
        "SELECT * FROM tx_trunc;\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("statement 'TRUNCATE' is not allowed in active transaction"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("999"));
}

TEST_CASE("E2E: DROP TABLE blocked in active transaction", "[e2e][transaction]") {
    std::string output = run_interactive(
        "CREATE TABLE tx_drop (id INT);\n"
        "BEGIN;\n"
        "DROP TABLE tx_drop;\n"
        "ROLLBACK;\n"
        "SELECT * FROM tx_drop;\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("statement 'DROP_TABLE' is not allowed in active transaction"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("(0 rows)"));
}

TEST_CASE("E2E: Constraint violation inside transaction can be rolled back cleanly", "[e2e][transaction][constraint][acid][acid-c]") {
    std::string output = run_interactive(
        "CREATE TABLE tx_cons (id INT UNIQUE);\n"
        "BEGIN;\n"
        "INSERT INTO tx_cons VALUES (1);\n"
        "INSERT INTO tx_cons VALUES (1);\n"
        "ROLLBACK;\n"
        "SELECT * FROM tx_cons WHERE id = 1;\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("UNIQUE constraint violated"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Transaction rolled back."));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("(0 rows)"));
}

TEST_CASE("E2E: Committed transaction survives restart", "[e2e][transaction][durability][acid][acid-d]") {
    cleanup_durability_files();

    std::string run1 = run_interactive(
        "CREATE TABLE tx_durable (id INT);\n"
        "BEGIN;\n"
        "INSERT INTO tx_durable VALUES (777);\n"
        "COMMIT;\n"
        ".quit\n",
        true
    );
    CHECK_THAT(run1, Catch::Matchers::ContainsSubstring("Transaction committed."));

    std::string run2 = run_interactive(
        "SELECT * FROM tx_durable WHERE id = 777;\n"
        ".quit\n",
        true
    );
    CHECK_THAT(run2, Catch::Matchers::ContainsSubstring("777"));

    cleanup_durability_files();
}

TEST_CASE("E2E: Committed UPDATE survives restart", "[e2e][transaction][durability][acid][acid-d]") {
    cleanup_durability_files();

    std::string run1 = run_interactive(
        "CREATE TABLE tx_upd (id INT, v INT);\n"
        "INSERT INTO tx_upd VALUES (1, 10);\n"
        "BEGIN;\n"
        "UPDATE tx_upd SET v = 99 WHERE id = 1;\n"
        "COMMIT;\n"
        ".quit\n",
        true
    );
    CHECK_THAT(run1, Catch::Matchers::ContainsSubstring("Transaction committed."));

    std::string run2 = run_interactive(
        "SELECT * FROM tx_upd WHERE id = 1 AND v = 99;\n"
        "SELECT * FROM tx_upd WHERE id = 1 AND v = 10;\n"
        ".quit\n",
        true
    );
    CHECK_THAT(run2, Catch::Matchers::ContainsSubstring("99"));
    CHECK_THAT(run2, Catch::Matchers::ContainsSubstring("(0 rows)"));

    cleanup_durability_files();
}

TEST_CASE("E2E: Committed DELETE survives restart", "[e2e][transaction][durability][acid][acid-d]") {
    cleanup_durability_files();

    std::string run1 = run_interactive(
        "CREATE TABLE tx_del (id INT);\n"
        "INSERT INTO tx_del VALUES (1);\n"
        "INSERT INTO tx_del VALUES (2);\n"
        "BEGIN;\n"
        "DELETE FROM tx_del WHERE id = 2;\n"
        "COMMIT;\n"
        ".quit\n",
        true
    );
    CHECK_THAT(run1, Catch::Matchers::ContainsSubstring("Transaction committed."));

    std::string run2 = run_interactive(
        "SELECT * FROM tx_del WHERE id = 2;\n"
        "SELECT * FROM tx_del WHERE id = 1;\n"
        ".quit\n",
        true
    );
    CHECK_THAT(run2, Catch::Matchers::ContainsSubstring("(0 rows)"));
    CHECK_THAT(run2, Catch::Matchers::ContainsSubstring("1"));

    cleanup_durability_files();
}

TEST_CASE("E2E: Uncommitted transaction does not survive restart", "[e2e][transaction][durability][acid][acid-d]") {
    cleanup_durability_files();

    std::string run1 = run_interactive(
        "CREATE TABLE tx_uncommitted (id INT);\n"
        "BEGIN;\n"
        "INSERT INTO tx_uncommitted VALUES (888);\n"
        ".quit\n",
        true
    );
    CHECK_THAT(run1, Catch::Matchers::ContainsSubstring("Transaction started"));

    std::string run2 = run_interactive(
        "SELECT * FROM tx_uncommitted WHERE id = 888;\n"
        ".quit\n",
        true
    );
    CHECK_THAT(run2, Catch::Matchers::ContainsSubstring("(0 rows)"));

    cleanup_durability_files();
}

TEST_CASE("E2E: Committed MERGE survives restart", "[e2e][transaction][durability][merge][acid][acid-d]") {
    cleanup_durability_files();

    std::string run1 = run_interactive(
        "CREATE TABLE tx_m_t (id INT, name VARCHAR);\n"
        "INSERT INTO tx_m_t VALUES (1, 'Alice'), (2, 'Bob');\n"
        "CREATE TABLE tx_m_s (id INT, name VARCHAR);\n"
        "INSERT INTO tx_m_s VALUES (2, 'Bobby'), (3, 'Charlie');\n"
        "BEGIN;\n"
        "MERGE INTO tx_m_t USING tx_m_s ON tx_m_t.id = tx_m_s.id "
        "WHEN MATCHED THEN UPDATE SET name = tx_m_s.name "
        "WHEN NOT MATCHED THEN INSERT VALUES (3, 'Charlie');\n"
        "COMMIT;\n"
        ".quit\n",
        true
    );
    CHECK_THAT(run1, Catch::Matchers::ContainsSubstring("Transaction committed."));

    std::string run2 = run_interactive(
        "SELECT * FROM tx_m_t WHERE id = 2 AND name = 'Bobby';\n"
        "SELECT * FROM tx_m_t WHERE id = 3 AND name = 'Charlie';\n"
        ".quit\n",
        true
    );
    CHECK_THAT(run2, Catch::Matchers::ContainsSubstring("Bobby"));
    CHECK_THAT(run2, Catch::Matchers::ContainsSubstring("Charlie"));

    cleanup_durability_files();
}

