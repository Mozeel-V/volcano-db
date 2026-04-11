#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>
#include <cstdlib>
#include <fstream>
#include <string>
#include <cstdio>

// Helper to run a shell command and capture its output
static std::string run_cmd(const std::string& cmd) {
    std::string result = "";
    std::string full_cmd = cmd + " > .test_out.tmp 2>&1";
    int ret = std::system(full_cmd.c_str());
    (void)ret;

    std::ifstream ifs(".test_out.tmp");
    if (ifs) {
        result.assign(std::istreambuf_iterator<char>(ifs), std::istreambuf_iterator<char>());
    }
    std::remove(".test_out.tmp");
    return result;
}

// Helper: write lines to a temp script, pipe into sqp, return output
static std::string run_interactive(const std::string& commands) {
    std::ofstream script(".cmd_test.sql");
    script << commands;
    script.close();
    std::string output = run_cmd("./sqp < .cmd_test.sql");
    std::remove(".cmd_test.sql");
    return output;
}

static std::string read_text_file(const std::string& path) {
    std::ifstream ifs(path);
    if (!ifs) return "";
    return std::string(std::istreambuf_iterator<char>(ifs), std::istreambuf_iterator<char>());
}

// ─────────────────────── .source / --file tests ───────────────────────

TEST_CASE("E2E: Execute SQL via --file", "[e2e][commands]") {
    std::ofstream script("dynamic_test.sql");
    script << "CREATE TABLE t1 (id INT);\n";
    script << "INSERT INTO t1 VALUES (100);\n";
    script << "SELECT * FROM t1;\n";
    script.close();

    std::string output = run_cmd("./sqp --file dynamic_test.sql");

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
    std::string output = run_cmd("./sqp --file does_not_exist.sql");
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

    std::string output = run_cmd("./sqp --file unterminated.sql");
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Warning: unterminated SQL statement at end of file"));

    std::remove("unterminated.sql");
}

// ─────────────────────── .help ───────────────────────

TEST_CASE("E2E: .help command", "[e2e][commands]") {
    std::string output = run_interactive(
        ".help\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Simple Query Processor"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring(".help"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring(".tables"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring(".schema"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring(".generate"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring(".save"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring(".source"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring(".benchmark"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring(".quit"));
}

// ─────────────────────── .save ───────────────────────

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

// ─────────────────────── .tables ───────────────────────

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

// ─────────────────────── .schema ───────────────────────

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

// ─────────────────────── .generate ───────────────────────

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
    // .generate with no arg defaults to 10000 — just verify it doesn't crash
    // and tables appear. We use a small number to keep it fast.
    std::string output = run_interactive(
        ".generate 5\n"
        "SELECT COUNT(*) FROM employees;\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("5"));
}

// ─────────────────────── .benchmark ───────────────────────

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

// ─────────────────────── .quit / .exit ───────────────────────

TEST_CASE("E2E: .quit exits cleanly", "[e2e][commands]") {
    std::string output = run_interactive(".quit\n");

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Bye!"));
}

TEST_CASE("E2E: .exit exits cleanly", "[e2e][commands]") {
    std::string output = run_interactive(".exit\n");

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Bye!"));
}

// ─────────────────────── Unknown command ───────────────────────

TEST_CASE("E2E: Unknown dot command", "[e2e][commands]") {
    std::string output = run_interactive(
        ".foobar\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Unknown command: .foobar"));
}

// ─────────────────────── --file usage error ───────────────────────

TEST_CASE("E2E: --file with no path", "[e2e][commands]") {
    std::string output = run_cmd("./sqp --file");
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Usage: sqp --file <script.sql>"));
}

// ─────────────────────── Bare arg as file ───────────────────────

TEST_CASE("E2E: Bare argument treated as script file", "[e2e][commands]") {
    std::ofstream script("bare_arg_test.sql");
    script << "CREATE TABLE bare_t (v INT);\n";
    script.close();

    std::string output = run_cmd("./sqp bare_arg_test.sql");
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Table 'bare_t' created."));

    std::remove("bare_arg_test.sql");
}

// ─────────────────────── DML Tests (INSERT/UPDATE/DELETE) ───────────────────────

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

// ─────────────────────── ALTER TABLE Tests ───────────────────────

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

// ─────────────────────── RENAME TABLE (standalone) Tests ───────────────────────

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

// ─────────────────────── DROP TABLE Tests ───────────────────────

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

// ─────────────────────── DROP VIEW Tests ───────────────────────

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

// ─────────────────────── DROP INDEX Tests ───────────────────────

TEST_CASE("E2E: DROP INDEX nonexistent", "[e2e][ddl]") {
    std::string output = run_interactive(
        "DROP INDEX fake_index;\n"
        ".quit\n"
    );

    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Index not found: fake_index"));
}

// ─────────────────────── TRUNCATE Tests ───────────────────────

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
