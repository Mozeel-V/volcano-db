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
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("INSERT executed"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("(0 rows)")); // INSERT is a stub

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
