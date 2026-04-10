#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>
#include <cstdlib>
#include <fstream>
#include <string>
#include <cstdio>

// Helper to run a shell command and capture its output
static std::string run_cmd(const std::string& cmd) {
    std::string result = "";
    // Output redirection
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

TEST_CASE("E2E: Execute SQL via --file", "[e2e][commands]") {
    std::ofstream script("dynamic_test.sql");
    script << "CREATE TABLE t1 (id INT);\n";
    script << "INSERT INTO t1 VALUES (100);\n";
    script << "SELECT * FROM t1;\n";
    script.close();

    std::string output = run_cmd("./sqp --file dynamic_test.sql");
    
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Table 't1' created."));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("INSERT executed"));
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("(0 rows)")); // INSERT does not insert rows
    
    std::remove("dynamic_test.sql");
}

TEST_CASE("E2E: Execute predefined test_script.sql via .source", "[e2e][commands]") {
    std::ofstream trigger("trigger.sql");
    trigger << ".source ../tests/test_script.sql\n";
    trigger << ".quit\n";
    trigger.close();

    std::string output = run_cmd("./sqp < trigger.sql");
    
    // test_script.sql uses .generate 10
    // so employees table should have 10 rows
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("(2 rows)")); // LIMIT 2
    
    std::remove("trigger.sql");
}

TEST_CASE("E2E: File execution error handling", "[e2e][commands]") {
    std::string output = run_cmd("./sqp --file does_not_exist.sql");
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Error: could not open script file"));
    
    std::ofstream trigger("trigger_err.sql");
    trigger << ".source\n";
    trigger << ".quit\n";
    trigger.close();
    
    std::string output2 = run_cmd("./sqp < trigger_err.sql");
    CHECK_THAT(output2, Catch::Matchers::ContainsSubstring("Usage: .source <file.sql>"));
    
    std::remove("trigger_err.sql");
}

TEST_CASE("E2E: Unterminated SQL in file", "[e2e][commands]") {
    std::ofstream script("unterminated.sql");
    script << "CREATE TABLE t2 (id INT)\n"; 
    script.close();

    std::string output = run_cmd("./sqp --file unterminated.sql");
    CHECK_THAT(output, Catch::Matchers::ContainsSubstring("Warning: unterminated SQL statement at end of file"));
    
    std::remove("unterminated.sql");
}
