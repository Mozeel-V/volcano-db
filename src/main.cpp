#include <iostream>
#include <string>
#include <sstream>
#include <memory>
#include <chrono>
#include <iomanip>
#include <fstream>

#include "ast/ast.h"
#include "storage/storage.h"
#include "planner/planner.h"
#include "optimizer/optimizer.h"
#include "executor/executor.h"
#include "executor/view_support.h"
#include "benchmark/benchmark.h"

// From Bison-generated parser
extern int yyparse();
extern ast::StmtPtr get_parsed_stmt();

// Flex buffer management
typedef struct yy_buffer_state* YY_BUFFER_STATE;
extern YY_BUFFER_STATE yy_scan_string(const char*);
extern void yy_delete_buffer(YY_BUFFER_STATE);

// Implement parse_sql using the Flex/Bison parser
namespace ast {
StmtPtr parse_sql(const std::string& sql) {
    YY_BUFFER_STATE buf = yy_scan_string(sql.c_str());
    int rc = yyparse();
    yy_delete_buffer(buf);
    if (rc != 0) return nullptr;
    return get_parsed_stmt();
}
}

static void print_help() {
    std::cout << R"(
Simple Query Processor — Commands:
    SQL queries:      SELECT, CREATE TABLE, CREATE INDEX, CREATE VIEW,
                                        CREATE MATERIALIZED VIEW, INSERT, LOAD
  EXPLAIN <query>   Show query plan
  EXPLAIN ANALYZE   Show plan + execution stats
  BENCHMARK <query> Run query with performance profiling

  Special commands:
    .help           Show this help
    .tables         List loaded tables
    .schema <tbl>   Show table schema
    .generate <n>   Generate sample data (n rows)
    .source <file>  Execute SQL commands from file
    .benchmark      Run benchmark suite
    .quit / .exit   Exit
)";
}

static std::string trim_copy(const std::string& s) {
    size_t start = s.find_first_not_of(" \t\r\n");
    if (start == std::string::npos) return "";
    size_t end = s.find_last_not_of(" \t\r\n");
    return s.substr(start, end - start + 1);
}

static bool starts_with(const std::string& value, const std::string& prefix) {
    return value.rfind(prefix, 0) == 0;
}

static void print_result(const executor::ExecResult& res, bool show_stats) {
    // Header
    for (size_t i = 0; i < res.columns.size(); i++) {
        if (i) std::cout << " | ";
        // Strip table prefix for display
        std::string col = res.columns[i];
        auto dot = col.find('.');
        if (dot != std::string::npos) col = col.substr(dot + 1);
        std::cout << std::setw(15) << std::left << col;
    }
    std::cout << "\n";
    for (size_t i = 0; i < res.columns.size(); i++) {
        if (i) std::cout << "-+-";
        std::cout << "---------------";
    }
    std::cout << "\n";

    // Rows
    int display_count = 0;
    for (auto& row : res.rows) {
        for (size_t i = 0; i < row.size() && i < res.columns.size(); i++) {
            if (i) std::cout << " | ";
            std::cout << std::setw(15) << std::left << storage::value_display(row[i]);
        }
        std::cout << "\n";
        display_count++;
        if (display_count >= 50 && res.rows.size() > 55) {
            std::cout << "... (" << res.rows.size() - 50 << " more rows)\n";
            break;
        }
    }
    std::cout << "(" << res.rows.size() << " rows)\n";

    if (show_stats) {
        std::cout << "\nExecution Statistics:\n";
        std::cout << "  Rows scanned:      " << res.stats.rows_scanned << "\n";
        std::cout << "  Rows filtered:     " << res.stats.rows_filtered << "\n";
        std::cout << "  Join comparisons:  " << res.stats.join_comparisons << "\n";
        std::cout << "  Rows produced:     " << res.stats.rows_produced << "\n";
        std::cout << "  Execution time:    " << std::fixed << std::setprecision(3)
                  << res.stats.exec_time_ms << " ms\n";
    }
}

static void execute_sql(const std::string& sql, storage::Catalog& catalog) {
    auto stmt = ast::parse_sql(sql);
    if (!stmt) {
        std::cout << "Error: failed to parse query\n";
        return;
    }

    try {
        switch (stmt->type) {
            case ast::StmtType::ST_CREATE_TABLE: {
                auto& ct = *stmt->create_table;
                auto tbl = std::make_shared<storage::Table>();
                tbl->name = ct.table_name;
                for (auto& cd : ct.columns) {
                    storage::DataType dt = storage::DataType::VARCHAR;
                    if (cd.data_type == "INT") dt = storage::DataType::INT;
                    else if (cd.data_type == "FLOAT") dt = storage::DataType::FLOAT;
                    tbl->schema.push_back({cd.name, dt});
                }
                catalog.add_table(tbl);
                std::cout << "Table '" << ct.table_name << "' created.\n";
                break;
            }
            case ast::StmtType::ST_CREATE_INDEX: {
                auto& ci = *stmt->create_index;
                catalog.create_index(ci.index_name, ci.table_name, ci.column_name, ci.hash_index);
                break;
            }
            case ast::StmtType::ST_CREATE_VIEW: {
                auto& cv = *stmt->create_view;
                if (catalog.get_table(cv.view_name) || catalog.has_view(cv.view_name)) {
                    throw std::runtime_error("Relation already exists: " + cv.view_name);
                }
                catalog.add_view(cv.view_name, cv.query, false);
                std::cout << "View '" << cv.view_name << "' created.\n";
                break;
            }
            case ast::StmtType::ST_CREATE_MATERIALIZED_VIEW: {
                auto& cv = *stmt->create_view;
                if (catalog.get_table(cv.view_name) || catalog.has_view(cv.view_name)) {
                    throw std::runtime_error("Relation already exists: " + cv.view_name);
                }

                std::vector<std::string> temp_tables;
                executor::materialize_dynamic_views_for_select(*cv.query, catalog, temp_tables);
                try {
                    auto mat_tbl = executor::materialize_select_to_table(cv.view_name, *cv.query, catalog);
                    catalog.add_table(mat_tbl);
                    catalog.add_view(cv.view_name, cv.query, true);
                    executor::cleanup_temporary_views(catalog, temp_tables);
                    std::cout << "Materialized view '" << cv.view_name << "' created ("
                              << mat_tbl->rows.size() << " rows).\n";
                } catch (...) {
                    executor::cleanup_temporary_views(catalog, temp_tables);
                    throw;
                }
                break;
            }
            case ast::StmtType::ST_INSERT: {
                std::cout << "INSERT executed (simplified — row data not captured in grammar).\n";
                break;
            }
            case ast::StmtType::ST_LOAD: {
                auto& ld = *stmt->load;
                auto* tbl = catalog.get_table(ld.table_name);
                if (!tbl) {
                    std::cout << "Table not found: " << ld.table_name << "\n";
                } else {
                    tbl->load_csv(ld.file_path);
                    std::cout << "Loaded " << tbl->rows.size() << " rows into '" << ld.table_name << "'\n";
                }
                break;
            }
            case ast::StmtType::ST_EXPLAIN: {
                std::vector<std::string> temp_tables;
                executor::materialize_dynamic_views_for_select(*stmt->select, catalog, temp_tables);

                try {
                    auto plan = planner::build_logical_plan(*stmt->select, catalog);
                    std::cout << "\n── Logical Plan (before optimization) ──\n";
                    std::cout << plan->to_string();

                    auto opt_plan = optimizer::optimize(plan, catalog);
                    std::cout << "\n── Optimized Plan ──\n";
                    std::cout << opt_plan->to_string();

                    if (stmt->explain_analyze) {
                        auto result = executor::execute(opt_plan, catalog);
                        std::cout << "\n── Execution Statistics ──\n";
                        std::cout << "  Rows scanned:     " << result.stats.rows_scanned << "\n";
                        std::cout << "  Rows filtered:    " << result.stats.rows_filtered << "\n";
                        std::cout << "  Join comparisons: " << result.stats.join_comparisons << "\n";
                        std::cout << "  Rows produced:    " << result.stats.rows_produced << "\n";
                        std::cout << "  Execution time:   " << std::fixed << std::setprecision(3)
                                  << result.stats.exec_time_ms << " ms\n";
                    }

                    executor::cleanup_temporary_views(catalog, temp_tables);
                } catch (...) {
                    executor::cleanup_temporary_views(catalog, temp_tables);
                    throw;
                }
                break;
            }
            case ast::StmtType::ST_SELECT: {
                auto result = executor::execute_select_with_views(*stmt->select, catalog);
                print_result(result, false);
                break;
            }
            case ast::StmtType::ST_BENCHMARK: {
                std::vector<std::string> temp_tables;
                executor::materialize_dynamic_views_for_select(*stmt->select, catalog, temp_tables);

                try {
                    auto plan_unopt = planner::build_logical_plan(*stmt->select, catalog);
                    auto result_unopt = executor::execute(plan_unopt, catalog);
                    std::cout << "Unoptimized: " << result_unopt.stats.exec_time_ms << " ms, "
                              << result_unopt.rows.size() << " rows\n";

                    auto plan_opt = planner::build_logical_plan(*stmt->select, catalog);
                    auto opt = optimizer::optimize(plan_opt, catalog);
                    auto result_opt = executor::execute(opt, catalog);
                    std::cout << "Optimized:   " << result_opt.stats.exec_time_ms << " ms, "
                              << result_opt.rows.size() << " rows\n";

                    double speedup = result_unopt.stats.exec_time_ms > 0 ?
                        result_unopt.stats.exec_time_ms / result_opt.stats.exec_time_ms : 1.0;
                    std::cout << "Speedup:     " << std::fixed << std::setprecision(2) << speedup << "x\n";

                    executor::cleanup_temporary_views(catalog, temp_tables);
                } catch (...) {
                    executor::cleanup_temporary_views(catalog, temp_tables);
                    throw;
                }
                break;
            }
        }
    } catch (const std::exception& e) {
        std::cout << "Error: " << e.what() << "\n";
    }
}

static bool run_script_file(const std::string& path, storage::Catalog& catalog);

static bool handle_dot_command(const std::string& line, storage::Catalog& catalog) {
    if (line == ".quit" || line == ".exit") return false;
    if (line == ".help") {
        print_help();
        return true;
    }
    if (line == ".tables") {
        for (auto& [name, tbl] : catalog.tables) {
            std::cout << "  " << name << " (" << tbl->rows.size() << " rows)\n";
        }
        for (auto& [name, view] : catalog.views) {
            if (!view->materialized) {
                std::cout << "  " << name << " (view)\n";
            }
        }
        return true;
    }
    if (starts_with(line, ".schema")) {
        std::string tname = trim_copy(line.substr(7));
        auto* tbl = catalog.get_table(tname);
        if (tbl) {
            std::cout << "Table: " << tbl->name << "\n";
            for (auto& col : tbl->schema) {
                std::string type_str = col.type == storage::DataType::INT ? "INT" :
                                      col.type == storage::DataType::FLOAT ? "FLOAT" : "VARCHAR";
                std::cout << "  " << col.name << " " << type_str << "\n";
            }
        } else {
            std::cout << "Table not found: " << tname << "\n";
        }
        return true;
    }
    if (starts_with(line, ".generate")) {
        size_t n = 10000;
        if (line.size() > 9) {
            std::string ns = trim_copy(line.substr(9));
            if (!ns.empty()) n = std::stoul(ns);
        }
        benchmark::generate_employees(catalog, n);
        benchmark::generate_departments(catalog, 10);
        benchmark::generate_orders(catalog, n / 2);
        return true;
    }
    if (starts_with(line, ".source")) {
        std::string path = trim_copy(line.substr(7));
        if (path.empty()) {
            std::cout << "Usage: .source <file.sql>\n";
            return true;
        }
        return run_script_file(path, catalog);
    }
    if (line == ".benchmark") {
        if (catalog.tables.empty()) {
            std::cout << "No tables loaded. Use .generate first.\n";
        } else {
            benchmark::run_benchmarks(catalog);
        }
        return true;
    }

    std::cout << "Unknown command: " << line << "\n";
    return true;
}

static bool process_input_line(const std::string& raw_line,
                               storage::Catalog& catalog,
                               std::string& buffer,
                               bool print_prompts) {
    std::string line = trim_copy(raw_line);
    if (line.empty()) {
        if (print_prompts) std::cout << "sqp> ";
        return true;
    }

    if (line[0] == '.') {
        bool keep_running = handle_dot_command(line, catalog);
        if (keep_running && print_prompts) std::cout << "sqp> ";
        return keep_running;
    }

    if (!buffer.empty()) buffer += " ";
    buffer += line;
    if (buffer.back() != ';') {
        if (print_prompts) std::cout << "  -> ";
        return true;
    }

    execute_sql(buffer, catalog);
    buffer.clear();
    if (print_prompts) std::cout << "sqp> ";
    return true;
}

static bool run_script_file(const std::string& path, storage::Catalog& catalog) {
    std::ifstream script(path);
    if (!script.is_open()) {
        std::cout << "Error: could not open script file '" << path << "'\n";
        return true;
    }

    std::string line;
    std::string buffer;
    while (std::getline(script, line)) {
        if (!process_input_line(line, catalog, buffer, false)) return false;
    }

    if (!trim_copy(buffer).empty()) {
        std::cout << "Warning: unterminated SQL statement at end of file '" << path << "'\n";
    }

    return true;
}

int main(int argc, char* argv[]) {
    storage::Catalog catalog;

    std::cout << "╔══════════════════════════════════════════════════╗\n";
    std::cout << "║     Simple Query Processor & Optimizer (SQP)     ║\n";
    std::cout << "║       Type .help for available commands          ║\n";
    std::cout << "╚══════════════════════════════════════════════════╝\n\n";

    // If argument provided, run it as a script
    if (argc > 1) {
        std::string arg = argv[1];
        if (arg == "--generate") {
            size_t n = argc > 2 ? std::stoul(argv[2]) : 10000;
            benchmark::generate_employees(catalog, n);
            benchmark::generate_departments(catalog, 10);
            benchmark::generate_orders(catalog, n / 2);
            benchmark::run_benchmarks(catalog);
            return 0;
        }
        if (arg == "--file") {
            if (argc < 3) {
                std::cout << "Usage: sqp --file <script.sql>\n";
                return 1;
            }
            run_script_file(argv[2], catalog);
            return 0;
        }

        // Treat a bare argument as a script file path.
        run_script_file(arg, catalog);
        return 0;
    }

    std::string line;
    std::string buffer;
    std::cout << "sqp> ";

    while (std::getline(std::cin, line)) {
        if (!process_input_line(line, catalog, buffer, true)) break;
    }

    std::cout << "\nBye!\n";
    return 0;
}
