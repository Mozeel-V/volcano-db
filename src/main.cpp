#include <iostream>
#include <string>
#include <sstream>
#include <memory>
#include <chrono>
#include <iomanip>

#include "ast/ast.h"
#include "storage/storage.h"
#include "planner/planner.h"
#include "optimizer/optimizer.h"
#include "executor/executor.h"
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
  SQL queries:      SELECT, CREATE TABLE, CREATE INDEX, INSERT, LOAD
  EXPLAIN <query>   Show query plan
  EXPLAIN ANALYZE   Show plan + execution stats
  BENCHMARK <query> Run query with performance profiling

  Special commands:
    .help           Show this help
    .tables         List loaded tables
    .schema <tbl>   Show table schema
    .generate <n>   Generate sample data (n rows)
    .benchmark      Run benchmark suite
    .quit / .exit   Exit
)";
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
    }

    std::string line;
    std::string buffer;
    std::cout << "sqp> ";

    while (std::getline(std::cin, line)) {
        // Trim
        size_t start = line.find_first_not_of(" \t\r\n");
        if (start == std::string::npos) { std::cout << "sqp> "; continue; }
        line = line.substr(start);
        size_t end = line.find_last_not_of(" \t\r\n");
        if (end != std::string::npos) line = line.substr(0, end + 1);

        if (line.empty()) { std::cout << "sqp> "; continue; }

        // Dot commands
        if (line[0] == '.') {
            if (line == ".quit" || line == ".exit") break;
            if (line == ".help") { print_help(); std::cout << "sqp> "; continue; }
            if (line == ".tables") {
                for (auto& [name, tbl] : catalog.tables) {
                    std::cout << "  " << name << " (" << tbl->rows.size() << " rows)\n";
                }
                std::cout << "sqp> "; continue;
            }
            if (line.substr(0, 7) == ".schema") {
                std::string tname = line.substr(7);
                size_t s = tname.find_first_not_of(" \t");
                if (s != std::string::npos) tname = tname.substr(s);
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
                std::cout << "sqp> "; continue;
            }
            if (line.substr(0, 9) == ".generate") {
                size_t n = 10000;
                if (line.size() > 9) {
                    std::string ns = line.substr(9);
                    size_t s = ns.find_first_not_of(" \t");
                    if (s != std::string::npos) n = std::stoul(ns.substr(s));
                }
                benchmark::generate_employees(catalog, n);
                benchmark::generate_departments(catalog, 10);
                benchmark::generate_orders(catalog, n / 2);
                std::cout << "sqp> "; continue;
            }
            if (line == ".benchmark") {
                if (catalog.tables.empty()) {
                    std::cout << "No tables loaded. Use .generate first.\n";
                } else {
                    benchmark::run_benchmarks(catalog);
                }
                std::cout << "sqp> "; continue;
            }
            std::cout << "Unknown command: " << line << "\n";
            std::cout << "sqp> "; continue;
        }

        // Accumulate multi-line SQL until semicolon
        buffer += " " + line;
        if (buffer.back() != ';') {
            std::cout << "  -> ";
            continue;
        }

        // Parse and execute
        auto stmt = ast::parse_sql(buffer);
        buffer.clear();

        if (!stmt) {
            std::cout << "Error: failed to parse query\n";
            std::cout << "sqp> "; continue;
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
                    break;
                }
                case ast::StmtType::ST_SELECT: {
                    auto plan = planner::build_logical_plan(*stmt->select, catalog);
                    auto opt_plan = optimizer::optimize(plan, catalog);
                    auto result = executor::execute(opt_plan, catalog);
                    print_result(result, false);
                    break;
                }
                case ast::StmtType::ST_BENCHMARK: {
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
                    break;
                }
            }
        } catch (const std::exception& e) {
            std::cout << "Error: " << e.what() << "\n";
        }
        std::cout << "sqp> ";
    }

    std::cout << "\nBye!\n";
    return 0;
}
