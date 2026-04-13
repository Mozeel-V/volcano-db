#include <iostream>
#include <string>
#include <sstream>
#include <memory>
#include <chrono>
#include <iomanip>
#include <fstream>
#include <algorithm>
#include <vector>

#include "ast/ast.h"
#include "storage/storage.h"
#include "planner/planner.h"
#include "optimizer/optimizer.h"
#include "executor/executor.h"
#include "executor/view_support.h"
#include "benchmark/benchmark.h"
#include <functional>

// ───── DML expression evaluator (for UPDATE/DELETE WHERE clauses) ─────
static storage::Value dml_eval(const ast::ExprPtr& e, const storage::Table* tbl, const storage::Row& row) {
    if (!e) return std::monostate{};
    switch (e->type) {
        case ast::ExprType::LITERAL_INT:    return e->int_val;
        case ast::ExprType::LITERAL_FLOAT:  return e->float_val;
        case ast::ExprType::LITERAL_STRING: return e->str_val;
        case ast::ExprType::LITERAL_NULL:   return std::monostate{};
        case ast::ExprType::COLUMN_REF: {
            // Try qualified name first (for MERGE combined tables: "target.id")
            if (!e->table_name.empty()) {
                int ci = tbl->column_index(e->table_name + "." + e->column_name);
                if (ci >= 0 && ci < (int)row.size()) return row[ci];
            }
            // Fall back to unqualified name
            int ci = tbl->column_index(e->column_name);
            if (ci >= 0 && ci < (int)row.size()) return row[ci];
            return std::monostate{};
        }
        case ast::ExprType::BINARY_OP: {
            auto lv = dml_eval(e->left, tbl, row);
            auto rv = dml_eval(e->right, tbl, row);
            switch (e->bin_op) {
                case ast::BinOp::OP_EQ:  return (int64_t)(storage::value_equal(lv, rv) ? 1 : 0);
                case ast::BinOp::OP_NEQ: return (int64_t)(storage::value_equal(lv, rv) ? 0 : 1);
                case ast::BinOp::OP_LT:  return (int64_t)(storage::value_less(lv, rv) ? 1 : 0);
                case ast::BinOp::OP_GT:  return (int64_t)(storage::value_less(rv, lv) ? 1 : 0);
                case ast::BinOp::OP_LTE: return (int64_t)((storage::value_less(lv, rv) || storage::value_equal(lv, rv)) ? 1 : 0);
                case ast::BinOp::OP_GTE: return (int64_t)((storage::value_less(rv, lv) || storage::value_equal(lv, rv)) ? 1 : 0);
                case ast::BinOp::OP_AND: return (int64_t)((storage::value_to_int(lv) && storage::value_to_int(rv)) ? 1 : 0);
                case ast::BinOp::OP_OR:  return (int64_t)((storage::value_to_int(lv) || storage::value_to_int(rv)) ? 1 : 0);
                case ast::BinOp::OP_ADD: return storage::value_add(lv, rv);
                case ast::BinOp::OP_SUB: return storage::value_sub(lv, rv);
                case ast::BinOp::OP_MUL: return storage::value_mul(lv, rv);
                case ast::BinOp::OP_DIV: return storage::value_div(lv, rv);
                default: return std::monostate{};
            }
        }
        case ast::ExprType::UNARY_OP: {
            auto ov = dml_eval(e->operand, tbl, row);
            switch (e->unary_op) {
                case ast::UnaryOp::OP_NOT: return (int64_t)(storage::value_to_int(ov) ? 0 : 1);
                case ast::UnaryOp::OP_IS_NULL: return (int64_t)(storage::value_is_null(ov) ? 1 : 0);
                case ast::UnaryOp::OP_IS_NOT_NULL: return (int64_t)(storage::value_is_null(ov) ? 0 : 1);
                case ast::UnaryOp::OP_NEG: {
                    if (storage::value_is_null(ov)) return std::monostate{};
                    if (std::holds_alternative<int64_t>(ov)) return -std::get<int64_t>(ov);
                    return -storage::value_to_double(ov);
                }
            }
            return std::monostate{};
        }
        default:
            return std::monostate{};
    }
}

static bool dml_eval_bool(const ast::ExprPtr& e, const storage::Table* tbl, const storage::Row& row) {
    return storage::value_to_int(dml_eval(e, tbl, row)) != 0;
}

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

// Last optimized plan for .plan command
static planner::LogicalNodePtr last_explain_plan;

static void print_help() {
    std::cout << R"(
Simple Query Processor — Commands:
    SQL queries:      SELECT, CREATE TABLE, CREATE INDEX, CREATE VIEW, CREATE MATERIALIZED VIEW, INSERT, LOAD
  EXPLAIN <query>   Show query plan (tree format)
  EXPLAIN ANALYZE   Show plan + execution stats (per-node)
  EXPLAIN FORMAT DOT <query>  Show plan in Graphviz DOT format
  BENCHMARK <query> Run query with performance profiling

  Special commands:
    .help           Show this help
    .tables         List loaded tables
    .schema <tbl>   Show table schema
    .generate <n>   Generate sample data (n rows)
    .save <file>    Save all current tables to a text file
    .source <file>  Execute SQL commands from file
    .plan           Show last EXPLAIN plan (tree format)
    .plan dot       Show last EXPLAIN plan (DOT format)
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

static std::string data_type_to_string(storage::DataType type) {
    switch (type) {
        case storage::DataType::INT: return "INT";
        case storage::DataType::FLOAT: return "FLOAT";
        case storage::DataType::VARCHAR: return "VARCHAR";
    }
    return "VARCHAR";
}

static void write_table_dump(std::ostream& out, const storage::Table& tbl) {
    out << "Table: " << tbl.name << "\n";
    out << "Rows: " << tbl.rows.size() << "\n";
    out << "Schema:\n";
    for (const auto& col : tbl.schema) {
        out << "  - " << col.name << " " << data_type_to_string(col.type) << "\n";
    }

    if (tbl.schema.empty()) {
        out << "Data:\n";
        out << "(no columns)\n\n";
        return;
    }

    std::vector<size_t> widths(tbl.schema.size(), 0);
    for (size_t i = 0; i < tbl.schema.size(); i++) {
        widths[i] = tbl.schema[i].name.size();
    }

    for (const auto& row : tbl.rows) {
        for (size_t i = 0; i < tbl.schema.size() && i < row.size(); i++) {
            widths[i] = std::max(widths[i], storage::value_display(row[i]).size());
        }
    }

    out << "Data:\n";
    for (size_t i = 0; i < tbl.schema.size(); i++) {
        if (i) out << " | ";
        out << std::left << std::setw(static_cast<int>(widths[i])) << tbl.schema[i].name;
    }
    out << "\n";

    for (size_t i = 0; i < tbl.schema.size(); i++) {
        if (i) out << "-+-";
        out << std::string(widths[i], '-');
    }
    out << "\n";

    if (tbl.rows.empty()) {
        out << "(empty table)\n\n";
        return;
    }

    for (const auto& row : tbl.rows) {
        for (size_t i = 0; i < tbl.schema.size(); i++) {
            if (i) out << " | ";
            std::string cell = i < row.size() ? storage::value_display(row[i]) : "NULL";
            out << std::left << std::setw(static_cast<int>(widths[i])) << cell;
        }
        out << "\n";
    }
    out << "\n";
}

static bool save_tables_to_file(const std::string& path, const storage::Catalog& catalog) {
    std::ofstream out(path, std::ios::out | std::ios::trunc);
    if (!out.is_open()) {
        std::cout << "Error: could not open output file '" << path << "'\n";
        return false;
    }

    out << "Simple Query Processor Table Dump\n";
    out << "=================================\n\n";

    std::vector<std::string> table_names;
    table_names.reserve(catalog.tables.size());
    for (const auto& [name, _] : catalog.tables) {
        table_names.push_back(name);
    }
    std::sort(table_names.begin(), table_names.end());

    if (table_names.empty()) {
        out << "No tables loaded.\n";
        std::cout << "Saved 0 tables to '" << path << "'\n";
        return true;
    }

    for (const auto& table_name : table_names) {
        write_table_dump(out, *catalog.tables.at(table_name));
    }

    std::cout << "Saved " << table_names.size() << " tables to '" << path << "'\n";
    return true;
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

static bool execute_sql(const std::string& sql, storage::Catalog& catalog) {
    auto stmt = ast::parse_sql(sql);
    if (!stmt) {
        std::cout << "Error: failed to parse query\n";
        return false;
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
                auto& ins = *stmt->insert;
                auto* tbl = catalog.get_table(ins.table_name);
                if (!tbl) throw std::runtime_error("Table not found: " + ins.table_name);

                // Fire BEFORE INSERT triggers
                for (auto* td : catalog.get_triggers(ins.table_name, storage::TriggerDef::ON_INSERT)) {
                    if (td->when == storage::TriggerDef::BEFORE) execute_sql(td->action_sql, catalog);
                }

                size_t inserted = 0;
                for (auto& val_exprs : ins.values) {
                    if (val_exprs.size() != tbl->schema.size()) {
                        throw std::runtime_error("Column count mismatch: expected " + std::to_string(tbl->schema.size()) + " but got " + std::to_string(val_exprs.size()));
                    }
                    storage::Row row;
                    for (auto& e : val_exprs) {
                        if (!e) { row.push_back(std::monostate{}); continue; }
                        switch (e->type) {
                            case ast::ExprType::LITERAL_INT:    row.push_back(e->int_val); break;
                            case ast::ExprType::LITERAL_FLOAT:  row.push_back(e->float_val); break;
                            case ast::ExprType::LITERAL_STRING: row.push_back(e->str_val); break;
                            default:                            row.push_back(std::monostate{}); break;
                        }
                    }
                    tbl->rows.push_back(std::move(row));
                    catalog.update_indexes_on_insert(ins.table_name, tbl->rows.size() - 1);
                    inserted++;
                }

                // Fire AFTER INSERT triggers
                for (auto* td : catalog.get_triggers(ins.table_name, storage::TriggerDef::ON_INSERT)) {
                    if (td->when == storage::TriggerDef::AFTER) execute_sql(td->action_sql, catalog);
                }

                std::cout << inserted << " row(s) inserted into '" << ins.table_name << "'.\n";
                break;
            }
            case ast::StmtType::ST_UPDATE: {
                auto& upd = *stmt->update;
                auto* tbl = catalog.get_table(upd.table_name);
                if (!tbl) throw std::runtime_error("Table not found: " + upd.table_name);

                // Fire BEFORE UPDATE triggers
                for (auto* td : catalog.get_triggers(upd.table_name, storage::TriggerDef::ON_UPDATE)) {
                    if (td->when == storage::TriggerDef::BEFORE) execute_sql(td->action_sql, catalog);
                }

                // Resolve column indices for assignments
                std::vector<std::pair<int, ast::ExprPtr>> resolved;
                for (auto& [col, expr] : upd.assignments) {
                    int idx = tbl->column_index(col);
                    if (idx < 0) {
                        throw std::runtime_error("Column not found: " + col);
                    }
                    resolved.emplace_back(idx, expr);
                }
                if ((int)resolved.size() != (int)upd.assignments.size()) break;

                size_t updated = 0;
                for (auto& row : tbl->rows) {
                    if (upd.where_clause && !dml_eval_bool(upd.where_clause, tbl, row))
                        continue;

                    // Apply assignments
                    for (auto& [idx, expr] : resolved) {
                        row[idx] = dml_eval(expr, tbl, row);
                    }
                    updated++;
                }

                // Rebuild indexes for this table
                for (auto& [key, idx] : catalog.indexes) {
                    if (idx->table_name == upd.table_name) idx->build(*tbl);
                }
                for (auto& [key, idx] : catalog.btree_indexes) {
                    if (idx->table_name == upd.table_name) idx->build(*tbl);
                }

                // Fire AFTER UPDATE triggers
                for (auto* td : catalog.get_triggers(upd.table_name, storage::TriggerDef::ON_UPDATE)) {
                    if (td->when == storage::TriggerDef::AFTER) execute_sql(td->action_sql, catalog);
                }

                std::cout << updated << " row(s) updated in '" << upd.table_name << "'.\n";
                break;
            }
            case ast::StmtType::ST_DELETE: {
                auto& del = *stmt->del;
                auto* tbl = catalog.get_table(del.table_name);
                if (!tbl) throw std::runtime_error("Table not found: " + del.table_name);

                // Fire BEFORE DELETE triggers
                for (auto* td : catalog.get_triggers(del.table_name, storage::TriggerDef::ON_DELETE)) {
                    if (td->when == storage::TriggerDef::BEFORE) execute_sql(td->action_sql, catalog);
                }

                size_t before = tbl->rows.size();
                if (!del.where_clause) {
                    tbl->rows.clear();
                } else {
                    tbl->rows.erase(
                        std::remove_if(tbl->rows.begin(), tbl->rows.end(),
                            [&](const storage::Row& row) {
                                return dml_eval_bool(del.where_clause, tbl, row);
                            }),
                        tbl->rows.end()
                    );
                }
                size_t deleted = before - tbl->rows.size();

                // Rebuild indexes for this table
                for (auto& [key, idx] : catalog.indexes) {
                    if (idx->table_name == del.table_name) idx->build(*tbl);
                }
                for (auto& [key, idx] : catalog.btree_indexes) {
                    if (idx->table_name == del.table_name) idx->build(*tbl);
                }

                // Fire AFTER DELETE triggers
                for (auto* td : catalog.get_triggers(del.table_name, storage::TriggerDef::ON_DELETE)) {
                    if (td->when == storage::TriggerDef::AFTER) execute_sql(td->action_sql, catalog);
                }

                std::cout << deleted << " row(s) deleted from '" << del.table_name << "'.\n";
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
                    auto opt_plan = optimizer::optimize(plan, catalog);

                    if (stmt->explain_dot) {
                        // DOT/Graphviz output
                        if (stmt->explain_analyze) {
                            auto result = executor::execute(opt_plan, catalog);
                            result.stats.rows_produced = result.rows.size();
                        }
                        std::cout << opt_plan->to_dot_string();
                    } else {
                        // Tree-connector output
                        std::cout << "\n── Logical Plan (before optimization) ──\n";
                        std::cout << plan->to_tree_string();

                        std::cout << "\n── Optimized Plan ──\n";
                        std::cout << opt_plan->to_tree_string();

                        if (stmt->explain_analyze) {
                            auto result = executor::execute(opt_plan, catalog);
                            std::cout << "\n── Optimized Plan (with actual stats) ──\n";
                            std::cout << opt_plan->to_tree_string();

                            std::cout << "\n── Execution Statistics ──\n";
                            std::cout << "  Rows scanned:     " << result.stats.rows_scanned << "\n";
                            std::cout << "  Rows filtered:    " << result.stats.rows_filtered << "\n";
                            std::cout << "  Join comparisons: " << result.stats.join_comparisons << "\n";
                            std::cout << "  Rows produced:    " << result.stats.rows_produced << "\n";
                            std::cout << "  Execution time:   " << std::fixed << std::setprecision(3)
                                      << result.stats.exec_time_ms << " ms\n";
                        }
                    }

                    // Store last plan for .plan command
                    last_explain_plan = opt_plan;

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
            case ast::StmtType::ST_ALTER_ADD_COL: {
                auto& alt = *stmt->alter;
                auto* tbl = catalog.get_table(alt.table_name);
                if (!tbl) { std::cout << "Error: table not found: " << alt.table_name << "\n"; break; }

                // Check column doesn't already exist
                if (tbl->column_index(alt.column_name) >= 0) {
                    std::cout << "Error: column '" << alt.column_name << "' already exists in table '" << alt.table_name << "'\n";
                    break;
                }

                // Parse data type
                storage::DataType dt = storage::DataType::VARCHAR;
                if (alt.column_type == "INT") dt = storage::DataType::INT;
                else if (alt.column_type == "FLOAT") dt = storage::DataType::FLOAT;

                // Append to schema
                tbl->schema.push_back({alt.column_name, dt});

                // Add NULL to every existing row
                for (auto& row : tbl->rows) {
                    row.push_back(std::monostate{});
                }

                // Rebuild indexes for this table
                for (auto& [key, idx] : catalog.indexes) {
                    if (idx->table_name == alt.table_name) idx->build(*tbl);
                }
                for (auto& [key, idx] : catalog.btree_indexes) {
                    if (idx->table_name == alt.table_name) idx->build(*tbl);
                }

                std::cout << "Column '" << alt.column_name << "' added to table '" << alt.table_name << "'.\n";
                break;
            }
            case ast::StmtType::ST_ALTER_DROP_COL: {
                auto& alt = *stmt->alter;
                auto* tbl = catalog.get_table(alt.table_name);
                if (!tbl) { std::cout << "Error: table not found: " << alt.table_name << "\n"; break; }

                int col_idx = tbl->column_index(alt.column_name);
                if (col_idx < 0) {
                    std::cout << "Error: column '" << alt.column_name << "' not found in table '" << alt.table_name << "'\n";
                    break;
                }

                if (tbl->schema.size() <= 1) {
                    std::cout << "Error: cannot drop the last column of table '" << alt.table_name << "'\n";
                    break;
                }

                // Erase from schema
                tbl->schema.erase(tbl->schema.begin() + col_idx);

                // Erase from every row
                for (auto& row : tbl->rows) {
                    if (col_idx < (int)row.size()) {
                        row.erase(row.begin() + col_idx);
                    }
                }

                // Remove any indexes on the dropped column, rebuild remaining
                std::string drop_key = alt.table_name + "." + alt.column_name;
                catalog.indexes.erase(drop_key);
                catalog.btree_indexes.erase(drop_key);

                for (auto& [key, idx] : catalog.indexes) {
                    if (idx->table_name == alt.table_name) idx->build(*tbl);
                }
                for (auto& [key, idx] : catalog.btree_indexes) {
                    if (idx->table_name == alt.table_name) idx->build(*tbl);
                }

                std::cout << "Column '" << alt.column_name << "' dropped from table '" << alt.table_name << "'.\n";
                break;
            }
            case ast::StmtType::ST_ALTER_RENAME_COL: {
                auto& alt = *stmt->alter;
                auto* tbl = catalog.get_table(alt.table_name);
                if (!tbl) { std::cout << "Error: table not found: " << alt.table_name << "\n"; break; }

                int col_idx = tbl->column_index(alt.column_name);
                if (col_idx < 0) {
                    std::cout << "Error: column '" << alt.column_name << "' not found in table '" << alt.table_name << "'\n";
                    break;
                }

                if (tbl->column_index(alt.new_name) >= 0) {
                    std::cout << "Error: column '" << alt.new_name << "' already exists in table '" << alt.table_name << "'\n";
                    break;
                }

                // Update schema
                tbl->schema[col_idx].name = alt.new_name;

                // Update index column_name fields and re-key index maps
                std::string old_key = alt.table_name + "." + alt.column_name;
                std::string new_key = alt.table_name + "." + alt.new_name;

                auto hit = catalog.indexes.find(old_key);
                if (hit != catalog.indexes.end()) {
                    hit->second->column_name = alt.new_name;
                    catalog.indexes[new_key] = hit->second;
                    catalog.indexes.erase(hit);
                }

                auto bit = catalog.btree_indexes.find(old_key);
                if (bit != catalog.btree_indexes.end()) {
                    bit->second->column_name = alt.new_name;
                    catalog.btree_indexes[new_key] = bit->second;
                    catalog.btree_indexes.erase(bit);
                }

                std::cout << "Column '" << alt.column_name << "' renamed to '" << alt.new_name << "' in table '" << alt.table_name << "'.\n";
                break;
            }
            case ast::StmtType::ST_ALTER_RENAME_TBL: {
                auto& alt = *stmt->alter;
                auto it = catalog.tables.find(alt.table_name);
                if (it == catalog.tables.end()) {
                    std::cout << "Error: table not found: " << alt.table_name << "\n";
                    break;
                }

                if (catalog.tables.find(alt.new_name) != catalog.tables.end()) {
                    std::cout << "Error: table '" << alt.new_name << "' already exists\n";
                    break;
                }

                // Move table to new name
                auto tbl = it->second;
                catalog.tables.erase(it);
                tbl->name = alt.new_name;
                catalog.tables[alt.new_name] = tbl;

                // Update hash index table_name fields and re-key
                std::vector<std::pair<std::string, std::shared_ptr<storage::HashIndex>>> h_updates;
                for (auto hi = catalog.indexes.begin(); hi != catalog.indexes.end(); ) {
                    if (hi->second->table_name == alt.table_name) {
                        hi->second->table_name = alt.new_name;
                        std::string new_key = alt.new_name + "." + hi->second->column_name;
                        h_updates.emplace_back(new_key, hi->second);
                        hi = catalog.indexes.erase(hi);
                    } else {
                        ++hi;
                    }
                }
                for (auto& [k, v] : h_updates) catalog.indexes[k] = v;

                // Update btree index table_name fields and re-key
                std::vector<std::pair<std::string, std::shared_ptr<storage::BTreeIndex>>> b_updates;
                for (auto bi = catalog.btree_indexes.begin(); bi != catalog.btree_indexes.end(); ) {
                    if (bi->second->table_name == alt.table_name) {
                        bi->second->table_name = alt.new_name;
                        std::string new_key = alt.new_name + "." + bi->second->column_name;
                        b_updates.emplace_back(new_key, bi->second);
                        bi = catalog.btree_indexes.erase(bi);
                    } else {
                        ++bi;
                    }
                }
                for (auto& [k, v] : b_updates) catalog.btree_indexes[k] = v;

                // Update views that reference the old table name
                auto vi = catalog.views.find(alt.table_name);
                if (vi != catalog.views.end()) {
                    auto view_def = vi->second;
                    catalog.views.erase(vi);
                    catalog.views[alt.new_name] = view_def;
                }

                std::cout << "Table '" << alt.table_name << "' renamed to '" << alt.new_name << "'.\n";
                break;
            }
            case ast::StmtType::ST_DROP_TABLE: {
                if (!catalog.get_table(stmt->drop_name)) {
                    std::cout << "Table not found: " << stmt->drop_name << "\n"; break;
                }
                // Remove all indexes on this table
                catalog.remove_indexes_for_table(stmt->drop_name);
                // Remove any views associated with this table
                catalog.views.erase(stmt->drop_name);
                catalog.tables.erase(stmt->drop_name);
                std::cout << "Table '" << stmt->drop_name << "' dropped.\n";
                break;
            }
            case ast::StmtType::ST_DROP_INDEX: {
                bool found = catalog.drop_index_by_name(stmt->drop_name);
                if (!found) std::cout << "Index not found: " << stmt->drop_name << "\n";
                else std::cout << "Index '" << stmt->drop_name << "' dropped.\n";
                break;
            }
            case ast::StmtType::ST_DROP_VIEW: {
                if (catalog.views.erase(stmt->drop_name) == 0)
                    std::cout << "View not found: " << stmt->drop_name << "\n";
                else
                    std::cout << "View '" << stmt->drop_name << "' dropped.\n";
                break;
            }
            case ast::StmtType::ST_TRUNCATE: {
                auto* tbl = catalog.get_table(stmt->drop_name);
                if (!tbl) { std::cout << "Table not found: " << stmt->drop_name << "\n"; break; }
                size_t count = tbl->rows.size();
                tbl->rows.clear();
                // Rebuild (clear) indexes
                for (auto& [key, idx] : catalog.indexes)
                    if (idx->table_name == stmt->drop_name) idx->build(*tbl);
                for (auto& [key, idx] : catalog.btree_indexes)
                    if (idx->table_name == stmt->drop_name) idx->build(*tbl);
                std::cout << "Table '" << stmt->drop_name << "' truncated (" << count << " rows removed).\n";
                break;
            }
            case ast::StmtType::ST_MERGE: {
                auto& mg = *stmt->merge;
                auto* target = catalog.get_table(mg.target_table);
                if (!target) throw std::runtime_error("Target table not found: " + mg.target_table);
                auto* source = catalog.get_table(mg.source_table);
                if (!source) throw std::runtime_error("Source table not found: " + mg.source_table);

                // Resolve SET column indices on the target table
                std::vector<std::pair<int, ast::ExprPtr>> resolved_set;
                for (auto& [col, expr] : mg.update_assignments) {
                    int idx = target->column_index(col);
                    if (idx < 0) throw std::runtime_error("Column not found in target: " + col);
                    resolved_set.emplace_back(idx, expr);
                }

                // Build a combined virtual table for ON condition evaluation
                // Schema: [target.col1, target.col2, ..., source.col1, source.col2, ...]
                storage::Table combined;
                for (auto& cd : target->schema)
                    combined.schema.push_back({mg.target_table + "." + cd.name, cd.type});
                for (auto& cd : source->schema)
                    combined.schema.push_back({mg.source_table + "." + cd.name, cd.type});

                size_t updated = 0, inserted = 0;
                size_t orig_target_size = target->rows.size();

                for (auto& src_row : source->rows) {
                    bool matched = false;
                    // Only check against original target rows (not newly inserted ones)
                    for (size_t ti = 0; ti < orig_target_size; ti++) {
                        auto& tgt_row = target->rows[ti];
                        // Build combined row: [target values..., source values...]
                        storage::Row combo;
                        combo.insert(combo.end(), tgt_row.begin(), tgt_row.end());
                        combo.insert(combo.end(), src_row.begin(), src_row.end());

                        if (mg.on_condition && dml_eval_bool(mg.on_condition, &combined, combo)) {
                            // WHEN MATCHED → UPDATE the target row
                            for (auto& [idx, expr] : resolved_set) {
                                tgt_row[idx] = dml_eval(expr, source, src_row);
                            }
                            matched = true;
                            updated++;
                            break;
                        }
                    }

                    if (!matched) {
                        // WHEN NOT MATCHED → INSERT into target
                        if (mg.insert_values.empty()) continue;
                        auto& val_exprs = mg.insert_values[0];
                        if (val_exprs.size() != target->schema.size()) {
                            throw std::runtime_error("MERGE INSERT column count mismatch: expected " +
                                std::to_string(target->schema.size()) + " but got " + std::to_string(val_exprs.size()));
                        }
                        storage::Row new_row;
                        for (auto& e : val_exprs) {
                            new_row.push_back(dml_eval(e, source, src_row));
                        }
                        target->rows.push_back(std::move(new_row));
                        catalog.update_indexes_on_insert(mg.target_table, target->rows.size() - 1);
                        inserted++;
                    }
                }

                // Rebuild indexes on target if rows were updated
                if (updated > 0) {
                    for (auto& [key, idx] : catalog.indexes) {
                        if (idx->table_name == mg.target_table) idx->build(*target);
                    }
                    for (auto& [key, idx] : catalog.btree_indexes) {
                        if (idx->table_name == mg.target_table) idx->build(*target);
                    }
                }

                std::cout << "MERGE into '" << mg.target_table << "': "
                          << updated << " row(s) updated, " << inserted << " row(s) inserted.\n";
                break;
            }
            case ast::StmtType::ST_CREATE_TRIGGER: {
                auto& tg = *stmt->create_trigger;
                if (!catalog.get_table(tg.table_name))
                    throw std::runtime_error("Table not found: " + tg.table_name);
                auto td = std::make_shared<storage::TriggerDef>();
                td->name = tg.trigger_name;
                td->table_name = tg.table_name;
                td->when = static_cast<storage::TriggerDef::When>(tg.when);
                td->event = static_cast<storage::TriggerDef::Event>(tg.event);
                td->action_sql = tg.action_sql;
                catalog.add_trigger(td);
                std::cout << "Trigger '" << tg.trigger_name << "' created on '" << tg.table_name << "'.\n";
                break;
            }
            case ast::StmtType::ST_DROP_TRIGGER: {
                if (!catalog.drop_trigger(stmt->drop_name))
                    throw std::runtime_error("Trigger not found: " + stmt->drop_name);
                std::cout << "Trigger '" << stmt->drop_name << "' dropped.\n";
                break;
            }
        }
    } catch (const std::exception& e) {
        std::cout << "Error: " << e.what() << "\n";
        return false;
    }
    return true;
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
    if (starts_with(line, ".save")) {
        std::string path = trim_copy(line.substr(5));
        if (path.empty()) {
            std::cout << "Usage: .save <file>\n";
            return true;
        }
        save_tables_to_file(path, catalog);
        return true;
    }
    if (starts_with(line, ".plan")) {
        std::string arg = trim_copy(line.substr(5));
        if (!last_explain_plan) {
            std::cout << "No plan available. Run EXPLAIN first.\n";
            return true;
        }
        if (arg == "dot") {
            std::cout << last_explain_plan->to_dot_string();
        } else {
            std::cout << "\n── Last Optimized Plan ──\n";
            std::cout << last_explain_plan->to_tree_string();
        }
        return true;
    }
    if (line == ".triggers") {
        if (catalog.triggers.empty()) {
            std::cout << "No triggers defined.\n";
        } else {
            const char* timings[] = {"BEFORE", "AFTER"};
            const char* events[] = {"INSERT", "UPDATE", "DELETE"};
            std::cout << "Name            | Table           | Timing  | Event   | Action\n";
            std::cout << "----------------+-----------------+---------+---------+---------------------------\n";
            for (auto& t : catalog.triggers) {
                printf("%-15s | %-15s | %-7s | %-7s | %s\n",
                    t->name.c_str(), t->table_name.c_str(),
                    timings[t->when], events[t->event], t->action_sql.c_str());
            }
        }
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
                               bool is_interactive) {
    std::string line = trim_copy(raw_line);
    if (line.empty()) {
        if (is_interactive) std::cout << "sqp> ";
        return true;
    }

    if (line[0] == '.') {
        bool keep_running = handle_dot_command(line, catalog);
        if (keep_running && is_interactive) std::cout << "sqp> ";
        return keep_running;
    }

    if (!buffer.empty()) buffer += " ";
    buffer += line;
    if (buffer.back() != ';') {
        if (is_interactive) std::cout << "  -> ";
        return true;
    }

    bool success = execute_sql(buffer, catalog);
    buffer.clear();

    // In script mode, stop on error; in interactive mode, continue
    if (!success && !is_interactive) return false;

    if (is_interactive) std::cout << "sqp> ";
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
