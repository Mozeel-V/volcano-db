#include <iostream>
#include <string>
#include <sstream>
#include <memory>
#include <chrono>
#include <iomanip>
#include <fstream>
#include <algorithm>
#include <vector>
#include <set>
#include <cstdint>
#include <mutex>
#include <thread>
#include <atomic>
#include <cstring>
#include <unordered_set>
#include <random>
#include <array>

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

#include "ast/ast.h"
#include "storage/storage.h"
#include "planner/planner.h"
#include "optimizer/optimizer.h"
#include "executor/executor.h"
#include "executor/functions.h"
#include "executor/view_support.h"
#include "benchmark/benchmark.h"
#include "storage/lock_manager.h"
#include "storage/transaction.h"
#include "storage/wal.h"
#include <functional>

enum class ServerAuthMode {
    NONE,
    PASSWORD,
};

static storage::Value dml_eval(const ast::ExprPtr& e, const storage::Table* tbl, const storage::Row& row) {
    if (!e) return std::monostate{};
    switch (e->type) {
        case ast::ExprType::LITERAL_INT:    return e->int_val;
        case ast::ExprType::LITERAL_FLOAT:  return e->float_val;
        case ast::ExprType::LITERAL_STRING: return e->str_val;
        case ast::ExprType::LITERAL_NULL:   return std::monostate{};
        case ast::ExprType::COLUMN_REF: {
            // We try qualified name first (for MERGE combined tables: "target.id")
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
                case ast::BinOp::OP_LIKE: return (int64_t)(storage::value_like(lv, rv) ? 1 : 0);
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
        case ast::ExprType::FUNC_CALL: {
            std::vector<storage::Value> args;
            args.reserve(e->args.size());
            for (const auto& arg : e->args) {
                args.push_back(dml_eval(arg, tbl, row));
            }

            storage::Value out;
            if (executor::try_eval_builtin_function(e->func_name, args, out)) {
                return out;
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
Simple Query Processor -- Commands:
  SQL queries:      SELECT, CREATE TABLE, CREATE INDEX, CREATE VIEW, CREATE MATERIALIZED VIEW, CREATE FUNCTION, INSERT, LOAD
  Transactions:     BEGIN [TRANSACTION], COMMIT, ROLLBACK
  EXPLAIN <query>   Show query plan (tree format)
  EXPLAIN ANALYZE <query>   Show plan + execution stats (per-node)
  EXPLAIN FORMAT DOT <query>  Show plan in Graphviz DOT format
  BENCHMARK <query> Run query with performance profiling

  Special commands:
    .help           Show this help
        .functions [builtins|udf]
                                        List built-in and/or user-defined functions
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

static std::string to_hex(const std::vector<uint8_t>& bytes) {
    static const char* kHex = "0123456789abcdef";
    std::string out;
    out.reserve(bytes.size() * 2);
    for (uint8_t b : bytes) {
        out.push_back(kHex[(b >> 4) & 0xF]);
        out.push_back(kHex[b & 0xF]);
    }
    return out;
}

static std::vector<uint8_t> random_bytes(size_t count) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int> dist(0, 255);
    std::vector<uint8_t> out(count);
    for (size_t i = 0; i < count; i++) out[i] = static_cast<uint8_t>(dist(gen));
    return out;
}

static std::array<uint8_t, 32> sha256_bytes(const std::string& data) {
    auto rotr = [](uint32_t x, uint32_t n) { return (x >> n) | (x << (32 - n)); };
    auto ch = [](uint32_t x, uint32_t y, uint32_t z) { return (x & y) ^ (~x & z); };
    auto maj = [](uint32_t x, uint32_t y, uint32_t z) { return (x & y) ^ (x & z) ^ (y & z); };
    auto bsig0 = [&](uint32_t x) { return rotr(x, 2) ^ rotr(x, 13) ^ rotr(x, 22); };
    auto bsig1 = [&](uint32_t x) { return rotr(x, 6) ^ rotr(x, 11) ^ rotr(x, 25); };
    auto ssig0 = [&](uint32_t x) { return rotr(x, 7) ^ rotr(x, 18) ^ (x >> 3); };
    auto ssig1 = [&](uint32_t x) { return rotr(x, 17) ^ rotr(x, 19) ^ (x >> 10); };

    static const uint32_t k[64] = {
        0x428a2f98,0x71374491,0xb5c0fbcf,0xe9b5dba5,0x3956c25b,0x59f111f1,0x923f82a4,0xab1c5ed5,
        0xd807aa98,0x12835b01,0x243185be,0x550c7dc3,0x72be5d74,0x80deb1fe,0x9bdc06a7,0xc19bf174,
        0xe49b69c1,0xefbe4786,0x0fc19dc6,0x240ca1cc,0x2de92c6f,0x4a7484aa,0x5cb0a9dc,0x76f988da,
        0x983e5152,0xa831c66d,0xb00327c8,0xbf597fc7,0xc6e00bf3,0xd5a79147,0x06ca6351,0x14292967,
        0x27b70a85,0x2e1b2138,0x4d2c6dfc,0x53380d13,0x650a7354,0x766a0abb,0x81c2c92e,0x92722c85,
        0xa2bfe8a1,0xa81a664b,0xc24b8b70,0xc76c51a3,0xd192e819,0xd6990624,0xf40e3585,0x106aa070,
        0x19a4c116,0x1e376c08,0x2748774c,0x34b0bcb5,0x391c0cb3,0x4ed8aa4a,0x5b9cca4f,0x682e6ff3,
        0x748f82ee,0x78a5636f,0x84c87814,0x8cc70208,0x90befffa,0xa4506ceb,0xbef9a3f7,0xc67178f2
    };

    uint32_t h0 = 0x6a09e667, h1 = 0xbb67ae85, h2 = 0x3c6ef372, h3 = 0xa54ff53a;
    uint32_t h4 = 0x510e527f, h5 = 0x9b05688c, h6 = 0x1f83d9ab, h7 = 0x5be0cd19;

    std::vector<uint8_t> msg(data.begin(), data.end());
    uint64_t bit_len = static_cast<uint64_t>(msg.size()) * 8ULL;
    msg.push_back(0x80);
    while ((msg.size() % 64) != 56) msg.push_back(0);
    for (int i = 7; i >= 0; --i) msg.push_back(static_cast<uint8_t>((bit_len >> (i * 8)) & 0xFF));

    for (size_t off = 0; off < msg.size(); off += 64) {
        uint32_t w[64] = {0};
        for (int i = 0; i < 16; i++) {
            w[i] = (static_cast<uint32_t>(msg[off + i * 4]) << 24) |
                   (static_cast<uint32_t>(msg[off + i * 4 + 1]) << 16) |
                   (static_cast<uint32_t>(msg[off + i * 4 + 2]) << 8) |
                   (static_cast<uint32_t>(msg[off + i * 4 + 3]));
        }
        for (int i = 16; i < 64; i++) {
            w[i] = ssig1(w[i - 2]) + w[i - 7] + ssig0(w[i - 15]) + w[i - 16];
        }

        uint32_t a = h0, b = h1, c = h2, d = h3;
        uint32_t e = h4, f = h5, g = h6, h = h7;
        for (int i = 0; i < 64; i++) {
            uint32_t t1 = h + bsig1(e) + ch(e, f, g) + k[i] + w[i];
            uint32_t t2 = bsig0(a) + maj(a, b, c);
            h = g; g = f; f = e; e = d + t1;
            d = c; c = b; b = a; a = t1 + t2;
        }
        h0 += a; h1 += b; h2 += c; h3 += d;
        h4 += e; h5 += f; h6 += g; h7 += h;
    }

    std::array<uint8_t, 32> out{};
    uint32_t hv[8] = {h0,h1,h2,h3,h4,h5,h6,h7};
    for (int i = 0; i < 8; i++) {
        out[i * 4] = static_cast<uint8_t>((hv[i] >> 24) & 0xFF);
        out[i * 4 + 1] = static_cast<uint8_t>((hv[i] >> 16) & 0xFF);
        out[i * 4 + 2] = static_cast<uint8_t>((hv[i] >> 8) & 0xFF);
        out[i * 4 + 3] = static_cast<uint8_t>(hv[i] & 0xFF);
    }
    return out;
}

static std::string sha256_hex(const std::string& data) {
    const auto digest = sha256_bytes(data);
    return to_hex(std::vector<uint8_t>(digest.begin(), digest.end()));
}

static std::pair<std::string, std::string> make_password_material(const std::string& password) {
    const std::string salt_hex = to_hex(random_bytes(16));
    const std::string verifier_hex = sha256_hex(salt_hex + password);
    return {salt_hex, verifier_hex};
}

static std::string data_type_to_string(storage::DataType type) {
    switch (type) {
        case storage::DataType::INT: return "INT";
        case storage::DataType::FLOAT: return "FLOAT";
        case storage::DataType::VARCHAR: return "VARCHAR";
    }
    return "VARCHAR";
}

static storage::DataType parse_data_type_name(const std::string& raw_type) {
    std::string type = raw_type;
    std::transform(type.begin(), type.end(), type.begin(),
                   [](unsigned char c) { return static_cast<char>(std::toupper(c)); });
    if (type == "INT") return storage::DataType::INT;
    if (type == "FLOAT") return storage::DataType::FLOAT;
    return storage::DataType::VARCHAR;
}

static ast::ExprPtr parse_function_body_expression(const std::string& body_sql) {
    auto parsed = ast::parse_sql("SELECT " + body_sql + " FROM __vdb_udf_dummy;");
    if (!parsed || parsed->type != ast::StmtType::ST_SELECT || !parsed->select ||
        parsed->select->select_list.size() != 1) {
        throw std::runtime_error("Invalid function body expression: " + body_sql);
    }
    return parsed->select->select_list[0];
}

enum class FunctionListMode {
    ALL,
    BUILTINS_ONLY,
    UDF_ONLY,
};

static void print_functions(const storage::Catalog& catalog, FunctionListMode mode = FunctionListMode::ALL) {
    auto builtins = executor::list_builtin_scalar_function_names();
    std::sort(builtins.begin(), builtins.end());

    if (mode != FunctionListMode::UDF_ONLY) {
        std::cout << "Built-in scalar functions (" << builtins.size() << "):\n";
        for (const auto& fn : builtins) {
            std::cout << "  - " << fn << "\n";
        }
    }

    if (mode == FunctionListMode::BUILTINS_ONLY) {
        return;
    }

    if (catalog.functions.empty()) {
        std::cout << "User-defined SQL functions: (none)\n";
        return;
    }

    std::vector<const storage::Catalog::FunctionDef*> user_functions;
    user_functions.reserve(catalog.functions.size());
    for (const auto& [_, def] : catalog.functions) {
        user_functions.push_back(def.get());
    }

    std::sort(user_functions.begin(), user_functions.end(),
              [](const storage::Catalog::FunctionDef* a, const storage::Catalog::FunctionDef* b) {
                  return a->name < b->name;
              });

    std::cout << "User-defined SQL functions (" << user_functions.size() << "):\n";
    for (const auto* fn : user_functions) {
        std::cout << "  - " << fn->name << "(";
        for (size_t i = 0; i < fn->params.size(); i++) {
            if (i) std::cout << ", ";
            std::cout << fn->params[i].name << " " << data_type_to_string(fn->params[i].type);
        }
        std::cout << ") RETURNS " << data_type_to_string(fn->return_type) << "\n";
    }
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
        std::cout << "  Subqueries exec:   " << res.stats.subqueries_executed << "\n";
        std::cout << "  Subqueries cached: " << res.stats.subqueries_cached << "\n";
        std::cout << "  Rows produced:     " << res.stats.rows_produced << "\n";
        std::cout << "  Execution time:    " << std::fixed << std::setprecision(3)
                  << res.stats.exec_time_ms << " ms\n";
    }
}

using DeletePlan = std::unordered_map<std::string, std::set<size_t>>;

static void collect_delete_plan(storage::Catalog& catalog,
                                const std::string& parent_table_name,
                                const std::set<size_t>& requested_rows,
                                DeletePlan& plan) {
    if (requested_rows.empty()) return;

    auto* parent_tbl = catalog.get_table(parent_table_name);
    if (!parent_tbl) return;

    auto& planned_rows = plan[parent_table_name];
    std::set<size_t> frontier;
    for (size_t r : requested_rows) {
        if (planned_rows.insert(r).second) {
            frontier.insert(r);
        }
    }
    if (frontier.empty()) return;

    for (auto& [child_name, child_tbl] : catalog.tables) {
        for (auto& fk : child_tbl->foreign_keys) {
            if (fk.ref_table != parent_table_name) continue;

            int parent_col = parent_tbl->column_index(fk.ref_column);
            int child_col = child_tbl->column_index(fk.column_name);
            if (parent_col < 0 || child_col < 0) continue;

            std::set<size_t> child_cascade_rows;
            for (size_t ci = 0; ci < child_tbl->rows.size(); ci++) {
                const auto& crow = child_tbl->rows[ci];
                if (storage::value_is_null(crow[child_col])) continue;

                bool references_frontier = false;
                for (size_t pi : frontier) {
                    if (pi >= parent_tbl->rows.size()) continue;
                    if (storage::value_equal(crow[child_col], parent_tbl->rows[pi][parent_col])) {
                        references_frontier = true;
                        break;
                    }
                }
                if (!references_frontier) continue;

                if (fk.on_delete == storage::FkDeleteAction::RESTRICT) {
                    auto it = plan.find(child_name);
                    bool already_planned = (it != plan.end() && it->second.count(ci) > 0);
                    if (!already_planned) {
                        throw std::runtime_error("Cannot delete: referenced by foreign key in '" + child_name + "'");
                    }
                } else {
                    child_cascade_rows.insert(ci);
                }
            }

            if (!child_cascade_rows.empty()) {
                collect_delete_plan(catalog, child_name, child_cascade_rows, plan);
            }
        }
    }
}

static void apply_delete_plan(storage::Catalog& catalog, const DeletePlan& plan) {
    for (const auto& [table_name, rows_to_delete] : plan) {
        auto* tbl = catalog.get_table(table_name);
        if (!tbl || rows_to_delete.empty()) continue;

        // Erase from back to front so indices remain valid.
        for (auto it = rows_to_delete.rbegin(); it != rows_to_delete.rend(); ++it) {
            if (*it < tbl->rows.size()) {
                tbl->rows.erase(tbl->rows.begin() + static_cast<std::ptrdiff_t>(*it));
            }
        }
    }

    // Rebuild indexes for all affected tables.
    for (const auto& [table_name, _] : plan) {
        auto* tbl = catalog.get_table(table_name);
        if (!tbl) continue;

        for (auto& [key, idx] : catalog.indexes) {
            if (idx->table_name == table_name) idx->build(*tbl);
        }
        for (auto& [key, idx] : catalog.btree_indexes) {
            if (idx->table_name == table_name) idx->build(*tbl);
        }
    }
}

static void collect_tables_from_tref(const ast::TableRefPtr& tref, std::set<std::string>& out_tables) {
    if (!tref) return;

    switch (tref->type) {
        case ast::TableRefType::BASE_TABLE:
            if (!tref->table_name.empty()) out_tables.insert(tref->table_name);
            break;
        case ast::TableRefType::TRT_JOIN:
            collect_tables_from_tref(tref->left, out_tables);
            collect_tables_from_tref(tref->right, out_tables);
            break;
        case ast::TableRefType::TRT_SUBQUERY:
            if (tref->subquery) {
                for (const auto& sub_tref : tref->subquery->from_clause) {
                    collect_tables_from_tref(sub_tref, out_tables);
                }
            }
            break;
    }
}

static std::set<std::string> collect_tables_from_select(const ast::SelectStmt& stmt) {
    std::set<std::string> tables;
    for (const auto& tref : stmt.from_clause) {
        collect_tables_from_tref(tref, tables);
    }
    if (stmt.set_rhs) {
        auto rhs_tables = collect_tables_from_select(*stmt.set_rhs);
        tables.insert(rhs_tables.begin(), rhs_tables.end());
    }
    return tables;
}

static const char* statement_type_name(ast::StmtType type) {
    switch (type) {
        case ast::StmtType::ST_SELECT: return "SELECT";
        case ast::StmtType::ST_BEGIN_TXN: return "BEGIN";
        case ast::StmtType::ST_COMMIT_TXN: return "COMMIT";
        case ast::StmtType::ST_ROLLBACK_TXN: return "ROLLBACK";
        case ast::StmtType::ST_CREATE_TABLE: return "CREATE_TABLE";
        case ast::StmtType::ST_CREATE_INDEX: return "CREATE_INDEX";
        case ast::StmtType::ST_CREATE_VIEW: return "CREATE_VIEW";
        case ast::StmtType::ST_CREATE_MATERIALIZED_VIEW: return "CREATE_MATERIALIZED_VIEW";
        case ast::StmtType::ST_INSERT: return "INSERT";
        case ast::StmtType::ST_UPDATE: return "UPDATE";
        case ast::StmtType::ST_DELETE: return "DELETE";
        case ast::StmtType::ST_LOAD: return "LOAD";
        case ast::StmtType::ST_EXPLAIN: return "EXPLAIN";
        case ast::StmtType::ST_BENCHMARK: return "BENCHMARK";
        case ast::StmtType::ST_ALTER_ADD_COL: return "ALTER_ADD_COLUMN";
        case ast::StmtType::ST_ALTER_DROP_COL: return "ALTER_DROP_COLUMN";
        case ast::StmtType::ST_ALTER_RENAME_COL: return "ALTER_RENAME_COLUMN";
        case ast::StmtType::ST_ALTER_RENAME_TBL: return "ALTER_RENAME_TABLE";
        case ast::StmtType::ST_DROP_TABLE: return "DROP_TABLE";
        case ast::StmtType::ST_DROP_INDEX: return "DROP_INDEX";
        case ast::StmtType::ST_DROP_VIEW: return "DROP_VIEW";
        case ast::StmtType::ST_CREATE_FUNCTION: return "CREATE_FUNCTION";
        case ast::StmtType::ST_DROP_FUNCTION: return "DROP_FUNCTION";
        case ast::StmtType::ST_TRUNCATE: return "TRUNCATE";
        case ast::StmtType::ST_MERGE: return "MERGE";
        case ast::StmtType::ST_CREATE_TRIGGER: return "CREATE_TRIGGER";
        case ast::StmtType::ST_DROP_TRIGGER: return "DROP_TRIGGER";
        case ast::StmtType::ST_CREATE_USER: return "CREATE_USER";
        case ast::StmtType::ST_ALTER_USER: return "ALTER_USER";
        case ast::StmtType::ST_DROP_USER: return "DROP_USER";
        case ast::StmtType::ST_GRANT: return "GRANT";
        case ast::StmtType::ST_REVOKE: return "REVOKE";
        default: return "UNKNOWN";
    }
}

static bool statement_is_transactional_write(ast::StmtType type) {
    switch (type) {
        case ast::StmtType::ST_INSERT:
        case ast::StmtType::ST_UPDATE:
        case ast::StmtType::ST_DELETE:
        case ast::StmtType::ST_MERGE:
            return true;
        default:
            return false;
    }
}

static bool statement_allowed_in_transaction(ast::StmtType type) {
    if (statement_is_transactional_write(type)) {
        return true;
    }

    switch (type) {
        case ast::StmtType::ST_SELECT:
        case ast::StmtType::ST_EXPLAIN:
        case ast::StmtType::ST_BENCHMARK:
        case ast::StmtType::ST_BEGIN_TXN:
        case ast::StmtType::ST_COMMIT_TXN:
        case ast::StmtType::ST_ROLLBACK_TXN:
        case ast::StmtType::ST_CREATE_USER:
        case ast::StmtType::ST_ALTER_USER:
        case ast::StmtType::ST_DROP_USER:
        case ast::StmtType::ST_GRANT:
        case ast::StmtType::ST_REVOKE:
            return true;
        default:
            return false;
    }
}

static bool statement_mutates_catalog(ast::StmtType type) {
    switch (type) {
        case ast::StmtType::ST_CREATE_TABLE:
        case ast::StmtType::ST_CREATE_INDEX:
        case ast::StmtType::ST_CREATE_VIEW:
        case ast::StmtType::ST_CREATE_MATERIALIZED_VIEW:
        case ast::StmtType::ST_INSERT:
        case ast::StmtType::ST_UPDATE:
        case ast::StmtType::ST_DELETE:
        case ast::StmtType::ST_LOAD:
        case ast::StmtType::ST_ALTER_ADD_COL:
        case ast::StmtType::ST_ALTER_DROP_COL:
        case ast::StmtType::ST_ALTER_RENAME_COL:
        case ast::StmtType::ST_ALTER_RENAME_TBL:
        case ast::StmtType::ST_DROP_TABLE:
        case ast::StmtType::ST_DROP_INDEX:
        case ast::StmtType::ST_DROP_VIEW:
        case ast::StmtType::ST_CREATE_FUNCTION:
        case ast::StmtType::ST_DROP_FUNCTION:
        case ast::StmtType::ST_TRUNCATE:
        case ast::StmtType::ST_MERGE:
        case ast::StmtType::ST_CREATE_TRIGGER:
        case ast::StmtType::ST_DROP_TRIGGER:
        case ast::StmtType::ST_CREATE_USER:
        case ast::StmtType::ST_ALTER_USER:
        case ast::StmtType::ST_DROP_USER:
        case ast::StmtType::ST_GRANT:
        case ast::StmtType::ST_REVOKE:
            return true;
        default:
            return false;
    }
}

static bool statement_requires_authorization(ast::StmtType type) {
    switch (type) {
        case ast::StmtType::ST_CREATE_USER:
        case ast::StmtType::ST_ALTER_USER:
        case ast::StmtType::ST_DROP_USER:
        case ast::StmtType::ST_GRANT:
        case ast::StmtType::ST_REVOKE:
            return false;
        default:
            return statement_mutates_catalog(type) || type == ast::StmtType::ST_SELECT || type == ast::StmtType::ST_EXPLAIN;
    }
}

static bool check_statement_authorization(const ast::Statement& stmt,
                                          const storage::Catalog& catalog,
                                          const std::string& current_user,
                                          std::string& auth_error) {
    if (current_user.empty()) {
        auth_error = "auth_required: no authenticated user";
        return false;
    }

    auto require = [&](const std::string& object_type,
                       const std::string& object_name,
                       storage::Catalog::Privilege privilege) {
        if (!catalog.has_privilege(current_user, object_type, object_name, privilege)) {
            auth_error = "permission_denied: user '" + current_user + "' lacks " +
                         storage::privilege_to_string(privilege) + " on " + object_type + " " + object_name;
            return false;
        }
        return true;
    };

    switch (stmt.type) {
        case ast::StmtType::ST_SELECT:
        case ast::StmtType::ST_EXPLAIN: {
            if (!stmt.select) return true;
            auto tables = collect_tables_from_select(*stmt.select);
            for (const auto& table : tables) {
                if (!require("TABLE", table, storage::Catalog::Privilege::PRIV_SELECT)) return false;
            }
            return true;
        }
        case ast::StmtType::ST_INSERT:
            return stmt.insert ? require("TABLE", stmt.insert->table_name, storage::Catalog::Privilege::PRIV_INSERT) : true;
        case ast::StmtType::ST_UPDATE:
            return stmt.update ? require("TABLE", stmt.update->table_name, storage::Catalog::Privilege::PRIV_UPDATE) : true;
        case ast::StmtType::ST_DELETE:
            return stmt.del ? require("TABLE", stmt.del->table_name, storage::Catalog::Privilege::PRIV_DELETE) : true;
        case ast::StmtType::ST_CREATE_INDEX:
            return stmt.create_index ? require("TABLE", stmt.create_index->table_name, storage::Catalog::Privilege::PRIV_ALTER) : true;
        case ast::StmtType::ST_ALTER_ADD_COL:
        case ast::StmtType::ST_ALTER_DROP_COL:
        case ast::StmtType::ST_ALTER_RENAME_COL:
        case ast::StmtType::ST_ALTER_RENAME_TBL:
            return stmt.alter ? require("TABLE", stmt.alter->table_name, storage::Catalog::Privilege::PRIV_ALTER) : true;
        case ast::StmtType::ST_DROP_TABLE:
            return require("TABLE", stmt.drop_name, storage::Catalog::Privilege::PRIV_DROP);
        case ast::StmtType::ST_DROP_VIEW:
            return require("VIEW", stmt.drop_name, storage::Catalog::Privilege::PRIV_DROP);
        case ast::StmtType::ST_CREATE_FUNCTION:
            return true;
        case ast::StmtType::ST_DROP_FUNCTION:
            return require("FUNCTION", stmt.drop_name, storage::Catalog::Privilege::PRIV_DROP);
        default:
            return true;
    }
}

static bool execute_sql(const std::string& sql,
                        storage::Catalog& catalog,
                        storage::TransactionManager& txn_manager,
                        storage::LockManager& lock_manager,
                        storage::WalManager& wal_manager,
                        const std::string& current_user = "local_admin",
                        bool enforce_authorization = false) {
    auto stmt = ast::parse_sql(sql);
    if (!stmt) {
        std::cout << "Error: failed to parse query\n";
        return false;
    }

    const bool txn_active_for_locks = txn_manager.in_transaction();
    const uint64_t lock_txn_id = txn_active_for_locks ? txn_manager.current_txn_id() : 0;
    std::set<std::string> statement_shared_locks;

    auto release_statement_read_locks = [&]() {
        if (!txn_active_for_locks) return;
        for (const auto& table_name : statement_shared_locks) {
            lock_manager.release_shared(table_name, lock_txn_id);
        }
        statement_shared_locks.clear();
    };

    bool checkpoint_after_statement = false;

    if (enforce_authorization && statement_requires_authorization(stmt->type)) {
        std::string auth_error;
        if (!check_statement_authorization(*stmt, catalog, current_user, auth_error)) {
            std::cout << "Error: " << auth_error << "\n";
            return false;
        }
    }

    if (txn_manager.in_transaction() && !statement_allowed_in_transaction(stmt->type)) {
        std::cout << "Error: statement '" << statement_type_name(stmt->type)
                  << "' is not allowed in active transaction"
                  << " (allowed: SELECT/EXPLAIN/BENCHMARK/INSERT/UPDATE/DELETE/MERGE/COMMIT/ROLLBACK).\n";
        return false;
    }

    try {
        switch (stmt->type) {
            case ast::StmtType::ST_BEGIN_TXN: {
                txn_manager.begin();
                wal_manager.log_begin(txn_manager.current_txn_id());
                std::cout << "Transaction started (id=" << txn_manager.current_txn_id() << ").\n";
                break;
            }
            case ast::StmtType::ST_COMMIT_TXN: {
                if (!txn_manager.in_transaction()) {
                    throw std::runtime_error("No active transaction");
                }
                uint64_t txn_id = txn_manager.current_txn_id();
                wal_manager.log_commit(txn_id);
                wal_manager.flush();
                lock_manager.release_all(txn_id);
                txn_manager.commit();
                wal_manager.checkpoint(catalog);
                std::cout << "Transaction committed.\n";
                break;
            }
            case ast::StmtType::ST_ROLLBACK_TXN: {
                if (!txn_manager.in_transaction()) {
                    throw std::runtime_error("No active transaction");
                }
                uint64_t txn_id = txn_manager.current_txn_id();
                wal_manager.log_rollback(txn_id);
                wal_manager.flush();
                lock_manager.release_all(txn_id);
                txn_manager.rollback(catalog);
                std::cout << "Transaction rolled back.\n";
                break;
            }
            case ast::StmtType::ST_CREATE_TABLE: {
                auto& ct = *stmt->create_table;
                auto tbl = std::make_shared<storage::Table>();
                tbl->name = ct.table_name;
                for (auto& cd : ct.columns) {
                    storage::ColumnSchema cs;
                    cs.name = cd.name;
                    cs.type = parse_data_type_name(cd.data_type);
                    cs.not_null = cd.not_null;
                    cs.primary_key = cd.primary_key;
                    cs.is_unique = cd.unique;
                    if (cd.has_default && cd.default_value) {
                        cs.has_default = true;
                        auto& e = *cd.default_value;
                        switch (e.type) {
                            case ast::ExprType::LITERAL_INT:    cs.default_value = e.int_val; break;
                            case ast::ExprType::LITERAL_FLOAT:  cs.default_value = e.float_val; break;
                            case ast::ExprType::LITERAL_STRING: cs.default_value = e.str_val; break;
                            default: cs.default_value = std::monostate{}; break;
                        }
                    }
                    if (cd.check_expr) tbl->check_constraints.push_back(cd.check_expr);
                    if (cd.has_fk) {
                        storage::ForeignKeyDef fk;
                        fk.column_name = cd.name;
                        fk.ref_table = cd.fk_ref_table;
                        fk.ref_column = cd.fk_ref_column;
                        fk.on_delete = (cd.fk_on_delete == ast::FkDeleteAction::CASCADE)
                            ? storage::FkDeleteAction::CASCADE
                            : storage::FkDeleteAction::RESTRICT;
                        tbl->foreign_keys.push_back(fk);
                    }
                    tbl->schema.push_back(cs);
                }
                catalog.add_table(tbl);
                // We auto-create BTree index for PRIMARY KEY columns
                for (auto& cs : tbl->schema) {
                    if (cs.primary_key) {
                        std::string idx_name = "pk_" + ct.table_name + "_" + cs.name;
                        catalog.create_index(idx_name, ct.table_name, cs.name, false);
                    }
                }
                catalog.set_object_owner("TABLE", ct.table_name, current_user);
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
                catalog.set_object_owner("VIEW", cv.view_name, current_user);
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
                    catalog.set_object_owner("VIEW", cv.view_name, current_user);
                    executor::cleanup_temporary_views(catalog, temp_tables);
                    std::cout << "Materialized view '" << cv.view_name << "' created ("
                              << mat_tbl->rows.size() << " rows).\n";
                } catch (...) {
                    executor::cleanup_temporary_views(catalog, temp_tables);
                    throw;
                }
                break;
            }
            case ast::StmtType::ST_CREATE_FUNCTION: {
                auto& cf = *stmt->create_function;
                auto fn = std::make_shared<storage::Catalog::FunctionDef>();
                fn->name = cf.function_name;
                fn->return_type = parse_data_type_name(cf.return_type);

                std::unordered_set<std::string> seen_params;
                for (const auto& param : cf.params) {
                    std::string normalized = param.name;
                    std::transform(normalized.begin(), normalized.end(), normalized.begin(),
                                   [](unsigned char c) { return static_cast<char>(std::toupper(c)); });
                    if (!seen_params.insert(normalized).second) {
                        throw std::runtime_error("Duplicate function parameter: " + param.name);
                    }
                    storage::Catalog::FunctionParam p;
                    p.name = param.name;
                    p.type = parse_data_type_name(param.data_type);
                    fn->params.push_back(std::move(p));
                }

                if (cf.body_expr) {
                    fn->body_expr = cf.body_expr;
                } else {
                    fn->body_expr = parse_function_body_expression(cf.body_sql);
                }
                if (!fn->body_expr) {
                    throw std::runtime_error("Function body is empty for function: " + cf.function_name);
                }

                catalog.add_function(fn);
                catalog.set_object_owner("FUNCTION", cf.function_name, current_user);
                std::cout << "Function '" << cf.function_name << "' created.\n";
                break;
            }
            case ast::StmtType::ST_CREATE_USER: {
                if (current_user != "local_admin") {
                    const auto* user = catalog.get_user(current_user);
                    if (!user || !user->is_superuser) {
                        throw std::runtime_error("permission_denied: only superuser can CREATE USER");
                    }
                }
                auto& cu = *stmt->create_user;
                auto [salt_hex, verifier_hex] = make_password_material(cu.password);
                catalog.add_user(cu.username, salt_hex, verifier_hex, false);
                std::cout << "User '" << cu.username << "' created.\n";
                break;
            }
            case ast::StmtType::ST_ALTER_USER: {
                if (current_user != "local_admin") {
                    const auto* user = catalog.get_user(current_user);
                    if (!user || (!user->is_superuser &&
                                  storage::normalize_identifier_key(current_user) !=
                                  storage::normalize_identifier_key(stmt->alter_user->username))) {
                        throw std::runtime_error("permission_denied: cannot ALTER USER for this principal");
                    }
                }
                auto& au = *stmt->alter_user;
                auto [salt_hex, verifier_hex] = make_password_material(au.password);
                if (!catalog.alter_user_password(au.username, salt_hex, verifier_hex)) {
                    throw std::runtime_error("User not found: " + au.username);
                }
                std::cout << "User '" << au.username << "' updated.\n";
                break;
            }
            case ast::StmtType::ST_DROP_USER: {
                if (current_user != "local_admin") {
                    const auto* user = catalog.get_user(current_user);
                    if (!user || !user->is_superuser) {
                        throw std::runtime_error("permission_denied: only superuser can DROP USER");
                    }
                }
                if (!catalog.drop_user(stmt->drop_name)) {
                    throw std::runtime_error("User not found: " + stmt->drop_name);
                }
                std::cout << "User '" << stmt->drop_name << "' dropped.\n";
                break;
            }
            case ast::StmtType::ST_GRANT:
            case ast::StmtType::ST_REVOKE: {
                if (current_user != "local_admin") {
                    const auto* user = catalog.get_user(current_user);
                    if (!user || !user->is_superuser) {
                        throw std::runtime_error("permission_denied: only superuser can manage grants");
                    }
                }
                auto& gr = *stmt->grant_revoke;
                std::unordered_set<storage::Catalog::Privilege> privileges;
                for (const auto& p : gr.privileges) {
                    privileges.insert(storage::privilege_from_string(p));
                }
                if (stmt->type == ast::StmtType::ST_GRANT) {
                    catalog.grant_privileges(current_user, gr.grantee, gr.object_type, gr.object_name, privileges);
                    std::cout << "Granted privileges on " << gr.object_type << " '" << gr.object_name
                              << "' to '" << gr.grantee << "'.\n";
                } else {
                    catalog.revoke_privileges(gr.grantee, gr.object_type, gr.object_name, privileges);
                    std::cout << "Revoked privileges on " << gr.object_type << " '" << gr.object_name
                              << "' from '" << gr.grantee << "'.\n";
                }
                break;
            }
            case ast::StmtType::ST_INSERT: {
                auto& ins = *stmt->insert;
                auto* tbl = catalog.get_table(ins.table_name);
                if (!tbl) throw std::runtime_error("Table not found: " + ins.table_name);

                if (txn_active_for_locks) {
                    lock_manager.acquire_exclusive(ins.table_name, lock_txn_id);
                }

                // We fire BEFORE INSERT triggers
                for (auto* td : catalog.get_triggers(ins.table_name, storage::TriggerDef::ON_INSERT)) {
                    if (td->when == storage::TriggerDef::BEFORE) {
                        for (auto& sql : td->action_sqls) execute_sql(sql, catalog, txn_manager, lock_manager, wal_manager, current_user, enforce_authorization);
                    }
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

                    // We enforce NOT NULL constraints
                    for (size_t i = 0; i < tbl->schema.size(); i++) {
                        if (tbl->schema[i].not_null && storage::value_is_null(row[i])) {
                            throw std::runtime_error("NOT NULL constraint violated for column '" + tbl->schema[i].name + "'");
                        }
                    }

                    // We enforce UNIQUE / PRIMARY KEY constraints
                    for (size_t i = 0; i < tbl->schema.size(); i++) {
                        if (tbl->schema[i].is_unique || tbl->schema[i].primary_key) {
                            if (storage::value_is_null(row[i])) continue;  // NULL is allowed in UNIQUE
                            for (auto& existing : tbl->rows) {
                                if (storage::value_equal(existing[i], row[i])) {
                                    throw std::runtime_error("UNIQUE constraint violated for column '" + tbl->schema[i].name + "': duplicate value '" + storage::value_display(row[i]) + "'");
                                }
                            }
                        }
                    }

                    // We enforce CHECK constraints
                    for (auto& chk : tbl->check_constraints) {
                        if (!dml_eval_bool(chk, tbl, row)) {
                            throw std::runtime_error("CHECK constraint violated");
                        }
                    }

                    // We enforce FOREIGN KEY constraints
                    for (auto& fk : tbl->foreign_keys) {
                        int fk_idx = tbl->column_index(fk.column_name);
                        if (fk_idx < 0 || storage::value_is_null(row[fk_idx])) continue;
                        auto* parent = catalog.get_table(fk.ref_table);
                        if (!parent) throw std::runtime_error("Referenced table not found: " + fk.ref_table);
                        int ref_idx = parent->column_index(fk.ref_column);
                        if (ref_idx < 0) throw std::runtime_error("Referenced column not found: " + fk.ref_column);
                        bool found = false;
                        for (auto& prow : parent->rows) {
                            if (storage::value_equal(prow[ref_idx], row[fk_idx])) { found = true; break; }
                        }
                        if (!found) throw std::runtime_error("Foreign key violation: value '" + storage::value_display(row[fk_idx]) + "' not found in " + fk.ref_table + "." + fk.ref_column);
                    }

                    tbl->rows.push_back(std::move(row));
                    size_t inserted_row_index = tbl->rows.size() - 1;
                    catalog.update_indexes_on_insert(ins.table_name, inserted_row_index);
                    txn_manager.log_insert(ins.table_name, inserted_row_index);
                    if (txn_manager.in_transaction()) {
                        wal_manager.log_insert(txn_manager.current_txn_id(), ins.table_name, inserted_row_index, tbl->rows[inserted_row_index]);
                    }
                    inserted++;
                }

                // We fire AFTER INSERT triggers
                for (auto* td : catalog.get_triggers(ins.table_name, storage::TriggerDef::ON_INSERT)) {
                    if (td->when == storage::TriggerDef::AFTER) {
                        for (auto& sql : td->action_sqls) execute_sql(sql, catalog, txn_manager, lock_manager, wal_manager, current_user, enforce_authorization);
                    }
                }

                std::cout << inserted << " row(s) inserted into '" << ins.table_name << "'.\n";
                break;
            }
            case ast::StmtType::ST_UPDATE: {
                auto& upd = *stmt->update;
                auto* tbl = catalog.get_table(upd.table_name);
                if (!tbl) throw std::runtime_error("Table not found: " + upd.table_name);

                if (txn_active_for_locks) {
                    lock_manager.acquire_exclusive(upd.table_name, lock_txn_id);
                }

                // We fire BEFORE UPDATE triggers
                for (auto* td : catalog.get_triggers(upd.table_name, storage::TriggerDef::ON_UPDATE)) {
                    if (td->when == storage::TriggerDef::BEFORE) {
                        for (auto& sql : td->action_sqls) execute_sql(sql, catalog, txn_manager, lock_manager, wal_manager, current_user, enforce_authorization);
                    }
                }

                // We resolve column indices for assignments
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
                for (size_t ri = 0; ri < tbl->rows.size(); ri++) {
                    auto& row = tbl->rows[ri];
                    if (upd.where_clause && !dml_eval_bool(upd.where_clause, tbl, row))
                        continue;

                    storage::Row before_row = row;
                    txn_manager.log_update(upd.table_name, ri, before_row);

                    // Apply assignments
                    for (auto& [idx, expr] : resolved) {
                        row[idx] = dml_eval(expr, tbl, row);
                    }

                    // We enforce NOT NULL on updated columns
                    for (auto& [idx, expr] : resolved) {
                        if (tbl->schema[idx].not_null && storage::value_is_null(row[idx])) {
                            throw std::runtime_error("NOT NULL constraint violated for column '" + tbl->schema[idx].name + "'");
                        }
                    }

                    // We enforce UNIQUE / PRIMARY KEY on updated columns
                    for (auto& [idx, expr] : resolved) {
                        if (tbl->schema[idx].is_unique || tbl->schema[idx].primary_key) {
                            if (storage::value_is_null(row[idx])) continue;
                            for (size_t oi = 0; oi < tbl->rows.size(); oi++) {
                                if (oi == ri) continue;
                                if (storage::value_equal(tbl->rows[oi][idx], row[idx])) {
                                    throw std::runtime_error("UNIQUE constraint violated for column '" + tbl->schema[idx].name + "'");
                                }
                            }
                        }
                    }

                    // We enforce CHECK constraints
                    for (auto& chk : tbl->check_constraints) {
                        if (!dml_eval_bool(chk, tbl, row)) {
                            throw std::runtime_error("CHECK constraint violated");
                        }
                    }

                    // We enforce FOREIGN KEY constraints on updated columns
                    for (auto& fk : tbl->foreign_keys) {
                        int fk_idx = tbl->column_index(fk.column_name);
                        if (fk_idx < 0 || storage::value_is_null(row[fk_idx])) continue;
                        bool was_updated = false;
                        for (auto& [idx2, expr2] : resolved) { if (idx2 == fk_idx) { was_updated = true; break; } }
                        if (!was_updated) continue;
                        auto* parent = catalog.get_table(fk.ref_table);
                        if (!parent) throw std::runtime_error("Referenced table not found: " + fk.ref_table);
                        int ref_idx = parent->column_index(fk.ref_column);
                        if (ref_idx < 0) throw std::runtime_error("Referenced column not found: " + fk.ref_column);
                        bool found = false;
                        for (auto& prow : parent->rows) {
                            if (storage::value_equal(prow[ref_idx], row[fk_idx])) { found = true; break; }
                        }
                        if (!found) throw std::runtime_error("Foreign key violation: value '" + storage::value_display(row[fk_idx]) + "' not found in " + fk.ref_table + "." + fk.ref_column);
                    }

                    if (txn_manager.in_transaction()) {
                        wal_manager.log_update(txn_manager.current_txn_id(), upd.table_name, ri, before_row, row);
                    }

                    updated++;
                }

                // We rebuild indexes for this table
                for (auto& [key, idx] : catalog.indexes) {
                    if (idx->table_name == upd.table_name) idx->build(*tbl);
                }
                for (auto& [key, idx] : catalog.btree_indexes) {
                    if (idx->table_name == upd.table_name) idx->build(*tbl);
                }

                // We fire AFTER UPDATE triggers
                for (auto* td : catalog.get_triggers(upd.table_name, storage::TriggerDef::ON_UPDATE)) {
                    if (td->when == storage::TriggerDef::AFTER) {
                        for (auto& sql : td->action_sqls) execute_sql(sql, catalog, txn_manager, lock_manager, wal_manager, current_user, enforce_authorization);
                    }
                }

                std::cout << updated << " row(s) updated in '" << upd.table_name << "'.\n";
                break;
            }
            case ast::StmtType::ST_DELETE: {
                auto& del = *stmt->del;
                auto* tbl = catalog.get_table(del.table_name);
                if (!tbl) throw std::runtime_error("Table not found: " + del.table_name);

                if (txn_active_for_locks) {
                    lock_manager.acquire_exclusive(del.table_name, lock_txn_id);
                }

                // We fire BEFORE DELETE triggers
                for (auto* td : catalog.get_triggers(del.table_name, storage::TriggerDef::ON_DELETE)) {
                    if (td->when == storage::TriggerDef::BEFORE) {
                        for (auto& sql : td->action_sqls) execute_sql(sql, catalog, txn_manager, lock_manager, wal_manager, current_user, enforce_authorization);
                    }
                }

                size_t before = tbl->rows.size();

                std::set<size_t> root_rows;
                for (size_t i = 0; i < tbl->rows.size(); i++) {
                    if (!del.where_clause || dml_eval_bool(del.where_clause, tbl, tbl->rows[i])) {
                        root_rows.insert(i);
                    }
                }

                DeletePlan delete_plan;
                collect_delete_plan(catalog, del.table_name, root_rows, delete_plan);

                if (txn_active_for_locks) {
                    for (const auto& [table_name, _] : delete_plan) {
                        lock_manager.acquire_exclusive(table_name, lock_txn_id);
                    }
                }

                for (const auto& [table_name, rows_to_delete] : delete_plan) {
                    auto* del_tbl = catalog.get_table(table_name);
                    if (!del_tbl) continue;
                    for (size_t row_idx : rows_to_delete) {
                        if (row_idx < del_tbl->rows.size()) {
                            txn_manager.log_delete(table_name, row_idx, del_tbl->rows[row_idx]);
                            if (txn_manager.in_transaction()) {
                                wal_manager.log_delete(txn_manager.current_txn_id(), table_name, row_idx, del_tbl->rows[row_idx]);
                            }
                        }
                    }
                }

                apply_delete_plan(catalog, delete_plan);

                size_t deleted = before - tbl->rows.size();

                // We fire AFTER DELETE triggers
                for (auto* td : catalog.get_triggers(del.table_name, storage::TriggerDef::ON_DELETE)) {
                    if (td->when == storage::TriggerDef::AFTER) {
                        for (auto& sql : td->action_sqls) execute_sql(sql, catalog, txn_manager, lock_manager, wal_manager, current_user, enforce_authorization);
                    }
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
                if (txn_active_for_locks) {
                    auto tables = collect_tables_from_select(*stmt->select);
                    for (const auto& table_name : tables) {
                        lock_manager.acquire_shared(table_name, lock_txn_id);
                        statement_shared_locks.insert(table_name);
                    }
                }

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
                        std::cout << "\n-- Logical Plan (before optimization) --\n";
                        std::cout << plan->to_tree_string();

                        std::cout << "\n-- Optimized Plan --\n";
                        std::cout << opt_plan->to_tree_string();

                        if (stmt->explain_analyze) {
                            auto result = executor::execute(opt_plan, catalog);
                            std::cout << "\n-- Optimized Plan (with actual stats) --\n";
                            std::cout << opt_plan->to_tree_string();

                            std::cout << "\n-- Execution Statistics --\n";
                            std::cout << "  Rows scanned:     " << result.stats.rows_scanned << "\n";
                            std::cout << "  Rows filtered:    " << result.stats.rows_filtered << "\n";
                            std::cout << "  Join comparisons: " << result.stats.join_comparisons << "\n";
                            std::cout << "  Subqueries executed: " << result.stats.subqueries_executed << "\n";
                            std::cout << "  Subqueries cached:   " << result.stats.subqueries_cached << "\n";
                            std::cout << "  Rows produced:    " << result.stats.rows_produced << "\n";
                            std::cout << "  Execution time:   " << std::fixed << std::setprecision(3)
                                      << result.stats.exec_time_ms << " ms\n";
                        }
                    }

                    // We store last plan for .plan command
                    last_explain_plan = opt_plan;

                    executor::cleanup_temporary_views(catalog, temp_tables);
                } catch (...) {
                    executor::cleanup_temporary_views(catalog, temp_tables);
                    throw;
                }
                break;
            }
            case ast::StmtType::ST_SELECT: {
                if (txn_active_for_locks) {
                    auto tables = collect_tables_from_select(*stmt->select);
                    for (const auto& table_name : tables) {
                        lock_manager.acquire_shared(table_name, lock_txn_id);
                        statement_shared_locks.insert(table_name);
                    }
                }

                auto result = executor::execute_select_with_views(*stmt->select, catalog);
                print_result(result, false);
                break;
            }
            case ast::StmtType::ST_BENCHMARK: {
                if (txn_active_for_locks) {
                    auto tables = collect_tables_from_select(*stmt->select);
                    for (const auto& table_name : tables) {
                        lock_manager.acquire_shared(table_name, lock_txn_id);
                        statement_shared_locks.insert(table_name);
                    }
                }

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

                // We check column doesn't already exist
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

                // Adding NULL to every existing row
                for (auto& row : tbl->rows) {
                    row.push_back(std::monostate{});
                }

                // We rebuild indexes for this table
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

                // Erasing from schema
                tbl->schema.erase(tbl->schema.begin() + col_idx);

                // We erase from every row
                for (auto& row : tbl->rows) {
                    if (col_idx < (int)row.size()) {
                        row.erase(row.begin() + col_idx);
                    }
                }

                // We remove any indexes on the dropped column, rebuild remaining
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

                // We update schema
                tbl->schema[col_idx].name = alt.new_name;

                // Updating index column_name fields and re-key index maps
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

                // We move table to new name
                auto tbl = it->second;
                catalog.tables.erase(it);
                tbl->name = alt.new_name;
                catalog.tables[alt.new_name] = tbl;

                // Updating hash index table_name fields and re-key
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

                // Updating btree index table_name fields and re-key
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

                // Updating views that reference the old table name
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
                // We remove all indexes on this table
                catalog.remove_indexes_for_table(stmt->drop_name);
                // We remove any views associated with this table
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
            case ast::StmtType::ST_DROP_FUNCTION: {
                if (!catalog.drop_function(stmt->drop_name)) {
                    std::cout << "Function not found: " << stmt->drop_name << "\n";
                } else {
                    std::cout << "Function '" << stmt->drop_name << "' dropped.\n";
                }
                break;
            }
            case ast::StmtType::ST_TRUNCATE: {
                auto* tbl = catalog.get_table(stmt->drop_name);
                if (!tbl) { std::cout << "Table not found: " << stmt->drop_name << "\n"; break; }
                size_t count = tbl->rows.size();
                tbl->rows.clear();
                // We rebuild (clear) indexes
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

                if (txn_active_for_locks) {
                    lock_manager.acquire_exclusive(mg.target_table, lock_txn_id);
                    lock_manager.acquire_shared(mg.source_table, lock_txn_id);
                    statement_shared_locks.insert(mg.source_table);
                }

                // We resolve SET column indices on the target table
                std::vector<std::pair<int, ast::ExprPtr>> resolved_set;
                for (auto& [col, expr] : mg.update_assignments) {
                    int idx = target->column_index(col);
                    if (idx < 0) throw std::runtime_error("Column not found in target: " + col);
                    resolved_set.emplace_back(idx, expr);
                }

                // We build a combined virtual table for ON condition evaluation
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
                        // We build combined row: [target values..., source values...]
                        storage::Row combo;
                        combo.insert(combo.end(), tgt_row.begin(), tgt_row.end());
                        combo.insert(combo.end(), src_row.begin(), src_row.end());

                        if (mg.on_condition && dml_eval_bool(mg.on_condition, &combined, combo)) {
                            // WHEN MATCHED -> UPDATE the target row
                            storage::Row before_row = tgt_row;
                            txn_manager.log_update(mg.target_table, ti, before_row);
                            for (auto& [idx, expr] : resolved_set) {
                                tgt_row[idx] = dml_eval(expr, source, src_row);
                            }
                            if (txn_manager.in_transaction()) {
                                wal_manager.log_update(txn_manager.current_txn_id(), mg.target_table, ti, before_row, tgt_row);
                            }
                            matched = true;
                            updated++;
                            break;
                        }
                    }

                    if (!matched) {
                        // WHEN NOT MATCHED -> INSERT into target
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
                        size_t inserted_row_index = target->rows.size() - 1;
                        catalog.update_indexes_on_insert(mg.target_table, inserted_row_index);
                        txn_manager.log_insert(mg.target_table, inserted_row_index);
                        if (txn_manager.in_transaction()) {
                            wal_manager.log_insert(txn_manager.current_txn_id(), mg.target_table, inserted_row_index, target->rows[inserted_row_index]);
                        }
                        inserted++;
                    }
                }

                // Rebuilding indexes on target if rows were updated
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
                td->action_sqls = tg.action_sqls;
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

        if (!txn_manager.in_transaction() && statement_mutates_catalog(stmt->type)) {
            checkpoint_after_statement = true;
        }
    } catch (const std::exception& e) {
        release_statement_read_locks();
        std::cout << "Error: " << e.what() << "\n";
        return false;
    }

    if (checkpoint_after_statement) {
        wal_manager.checkpoint(catalog);
    }

    release_statement_read_locks();
    return true;
}

static bool run_script_file(const std::string& path,
                            storage::Catalog& catalog,
                            storage::TransactionManager& txn_manager,
                                     storage::LockManager& lock_manager,
                                     storage::WalManager& wal_manager);

static bool handle_dot_command(const std::string& line,
                               storage::Catalog& catalog,
                               storage::TransactionManager& txn_manager,
                               storage::LockManager& lock_manager,
                               storage::WalManager& wal_manager,
                               const std::string& current_user = "local_admin",
                               bool enforce_authorization = false,
                               bool is_remote = false) {
    auto can_read_table = [&](const std::string& table_name) {
        if (!enforce_authorization) return true;
        return catalog.has_privilege(current_user, "TABLE", table_name, storage::Catalog::Privilege::PRIV_SELECT);
    };

    auto can_read_view = [&](const std::string& view_name) {
        if (!enforce_authorization) return true;
        return catalog.has_privilege(current_user, "VIEW", view_name, storage::Catalog::Privilege::PRIV_SELECT);
    };

    auto can_list_function = [&](const std::string& function_name) {
        if (!enforce_authorization) return true;
        return catalog.has_privilege(current_user, "FUNCTION", function_name, storage::Catalog::Privilege::PRIV_EXECUTE);
    };

    auto can_admin_table = [&](const std::string& table_name) {
        if (!enforce_authorization) return true;
        return catalog.has_privilege(current_user, "TABLE", table_name, storage::Catalog::Privilege::PRIV_ALTER);
    };

    if (line == ".quit" || line == ".exit") return false;
    if (line == ".help") {
        print_help();
        return true;
    }
    if (starts_with(line, ".functions")) {
        if (line.size() > 10 && line[10] != ' ') {
            std::cout << "Unknown command: " << line << "\n";
            return true;
        }

        std::string arg = trim_copy(line.substr(10));
        if (arg.empty()) {
            if (!enforce_authorization) {
                print_functions(catalog, FunctionListMode::ALL);
            } else {
                std::cout << "Built-in SQL functions:\n";
                auto builtins = executor::list_builtin_scalar_function_names();
                for (const auto& name : builtins) std::cout << "  " << name << "\n";

                std::vector<const storage::Catalog::FunctionDef*> user_functions;
                for (const auto& [_, def] : catalog.functions) {
                    if (def && can_list_function(def->name)) user_functions.push_back(def.get());
                }
                std::sort(user_functions.begin(), user_functions.end(),
                          [](const storage::Catalog::FunctionDef* a, const storage::Catalog::FunctionDef* b) {
                              return storage::normalize_identifier_key(a->name) < storage::normalize_identifier_key(b->name);
                          });
                std::cout << "User-defined SQL functions (authorized) (" << user_functions.size() << "):\n";
                for (const auto* fn : user_functions) {
                    std::cout << "  " << fn->name << "(";
                    for (size_t i = 0; i < fn->params.size(); i++) {
                        if (i) std::cout << ", ";
                        std::cout << fn->params[i].name << " " << data_type_to_string(fn->params[i].type);
                    }
                    std::cout << ") RETURNS " << data_type_to_string(fn->return_type) << "\n";
                }
            }
        } else if (arg == "builtins") {
            print_functions(catalog, FunctionListMode::BUILTINS_ONLY);
        } else if (arg == "udf") {
            if (!enforce_authorization) {
                print_functions(catalog, FunctionListMode::UDF_ONLY);
            } else {
                std::vector<const storage::Catalog::FunctionDef*> user_functions;
                for (const auto& [_, def] : catalog.functions) {
                    if (def && can_list_function(def->name)) user_functions.push_back(def.get());
                }
                std::sort(user_functions.begin(), user_functions.end(),
                          [](const storage::Catalog::FunctionDef* a, const storage::Catalog::FunctionDef* b) {
                              return storage::normalize_identifier_key(a->name) < storage::normalize_identifier_key(b->name);
                          });
                std::cout << "User-defined SQL functions (authorized) (" << user_functions.size() << "):\n";
                for (const auto* fn : user_functions) {
                    std::cout << "  " << fn->name << "(";
                    for (size_t i = 0; i < fn->params.size(); i++) {
                        if (i) std::cout << ", ";
                        std::cout << fn->params[i].name << " " << data_type_to_string(fn->params[i].type);
                    }
                    std::cout << ") RETURNS " << data_type_to_string(fn->return_type) << "\n";
                }
            }
        } else {
            std::cout << "Usage: .functions [builtins|udf]\n";
        }
        return true;
    }
    if (line == ".tables") {
        size_t visible = 0;
        for (auto& [name, tbl] : catalog.tables) {
            if (can_read_table(name)) {
                std::cout << "  " << name << " (" << tbl->rows.size() << " rows)\n";
                visible++;
            }
        }
        for (auto& [name, view] : catalog.views) {
            if (!view->materialized && can_read_view(name)) {
                std::cout << "  " << name << " (view)\n";
                visible++;
            }
        }
        if (enforce_authorization && visible == 0) {
            std::cout << "No visible tables/views for current principal.\n";
        }
        return true;
    }
    if (starts_with(line, ".schema")) {
        std::string tname = trim_copy(line.substr(7));
        if (tname.empty()) {
            std::cout << "Usage: .schema <table_or_view>\n";
            return true;
        }
        auto* view = catalog.get_view(tname);
        if (view) {
            if (!can_read_view(tname)) {
                std::cout << "Permission denied: missing SELECT on VIEW " << tname << "\n";
                return true;
            }
            std::cout << "View: " << tname << " (" << (view->materialized ? "materialized" : "logical") << ")\n";
            return true;
        }

        auto* tbl = catalog.get_table(tname);
        if (tbl) {
            if (!can_read_table(tname)) {
                std::cout << "Permission denied: missing SELECT on TABLE " << tname << "\n";
                return true;
            }
            std::cout << "Table: " << tbl->name << "\n";
            for (auto& col : tbl->schema) {
                std::string type_str = col.type == storage::DataType::INT ? "INT" :
                                      col.type == storage::DataType::FLOAT ? "FLOAT" : "VARCHAR";
                std::cout << "  " << col.name << " " << type_str << "\n";
            }
        } else {
            std::cout << "Object not found: " << tname << "\n";
        }
        return true;
    }
    if (starts_with(line, ".generate")) {
        if (is_remote) {
            std::cout << "Command not supported over server protocol: .generate\n";
            return true;
        }
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
        if (is_remote) {
            std::cout << "Command not supported over server protocol: .save\n";
            return true;
        }
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
            std::cout << "\n-- Last Optimized Plan --\n";
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
                if (!can_admin_table(t->table_name)) continue;
                std::string combined;
                for (size_t si = 0; si < t->action_sqls.size(); si++) {
                    if (si > 0) combined += "; ";
                    combined += t->action_sqls[si];
                }
                printf("%-15s | %-15s | %-7s | %-7s | %s\n",
                    t->name.c_str(), t->table_name.c_str(),
                    timings[t->when], events[t->event], combined.c_str());
            }
        }
        return true;
    }
    if (starts_with(line, ".source")) {
        if (is_remote) {
            std::cout << "Command not supported over server protocol: .source\n";
            return true;
        }
        std::string path = trim_copy(line.substr(7));
        if (path.empty()) {
            std::cout << "Usage: .source <file.sql>\n";
            return true;
        }
        return run_script_file(path, catalog, txn_manager, lock_manager, wal_manager);
    }
    if (line == ".benchmark") {
        if (is_remote) {
            std::cout << "Command not supported over server protocol: .benchmark\n";
            return true;
        }
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
                               storage::TransactionManager& txn_manager,
                               storage::LockManager& lock_manager,
                               storage::WalManager& wal_manager,
                               std::string& buffer,
                               bool is_interactive) {
    std::string line = trim_copy(raw_line);
    if (line.empty()) {
        if (is_interactive) std::cout << "sqp> ";
        return true;
    }

    if (line[0] == '.') {
        bool keep_running = handle_dot_command(line, catalog, txn_manager, lock_manager, wal_manager,
                                               "local_admin", false, false);
        if (keep_running && is_interactive) std::cout << "sqp> ";
        return keep_running;
    }

    if (!buffer.empty()) buffer += " ";
    buffer += line;
    if (buffer.back() != ';') {
        if (is_interactive) std::cout << "  -> ";
        return true;
    }

    bool success = execute_sql(buffer, catalog, txn_manager, lock_manager, wal_manager);
    buffer.clear();

    // In script mode, stop on error; in interactive mode, continue
    if (!success && !is_interactive) return false;

    if (is_interactive) std::cout << "sqp> ";
    return true;
}

static bool run_script_file(const std::string& path,
                            storage::Catalog& catalog,
                            storage::TransactionManager& txn_manager,
                            storage::LockManager& lock_manager,
                            storage::WalManager& wal_manager) {
    std::ifstream script(path);
    if (!script.is_open()) {
        std::cout << "Error: could not open script file '" << path << "'\n";
        return true;
    }

    std::string line;
    std::string buffer;
    while (std::getline(script, line)) {
        if (!process_input_line(line, catalog, txn_manager, lock_manager, wal_manager, buffer, false)) return false;
    }

    if (!trim_copy(buffer).empty()) {
        std::cout << "Warning: unterminated SQL statement at end of file '" << path << "'\n";
    }

    return true;
}

#ifdef _WIN32
using SocketHandle = SOCKET;
static constexpr SocketHandle INVALID_SOCKET_HANDLE = INVALID_SOCKET;
#else
using SocketHandle = int;
static constexpr SocketHandle INVALID_SOCKET_HANDLE = -1;
#endif

static void close_socket_handle(SocketHandle sock) {
#ifdef _WIN32
    closesocket(sock);
#else
    close(sock);
#endif
}

static bool init_socket_layer() {
#ifdef _WIN32
    WSADATA wsa_data;
    return WSAStartup(MAKEWORD(2, 2), &wsa_data) == 0;
#else
    return true;
#endif
}

static void shutdown_socket_layer() {
#ifdef _WIN32
    WSACleanup();
#endif
}

static bool send_all(SocketHandle sock, const std::string& payload) {
    size_t sent = 0;
    while (sent < payload.size()) {
        int n = send(sock, payload.data() + sent, static_cast<int>(payload.size() - sent), 0);
        if (n <= 0) return false;
        sent += static_cast<size_t>(n);
    }
    return true;
}

static bool execute_sql_with_capture(const std::string& sql,
                                     storage::Catalog& catalog,
                                     storage::TransactionManager& txn_manager,
                                     storage::LockManager& lock_manager,
                                     storage::WalManager& wal_manager,
                                     std::mutex& engine_mutex,
                                     const std::string& current_user,
                                     bool enforce_authorization,
                                     std::string& captured) {
    std::lock_guard<std::mutex> guard(engine_mutex);
    std::ostringstream oss;
    auto* old_buf = std::cout.rdbuf(oss.rdbuf());
    bool ok = execute_sql(sql, catalog, txn_manager, lock_manager, wal_manager,
                          current_user, enforce_authorization);
    std::cout.rdbuf(old_buf);
    captured = oss.str();
    return ok;
}

static bool execute_dot_command_with_capture(const std::string& line,
                                             storage::Catalog& catalog,
                                             storage::TransactionManager& txn_manager,
                                             storage::LockManager& lock_manager,
                                             storage::WalManager& wal_manager,
                                             std::mutex& engine_mutex,
                                             const std::string& current_user,
                                             bool enforce_authorization,
                                             std::string& captured) {
    std::lock_guard<std::mutex> guard(engine_mutex);
    std::ostringstream oss;
    auto* old_buf = std::cout.rdbuf(oss.rdbuf());
    bool ok = handle_dot_command(line, catalog, txn_manager, lock_manager, wal_manager,
                                 current_user, enforce_authorization, true);
    std::cout.rdbuf(old_buf);
    captured = oss.str();
    return ok;
}

static void handle_server_client(SocketHandle client_sock,
                                 std::string session_id,
                                 storage::Catalog& catalog,
                                 storage::TransactionManager& txn_manager,
                                 storage::LockManager& lock_manager,
                                 storage::WalManager& wal_manager,
                                 std::mutex& engine_mutex,
                                 ServerAuthMode auth_mode) {
    std::string hello = "HELLO VDB\nSESSION " + session_id + "\n";
    if (!send_all(client_sock, hello)) {
        close_socket_handle(client_sock);
        return;
    }

    std::string recv_buffer;
    std::string sql_buffer;
    std::string auth_user = "anonymous";
    std::string auth_nonce;
    bool authenticated = (auth_mode == ServerAuthMode::NONE);
    char tmp[4096];

    while (true) {
        int n = recv(client_sock, tmp, sizeof(tmp), 0);
        if (n <= 0) break;
        recv_buffer.append(tmp, static_cast<size_t>(n));

        size_t nl_pos;
        while ((nl_pos = recv_buffer.find('\n')) != std::string::npos) {
            std::string line = recv_buffer.substr(0, nl_pos);
            recv_buffer.erase(0, nl_pos + 1);
            if (!line.empty() && line.back() == '\r') line.pop_back();

            std::string cmd = trim_copy(line);
            if (cmd.empty()) continue;

            if (cmd == "PING") {
                if (!send_all(client_sock, "PONG\n")) goto client_done;
                continue;
            }

            if (starts_with(cmd, "AUTH_START ")) {
                if (auth_mode != ServerAuthMode::PASSWORD) {
                    if (!send_all(client_sock, "AUTH_ERROR auth_not_enabled\n")) goto client_done;
                    continue;
                }
                auth_user = trim_copy(cmd.substr(std::string("AUTH_START ").size()));
                auto* user = catalog.get_user(auth_user);
                if (!user || user->is_locked) {
                    auth_nonce = to_hex(random_bytes(16));
                    if (!send_all(client_sock, "AUTH_ERROR auth_failed\n")) goto client_done;
                    continue;
                }
                auth_nonce = to_hex(random_bytes(16));
                std::string challenge = "AUTH_CHALLENGE " + user->salt_hex + " " + auth_nonce + " sha256\n";
                if (!send_all(client_sock, challenge)) goto client_done;
                continue;
            }

            if (starts_with(cmd, "AUTH_PROOF ")) {
                if (auth_mode != ServerAuthMode::PASSWORD) {
                    if (!send_all(client_sock, "AUTH_ERROR auth_not_enabled\n")) goto client_done;
                    continue;
                }
                if (auth_nonce.empty() || auth_user.empty()) {
                    if (!send_all(client_sock, "AUTH_ERROR auth_nonce_expired\n")) goto client_done;
                    continue;
                }
                const std::string proof = trim_copy(cmd.substr(std::string("AUTH_PROOF ").size()));
                auto* user = catalog.get_user(auth_user);
                if (!user || user->is_locked) {
                    auth_nonce.clear();
                    if (!send_all(client_sock, "AUTH_ERROR auth_failed\n")) goto client_done;
                    continue;
                }
                const std::string expected = sha256_hex(user->password_verifier_hex + auth_nonce);
                auth_nonce.clear();
                if (proof != expected) {
                    if (!send_all(client_sock, "AUTH_ERROR auth_failed\n")) goto client_done;
                    continue;
                }
                authenticated = true;
                if (!send_all(client_sock, "AUTH_OK " + auth_user + "\n")) goto client_done;
                continue;
            }

            if (cmd == "QUIT" || cmd == ".quit" || cmd == ".exit") {
                send_all(client_sock, "BYE\n");
                goto client_done;
            }

            if (!cmd.empty() && cmd.front() == '.') {
                if (auth_mode == ServerAuthMode::PASSWORD && !authenticated) {
                    if (!send_all(client_sock, "ERROR\nauth_required\nEND\n")) goto client_done;
                    continue;
                }
                std::string captured;
                bool ok = execute_dot_command_with_capture(cmd, catalog, txn_manager, lock_manager, wal_manager,
                                                           engine_mutex, auth_user,
                                                           auth_mode == ServerAuthMode::PASSWORD,
                                                           captured);
                if (!ok) {
                    send_all(client_sock, "BYE\n");
                    goto client_done;
                }
                if (!send_all(client_sock, "OK\n")) goto client_done;
                if (!captured.empty() && !send_all(client_sock, captured)) goto client_done;
                if (!send_all(client_sock, "END\n")) goto client_done;
                continue;
            }

            if (auth_mode == ServerAuthMode::PASSWORD && !authenticated) {
                if (!send_all(client_sock, "ERROR\nauth_required\nEND\n")) goto client_done;
                continue;
            }

            if (!sql_buffer.empty()) sql_buffer += " ";
            sql_buffer += cmd;
            if (sql_buffer.back() != ';') {
                if (!send_all(client_sock, "CONTINUE\n")) goto client_done;
                continue;
            }

            std::string captured;
            bool ok = execute_sql_with_capture(sql_buffer, catalog, txn_manager, lock_manager, wal_manager,
                                               engine_mutex, auth_user,
                                               auth_mode == ServerAuthMode::PASSWORD,
                                               captured);
            sql_buffer.clear();

            if (!send_all(client_sock, ok ? "OK\n" : "ERROR\n")) goto client_done;
            if (!captured.empty() && !send_all(client_sock, captured)) goto client_done;
            if (!send_all(client_sock, "END\n")) goto client_done;
        }
    }

client_done:
    close_socket_handle(client_sock);
}

static int run_server(const std::string& host,
                      uint16_t port,
                      storage::Catalog& catalog,
                      storage::TransactionManager& txn_manager,
                      storage::LockManager& lock_manager,
                      storage::WalManager& wal_manager,
                      ServerAuthMode auth_mode) {
    if (!init_socket_layer()) {
        std::cout << "Error: failed to initialize socket layer.\n";
        return 1;
    }

    SocketHandle listen_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_sock == INVALID_SOCKET_HANDLE) {
        std::cout << "Error: failed to create server socket.\n";
        shutdown_socket_layer();
        return 1;
    }

    int reuse = 1;
    setsockopt(listen_sock, SOL_SOCKET, SO_REUSEADDR,
               reinterpret_cast<const char*>(&reuse), sizeof(reuse));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (host == "0.0.0.0" || host == "*") {
        addr.sin_addr.s_addr = htonl(INADDR_ANY);
    } else if (inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) {
        std::cout << "Error: invalid --host value '" << host << "' (IPv4 only in v1).\n";
        close_socket_handle(listen_sock);
        shutdown_socket_layer();
        return 1;
    }

    if (bind(listen_sock, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        std::cout << "Error: failed to bind " << host << ":" << port << "\n";
        close_socket_handle(listen_sock);
        shutdown_socket_layer();
        return 1;
    }
    if (listen(listen_sock, 64) != 0) {
        std::cout << "Error: failed to listen on " << host << ":" << port << "\n";
        close_socket_handle(listen_sock);
        shutdown_socket_layer();
        return 1;
    }

    std::cout << "VDB server listening on " << host << ":" << port << "\n";
    std::cout << "Session model: one session per remote endpoint (IP:port).\n";
    std::cout << "Auth mode: " << (auth_mode == ServerAuthMode::PASSWORD ? "password" : "none") << "\n";

    std::mutex engine_mutex;
    while (true) {
        sockaddr_in peer{};
        socklen_t peer_len = sizeof(peer);
        SocketHandle client_sock = accept(listen_sock, reinterpret_cast<sockaddr*>(&peer), &peer_len);
        if (client_sock == INVALID_SOCKET_HANDLE) {
            continue;
        }

        char ip_buf[INET_ADDRSTRLEN] = {0};
        const char* ntop = inet_ntop(AF_INET, &peer.sin_addr, ip_buf, sizeof(ip_buf));
        std::string ip = ntop ? ntop : std::string("unknown");
        uint16_t peer_port = ntohs(peer.sin_port);
        std::string session_id = ip + ":" + std::to_string(peer_port);

        std::thread(
            handle_server_client,
            client_sock,
            session_id,
            std::ref(catalog),
            std::ref(txn_manager),
            std::ref(lock_manager),
            std::ref(wal_manager),
            std::ref(engine_mutex),
            auth_mode
        ).detach();
    }

    close_socket_handle(listen_sock);
    shutdown_socket_layer();
    return 0;
}

int main(int argc, char* argv[]) {
    storage::Catalog catalog;
    storage::TransactionManager txn_manager;
    storage::LockManager lock_manager;
    storage::WalManager wal_manager;

    auto recovery_stats = wal_manager.recover(catalog);

    std::cout << "|==================================================|\n";
    std::cout << "|     Simple Query Processor & Optimizer (SQP)     |\n";
    std::cout << "|       Type .help for available commands          |\n";
    std::cout << "|==================================================|\n\n";

    if (recovery_stats.checkpoint_loaded || recovery_stats.records_scanned > 0) {
        std::cout << "Recovery complete: "
                  << recovery_stats.transactions_committed << " committed transaction(s), "
                  << recovery_stats.redo_records << " redo record(s) applied";
        if (recovery_stats.checkpoint_loaded) {
            std::cout << " (checkpoint LSN " << recovery_stats.checkpoint_lsn << ")";
        }
        std::cout << ".\n\n";
    }

    // If argument provided, run it as a script
    if (argc > 1) {
        std::string arg = argv[1];
        if (arg == "--server") {
            std::string host = "127.0.0.1";
            uint16_t port = 54330;
            ServerAuthMode auth_mode = ServerAuthMode::NONE;

            for (int i = 2; i < argc; i++) {
                std::string opt = argv[i];
                if (opt == "--host") {
                    if (i + 1 >= argc) {
                        std::cout << "Usage: vdb --server [--host <ipv4>] [--port <port>]\n";
                        return 1;
                    }
                    host = argv[++i];
                    continue;
                }
                if (opt == "--port") {
                    if (i + 1 >= argc) {
                        std::cout << "Usage: vdb --server [--host <ipv4>] [--port <port>]\n";
                        return 1;
                    }
                    int parsed_port = std::stoi(argv[++i]);
                    if (parsed_port <= 0 || parsed_port > 65535) {
                        std::cout << "Error: --port must be in range 1..65535\n";
                        return 1;
                    }
                    port = static_cast<uint16_t>(parsed_port);
                    continue;
                }
                if (opt == "--auth-mode") {
                    if (i + 1 >= argc) {
                        std::cout << "Usage: vdb --server [--host <ipv4>] [--port <port>] [--auth-mode none|password]\n";
                        return 1;
                    }
                    std::string mode = trim_copy(argv[++i]);
                    if (mode == "none") {
                        auth_mode = ServerAuthMode::NONE;
                    } else if (mode == "password") {
                        auth_mode = ServerAuthMode::PASSWORD;
                    } else {
                        std::cout << "Error: --auth-mode must be 'none' or 'password'\n";
                        return 1;
                    }
                    continue;
                }

                std::cout << "Unknown server option: " << opt << "\n";
                std::cout << "Usage: vdb --server [--host <ipv4>] [--port <port>] [--auth-mode none|password]\n";
                return 1;
            }

            if (auth_mode == ServerAuthMode::PASSWORD && !catalog.get_user("admin")) {
                auto [salt_hex, verifier_hex] = make_password_material("admin");
                catalog.add_user("admin", salt_hex, verifier_hex, true);
                std::cout << "Bootstrapped superuser 'admin' with default password 'admin' (change immediately).\n";
            }

            return run_server(host, port, catalog, txn_manager, lock_manager, wal_manager, auth_mode);
        }
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
            run_script_file(argv[2], catalog, txn_manager, lock_manager, wal_manager);
            return 0;
        }

        // Treat a bare argument as a script file path.
        run_script_file(arg, catalog, txn_manager, lock_manager, wal_manager);
        return 0;
    }

    std::string line;
    std::string buffer;
    std::cout << "sqp> ";

    while (std::getline(std::cin, line)) {
        if (!process_input_line(line, catalog, txn_manager, lock_manager, wal_manager, buffer, true)) break;
    }

    std::cout << "\nBye!\n";
    return 0;
}
