#include "executor/view_support.h"

#include <stdexcept>
#include <unordered_set>
#include <unordered_map>

#include "optimizer/optimizer.h"

namespace executor {

using namespace ast;
using namespace storage;

static ExecResult execute_select_core(const SelectStmt& stmt, Catalog& catalog) {
    auto merge_stats = [](ExecStats& dst, const ExecStats& src) {
        dst.rows_scanned += src.rows_scanned;
        dst.rows_filtered += src.rows_filtered;
        dst.join_comparisons += src.join_comparisons;
        dst.subqueries_executed += src.subqueries_executed;
        dst.subqueries_cached += src.subqueries_cached;
        dst.exec_time_ms += src.exec_time_ms;
    };

    auto row_key = [](const Row& row) -> std::string {
        std::string key;
        key.reserve(row.size() * 8);
        for (const auto& v : row) {
            key += value_display(v);
            key.push_back('\x1f');
        }
        return key;
    };

    if (stmt.set_op != SetOpType::SO_NONE && stmt.set_rhs) {
        SelectStmt lhs_stmt = stmt;
        lhs_stmt.set_op = SetOpType::SO_NONE;
        lhs_stmt.set_rhs.reset();

        ExecResult lhs = execute_select_core(lhs_stmt, catalog);
        ExecResult rhs = execute_select_core(*stmt.set_rhs, catalog);

        if (lhs.columns.size() != rhs.columns.size()) {
            throw std::runtime_error("Set operation requires same number of columns on both sides");
        }

        ExecResult out;
        out.columns = lhs.columns;
        merge_stats(out.stats, lhs.stats);
        merge_stats(out.stats, rhs.stats);

        if (stmt.set_op == SetOpType::SO_UNION_ALL) {
            out.rows = lhs.rows;
            out.rows.insert(out.rows.end(), rhs.rows.begin(), rhs.rows.end());
            out.stats.rows_produced = out.rows.size();
            return out;
        }

        std::unordered_set<std::string> lhs_keys;
        lhs_keys.reserve(lhs.rows.size() * 2 + 1);
        for (const auto& r : lhs.rows) lhs_keys.insert(row_key(r));

        std::unordered_set<std::string> rhs_keys;
        rhs_keys.reserve(rhs.rows.size() * 2 + 1);
        for (const auto& r : rhs.rows) rhs_keys.insert(row_key(r));

        std::unordered_set<std::string> seen;
        seen.reserve(lhs.rows.size() + rhs.rows.size() + 1);

        if (stmt.set_op == SetOpType::SO_UNION) {
            for (const auto& r : lhs.rows) {
                std::string k = row_key(r);
                if (seen.insert(k).second) out.rows.push_back(r);
            }
            for (const auto& r : rhs.rows) {
                std::string k = row_key(r);
                if (seen.insert(k).second) out.rows.push_back(r);
            }
        } else if (stmt.set_op == SetOpType::SO_INTERSECT) {
            for (const auto& r : lhs.rows) {
                std::string k = row_key(r);
                if (rhs_keys.count(k) && seen.insert(k).second) out.rows.push_back(r);
            }
        } else if (stmt.set_op == SetOpType::SO_EXCEPT) {
            for (const auto& r : lhs.rows) {
                std::string k = row_key(r);
                if (!rhs_keys.count(k) && seen.insert(k).second) out.rows.push_back(r);
            }
        }

        out.stats.rows_produced = out.rows.size();
        return out;
    }

    auto plan = planner::build_logical_plan(stmt, catalog);
    auto opt = optimizer::optimize(plan, catalog);
    return execute(opt, catalog);
}

static std::string simple_column_name(const std::string& col) {
    auto dot = col.find_last_of('.');
    if (dot == std::string::npos) return col;
    return col.substr(dot + 1);
}

static DataType infer_col_type(const ExecResult& res, size_t col_idx) {
    for (const auto& row : res.rows) {
        if (col_idx >= row.size()) continue;
        const auto& v = row[col_idx];
        if (std::holds_alternative<int64_t>(v)) return DataType::INT;
        if (std::holds_alternative<double>(v)) return DataType::FLOAT;
        if (std::holds_alternative<std::string>(v)) return DataType::VARCHAR;
    }
    return DataType::VARCHAR;
}

std::shared_ptr<Table> materialize_select_to_table(
    const std::string& table_name,
    const SelectStmt& stmt,
    Catalog& catalog) {
    ExecResult res = execute_select_core(stmt, catalog);

    auto tbl = std::make_shared<Table>();
    tbl->name = table_name;
    for (size_t i = 0; i < res.columns.size(); i++) {
        ColumnSchema col;
        col.name = simple_column_name(res.columns[i]);
        col.type = infer_col_type(res, i);
        tbl->schema.push_back(col);
    }
    tbl->rows = std::move(res.rows);
    return tbl;
}

static void materialize_dynamic_views_for_tref(
    const TableRefPtr& tref,
    Catalog& catalog,
    std::vector<std::string>& temp_tables,
    std::unordered_set<std::string>& resolving);

static void materialize_dynamic_views_for_select_impl(
    const SelectStmt& stmt,
    Catalog& catalog,
    std::vector<std::string>& temp_tables,
    std::unordered_set<std::string>& resolving) {
    for (const auto& tref : stmt.from_clause) {
        materialize_dynamic_views_for_tref(tref, catalog, temp_tables, resolving);
    }
    if (stmt.set_rhs) {
        materialize_dynamic_views_for_select_impl(*stmt.set_rhs, catalog, temp_tables, resolving);
    }
}

static void materialize_dynamic_views_for_tref(
    const TableRefPtr& tref,
    Catalog& catalog,
    std::vector<std::string>& temp_tables,
    std::unordered_set<std::string>& resolving) {
    if (!tref) return;

    if (tref->type == TableRefType::BASE_TABLE) {
        if (catalog.get_table(tref->table_name)) return;

        auto* view = catalog.get_view(tref->table_name);
        if (!view) {
            throw std::runtime_error("Table or view not found: " + tref->table_name);
        }

        if (view->materialized) {
            // Materialized view should be persisted as a table; rehydrate on-demand if missing.
            if (!catalog.get_table(tref->table_name)) {
                std::unordered_set<std::string> nested_resolving = resolving;
                materialize_dynamic_views_for_select_impl(*view->query, catalog, temp_tables, nested_resolving);
                catalog.add_table(materialize_select_to_table(tref->table_name, *view->query, catalog));
            }
            return;
        }

        if (resolving.find(tref->table_name) != resolving.end()) {
            throw std::runtime_error("Cyclic view reference detected: " + tref->table_name);
        }

        resolving.insert(tref->table_name);
        materialize_dynamic_views_for_select_impl(*view->query, catalog, temp_tables, resolving);
        catalog.add_table(materialize_select_to_table(tref->table_name, *view->query, catalog));
        temp_tables.push_back(tref->table_name);
        resolving.erase(tref->table_name);
        return;
    }

    if (tref->type == TableRefType::TRT_JOIN) {
        materialize_dynamic_views_for_tref(tref->left, catalog, temp_tables, resolving);
        materialize_dynamic_views_for_tref(tref->right, catalog, temp_tables, resolving);
        return;
    }

    if (tref->type == TableRefType::TRT_SUBQUERY && tref->subquery) {
        materialize_dynamic_views_for_select_impl(*tref->subquery, catalog, temp_tables, resolving);
    }
}

void materialize_dynamic_views_for_select(
    const SelectStmt& stmt,
    Catalog& catalog,
    std::vector<std::string>& temp_tables) {
    std::unordered_set<std::string> resolving;
    materialize_dynamic_views_for_select_impl(stmt, catalog, temp_tables, resolving);
}

void cleanup_temporary_views(Catalog& catalog, const std::vector<std::string>& temp_tables) {
    std::unordered_set<std::string> unique(temp_tables.begin(), temp_tables.end());
    for (const auto& name : unique) {
        catalog.tables.erase(name);
    }
}

ExecResult execute_select_with_views(const SelectStmt& stmt, Catalog& catalog) {
    std::vector<std::string> temp_tables;
    materialize_dynamic_views_for_select(stmt, catalog, temp_tables);
    try {
        ExecResult out = execute_select_core(stmt, catalog);
        cleanup_temporary_views(catalog, temp_tables);
        return out;
    } catch (...) {
        cleanup_temporary_views(catalog, temp_tables);
        throw;
    }
}

} // namespace executor
