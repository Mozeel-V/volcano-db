#include "executor/executor.h"
#include "executor/functions.h"
#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <functional>
#include <cmath>
#include <iostream>
#include <stdexcept>

namespace executor {

using namespace storage;
using namespace planner;
using namespace ast;

// Evaluation context: maps column name -> index in current row
struct EvalCtx {
    storage::Catalog* catalog = nullptr;
    std::vector<std::string> col_names;
    const Row* row = nullptr;
    const EvalCtx* outer = nullptr;
    mutable bool accessed_outer = false;
    ExecStats* stats = nullptr;
    int udf_depth = 0;

    int find_col(const std::string& name) const {
        for (int i = (int)col_names.size() - 1; i >= 0; i--) {
            if (col_names[i] == name) return i;
            // Also match "table.col" style
            auto dot = col_names[i].find('.');
            if (dot != std::string::npos && col_names[i].substr(dot+1) == name) return i;
        }
        return -1;
    }

    int find_col_qualified(const std::string& tbl, const std::string& name, bool strict = false) const {
        std::string full = tbl + "." + name;
        for (int i = (int)col_names.size() - 1; i >= 0; i--) {
            if (col_names[i] == full) return i;
            // Hack for unqualified storage since exec_scan doesn't prefix
            // If the table names happen to match the requested tbl prefix somewhere else we'd need more
            // but for simple subqueries, we'll try a strict match first
        }
        if (strict) return -1;
        // Fallback: search by column name alone
        return find_col(name);
    }
};

static bool compare_with_op(const Value& lv, const Value& rv, BinOp op) {
    switch (op) {
        case BinOp::OP_EQ:  return value_equal(lv, rv);
        case BinOp::OP_NEQ: return !value_equal(lv, rv);
        case BinOp::OP_LT:  return value_less(lv, rv);
        case BinOp::OP_GT:  return value_less(rv, lv);
        case BinOp::OP_LTE: return value_less(lv, rv) || value_equal(lv, rv);
        case BinOp::OP_GTE: return value_less(rv, lv) || value_equal(lv, rv);
        default:            return false;
    }
}

static Value eval_expr(const ExprPtr& expr, const EvalCtx& ctx) {
    if (!expr) return std::monostate{};
    switch (expr->type) {
        case ExprType::LITERAL_INT:    return expr->int_val;
        case ExprType::LITERAL_FLOAT:  return expr->float_val;
        case ExprType::LITERAL_STRING: return expr->str_val;
        case ExprType::LITERAL_NULL:   return std::monostate{};
        case ExprType::STAR:           return std::monostate{};
        case ExprType::COLUMN_REF: {
            int idx = -1;
            // 1. Try checking strictly if table name is specified
            if (!expr->table_name.empty()) {
                idx = ctx.find_col_qualified(expr->table_name, expr->column_name, true);
                if (idx >= 0 && ctx.row && idx < (int)ctx.row->size()) {
                    return (*ctx.row)[idx];
                }
            } else {
                // 1b. If no table name specified, ALWAYS prioritize local unqualified search FIRST
                idx = ctx.find_col(expr->column_name);
                if (idx >= 0 && ctx.row && idx < (int)ctx.row->size()) {
                    return (*ctx.row)[idx];
                }
            }

            // 2. Fallback to outer context
            if (ctx.outer) {
                Value outer_val = eval_expr(expr, *ctx.outer);
                if (!std::holds_alternative<std::monostate>(outer_val)) {
                    ctx.accessed_outer = true;
                    ctx.outer->accessed_outer = true; // Propagate upwards to trigger correlated cache omission
                    return outer_val;
                }
            }

            // 3. Last resort fallback for locally qualified columns that were stripped of prefixes
            if (!expr->table_name.empty()) {
                idx = ctx.find_col(expr->column_name);
                if (idx >= 0 && ctx.row && idx < (int)ctx.row->size()) {
                    return (*ctx.row)[idx];
                }
            }

            return std::monostate{};
        }
        case ExprType::BINARY_OP: {
            Value lv = eval_expr(expr->left, ctx);
            Value rv = eval_expr(expr->right, ctx);
            switch (expr->bin_op) {
                case BinOp::OP_ADD:  return value_add(lv, rv);
                case BinOp::OP_SUB:  return value_sub(lv, rv);
                case BinOp::OP_MUL:  return value_mul(lv, rv);
                case BinOp::OP_DIV:  return value_div(lv, rv);
                case BinOp::OP_MOD: {
                    if (value_is_null(lv) || value_is_null(rv)) return std::monostate{};
                    return value_to_int(lv) % value_to_int(rv);
                }
                case BinOp::OP_EQ:  return (int64_t)(value_equal(lv, rv) ? 1 : 0);
                case BinOp::OP_NEQ: return (int64_t)(value_equal(lv, rv) ? 0 : 1);
                case BinOp::OP_LT:  return (int64_t)(value_less(lv, rv) ? 1 : 0);
                case BinOp::OP_GT:  return (int64_t)(value_less(rv, lv) ? 1 : 0);
                case BinOp::OP_LTE: return (int64_t)((value_less(lv, rv) || value_equal(lv, rv)) ? 1 : 0);
                case BinOp::OP_GTE: return (int64_t)((value_less(rv, lv) || value_equal(lv, rv)) ? 1 : 0);
                case BinOp::OP_AND: return (int64_t)((value_to_int(lv) && value_to_int(rv)) ? 1 : 0);
                case BinOp::OP_OR:  return (int64_t)((value_to_int(lv) || value_to_int(rv)) ? 1 : 0);
                case BinOp::OP_LIKE: return (int64_t)(value_like(lv, rv) ? 1 : 0);
            }
            return std::monostate{};
        }
        case ExprType::UNARY_OP: {
            Value ov = eval_expr(expr->operand, ctx);
            switch (expr->unary_op) {
                case UnaryOp::OP_NOT: return (int64_t)(value_to_int(ov) ? 0 : 1);
                case UnaryOp::OP_NEG: {
                    if (value_is_null(ov)) return std::monostate{};
                    if (std::holds_alternative<int64_t>(ov)) return -std::get<int64_t>(ov);
                    return -value_to_double(ov);
                }
                case UnaryOp::OP_IS_NULL:     return (int64_t)(value_is_null(ov) ? 1 : 0);
                case UnaryOp::OP_IS_NOT_NULL: return (int64_t)(value_is_null(ov) ? 0 : 1);
            }
            return std::monostate{};
        }
        case ExprType::FUNC_CALL: {
            if (expr->is_window_function) {
                throw std::runtime_error("Window functions are only supported in SELECT projection");
            }
            // In HAVING context, resolve aggregate by matching output column name
            std::string fn_str = expr->to_string();
            int idx = ctx.find_col(fn_str);
            if (idx >= 0 && ctx.row && idx < (int)ctx.row->size())
                return (*ctx.row)[idx];

            std::vector<Value> arg_values;
            arg_values.reserve(expr->args.size());
            for (const auto& arg : expr->args) {
                arg_values.push_back(eval_expr(arg, ctx));
            }

            Value builtin_result;
            if (try_eval_builtin_function(expr->func_name, arg_values, builtin_result)) {
                return builtin_result;
            }

            if (ctx.catalog) {
                const auto* udf = ctx.catalog->get_function(expr->func_name);
                if (udf) {
                    if (!udf->body_expr) {
                        throw std::runtime_error("Function has no body: " + expr->func_name);
                    }
                    if (arg_values.size() != udf->params.size()) {
                        throw std::runtime_error("Function " + normalize_function_name(expr->func_name) +
                                                 " expects " + std::to_string(udf->params.size()) +
                                                 " argument(s), got " + std::to_string(arg_values.size()));
                    }
                    if (ctx.udf_depth >= 16) {
                        throw std::runtime_error("UDF recursion depth exceeded for function: " + expr->func_name);
                    }

                    Row udf_row;
                    udf_row.reserve(arg_values.size());
                    EvalCtx udf_ctx;
                    udf_ctx.catalog = ctx.catalog;
                    udf_ctx.stats = ctx.stats;
                    udf_ctx.outer = nullptr;
                    udf_ctx.udf_depth = ctx.udf_depth + 1;

                    for (size_t i = 0; i < udf->params.size(); i++) {
                        udf_ctx.col_names.push_back(udf->params[i].name);
                        udf_row.push_back(arg_values[i]);
                    }
                    udf_ctx.row = &udf_row;
                    return eval_expr(udf->body_expr, udf_ctx);
                }
            }

            if (is_aggregate_function_name(expr->func_name)) {
                return std::monostate{};
            }

            throw std::runtime_error("Unknown function: " + expr->func_name);
        }
        case ExprType::SUBQUERY: {
            if (!ctx.catalog || !expr->subquery) return std::monostate{};
            if (ctx.stats && ctx.stats->subquery_cache.count(expr.get())) {
                ctx.stats->subqueries_cached++;
                return ctx.stats->subquery_cache[expr.get()];
            }
            ctx.accessed_outer = false;
            auto plan = planner::build_logical_plan(*expr->subquery, *ctx.catalog);
            if (ctx.stats) ctx.stats->subqueries_executed++;
            auto res = execute(plan, *ctx.catalog, &ctx);
            if (res.rows.empty() || res.rows[0].empty()) return std::monostate{};
            Value val = res.rows[0][0];
            if (!ctx.accessed_outer && ctx.stats) {
                ctx.stats->subquery_cache[expr.get()] = val;
            }
            return val; // Scalar
        }
        case ExprType::EXISTS_EXPR: {
            if (!ctx.catalog || !expr->subquery) return (int64_t)0;
            if (ctx.stats && ctx.stats->subquery_cache.count(expr.get())) {
                ctx.stats->subqueries_cached++;
                return ctx.stats->subquery_cache[expr.get()];
            }
            ctx.accessed_outer = false;
            auto plan = planner::build_logical_plan(*expr->subquery, *ctx.catalog);
            if (ctx.stats) ctx.stats->subqueries_executed++;
            auto res = execute(plan, *ctx.catalog, &ctx);
            Value val = (int64_t)(!res.rows.empty() ? 1 : 0);
            if (!ctx.accessed_outer && ctx.stats) {
                ctx.stats->subquery_cache[expr.get()] = val;
            }
            return val;
        }
        case ExprType::QUANTIFIED_EXPR: {
            if (!ctx.catalog || !expr->subquery) return (int64_t)0;

            Value lv = eval_expr(expr->left, ctx);
            ctx.accessed_outer = false;

            auto plan = planner::build_logical_plan(*expr->subquery, *ctx.catalog);
            if (ctx.stats) ctx.stats->subqueries_executed++;
            auto res = execute(plan, *ctx.catalog, &ctx);

            if (expr->quantifier == Quantifier::Q_ALL) {
                // SQL: comparison with ALL over empty set is true.
                if (res.rows.empty()) return (int64_t)1;
                for (const auto& row : res.rows) {
                    if (row.empty()) continue;
                    if (!compare_with_op(lv, row[0], expr->quant_cmp_op)) {
                        return (int64_t)0;
                    }
                }
                return (int64_t)1;
            }

            // SOME/ANY: true if any comparison is true, false for empty set.
            for (const auto& row : res.rows) {
                if (row.empty()) continue;
                if (compare_with_op(lv, row[0], expr->quant_cmp_op)) {
                    return (int64_t)1;
                }
            }
            return (int64_t)0;
        }
        case ExprType::IN_EXPR: {
            Value lv = eval_expr(expr->left, ctx);
            if (expr->subquery) {
                if (!ctx.catalog) return (int64_t)0;
                
                // 1. Check cache first!
                if (ctx.stats && ctx.stats->in_subquery_cache.count(expr.get())) {
                    ctx.stats->subqueries_cached++;
                    std::string lkey = value_display(lv);
                    return (int64_t)(ctx.stats->in_subquery_cache[expr.get()].count(lkey) ? 1 : 0);
                }

                ctx.accessed_outer = false;
                auto plan = planner::build_logical_plan(*expr->subquery, *ctx.catalog);
                if (ctx.stats) ctx.stats->subqueries_executed++;
                auto res = execute(plan, *ctx.catalog, &ctx);

                // 2. If uncorrelated: build Hash Set and Cache!
                if (!ctx.accessed_outer && ctx.stats) {
                    std::unordered_set<std::string> val_set;
                    for (const auto& row : res.rows) {
                        if (!row.empty()) {
                            val_set.insert(value_display(row[0]));
                        }
                    }
                    ctx.stats->in_subquery_cache[expr.get()] = std::move(val_set);
                    
                    std::string lkey = value_display(lv);
                    return (int64_t)(ctx.stats->in_subquery_cache[expr.get()].count(lkey) ? 1 : 0);
                }

                // 3. If correlated: just evaluate sequentially
                Value in_result = (int64_t)0;
                for (const auto& row : res.rows) {
                    if (!row.empty() && value_equal(lv, row[0])) {
                        in_result = (int64_t)1;
                        break;
                    }
                }
                return in_result;
            } else {
                for (auto& item : expr->in_list) {
                    Value iv = eval_expr(item, ctx);
                    if (value_equal(lv, iv)) return (int64_t)1;
                }
                return (int64_t)0;
            }
        }
        case ExprType::BETWEEN_EXPR: {
            Value v = eval_expr(expr->operand, ctx);
            Value lo = eval_expr(expr->between_low, ctx);
            Value hi = eval_expr(expr->between_high, ctx);
            bool ge_lo = !value_less(v, lo);
            bool le_hi = !value_less(hi, v);
            return (int64_t)((ge_lo && le_hi) ? 1 : 0);
        }
        case ExprType::CASE_EXPR: {
            size_t n = std::min(expr->case_when_conds.size(), expr->case_then_exprs.size());
            if (expr->case_base) {
                Value base = eval_expr(expr->case_base, ctx);
                for (size_t i = 0; i < n; i++) {
                    Value when_val = eval_expr(expr->case_when_conds[i], ctx);
                    if (value_equal(base, when_val)) {
                        return eval_expr(expr->case_then_exprs[i], ctx);
                    }
                }
            } else {
                for (size_t i = 0; i < n; i++) {
                    Value cond = eval_expr(expr->case_when_conds[i], ctx);
                    if (value_to_int(cond) != 0) {
                        return eval_expr(expr->case_then_exprs[i], ctx);
                    }
                }
            }
            if (expr->case_else_expr) {
                return eval_expr(expr->case_else_expr, ctx);
            }
            return std::monostate{};
        }
        default:
            return std::monostate{};
    }
}

static bool eval_bool(const ExprPtr& expr, const EvalCtx& ctx) {
    Value v = eval_expr(expr, ctx);
    return value_to_int(v) != 0;
}

static bool values_equal_for_window(const Value& a, const Value& b) {
    if (value_is_null(a) && value_is_null(b)) return true;
    if (value_is_null(a) || value_is_null(b)) return false;
    return value_equal(a, b);
}

static bool is_window_function_expr(const ExprPtr& expr) {
    return expr && expr->type == ExprType::FUNC_CALL && expr->is_window_function;
}

static std::vector<Value> compute_window_function_values(const ExprPtr& expr,
                                                         const ExecResult& child,
                                                         const EvalCtx& base_ctx) {
    if (!is_window_function_expr(expr)) {
        return std::vector<Value>(child.rows.size(), std::monostate{});
    }

    std::string fn = normalize_function_name(expr->func_name);
    if (fn != "ROW_NUMBER" && fn != "RANK" && fn != "DENSE_RANK") {
        throw std::runtime_error("Unsupported window function: " + expr->func_name);
    }
    if (expr->distinct_func) {
        throw std::runtime_error("DISTINCT is not supported for window function: " + expr->func_name);
    }

    std::vector<Value> out(child.rows.size(), std::monostate{});
    std::unordered_map<std::string, std::vector<size_t>> partitions;

    for (size_t ri = 0; ri < child.rows.size(); ri++) {
        EvalCtx ctx = base_ctx;
        ctx.row = &child.rows[ri];

        std::string partition_key;
        for (const auto& part_expr : expr->window_partition_by) {
            partition_key += value_display(eval_expr(part_expr, ctx));
            partition_key += "\x1f";
        }
        partitions[partition_key].push_back(ri);
    }

    for (auto& [_, indices] : partitions) {
        if (!expr->window_order_exprs.empty()) {
            std::stable_sort(indices.begin(), indices.end(),
                [&](size_t a_idx, size_t b_idx) {
                    EvalCtx ac = base_ctx;
                    ac.row = &child.rows[a_idx];
                    EvalCtx bc = base_ctx;
                    bc.row = &child.rows[b_idx];

                    for (size_t oi = 0; oi < expr->window_order_exprs.size(); oi++) {
                        Value av = eval_expr(expr->window_order_exprs[oi], ac);
                        Value bv = eval_expr(expr->window_order_exprs[oi], bc);
                        bool asc = (oi < expr->window_order_asc.size()) ? (expr->window_order_asc[oi] != 0) : true;

                        if (values_equal_for_window(av, bv)) continue;

                        if (value_is_null(av) && !value_is_null(bv)) return !asc;
                        if (!value_is_null(av) && value_is_null(bv)) return asc;

                        bool lt = value_less(av, bv);
                        return asc ? lt : !lt;
                    }
                    return a_idx < b_idx;
                }
            );
        }

        if (fn == "ROW_NUMBER") {
            for (size_t i = 0; i < indices.size(); i++) {
                out[indices[i]] = static_cast<int64_t>(i + 1);
            }
            continue;
        }

        if (expr->window_order_exprs.empty()) {
            for (size_t idx : indices) out[idx] = static_cast<int64_t>(1);
            continue;
        }

        int64_t rank = 1;
        int64_t dense_rank = 1;

        std::vector<Value> prev_keys;
        prev_keys.reserve(expr->window_order_exprs.size());

        for (size_t i = 0; i < indices.size(); i++) {
            EvalCtx cur_ctx = base_ctx;
            cur_ctx.row = &child.rows[indices[i]];

            std::vector<Value> cur_keys;
            cur_keys.reserve(expr->window_order_exprs.size());
            for (const auto& order_expr : expr->window_order_exprs) {
                cur_keys.push_back(eval_expr(order_expr, cur_ctx));
            }

            bool peer = (i > 0);
            if (peer) {
                for (size_t k = 0; k < cur_keys.size(); k++) {
                    if (!values_equal_for_window(cur_keys[k], prev_keys[k])) {
                        peer = false;
                        break;
                    }
                }
            }

            if (!peer && i > 0) {
                rank = static_cast<int64_t>(i + 1);
                dense_rank++;
            }

            if (fn == "RANK") {
                out[indices[i]] = static_cast<int64_t>(rank);
            } else {
                out[indices[i]] = static_cast<int64_t>(dense_rank);
            }
            prev_keys = std::move(cur_keys);
        }
    }

    return out;
}

static ExecResult exec_node(const LogicalNodePtr& node, Catalog& catalog, ExecStats& stats);

static ExecResult exec_scan(const LogicalNodePtr& node, Catalog& catalog, ExecStats& stats) {
    Table* tbl = catalog.get_table(node->table_name);
    if (!tbl) {
        std::cerr << "Table not found: " << node->table_name << "\n";
        return {};
    }
    ExecResult res;
    std::string prefix = node->table_alias.empty() ? node->table_name : node->table_alias;
    for (auto& col : tbl->schema) {
        res.columns.push_back(prefix + "." + col.name);
    }
    res.rows = tbl->rows;
    stats.rows_scanned += res.rows.size();
    return res;
}

static Value literal_to_value(const ExprPtr& expr) {
    if (!expr) return std::monostate{};
    switch (expr->type) {
        case ExprType::LITERAL_INT:    return expr->int_val;
        case ExprType::LITERAL_FLOAT:  return expr->float_val;
        case ExprType::LITERAL_STRING: return expr->str_val;
        default:                       return std::monostate{};
    }
}

static ExecResult exec_index_scan(const LogicalNodePtr& node, Catalog& catalog, ExecStats& stats) {
    Table* tbl = catalog.get_table(node->table_name);
    if (!tbl) {
        std::cerr << "Table not found: " << node->table_name << "\n";
        return {};
    }

    ExecResult res;
    std::string prefix = node->table_alias.empty() ? node->table_name : node->table_alias;
    for (auto& col : tbl->schema) {
        res.columns.push_back(prefix + "." + col.name);
    }

    std::vector<size_t> row_indices;

    if (!node->index_range) {
        // Equality lookup
        Value key = literal_to_value(node->index_key);

        // We try hash index first
        HashIndex* hidx = catalog.get_index(node->table_name, node->index_column);
        if (hidx) {
            if (std::holds_alternative<int64_t>(key)) {
                row_indices = hidx->lookup_int(std::get<int64_t>(key));
            } else if (std::holds_alternative<std::string>(key)) {
                row_indices = hidx->lookup_str(std::get<std::string>(key));
            } else if (std::holds_alternative<double>(key)) {
                row_indices = hidx->lookup_int((int64_t)std::get<double>(key));
            }
        } else {
            // We try btree
            BTreeIndex* bidx = catalog.get_btree_index(node->table_name, node->index_column);
            if (bidx) {
                row_indices = bidx->lookup_exact(key);
            }
        }
    } else if (node->index_range_low && node->index_range_high) {
        // BETWEEN range scan
        BTreeIndex* bidx = catalog.get_btree_index(node->table_name, node->index_column);
        if (bidx) {
            Value lo = literal_to_value(node->index_range_low);
            Value hi = literal_to_value(node->index_range_high);
            row_indices = bidx->lookup_range(lo, hi);
        }
    } else if (node->predicate) {
        // Range comparison (<, >, <=, >=)
        BTreeIndex* bidx = catalog.get_btree_index(node->table_name, node->index_column);
        if (bidx && node->predicate->type == ExprType::BINARY_OP) {
            // We determine if column is on left or right
            bool col_on_left = (node->predicate->left &&
                                node->predicate->left->type == ExprType::COLUMN_REF);
            Value lit = col_on_left ? literal_to_value(node->predicate->right)
                                    : literal_to_value(node->predicate->left);
            BinOp op = node->predicate->bin_op;
            // If column is on the right, flip the operator
            if (!col_on_left) {
                if (op == BinOp::OP_LT) op = BinOp::OP_GT;
                else if (op == BinOp::OP_GT) op = BinOp::OP_LT;
                else if (op == BinOp::OP_LTE) op = BinOp::OP_GTE;
                else if (op == BinOp::OP_GTE) op = BinOp::OP_LTE;
            }
            switch (op) {
                case BinOp::OP_LT:  row_indices = bidx->lookup_lt(lit);  break;
                case BinOp::OP_GT:  row_indices = bidx->lookup_gt(lit);  break;
                case BinOp::OP_LTE: row_indices = bidx->lookup_lte(lit); break;
                case BinOp::OP_GTE: row_indices = bidx->lookup_gte(lit); break;
                default: break;
            }
        }
    }

    // We collect matching rows
    for (size_t idx : row_indices) {
        if (idx < tbl->rows.size()) {
            res.rows.push_back(tbl->rows[idx]);
        }
    }
    stats.rows_scanned += row_indices.size();
    return res;
}

static ExecResult exec_filter(const LogicalNodePtr& node, Catalog& catalog, ExecStats& stats) {
    auto child = exec_node(node->left, catalog, stats);
    EvalCtx ctx;
    ctx.catalog = &catalog;
    ctx.col_names = child.columns;
    ctx.outer = stats.outer_ctx;
    ctx.stats = &stats;

    ExecResult res;
    res.columns = child.columns;
    for (auto& row : child.rows) {
        ctx.row = &row;
        if (eval_bool(node->predicate, ctx)) {
            res.rows.push_back(row);
        } else {
            stats.rows_filtered++;
        }
    }
    return res;
}

static ExecResult exec_projection(const LogicalNodePtr& node, Catalog& catalog, ExecStats& stats) {
    auto child = exec_node(node->left, catalog, stats);
    EvalCtx ctx;
    ctx.catalog = &catalog;
    ctx.col_names = child.columns;
    ctx.outer = stats.outer_ctx;
    ctx.stats = &stats;

    ExecResult res;
    // We build output column names
    for (size_t i = 0; i < node->projections.size(); i++) {
        if (node->projections[i]->type == ExprType::STAR) {
            // We expand *
            for (auto& c : child.columns) res.columns.push_back(c);
        } else {
            res.columns.push_back(node->output_names[i]);
        }
    }

    std::unordered_map<size_t, std::vector<Value>> window_values_by_projection;
    for (size_t pi = 0; pi < node->projections.size(); pi++) {
        if (is_window_function_expr(node->projections[pi])) {
            window_values_by_projection[pi] = compute_window_function_values(node->projections[pi], child, ctx);
        }
    }

    for (size_t ri = 0; ri < child.rows.size(); ri++) {
        auto& row = child.rows[ri];
        ctx.row = &row;
        Row out;
        for (size_t pi = 0; pi < node->projections.size(); pi++) {
            auto& proj = node->projections[pi];
            if (proj->type == ExprType::STAR) {
                for (auto& v : row) out.push_back(v);
            } else if (is_window_function_expr(proj)) {
                out.push_back(window_values_by_projection[pi][ri]);
            } else {
                out.push_back(eval_expr(proj, ctx));
            }
        }
        res.rows.push_back(std::move(out));
    }
    return res;
}

static ExecResult exec_nested_loop_join(const LogicalNodePtr& node, Catalog& catalog, ExecStats& stats) {
    auto left = exec_node(node->left, catalog, stats);
    auto right = exec_node(node->right, catalog, stats);

    ExecResult res;
    res.columns = left.columns;
    res.columns.insert(res.columns.end(), right.columns.begin(), right.columns.end());

    EvalCtx ctx;
    ctx.catalog = &catalog;
    ctx.col_names = res.columns;
    ctx.outer = stats.outer_ctx;
    ctx.stats = &stats;

    for (auto& lr : left.rows) {
        for (auto& rr : right.rows) {
            stats.join_comparisons++;
            Row combined = lr;
            combined.insert(combined.end(), rr.begin(), rr.end());
            if (node->join_cond) {
                ctx.row = &combined;
                if (!eval_bool(node->join_cond, ctx)) continue;
            }
            res.rows.push_back(std::move(combined));
        }
    }
    return res;
}

static ExecResult exec_hash_join(const LogicalNodePtr& node, Catalog& catalog, ExecStats& stats) {
    auto left = exec_node(node->left, catalog, stats);
    auto right = exec_node(node->right, catalog, stats);

    ExecResult res;
    res.columns = left.columns;
    res.columns.insert(res.columns.end(), right.columns.begin(), right.columns.end());

    // We extract join key columns from condition (if it's an equality condition)
    // For simplicity, we try to identify col = col patterns
    int left_key = -1, right_key = -1;
    if (node->join_cond && node->join_cond->type == ExprType::BINARY_OP &&
        node->join_cond->bin_op == BinOp::OP_EQ) {
        auto& lhs = node->join_cond->left;
        auto& rhs = node->join_cond->right;
        if (lhs->type == ExprType::COLUMN_REF && rhs->type == ExprType::COLUMN_REF) {
            EvalCtx lctx; lctx.catalog = &catalog; lctx.col_names = left.columns;
            EvalCtx rctx; rctx.catalog = &catalog; rctx.col_names = right.columns;

            left_key = lhs->table_name.empty() ?
                lctx.find_col(lhs->column_name) :
                lctx.find_col_qualified(lhs->table_name, lhs->column_name);
            right_key = rhs->table_name.empty() ?
                rctx.find_col(rhs->column_name) :
                rctx.find_col_qualified(rhs->table_name, rhs->column_name);

            // We try swapping if not found
            if (left_key < 0 || right_key < 0) {
                left_key = rhs->table_name.empty() ?
                    lctx.find_col(rhs->column_name) :
                    lctx.find_col_qualified(rhs->table_name, rhs->column_name);
                right_key = lhs->table_name.empty() ?
                    rctx.find_col(lhs->column_name) :
                    rctx.find_col_qualified(lhs->table_name, lhs->column_name);
            }
        }
    }

    if (left_key >= 0 && right_key >= 0) {
        // We build hash table on left (smaller) side
        std::unordered_map<std::string, std::vector<size_t>> hash_table;
        for (size_t i = 0; i < left.rows.size(); i++) {
            std::string key = value_display(left.rows[i][left_key]);
            hash_table[key].push_back(i);
        }
        // Probe with right side
        for (auto& rr : right.rows) {
            std::string key = value_display(rr[right_key]);
            auto it = hash_table.find(key);
            if (it != hash_table.end()) {
                for (size_t li : it->second) {
                    stats.join_comparisons++;
                    Row combined = left.rows[li];
                    combined.insert(combined.end(), rr.begin(), rr.end());
                    res.rows.push_back(std::move(combined));
                }
            }
        }
    } else {
        // Fallback to nested loop
        return exec_nested_loop_join(node, catalog, stats);
    }
    return res;
}

static ExecResult exec_join(const LogicalNodePtr& node, Catalog& catalog, ExecStats& stats) {
    if (node->join_algo == JoinAlgo::HASH_JOIN) {
        return exec_hash_join(node, catalog, stats);
    }
    return exec_nested_loop_join(node, catalog, stats);
}

static ExecResult exec_aggregation(const LogicalNodePtr& node, Catalog& catalog, ExecStats& stats) {
    auto child = exec_node(node->left, catalog, stats);
    EvalCtx ctx;
    ctx.catalog = &catalog;
    ctx.col_names = child.columns;
    ctx.outer = stats.outer_ctx;
    ctx.stats = &stats;

    ExecResult res;
    res.columns = node->agg_output_names;

    // We group rows
    struct GroupState {
        Row group_key;
        std::vector<Row*> rows;
    };
    std::unordered_map<std::string, GroupState> groups;

    auto compute_group_key = [&](const Row& row) -> std::string {
        ctx.row = &row;
        std::string key;
        for (auto& ge : node->group_exprs) {
            Value v = eval_expr(ge, ctx);
            key += value_display(v) + "|";
        }
        return key;
    };

    if (node->group_exprs.empty()) {
        // Single group even if empty - needed for COUNT(*) on empty tables
        groups[""].rows.reserve(child.rows.size());
        for (auto& row : child.rows) {
            groups[""].rows.push_back(&row);
        }
    } else {
        for (auto& row : child.rows) {
            std::string key = compute_group_key(row);
            groups[key].rows.push_back(&row);
        }
    }

    // For each group, compute aggregates
    for (auto& [key, gs] : groups) {
        Row out_row;
        for (auto& agg_expr : node->agg_exprs) {
            if (agg_expr->type == ExprType::FUNC_CALL) {
                std::string fn = agg_expr->func_name;
                std::transform(fn.begin(), fn.end(), fn.begin(), ::toupper);

                if (fn == "COUNT") {
                    if (!agg_expr->args.empty() && agg_expr->args[0]->type == ExprType::STAR) {
                        out_row.push_back((int64_t)gs.rows.size());
                    } else {
                        int64_t cnt = 0;
                        for (auto* r : gs.rows) {
                            ctx.row = r;
                            Value v = eval_expr(agg_expr->args[0], ctx);
                            if (!value_is_null(v)) cnt++;
                        }
                        out_row.push_back(cnt);
                    }
                } else if (fn == "SUM") {
                    double sum = 0;
                    bool all_int = true;
                    for (auto* r : gs.rows) {
                        ctx.row = r;
                        Value v = eval_expr(agg_expr->args[0], ctx);
                        if (!value_is_null(v)) {
                            sum += value_to_double(v);
                            if (!std::holds_alternative<int64_t>(v)) all_int = false;
                        }
                    }
                    if (all_int) out_row.push_back((int64_t)sum);
                    else out_row.push_back(sum);
                } else if (fn == "AVG") {
                    double sum = 0; int64_t cnt = 0;
                    for (auto* r : gs.rows) {
                        ctx.row = r;
                        Value v = eval_expr(agg_expr->args[0], ctx);
                        if (!value_is_null(v)) { sum += value_to_double(v); cnt++; }
                    }
                    if (cnt > 0) out_row.push_back(sum / cnt);
                    else out_row.push_back(std::monostate{});
                } else if (fn == "MIN") {
                    Value minv = std::monostate{};
                    for (auto* r : gs.rows) {
                        ctx.row = r;
                        Value v = eval_expr(agg_expr->args[0], ctx);
                        if (!value_is_null(v) && (value_is_null(minv) || value_less(v, minv)))
                            minv = v;
                    }
                    out_row.push_back(minv);
                } else if (fn == "MAX") {
                    Value maxv = std::monostate{};
                    for (auto* r : gs.rows) {
                        ctx.row = r;
                        Value v = eval_expr(agg_expr->args[0], ctx);
                        if (!value_is_null(v) && (value_is_null(maxv) || value_less(maxv, v)))
                            maxv = v;
                    }
                    out_row.push_back(maxv);
                }
            } else {
                // Non-aggregate in GROUP BY output -- evaluate on first row of group
                if (!gs.rows.empty()) {
                    ctx.row = gs.rows[0];
                    out_row.push_back(eval_expr(agg_expr, ctx));
                } else {
                    out_row.push_back(std::monostate{});
                }
            }
        }
        res.rows.push_back(std::move(out_row));
    }
    return res;
}

static ExecResult exec_sort(const LogicalNodePtr& node, Catalog& catalog, ExecStats& stats) {
    auto child = exec_node(node->left, catalog, stats);
    EvalCtx ctx;
    ctx.catalog = &catalog;
    ctx.col_names = child.columns;
    ctx.outer = stats.outer_ctx;
    ctx.stats = &stats;

    std::sort(child.rows.begin(), child.rows.end(),
        [&](const Row& a, const Row& b) {
            for (auto& sk : node->sort_keys) {
                EvalCtx ac = ctx; ac.row = &a;
                EvalCtx bc = ctx; bc.row = &b;
                Value av = eval_expr(sk.expr, ac);
                Value bv = eval_expr(sk.expr, bc);
                if (value_equal(av, bv)) continue;
                bool lt = value_less(av, bv);
                return sk.ascending ? lt : !lt;
            }
            return false;
        });

    ExecResult res;
    res.columns = child.columns;
    res.rows = std::move(child.rows);
    return res;
}

static ExecResult exec_limit(const LogicalNodePtr& node, Catalog& catalog, ExecStats& stats) {
    auto child = exec_node(node->left, catalog, stats);
    ExecResult res;
    res.columns = child.columns;

    int64_t start = node->offset_count;
    int64_t count = node->limit_count;
    for (int64_t i = start; i < (int64_t)child.rows.size() && (count < 0 || i < start + count); i++) {
        res.rows.push_back(child.rows[i]);
    }
    return res;
}

static ExecResult exec_distinct(const LogicalNodePtr& node, Catalog& catalog, ExecStats& stats) {
    auto child = exec_node(node->left, catalog, stats);
    ExecResult res;
    res.columns = child.columns;

    std::unordered_set<std::string> seen;
    for (auto& row : child.rows) {
        std::string key;
        for (auto& v : row) key += value_display(v) + "|";
        if (seen.insert(key).second) {
            res.rows.push_back(row);
        }
    }
    return res;
}

static ExecResult exec_node(const LogicalNodePtr& node, Catalog& catalog, ExecStats& stats) {
    if (!node) return {};
    auto t0 = std::chrono::high_resolution_clock::now();
    ExecResult res;
    switch (node->type) {
        case LogicalNodeType::TABLE_SCAN:   res = exec_scan(node, catalog, stats); break;
        case LogicalNodeType::INDEX_SCAN:    res = exec_index_scan(node, catalog, stats); break;
        case LogicalNodeType::FILTER:       res = exec_filter(node, catalog, stats); break;
        case LogicalNodeType::PROJECTION:   res = exec_projection(node, catalog, stats); break;
        case LogicalNodeType::JOIN:         res = exec_join(node, catalog, stats); break;
        case LogicalNodeType::AGGREGATION:  res = exec_aggregation(node, catalog, stats); break;
        case LogicalNodeType::SORT:         res = exec_sort(node, catalog, stats); break;
        case LogicalNodeType::LIMIT:        res = exec_limit(node, catalog, stats); break;
        case LogicalNodeType::DISTINCT:     res = exec_distinct(node, catalog, stats); break;
    }
    auto t1 = std::chrono::high_resolution_clock::now();
    node->actual_rows = res.rows.size();
    node->actual_time_ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
    node->has_actual_stats = true;
    return res;
}

ExecResult execute(PhysicalNodePtr plan, Catalog& catalog, const EvalCtx* outer_ctx) {
    ExecStats stats;
    stats.outer_ctx = outer_ctx;
    auto start = std::chrono::high_resolution_clock::now();
    ExecResult res = exec_node(plan, catalog, stats);
    auto end = std::chrono::high_resolution_clock::now();
    stats.exec_time_ms = std::chrono::duration<double, std::milli>(end - start).count();
    stats.rows_produced = res.rows.size();
    res.stats = stats;
    return res;
}

} // namespace executor
