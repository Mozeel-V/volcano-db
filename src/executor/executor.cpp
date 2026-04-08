#include "executor/executor.h"
#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <functional>
#include <cmath>
#include <iostream>

namespace executor {

using namespace storage;
using namespace planner;
using namespace ast;

// ───── Expression evaluator ─────

// Evaluation context: maps column name → index in current row
struct EvalCtx {
    std::vector<std::string> col_names;
    const Row* row = nullptr;

    int find_col(const std::string& name) const {
        for (int i = (int)col_names.size() - 1; i >= 0; i--) {
            if (col_names[i] == name) return i;
            // Also match "table.col" style
            auto dot = col_names[i].find('.');
            if (dot != std::string::npos && col_names[i].substr(dot+1) == name) return i;
        }
        return -1;
    }

    int find_col_qualified(const std::string& tbl, const std::string& name) const {
        std::string full = tbl + "." + name;
        for (int i = (int)col_names.size() - 1; i >= 0; i--) {
            if (col_names[i] == full) return i;
        }
        // Fallback: search by column name alone
        return find_col(name);
    }
};

static Value eval_expr(const ExprPtr& expr, const EvalCtx& ctx) {
    if (!expr) return std::monostate{};
    switch (expr->type) {
        case ExprType::LITERAL_INT:    return expr->int_val;
        case ExprType::LITERAL_FLOAT:  return expr->float_val;
        case ExprType::LITERAL_STRING: return expr->str_val;
        case ExprType::LITERAL_NULL:   return std::monostate{};
        case ExprType::STAR:           return std::monostate{};
        case ExprType::COLUMN_REF: {
            int idx;
            if (!expr->table_name.empty()) {
                idx = ctx.find_col_qualified(expr->table_name, expr->column_name);
            } else {
                idx = ctx.find_col(expr->column_name);
            }
            if (idx < 0 || !ctx.row || idx >= (int)ctx.row->size()) return std::monostate{};
            return (*ctx.row)[idx];
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
                case BinOp::OP_LIKE: {
                    // Simple LIKE: only handle % at start/end
                    std::string sv = value_to_string(lv);
                    std::string pat = value_to_string(rv);
                    if (pat.empty()) return (int64_t)(sv.empty() ? 1 : 0);
                    if (pat.front() == '%' && pat.back() == '%' && pat.size() > 2) {
                        std::string mid = pat.substr(1, pat.size()-2);
                        return (int64_t)(sv.find(mid) != std::string::npos ? 1 : 0);
                    }
                    if (pat.front() == '%') {
                        std::string suffix = pat.substr(1);
                        return (int64_t)(sv.size() >= suffix.size() &&
                                sv.compare(sv.size()-suffix.size(), suffix.size(), suffix) == 0 ? 1 : 0);
                    }
                    if (pat.back() == '%') {
                        std::string prefix = pat.substr(0, pat.size()-1);
                        return (int64_t)(sv.compare(0, prefix.size(), prefix) == 0 ? 1 : 0);
                    }
                    return (int64_t)(sv == pat ? 1 : 0);
                }
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
            // In HAVING context, resolve aggregate by matching output column name
            std::string fn_str = expr->to_string();
            int idx = ctx.find_col(fn_str);
            if (idx >= 0 && ctx.row && idx < (int)ctx.row->size())
                return (*ctx.row)[idx];
            return std::monostate{};
        }
        case ExprType::IN_EXPR: {
            Value lv = eval_expr(expr->left, ctx);
            for (auto& item : expr->in_list) {
                Value iv = eval_expr(item, ctx);
                if (value_equal(lv, iv)) return (int64_t)1;
            }
            return (int64_t)0;
        }
        case ExprType::BETWEEN_EXPR: {
            Value v = eval_expr(expr->operand, ctx);
            Value lo = eval_expr(expr->between_low, ctx);
            Value hi = eval_expr(expr->between_high, ctx);
            bool ge_lo = !value_less(v, lo);
            bool le_hi = !value_less(hi, v);
            return (int64_t)((ge_lo && le_hi) ? 1 : 0);
        }
        default:
            return std::monostate{};
    }
}

static bool eval_bool(const ExprPtr& expr, const EvalCtx& ctx) {
    Value v = eval_expr(expr, ctx);
    return value_to_int(v) != 0;
}

// ───── Forward declaration ─────
static ExecResult exec_node(const LogicalNodePtr& node, Catalog& catalog, ExecStats& stats);

// ───── Operator implementations ─────

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

static ExecResult exec_filter(const LogicalNodePtr& node, Catalog& catalog, ExecStats& stats) {
    auto child = exec_node(node->left, catalog, stats);
    EvalCtx ctx;
    ctx.col_names = child.columns;

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
    ctx.col_names = child.columns;

    ExecResult res;
    // Build output column names
    for (size_t i = 0; i < node->projections.size(); i++) {
        if (node->projections[i]->type == ExprType::STAR) {
            // Expand *
            for (auto& c : child.columns) res.columns.push_back(c);
        } else {
            res.columns.push_back(node->output_names[i]);
        }
    }

    for (auto& row : child.rows) {
        ctx.row = &row;
        Row out;
        for (auto& proj : node->projections) {
            if (proj->type == ExprType::STAR) {
                for (auto& v : row) out.push_back(v);
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
    ctx.col_names = res.columns;

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

    // Extract join key columns from condition (if it's an equality condition)
    // For simplicity, we try to identify col = col patterns
    int left_key = -1, right_key = -1;
    if (node->join_cond && node->join_cond->type == ExprType::BINARY_OP &&
        node->join_cond->bin_op == BinOp::OP_EQ) {
        auto& lhs = node->join_cond->left;
        auto& rhs = node->join_cond->right;
        if (lhs->type == ExprType::COLUMN_REF && rhs->type == ExprType::COLUMN_REF) {
            EvalCtx lctx; lctx.col_names = left.columns;
            EvalCtx rctx; rctx.col_names = right.columns;

            left_key = lhs->table_name.empty() ?
                lctx.find_col(lhs->column_name) :
                lctx.find_col_qualified(lhs->table_name, lhs->column_name);
            right_key = rhs->table_name.empty() ?
                rctx.find_col(rhs->column_name) :
                rctx.find_col_qualified(rhs->table_name, rhs->column_name);

            // Try swapping if not found
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
        // Build hash table on left (smaller) side
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
    ctx.col_names = child.columns;

    ExecResult res;
    res.columns = node->agg_output_names;

    // Group rows
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
        // Single group even if empty – needed for COUNT(*) on empty tables
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
                // Non-aggregate in GROUP BY output — evaluate on first row of group
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
    ctx.col_names = child.columns;

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

// ───── Dispatch ─────
static ExecResult exec_node(const LogicalNodePtr& node, Catalog& catalog, ExecStats& stats) {
    if (!node) return {};
    switch (node->type) {
        case LogicalNodeType::TABLE_SCAN:   return exec_scan(node, catalog, stats);
        case LogicalNodeType::FILTER:       return exec_filter(node, catalog, stats);
        case LogicalNodeType::PROJECTION:   return exec_projection(node, catalog, stats);
        case LogicalNodeType::JOIN:         return exec_join(node, catalog, stats);
        case LogicalNodeType::AGGREGATION:  return exec_aggregation(node, catalog, stats);
        case LogicalNodeType::SORT:         return exec_sort(node, catalog, stats);
        case LogicalNodeType::LIMIT:        return exec_limit(node, catalog, stats);
        case LogicalNodeType::DISTINCT:     return exec_distinct(node, catalog, stats);
    }
    return {};
}

// ───── Public entry ─────
ExecResult execute(PhysicalNodePtr plan, Catalog& catalog) {
    ExecStats stats;
    auto start = std::chrono::high_resolution_clock::now();
    ExecResult res = exec_node(plan, catalog, stats);
    auto end = std::chrono::high_resolution_clock::now();
    stats.exec_time_ms = std::chrono::duration<double, std::milli>(end - start).count();
    stats.rows_produced = res.rows.size();
    res.stats = stats;
    return res;
}

} // namespace executor
