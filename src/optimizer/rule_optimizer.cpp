#include "optimizer/optimizer.h"
#include "executor/functions.h"
#include <algorithm>
#include <iostream>

namespace optimizer {

using namespace planner;
using namespace ast;

static bool is_literal_expr(const ExprPtr& expr) {
    return expr && (expr->type == ExprType::LITERAL_INT ||
                    expr->type == ExprType::LITERAL_FLOAT ||
                    expr->type == ExprType::LITERAL_STRING ||
                    expr->type == ExprType::LITERAL_NULL);
}

static storage::Value literal_to_value(const ExprPtr& expr) {
    if (!expr) return std::monostate{};
    switch (expr->type) {
        case ExprType::LITERAL_INT: return expr->int_val;
        case ExprType::LITERAL_FLOAT: return expr->float_val;
        case ExprType::LITERAL_STRING: return expr->str_val;
        case ExprType::LITERAL_NULL: return std::monostate{};
        default: return std::monostate{};
    }
}

static ExprPtr value_to_literal_expr(const storage::Value& v) {
    if (storage::value_is_null(v)) {
        auto e = std::make_shared<Expr>();
        e->type = ExprType::LITERAL_NULL;
        return e;
    }
    if (std::holds_alternative<int64_t>(v)) return Expr::make_int(std::get<int64_t>(v));
    if (std::holds_alternative<double>(v)) return Expr::make_float(std::get<double>(v));
    if (std::holds_alternative<std::string>(v)) return Expr::make_string(std::get<std::string>(v));
    auto e = std::make_shared<Expr>();
    e->type = ExprType::LITERAL_NULL;
    return e;
}

static bool eval_literal_expr(const ExprPtr& expr, storage::Value& out) {
    if (!expr) return false;

    if (is_literal_expr(expr)) {
        out = literal_to_value(expr);
        return true;
    }

    if (expr->type == ExprType::UNARY_OP && expr->operand && is_literal_expr(expr->operand)) {
        storage::Value ov = literal_to_value(expr->operand);
        switch (expr->unary_op) {
            case UnaryOp::OP_NOT: out = (int64_t)(storage::value_to_int(ov) ? 0 : 1); return true;
            case UnaryOp::OP_NEG:
                if (storage::value_is_null(ov)) { out = std::monostate{}; return true; }
                if (std::holds_alternative<int64_t>(ov)) { out = -std::get<int64_t>(ov); return true; }
                out = -storage::value_to_double(ov); return true;
            case UnaryOp::OP_IS_NULL: out = (int64_t)(storage::value_is_null(ov) ? 1 : 0); return true;
            case UnaryOp::OP_IS_NOT_NULL: out = (int64_t)(storage::value_is_null(ov) ? 0 : 1); return true;
        }
    }

    if (expr->type == ExprType::BINARY_OP && expr->left && expr->right &&
        is_literal_expr(expr->left) && is_literal_expr(expr->right)) {
        storage::Value lv = literal_to_value(expr->left);
        storage::Value rv = literal_to_value(expr->right);
        switch (expr->bin_op) {
            case BinOp::OP_ADD: out = storage::value_add(lv, rv); return true;
            case BinOp::OP_SUB: out = storage::value_sub(lv, rv); return true;
            case BinOp::OP_MUL: out = storage::value_mul(lv, rv); return true;
            case BinOp::OP_DIV: out = storage::value_div(lv, rv); return true;
            case BinOp::OP_MOD:
                if (storage::value_is_null(lv) || storage::value_is_null(rv)) out = std::monostate{};
                else out = storage::value_to_int(lv) % storage::value_to_int(rv);
                return true;
            case BinOp::OP_EQ: out = (int64_t)(storage::value_equal(lv, rv) ? 1 : 0); return true;
            case BinOp::OP_NEQ: out = (int64_t)(storage::value_equal(lv, rv) ? 0 : 1); return true;
            case BinOp::OP_LT: out = (int64_t)(storage::value_less(lv, rv) ? 1 : 0); return true;
            case BinOp::OP_GT: out = (int64_t)(storage::value_less(rv, lv) ? 1 : 0); return true;
            case BinOp::OP_LTE: out = (int64_t)((storage::value_less(lv, rv) || storage::value_equal(lv, rv)) ? 1 : 0); return true;
            case BinOp::OP_GTE: out = (int64_t)((storage::value_less(rv, lv) || storage::value_equal(lv, rv)) ? 1 : 0); return true;
            case BinOp::OP_AND: out = (int64_t)((storage::value_to_int(lv) && storage::value_to_int(rv)) ? 1 : 0); return true;
            case BinOp::OP_OR: out = (int64_t)((storage::value_to_int(lv) || storage::value_to_int(rv)) ? 1 : 0); return true;
            case BinOp::OP_LIKE: out = (int64_t)(storage::value_like(lv, rv) ? 1 : 0); return true;
        }
    }

    if (expr->type == ExprType::FUNC_CALL) {
        if (expr->is_window_function || executor::is_aggregate_function_name(expr->func_name)) {
            return false;
        }
        std::vector<storage::Value> args;
        args.reserve(expr->args.size());
        for (const auto& arg : expr->args) {
            if (!is_literal_expr(arg)) return false;
            args.push_back(literal_to_value(arg));
        }
        storage::Value folded;
        if (executor::try_eval_builtin_function(expr->func_name, args, folded)) {
            out = folded;
            return true;
        }
    }

    return false;
}

static ExprPtr fold_constants_in_expr(const ExprPtr& expr) {
    if (!expr) return nullptr;

    if (expr->left) expr->left = fold_constants_in_expr(expr->left);
    if (expr->right) expr->right = fold_constants_in_expr(expr->right);
    if (expr->operand) expr->operand = fold_constants_in_expr(expr->operand);
    for (auto& arg : expr->args) arg = fold_constants_in_expr(arg);
    for (auto& p : expr->window_partition_by) p = fold_constants_in_expr(p);
    for (auto& o : expr->window_order_exprs) o = fold_constants_in_expr(o);
    if (expr->between_low) expr->between_low = fold_constants_in_expr(expr->between_low);
    if (expr->between_high) expr->between_high = fold_constants_in_expr(expr->between_high);
    if (expr->case_base) expr->case_base = fold_constants_in_expr(expr->case_base);
    if (expr->case_else_expr) expr->case_else_expr = fold_constants_in_expr(expr->case_else_expr);
    for (auto& w : expr->case_when_conds) w = fold_constants_in_expr(w);
    for (auto& t : expr->case_then_exprs) t = fold_constants_in_expr(t);

    storage::Value folded;
    if (eval_literal_expr(expr, folded)) {
        return value_to_literal_expr(folded);
    }
    return expr;
}

static void fold_constants_in_plan(const LogicalNodePtr& node) {
    if (!node) return;
    fold_constants_in_plan(node->left);
    fold_constants_in_plan(node->right);

    if (node->predicate) node->predicate = fold_constants_in_expr(node->predicate);
    if (node->join_cond) node->join_cond = fold_constants_in_expr(node->join_cond);
    if (node->index_key) node->index_key = fold_constants_in_expr(node->index_key);
    if (node->index_range_low) node->index_range_low = fold_constants_in_expr(node->index_range_low);
    if (node->index_range_high) node->index_range_high = fold_constants_in_expr(node->index_range_high);

    for (auto& proj : node->projections) proj = fold_constants_in_expr(proj);
    for (auto& grp : node->group_exprs) grp = fold_constants_in_expr(grp);
    for (auto& agg : node->agg_exprs) agg = fold_constants_in_expr(agg);
    for (auto& sort_key : node->sort_keys) sort_key.expr = fold_constants_in_expr(sort_key.expr);
}

static void collect_conjuncts(ExprPtr expr, std::vector<ExprPtr>& out) {
    if (!expr) return;
    if (expr->type == ExprType::BINARY_OP && expr->bin_op == BinOp::OP_AND) {
        collect_conjuncts(expr->left, out);
        collect_conjuncts(expr->right, out);
    } else {
        out.push_back(expr);
    }
}

static ExprPtr conjoin(const std::vector<ExprPtr>& preds) {
    if (preds.empty()) return nullptr;
    ExprPtr result = preds[0];
    for (size_t i = 1; i < preds.size(); i++) {
        result = Expr::make_binary(BinOp::OP_AND, result, preds[i]);
    }
    return result;
}

static bool expr_references_table(const ExprPtr& expr, const std::string& table) {
    if (!expr) return false;
    if (expr->type == ExprType::COLUMN_REF) {
        return expr->table_name == table || expr->table_name.empty();
    }
    bool l = expr->left ? expr_references_table(expr->left, table) : false;
    bool r = expr->right ? expr_references_table(expr->right, table) : false;
    bool o = expr->operand ? expr_references_table(expr->operand, table) : false;
    return l || r || o;
}

static bool expr_only_references(const ExprPtr& expr, const std::string& table) {
    if (!expr) return true;
    if (expr->type == ExprType::COLUMN_REF) {
        return expr->table_name.empty() || expr->table_name == table;
    }
    if (expr->type == ExprType::LITERAL_INT || expr->type == ExprType::LITERAL_FLOAT ||
        expr->type == ExprType::LITERAL_STRING || expr->type == ExprType::LITERAL_NULL ||
        expr->type == ExprType::STAR) return true;
    bool ok = true;
    if (expr->left) ok = ok && expr_only_references(expr->left, table);
    if (expr->right) ok = ok && expr_only_references(expr->right, table);
    if (expr->operand) ok = ok && expr_only_references(expr->operand, table);
    for (auto& a : expr->args) ok = ok && expr_only_references(a, table);
    return ok;
}

static std::string get_scan_table(const LogicalNodePtr& node) {
    if (!node) return "";
    if (node->type == LogicalNodeType::TABLE_SCAN) return node->table_name;
    if (node->left) return get_scan_table(node->left);
    return "";
}

static LogicalNodePtr push_selection_down(LogicalNodePtr node) {
    if (!node) return nullptr;

    // Recurse into children first
    node->left = push_selection_down(node->left);
    node->right = push_selection_down(node->right);

    // If this is a Filter on top of a Join, try to push predicates down
    if (node->type == LogicalNodeType::FILTER && node->left &&
        node->left->type == LogicalNodeType::JOIN) {

        auto join = node->left;
        std::string left_table = get_scan_table(join->left);
        std::string right_table = get_scan_table(join->right);

        std::vector<ExprPtr> conjuncts;
        collect_conjuncts(node->predicate, conjuncts);

        std::vector<ExprPtr> left_preds, right_preds, remaining;
        for (auto& pred : conjuncts) {
            if (!left_table.empty() && expr_only_references(pred, left_table)) {
                left_preds.push_back(pred);
            } else if (!right_table.empty() && expr_only_references(pred, right_table)) {
                right_preds.push_back(pred);
            } else {
                remaining.push_back(pred);
            }
        }

        // Push filters down to children
        if (!left_preds.empty()) {
            auto f = std::make_shared<LogicalNode>();
            f->type = LogicalNodeType::FILTER;
            f->predicate = conjoin(left_preds);
            f->left = join->left;
            join->left = f;
        }
        if (!right_preds.empty()) {
            auto f = std::make_shared<LogicalNode>();
            f->type = LogicalNodeType::FILTER;
            f->predicate = conjoin(right_preds);
            f->left = join->right;
            join->right = f;
        }

        if (remaining.empty()) {
            return join; // no more top-level filter needed
        } else {
            node->predicate = conjoin(remaining);
            return node;
        }
    }

    // Push filter below projection
    if (node->type == LogicalNodeType::FILTER && node->left &&
        node->left->type == LogicalNodeType::PROJECTION) {
        auto proj = node->left;
        // We move filter below projection
        node->left = proj->left;
        proj->left = node;
        // Recurse again
        proj->left = push_selection_down(proj->left);
        return proj;
    }

    return node;
}

// Simplified: we don't remove columns from early scans, but we ensure
// projections don't block filter pushdown (handled above).

// For a chain of cross/inner joins, put smaller tables first
static LogicalNodePtr reorder_joins(LogicalNodePtr node, storage::Catalog& catalog) {
    if (!node) return nullptr;
    node->left = reorder_joins(node->left, catalog);
    node->right = reorder_joins(node->right, catalog);

    if (node->type == LogicalNodeType::JOIN &&
        node->join_type == JoinType::JT_INNER) {
        std::string lt = get_scan_table(node->left);
        std::string rt = get_scan_table(node->right);
        size_t lc = catalog.table_cardinality(lt);
        size_t rc = catalog.table_cardinality(rt);
        // Put smaller table on the left (build side for hash join)
        if (rc < lc) {
            std::swap(node->left, node->right);
        }
    }
    return node;
}

LogicalNodePtr optimize_rules(LogicalNodePtr plan) {
    fold_constants_in_plan(plan);
    plan = push_selection_down(plan);
    return plan;
}

} // namespace optimizer
