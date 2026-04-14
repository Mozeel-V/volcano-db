#include "optimizer/optimizer.h"
#include <algorithm>
#include <iostream>

namespace optimizer {

using namespace planner;
using namespace ast;

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
    plan = push_selection_down(plan);
    return plan;
}

} // namespace optimizer
