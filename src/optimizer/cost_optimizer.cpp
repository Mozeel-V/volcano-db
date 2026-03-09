#include "optimizer/optimizer.h"
#include <cmath>
#include <algorithm>
#include <iostream>

namespace optimizer {

using namespace planner;
using namespace ast;

// ───── Estimate selectivity of a predicate ─────
static double estimate_selectivity(const ExprPtr& pred, storage::Catalog& catalog,
                                   const std::string& table) {
    if (!pred) return 1.0;
    if (pred->type == ExprType::BINARY_OP) {
        if (pred->bin_op == BinOp::OP_AND) {
            return estimate_selectivity(pred->left, catalog, table) *
                   estimate_selectivity(pred->right, catalog, table);
        }
        if (pred->bin_op == BinOp::OP_OR) {
            double l = estimate_selectivity(pred->left, catalog, table);
            double r = estimate_selectivity(pred->right, catalog, table);
            return l + r - l * r;
        }
        if (pred->bin_op == BinOp::OP_EQ) {
            // If col = literal, selectivity = 1/NDV
            if (pred->left && pred->left->type == ExprType::COLUMN_REF) {
                size_t ndv = catalog.column_distinct(table, pred->left->column_name);
                if (ndv > 0) return 1.0 / ndv;
            }
            return 0.1; // default for equality
        }
        if (pred->bin_op == BinOp::OP_LT || pred->bin_op == BinOp::OP_GT ||
            pred->bin_op == BinOp::OP_LTE || pred->bin_op == BinOp::OP_GTE) {
            return 0.33; // range predicate default
        }
        if (pred->bin_op == BinOp::OP_NEQ) {
            return 0.9;
        }
        if (pred->bin_op == BinOp::OP_LIKE) {
            return 0.25;
        }
    }
    if (pred->type == ExprType::UNARY_OP && pred->unary_op == UnaryOp::OP_NOT) {
        return 1.0 - estimate_selectivity(pred->operand, catalog, table);
    }
    return 0.5; // default
}

// ───── Annotate plan with row estimates ─────
static void estimate_rows(LogicalNodePtr node, storage::Catalog& catalog) {
    if (!node) return;
    estimate_rows(node->left, catalog);
    estimate_rows(node->right, catalog);

    switch (node->type) {
        case LogicalNodeType::TABLE_SCAN: {
            size_t card = catalog.table_cardinality(node->table_name);
            node->estimated_rows = (double)(card > 0 ? card : 1000);
            node->estimated_cost = node->estimated_rows;
            break;
        }
        case LogicalNodeType::FILTER: {
            double input_rows = node->left ? node->left->estimated_rows : 1000;
            std::string tbl = "";
            if (node->left) tbl = node->left->table_name;
            double sel = estimate_selectivity(node->predicate, catalog, tbl);
            node->estimated_rows = input_rows * sel;
            node->estimated_cost = (node->left ? node->left->estimated_cost : 0) + input_rows;
            break;
        }
        case LogicalNodeType::PROJECTION: {
            node->estimated_rows = node->left ? node->left->estimated_rows : 0;
            node->estimated_cost = (node->left ? node->left->estimated_cost : 0) + node->estimated_rows * 0.01;
            break;
        }
        case LogicalNodeType::JOIN: {
            double l = node->left ? node->left->estimated_rows : 1;
            double r = node->right ? node->right->estimated_rows : 1;
            double lc = node->left ? node->left->estimated_cost : 0;
            double rc = node->right ? node->right->estimated_cost : 0;
            if (node->join_algo == JoinAlgo::HASH_JOIN) {
                node->estimated_rows = l * r * 0.1; // assume 10% join selectivity
                node->estimated_cost = lc + rc + l + r; // build + probe
            } else {
                node->estimated_rows = l * r * 0.1;
                node->estimated_cost = lc + rc + l * r; // nested loop
            }
            break;
        }
        case LogicalNodeType::AGGREGATION: {
            double input = node->left ? node->left->estimated_rows : 0;
            node->estimated_rows = std::max(1.0, input * 0.1); // groups ~ 10% of input
            if (!node->group_exprs.empty()) {
                node->estimated_rows = std::max(1.0, std::sqrt(input));
            }
            node->estimated_cost = (node->left ? node->left->estimated_cost : 0) + input;
            break;
        }
        case LogicalNodeType::SORT: {
            double input = node->left ? node->left->estimated_rows : 0;
            node->estimated_rows = input;
            double sort_cost = input > 0 ? input * std::log2(input + 1) : 0;
            node->estimated_cost = (node->left ? node->left->estimated_cost : 0) + sort_cost;
            break;
        }
        case LogicalNodeType::LIMIT: {
            double input = node->left ? node->left->estimated_rows : 0;
            node->estimated_rows = node->limit_count >= 0 ?
                std::min(input, (double)node->limit_count) : input;
            node->estimated_cost = (node->left ? node->left->estimated_cost : 0);
            break;
        }
        case LogicalNodeType::DISTINCT: {
            double input = node->left ? node->left->estimated_rows : 0;
            node->estimated_rows = std::max(1.0, input * 0.5);
            node->estimated_cost = (node->left ? node->left->estimated_cost : 0) + input;
            break;
        }
    }
}

// ───── Choose join algorithm ─────
static void choose_join_algo(LogicalNodePtr node, storage::Catalog& catalog) {
    if (!node) return;
    choose_join_algo(node->left, catalog);
    choose_join_algo(node->right, catalog);

    if (node->type == LogicalNodeType::JOIN) {
        double l = node->left ? node->left->estimated_rows : 1;
        double r = node->right ? node->right->estimated_rows : 1;

        // Use hash join for larger inputs
        if (l * r > 10000 || l > 100 || r > 100) {
            node->join_algo = JoinAlgo::HASH_JOIN;
        } else {
            node->join_algo = JoinAlgo::NESTED_LOOP;
        }
    }
}

// ───── Cost-based optimizer entry ─────
LogicalNodePtr optimize_cost(LogicalNodePtr plan, storage::Catalog& catalog) {
    estimate_rows(plan, catalog);
    choose_join_algo(plan, catalog);
    // Re-estimate after algorithm choice
    estimate_rows(plan, catalog);
    return plan;
}

// ───── Combined pipeline ─────
LogicalNodePtr optimize(LogicalNodePtr plan, storage::Catalog& catalog) {
    plan = optimize_rules(plan);
    plan = optimize_cost(plan, catalog);
    return plan;
}

} // namespace optimizer
