#pragma once
#include <string>
#include <vector>
#include <memory>
#include "ast/ast.h"
#include "storage/storage.h"

namespace planner {

// ───── Logical plan node types ─────
enum class LogicalNodeType {
    TABLE_SCAN,
    FILTER,
    PROJECTION,
    JOIN,
    AGGREGATION,
    SORT,
    LIMIT,
    DISTINCT,
};

enum class JoinAlgo {
    NESTED_LOOP,
    HASH_JOIN,
};

struct LogicalNode {
    LogicalNodeType type;
    // TABLE_SCAN
    std::string table_name;
    std::string table_alias;  // alias used in query (empty if none)
    // FILTER
    ast::ExprPtr predicate;
    // PROJECTION
    std::vector<ast::ExprPtr> projections;
    std::vector<std::string> output_names;
    // JOIN
    ast::JoinType join_type = ast::JoinType::JT_INNER;
    ast::ExprPtr join_cond;
    JoinAlgo join_algo = JoinAlgo::NESTED_LOOP;
    // AGGREGATION
    std::vector<ast::ExprPtr> group_exprs;
    std::vector<ast::ExprPtr> agg_exprs;
    std::vector<std::string> agg_output_names;
    // SORT
    std::vector<ast::OrderItem> sort_keys;
    // LIMIT
    int64_t limit_count = -1;
    int64_t offset_count = 0;
    // DISTINCT (no extra fields)

    // Children
    std::shared_ptr<LogicalNode> left;
    std::shared_ptr<LogicalNode> right;

    // Estimated cost (populated by optimizer)
    double estimated_rows = 0;
    double estimated_cost = 0;

    std::string to_string(int indent = 0) const;
};

using LogicalNodePtr = std::shared_ptr<LogicalNode>;

// ───── Logical plan builder ─────
LogicalNodePtr build_logical_plan(const ast::SelectStmt& stmt, storage::Catalog& catalog);

// ───── Physical plan (same structure, with algorithm choices) ─────
using PhysicalNodePtr = LogicalNodePtr;  // reuse for simplicity

} // namespace planner
