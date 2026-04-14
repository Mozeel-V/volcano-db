#pragma once
#include <string>
#include <vector>
#include <memory>
#include "ast/ast.h"
#include "storage/storage.h"

namespace planner {

enum class LogicalNodeType {
    TABLE_SCAN,
    INDEX_SCAN,
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
    // INDEX_SCAN
    std::string index_column;       // column the index covers
    ast::ExprPtr index_key;         // literal value for equality lookup
    bool index_range = false;       // true if this is a range scan (B-tree)
    ast::ExprPtr index_range_low;   // low bound for range scan
    ast::ExprPtr index_range_high;  // high bound for range scan

    // Children
    std::shared_ptr<LogicalNode> left;
    std::shared_ptr<LogicalNode> right;

    // Estimated cost (populated by optimizer)
    double estimated_rows = 0;
    double estimated_cost = 0;

    // Actual execution stats (populated by executor for EXPLAIN ANALYZE)
    size_t actual_rows = 0;
    double actual_time_ms = 0;
    bool has_actual_stats = false;

    std::string to_string(int indent = 0) const;
    std::string to_tree_string(const std::string& prefix = "", bool is_last = true) const;
    std::string to_dot_string() const;
};

using LogicalNodePtr = std::shared_ptr<LogicalNode>;

LogicalNodePtr build_logical_plan(const ast::SelectStmt& stmt, storage::Catalog& catalog);

using PhysicalNodePtr = LogicalNodePtr;  // reuse for simplicity

} // namespace planner
