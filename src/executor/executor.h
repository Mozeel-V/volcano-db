#pragma once
#include <vector>
#include <string>
#include <chrono>
#include <unordered_map>
#include <unordered_set>
#include "planner/planner.h"
#include "storage/storage.h"

namespace executor {

struct EvalCtx;

struct ExecStats {
    size_t rows_scanned = 0;
    size_t rows_filtered = 0;
    size_t join_comparisons = 0;
    size_t rows_produced = 0;
    size_t subqueries_executed = 0;
    size_t subqueries_cached = 0;
    const EvalCtx* outer_ctx = nullptr;
    double exec_time_ms = 0;
    std::unordered_map<const ast::Expr*, storage::Value> subquery_cache;
    std::unordered_map<const ast::Expr*, std::unordered_set<std::string>> in_subquery_cache;
};

struct ExecResult {
    std::vector<std::string> columns;
    std::vector<storage::Row> rows;
    ExecStats stats;
};

// Execute a physical plan tree
ExecResult execute(planner::PhysicalNodePtr plan, storage::Catalog& catalog, const EvalCtx* outer_ctx = nullptr);

} // namespace executor
