#pragma once
#include <vector>
#include <string>
#include <chrono>
#include "planner/planner.h"
#include "storage/storage.h"

namespace executor {

struct ExecStats {
    size_t rows_scanned = 0;
    size_t rows_filtered = 0;
    size_t join_comparisons = 0;
    size_t rows_produced = 0;
    double exec_time_ms = 0;
};

struct ExecResult {
    std::vector<std::string> columns;
    std::vector<storage::Row> rows;
    ExecStats stats;
};

// Execute a physical plan tree
ExecResult execute(planner::PhysicalNodePtr plan, storage::Catalog& catalog);

} // namespace executor
