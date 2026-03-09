#pragma once
#include "planner/planner.h"
#include "storage/storage.h"

namespace optimizer {

// Rule-based optimization
planner::LogicalNodePtr optimize_rules(planner::LogicalNodePtr plan);

// Cost-based optimization
planner::LogicalNodePtr optimize_cost(planner::LogicalNodePtr plan, storage::Catalog& catalog);

// Combined optimization pipeline
planner::LogicalNodePtr optimize(planner::LogicalNodePtr plan, storage::Catalog& catalog);

} // namespace optimizer
