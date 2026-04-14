#pragma once
#include <memory>
#include <string>
#include <vector>

#include "ast/ast.h"
#include "executor/executor.h"
#include "storage/storage.h"

namespace executor {

// Execute a SELECT while resolving normal views on-demand for this execution.
ExecResult execute_select_with_views(const ast::SelectStmt& stmt, storage::Catalog& catalog);

// Materialize a SELECT result into a concrete table (used by materialized views).
std::shared_ptr<storage::Table> materialize_select_to_table(
    const std::string& table_name,
    const ast::SelectStmt& stmt,
    storage::Catalog& catalog);

// Create temporary tables for non-materialized views used by a query tree.
void materialize_dynamic_views_for_select(
    const ast::SelectStmt& stmt,
    storage::Catalog& catalog,
    std::vector<std::string>& temp_tables);

// We remove temporary view tables created by materialize_dynamic_views_for_select.
void cleanup_temporary_views(storage::Catalog& catalog, const std::vector<std::string>& temp_tables);

} // namespace executor
