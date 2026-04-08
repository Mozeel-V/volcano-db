#pragma once
#include "storage/storage.h"
#include "executor/executor.h"

namespace benchmark {

// Generate synthetic datasets
void generate_employees(storage::Catalog& catalog, size_t num_rows);
void generate_departments(storage::Catalog& catalog, size_t num_depts);
void generate_orders(storage::Catalog& catalog, size_t num_rows);

// Run benchmark suite
void run_benchmarks(storage::Catalog& catalog);

} // namespace benchmark
