#include "benchmark/benchmark.h"
#include "planner/planner.h"
#include "optimizer/optimizer.h"
#include <iostream>
#include <chrono>
#include <iomanip>

namespace benchmark {

using namespace storage;
using namespace planner;
using namespace optimizer;
using namespace executor;

extern ast::StmtPtr get_parsed_stmt();

static void run_query(const std::string& label, const std::string& sql,
                      Catalog& catalog, bool optimized) {
    auto stmt = ast::parse_sql(sql);
    if (!stmt || stmt->type != ast::StmtType::ST_SELECT || !stmt->select) {
        std::cerr << "  [" << label << "] Parse error\n";
        return;
    }

    auto plan = build_logical_plan(*stmt->select, catalog);
    if (optimized) {
        plan = optimize(plan, catalog);
    }

    auto result = execute(plan, catalog);

    std::cout << "  [" << label << "] "
              << result.rows.size() << " rows, "
              << std::fixed << std::setprecision(2) << result.stats.exec_time_ms << " ms"
              << " (scanned=" << result.stats.rows_scanned
              << " filtered=" << result.stats.rows_filtered
              << " joins=" << result.stats.join_comparisons << ")\n";
}

void run_benchmarks(Catalog& catalog) {
    std::cout << "\n═══════ BENCHMARK SUITE ═══════\n\n";

    std::cout << "── Selection Queries ──\n";
    run_query("sel-unopt", "SELECT * FROM employees WHERE salary > 100000", catalog, false);
    run_query("sel-opt",   "SELECT * FROM employees WHERE salary > 100000", catalog, true);

    std::cout << "\n── Aggregation Queries ──\n";
    run_query("agg-unopt", "SELECT dept, COUNT(*), AVG(salary) FROM employees GROUP BY dept", catalog, false);
    run_query("agg-opt",   "SELECT dept, COUNT(*), AVG(salary) FROM employees GROUP BY dept", catalog, true);

    std::cout << "\n── Join Queries ──\n";
    run_query("join-unopt", "SELECT e.name, d.budget FROM employees e JOIN departments d ON e.dept = d.dept_name WHERE e.salary > 80000", catalog, false);
    run_query("join-opt",   "SELECT e.name, d.budget FROM employees e JOIN departments d ON e.dept = d.dept_name WHERE e.salary > 80000", catalog, true);

    std::cout << "\n── Sort Queries ──\n";
    run_query("sort-unopt", "SELECT name, salary FROM employees ORDER BY salary DESC LIMIT 10", catalog, false);
    run_query("sort-opt",   "SELECT name, salary FROM employees ORDER BY salary DESC LIMIT 10", catalog, true);

    std::cout << "\n═══════════════════════════════\n";
}

} // namespace benchmark
