#include "planner/planner.h"
#include <algorithm>
#include <sstream>
#include <iomanip>

namespace planner {

// ───── Helper: generate node label string ─────
static std::string node_label(const LogicalNode& n) {
    std::string s;
    switch (n.type) {
        case LogicalNodeType::TABLE_SCAN:
            s = "SeqScan(" + n.table_name + ")"; break;
        case LogicalNodeType::INDEX_SCAN:
            s = "IndexScan(" + n.table_name + "." + n.index_column;
            if (n.index_range) s += " RANGE";
            else if (n.index_key) s += " = " + n.index_key->to_string();
            s += ")"; break;
        case LogicalNodeType::FILTER:
            s = "Filter(" + (n.predicate ? n.predicate->to_string() : "?") + ")"; break;
        case LogicalNodeType::PROJECTION: {
            s = "Projection(";
            for (size_t i = 0; i < n.output_names.size(); i++) {
                if (i) s += ", ";
                s += n.output_names[i];
            }
            s += ")"; break;
        }
        case LogicalNodeType::JOIN:
            s = (n.join_algo == JoinAlgo::HASH_JOIN ? "HashJoin" : "NestedLoopJoin");
            s += "(" + (n.join_cond ? n.join_cond->to_string() : "cross") + ")"; break;
        case LogicalNodeType::AGGREGATION: {
            s = "Aggregate(";
            for (size_t i = 0; i < n.agg_output_names.size(); i++) {
                if (i) s += ", ";
                s += n.agg_output_names[i];
            }
            s += ")"; break;
        }
        case LogicalNodeType::SORT:
            s = "Sort(";
            for (size_t i = 0; i < n.sort_keys.size(); i++) {
                if (i) s += ", ";
                s += n.sort_keys[i].expr->to_string();
                s += n.sort_keys[i].ascending ? " ASC" : " DESC";
            }
            s += ")"; break;
        case LogicalNodeType::LIMIT:
            s = "Limit(" + std::to_string(n.limit_count) + ", offset=" + std::to_string(n.offset_count) + ")"; break;
        case LogicalNodeType::DISTINCT:
            s = "Distinct"; break;
    }
    return s;
}

// ───── Pretty-print plan tree (legacy indented format) ─────
std::string LogicalNode::to_string(int indent) const {
    std::string pad(indent * 2, ' ');
    std::string s = pad + node_label(*this);
    if (estimated_rows > 0) {
        s += "  [est_rows=" + std::to_string((int)estimated_rows) +
             " cost=" + std::to_string((int)estimated_cost) + "]";
    }
    s += "\n";
    if (left) s += left->to_string(indent + 1);
    if (right) s += right->to_string(indent + 1);
    return s;
}

// ───── Tree-connector visualization ─────
std::string LogicalNode::to_tree_string(const std::string& prefix, bool is_last) const {
    std::string line;
    line += prefix;
    line += is_last ? "└── " : "├── ";
    line += node_label(*this);

    // Cost estimates
    if (estimated_rows > 0) {
        line += "  [est_rows=" + std::to_string((int)estimated_rows) +
                " cost=" + std::to_string((int)estimated_cost);
        // Per-node actual stats (EXPLAIN ANALYZE)
        if (has_actual_stats) {
            line += " | actual=" + std::to_string(actual_rows);
            std::ostringstream oss;
            oss << std::fixed << std::setprecision(3) << actual_time_ms;
            line += " time=" + oss.str() + "ms";
        }
        line += "]";
    } else if (has_actual_stats) {
        std::ostringstream oss;
        oss << std::fixed << std::setprecision(3) << actual_time_ms;
        line += "  [actual=" + std::to_string(actual_rows) + " time=" + oss.str() + "ms]";
    }
    line += "\n";

    std::string child_prefix = prefix + (is_last ? "    " : "│   ");
    if (left && right) {
        line += left->to_tree_string(child_prefix, false);
        line += right->to_tree_string(child_prefix, true);
    } else if (left) {
        line += left->to_tree_string(child_prefix, true);
    }
    return line;
}

// ───── DOT/Graphviz export ─────
static void to_dot_helper(const LogicalNode& n, std::string& dot, int& id) {
    int my_id = id++;
    std::string label = node_label(n);
    // Escape quotes in label
    std::string escaped;
    for (char c : label) {
        if (c == '"') escaped += "\\\"";
        else escaped += c;
    }

    // Pick color based on node type
    std::string color;
    switch (n.type) {
        case LogicalNodeType::TABLE_SCAN:  color = "lightyellow"; break;
        case LogicalNodeType::INDEX_SCAN:  color = "lightgreen"; break;
        case LogicalNodeType::FILTER:      color = "lightsalmon"; break;
        case LogicalNodeType::PROJECTION:  color = "lightblue"; break;
        case LogicalNodeType::JOIN:        color = "plum"; break;
        case LogicalNodeType::AGGREGATION: color = "lightcoral"; break;
        case LogicalNodeType::SORT:        color = "lightskyblue"; break;
        case LogicalNodeType::LIMIT:       color = "lightgoldenrodyellow"; break;
        case LogicalNodeType::DISTINCT:    color = "paleturquoise"; break;
    }

    dot += "  n" + std::to_string(my_id) + " [label=\"" + escaped;
    if (n.estimated_rows > 0)
        dot += "\\nest=" + std::to_string((int)n.estimated_rows) + " cost=" + std::to_string((int)n.estimated_cost);
    if (n.has_actual_stats) {
        std::ostringstream oss;
        oss << std::fixed << std::setprecision(3) << n.actual_time_ms;
        dot += "\\nactual=" + std::to_string(n.actual_rows) + " time=" + oss.str() + "ms";
    }
    dot += "\", fillcolor=\"" + color + "\"];\n";

    if (n.left) {
        int child_id = id;
        to_dot_helper(*n.left, dot, id);
        dot += "  n" + std::to_string(my_id) + " -> n" + std::to_string(child_id) + ";\n";
    }
    if (n.right) {
        int child_id = id;
        to_dot_helper(*n.right, dot, id);
        dot += "  n" + std::to_string(my_id) + " -> n" + std::to_string(child_id) + ";\n";
    }
}

std::string LogicalNode::to_dot_string() const {
    std::string dot = "digraph QueryPlan {\n";
    dot += "  rankdir=TB;\n";
    dot += "  node [shape=record, style=filled, fontname=\"Courier\"];\n";
    int id = 0;
    to_dot_helper(*this, dot, id);
    dot += "}\n";
    return dot;
}

// ───── Build logical plan from AST ─────

static bool has_aggregates(const ast::ExprPtr& expr) {
    if (!expr) return false;
    if (expr->type == ast::ExprType::FUNC_CALL) {
        std::string fn = expr->func_name;
        std::transform(fn.begin(), fn.end(), fn.begin(), ::toupper);
        if (fn == "COUNT" || fn == "SUM" || fn == "AVG" || fn == "MIN" || fn == "MAX")
            return true;
    }
    if (has_aggregates(expr->left)) return true;
    if (has_aggregates(expr->right)) return true;
    if (has_aggregates(expr->operand)) return true;
    for (auto& a : expr->args) if (has_aggregates(a)) return true;
    return false;
}

static LogicalNodePtr build_from(const ast::TableRefPtr& tr, storage::Catalog& catalog) {
    if (tr->type == ast::TableRefType::BASE_TABLE) {
        auto node = std::make_shared<LogicalNode>();
        node->type = LogicalNodeType::TABLE_SCAN;
        node->table_name = tr->table_name;
        node->table_alias = tr->alias;
        return node;
    }
    if (tr->type == ast::TableRefType::TRT_JOIN) {
        auto node = std::make_shared<LogicalNode>();
        node->type = LogicalNodeType::JOIN;
        node->join_type = tr->join_type;
        node->join_cond = tr->join_cond;
        node->left = build_from(tr->left, catalog);
        node->right = build_from(tr->right, catalog);
        return node;
    }
    // Subquery — create a scan placeholder
    auto node = std::make_shared<LogicalNode>();
    node->type = LogicalNodeType::TABLE_SCAN;
    node->table_name = "(subquery)";
    return node;
}

LogicalNodePtr build_logical_plan(const ast::SelectStmt& stmt, storage::Catalog& catalog) {
    LogicalNodePtr current;

    // 1. FROM clause → table scans / joins
    if (stmt.from_clause.size() == 1) {
        current = build_from(stmt.from_clause[0], catalog);
    } else if (stmt.from_clause.size() > 1) {
        // Cross join multiple tables
        current = build_from(stmt.from_clause[0], catalog);
        for (size_t i = 1; i < stmt.from_clause.size(); i++) {
            auto join_node = std::make_shared<LogicalNode>();
            join_node->type = LogicalNodeType::JOIN;
            join_node->join_type = ast::JoinType::JT_CROSS;
            join_node->left = current;
            join_node->right = build_from(stmt.from_clause[i], catalog);
            current = join_node;
        }
    }

    // 2. WHERE → Filter
    if (stmt.where_clause) {
        auto filter = std::make_shared<LogicalNode>();
        filter->type = LogicalNodeType::FILTER;
        filter->predicate = stmt.where_clause;
        filter->left = current;
        current = filter;
    }

    // 3. GROUP BY / Aggregation
    bool need_agg = !stmt.group_by.empty();
    if (!need_agg) {
        for (auto& sel : stmt.select_list) {
            if (has_aggregates(sel)) { need_agg = true; break; }
        }
    }
    if (need_agg) {
        auto agg = std::make_shared<LogicalNode>();
        agg->type = LogicalNodeType::AGGREGATION;
        agg->group_exprs = stmt.group_by;
        agg->agg_exprs = stmt.select_list;
        for (auto& e : stmt.select_list) {
            agg->agg_output_names.push_back(e->alias.empty() ? e->to_string() : e->alias);
        }
        agg->left = current;
        current = agg;
    }

    // 4. HAVING → Filter on aggregated result
    if (stmt.having_clause) {
        auto filter = std::make_shared<LogicalNode>();
        filter->type = LogicalNodeType::FILTER;
        filter->predicate = stmt.having_clause;
        filter->left = current;
        current = filter;
    }

    // 5. Projection (if no aggregation or additional projection needed)
    if (!need_agg) {
        auto proj = std::make_shared<LogicalNode>();
        proj->type = LogicalNodeType::PROJECTION;
        proj->projections = stmt.select_list;
        for (auto& e : stmt.select_list) {
            proj->output_names.push_back(e->alias.empty() ? e->to_string() : e->alias);
        }
        proj->left = current;
        current = proj;
    }

    // 6. DISTINCT
    if (stmt.distinct) {
        auto dist = std::make_shared<LogicalNode>();
        dist->type = LogicalNodeType::DISTINCT;
        dist->left = current;
        current = dist;
    }

    // 7. ORDER BY → Sort
    if (!stmt.order_by.empty()) {
        auto sort = std::make_shared<LogicalNode>();
        sort->type = LogicalNodeType::SORT;
        sort->sort_keys = stmt.order_by;
        sort->left = current;
        current = sort;
    }

    // 8. LIMIT / OFFSET
    if (stmt.limit >= 0) {
        auto lim = std::make_shared<LogicalNode>();
        lim->type = LogicalNodeType::LIMIT;
        lim->limit_count = stmt.limit;
        lim->offset_count = stmt.offset;
        lim->left = current;
        current = lim;
    }

    return current;
}

} // namespace planner
