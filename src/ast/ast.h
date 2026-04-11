#pragma once
#include <string>
#include <vector>
#include <memory>
#include <variant>
#include <iostream>
#include <sstream>

namespace ast {

// Forward declarations
struct Expr;
struct SelectStmt;
struct Statement;

using ExprPtr = std::shared_ptr<Expr>;
using StmtPtr = std::shared_ptr<Statement>;

// ───── Expression types ─────

enum class ExprType {
    COLUMN_REF,
    LITERAL_INT,
    LITERAL_FLOAT,
    LITERAL_STRING,
    LITERAL_NULL,
    BINARY_OP,
    UNARY_OP,
    FUNC_CALL,
    STAR,           // *
    SUBQUERY,
    IN_EXPR,
    EXISTS_EXPR,
    BETWEEN_EXPR,
    CASE_EXPR,
    CAST_EXPR,
};

enum class BinOp {
    OP_ADD, OP_SUB, OP_MUL, OP_DIV, OP_MOD,
    OP_EQ, OP_NEQ, OP_LT, OP_GT, OP_LTE, OP_GTE,
    OP_AND, OP_OR,
    OP_LIKE,
};

enum class UnaryOp {
    OP_NOT, OP_NEG, OP_IS_NULL, OP_IS_NOT_NULL,
};

struct Expr {
    ExprType type;
    // COLUMN_REF
    std::string table_name;  // optional qualifier
    std::string column_name;
    // LITERAL
    int64_t int_val = 0;
    double float_val = 0.0;
    std::string str_val;
    // BINARY_OP
    BinOp bin_op;
    ExprPtr left;
    ExprPtr right;
    // UNARY_OP
    UnaryOp unary_op;
    ExprPtr operand;
    // FUNC_CALL (aggregates + scalar)
    std::string func_name;
    std::vector<ExprPtr> args;
    bool distinct_func = false;
    // SUBQUERY
    std::shared_ptr<SelectStmt> subquery;
    // IN list
    std::vector<ExprPtr> in_list;
    // BETWEEN
    ExprPtr between_low;
    ExprPtr between_high;
    // Alias for SELECT list
    std::string alias;

    std::string to_string() const;
    static ExprPtr make_column(const std::string& col, const std::string& tbl = "");
    static ExprPtr make_int(int64_t v);
    static ExprPtr make_float(double v);
    static ExprPtr make_string(const std::string& v);
    static ExprPtr make_star();
    static ExprPtr make_binary(BinOp op, ExprPtr l, ExprPtr r);
    static ExprPtr make_unary(UnaryOp op, ExprPtr operand);
    static ExprPtr make_func(const std::string& name, std::vector<ExprPtr> args, bool dist = false);
};

// ───── Table references ─────

enum class TableRefType {
    BASE_TABLE,
    TRT_JOIN,
    TRT_SUBQUERY,
};

enum class JoinType {
    JT_INNER,
    JT_LEFT,
    JT_RIGHT,
    JT_FULL,
    JT_CROSS,
};

struct TableRef {
    TableRefType type;
    // BASE_TABLE
    std::string table_name;
    std::string alias;
    // JOIN
    JoinType join_type;
    std::shared_ptr<TableRef> left;
    std::shared_ptr<TableRef> right;
    ExprPtr join_cond;
    // SUBQUERY
    std::shared_ptr<SelectStmt> subquery;
};

using TableRefPtr = std::shared_ptr<TableRef>;

// ───── Order-by item ─────

struct OrderItem {
    ExprPtr expr;
    bool ascending = true;
};

// ───── CTE (WITH clause) ─────

struct CTE {
    std::string name;
    std::shared_ptr<SelectStmt> query;
};

// ───── SELECT statement ─────

enum class SetOpType {
    SO_NONE, SO_UNION, SO_UNION_ALL, SO_INTERSECT, SO_EXCEPT,
};

struct SelectStmt {
    bool distinct = false;
    std::vector<ExprPtr> select_list;          // projections
    std::vector<TableRefPtr> from_clause;      // table refs
    ExprPtr where_clause;                      // filter
    std::vector<ExprPtr> group_by;             // grouping
    ExprPtr having_clause;                     // having filter
    std::vector<OrderItem> order_by;           // sorting
    int64_t limit = -1;                        // -1 = no limit
    int64_t offset = 0;
    // Set operations
    SetOpType set_op = SetOpType::SO_NONE;
    std::shared_ptr<SelectStmt> set_rhs;
    // CTEs
    std::vector<CTE> ctes;

    std::string to_string() const;
};

// ───── DDL statements ─────

struct ColumnDef {
    std::string name;
    std::string data_type;
};

struct CreateTableStmt {
    std::string table_name;
    std::vector<ColumnDef> columns;
};

struct CreateIndexStmt {
    std::string index_name;
    std::string table_name;
    std::string column_name;
    bool hash_index = false;  // true=hash, false=btree
};

struct InsertStmt {
    std::string table_name;
    std::vector<std::string> columns;
    std::vector<std::vector<ExprPtr>> values;   // rows of values
};

struct UpdateStmt {
    std::string table_name;
    std::vector<std::pair<std::string, ExprPtr>> assignments;  // col = expr pairs
    ExprPtr where_clause;
};

struct DeleteStmt {
    std::string table_name;
    ExprPtr where_clause;
};

struct LoadStmt {
    std::string table_name;
    std::string file_path;
};

struct CreateViewStmt {
    std::string view_name;
    std::shared_ptr<SelectStmt> query;
    bool materialized = false;
};

struct AlterStmt {
    std::string table_name;
    std::string column_name;   // for ADD/DROP/RENAME col
    std::string column_type;   // for ADD col
    std::string new_name;      // for RENAME col/table
};

// ───── Top-level statement ─────

enum class StmtType {
    ST_SELECT,
    ST_CREATE_TABLE,
    ST_CREATE_INDEX,
    ST_CREATE_VIEW,
    ST_CREATE_MATERIALIZED_VIEW,
    ST_INSERT,
    ST_UPDATE,
    ST_DELETE,
    ST_LOAD,
    ST_EXPLAIN,
    ST_BENCHMARK,
    ST_ALTER_ADD_COL,
    ST_ALTER_DROP_COL,
    ST_ALTER_RENAME_COL,
    ST_ALTER_RENAME_TBL,
    ST_DROP_TABLE,
    ST_DROP_INDEX,
    ST_DROP_VIEW,
    ST_TRUNCATE,
};

struct Statement {
    StmtType type;
    std::shared_ptr<SelectStmt> select;
    std::shared_ptr<CreateTableStmt> create_table;
    std::shared_ptr<CreateIndexStmt> create_index;
    std::shared_ptr<CreateViewStmt> create_view;
    std::shared_ptr<InsertStmt> insert;
    std::shared_ptr<UpdateStmt> update;
    std::shared_ptr<DeleteStmt> del;
    std::shared_ptr<LoadStmt> load;
    std::shared_ptr<AlterStmt> alter;
    std::string drop_name;  // name of table/index/view to drop, or table to truncate
    bool explain_analyze = false;
};

// Parser interface
StmtPtr parse_sql(const std::string& sql);

} // namespace ast
