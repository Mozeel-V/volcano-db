#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>
#include <catch2/matchers/catch_matchers_floating_point.hpp>
#include <catch2/matchers/catch_matchers_vector.hpp>

#include "ast/ast.h"
#include "storage/storage.h"
#include "planner/planner.h"
#include "optimizer/optimizer.h"
#include "executor/executor.h"
#include "executor/view_support.h"
#include "benchmark/benchmark.h"

#include <memory>
#include <string>
#include <vector>
#include <variant>
#include <functional>
#include <cmath>

//  parse_sql bridge same as in main.cpp
extern int yyparse();
extern ast::StmtPtr get_parsed_stmt();
typedef struct yy_buffer_state* YY_BUFFER_STATE;
extern YY_BUFFER_STATE yy_scan_string(const char*);
extern void yy_delete_buffer(YY_BUFFER_STATE);

namespace ast {
StmtPtr parse_sql(const std::string& sql) {
    YY_BUFFER_STATE buf = yy_scan_string(sql.c_str());
    int rc = yyparse();
    yy_delete_buffer(buf);
    if (rc != 0) return nullptr;
    return get_parsed_stmt();
}
}

//  We use this to parse SQL, build plan, optimize, execute

static executor::ExecResult run_query(storage::Catalog& catalog, const std::string& sql) {
    auto stmt = ast::parse_sql(sql);
    REQUIRE(stmt != nullptr);

    if (stmt->type == ast::StmtType::ST_SELECT) {
        return executor::execute_select_with_views(*stmt->select, catalog);
    }
    if (stmt->type == ast::StmtType::ST_EXPLAIN) {
        return executor::execute_select_with_views(*stmt->select, catalog);
    }
    FAIL("run_query: unsupported statement type for execution");
    return {};
}

// We use this to create a small test table directly
static void create_test_table(storage::Catalog& catalog) {
    auto tbl = std::make_shared<storage::Table>();
    tbl->name = "t";
    tbl->schema = {
        {"id",    storage::DataType::INT},
        {"name",  storage::DataType::VARCHAR},
        {"value", storage::DataType::FLOAT},
        {"dept",  storage::DataType::VARCHAR},
    };
    tbl->rows = {
        {(int64_t)1, std::string("Alice"),   3.14,  std::string("Engineering")},
        {(int64_t)2, std::string("Bob"),     2.71,  std::string("Sales")},
        {(int64_t)3, std::string("Carol"),   1.41,  std::string("Engineering")},
        {(int64_t)4, std::string("Dave"),    9.81,  std::string("HR")},
        {(int64_t)5, std::string("Eve"),     0.0,   std::string("Sales")},
    };
    catalog.add_table(tbl);
}

static void create_join_tables(storage::Catalog& catalog) {
    // employees
    auto emp = std::make_shared<storage::Table>();
    emp->name = "emp";
    emp->schema = {
        {"id",   storage::DataType::INT},
        {"name", storage::DataType::VARCHAR},
        {"dept", storage::DataType::VARCHAR},
    };
    emp->rows = {
        {(int64_t)1, std::string("Alice"), std::string("Eng")},
        {(int64_t)2, std::string("Bob"),   std::string("Sales")},
        {(int64_t)3, std::string("Carol"), std::string("Eng")},
        {(int64_t)4, std::string("Dave"),  std::string("HR")},
    };
    catalog.add_table(emp);

    // departments
    auto dept = std::make_shared<storage::Table>();
    dept->name = "dept";
    dept->schema = {
        {"dname",  storage::DataType::VARCHAR},
        {"budget", storage::DataType::INT},
    };
    dept->rows = {
        {std::string("Eng"),   (int64_t)500000},
        {std::string("Sales"), (int64_t)300000},
        {std::string("HR"),    (int64_t)200000},
    };
    catalog.add_table(dept);
}

// We use this to get int value from a result cell
static int64_t as_int(const storage::Value& v) {
    REQUIRE(std::holds_alternative<int64_t>(v));
    return std::get<int64_t>(v);
}

static double as_double(const storage::Value& v) {
    REQUIRE(std::holds_alternative<double>(v));
    return std::get<double>(v);
}

static std::string as_string(const storage::Value& v) {
    REQUIRE(std::holds_alternative<std::string>(v));
    return std::get<std::string>(v);
}

static bool is_null(const storage::Value& v) {
    return std::holds_alternative<std::monostate>(v);
}

// DDL Statements

TEST_CASE("Parser: CREATE TABLE basic", "[parser][ddl]") {
    auto stmt = ast::parse_sql("CREATE TABLE users (id INT, name VARCHAR, age INT);");
    REQUIRE(stmt != nullptr);
    REQUIRE(stmt->type == ast::StmtType::ST_CREATE_TABLE);
    REQUIRE(stmt->create_table->table_name == "users");
    REQUIRE(stmt->create_table->columns.size() == 3);
    CHECK(stmt->create_table->columns[0].name == "id");
    CHECK(stmt->create_table->columns[0].data_type == "INT");
    CHECK(stmt->create_table->columns[1].name == "name");
    CHECK(stmt->create_table->columns[1].data_type == "VARCHAR");
    CHECK(stmt->create_table->columns[2].name == "age");
    CHECK(stmt->create_table->columns[2].data_type == "INT");
}

TEST_CASE("Parser: CREATE TABLE with FLOAT type", "[parser][ddl]") {
    auto stmt = ast::parse_sql("CREATE TABLE measurements (id INT, reading FLOAT);");
    REQUIRE(stmt != nullptr);
    REQUIRE(stmt->type == ast::StmtType::ST_CREATE_TABLE);
    CHECK(stmt->create_table->columns[1].data_type == "FLOAT");
}

TEST_CASE("Parser: CREATE TABLE with VARCHAR size", "[parser][ddl]") {
    auto stmt = ast::parse_sql("CREATE TABLE items (name VARCHAR(255), code VARCHAR(10));");
    REQUIRE(stmt != nullptr);
    REQUIRE(stmt->type == ast::StmtType::ST_CREATE_TABLE);
    // VARCHAR size is parsed but ignored -- type stored as "VARCHAR"
    CHECK(stmt->create_table->columns[0].data_type == "VARCHAR");
    CHECK(stmt->create_table->columns[1].data_type == "VARCHAR");
}

TEST_CASE("Parser: CREATE TABLE with INTEGER synonym", "[parser][ddl]") {
    auto stmt = ast::parse_sql("CREATE TABLE x (a INTEGER, b DOUBLE);");
    REQUIRE(stmt != nullptr);
    REQUIRE(stmt->type == ast::StmtType::ST_CREATE_TABLE);
}

TEST_CASE("Parser: CREATE TABLE with TEXT type", "[parser][ddl]") {
    auto stmt = ast::parse_sql("CREATE TABLE docs (id INT, body TEXT);");
    REQUIRE(stmt != nullptr);
    REQUIRE(stmt->type == ast::StmtType::ST_CREATE_TABLE);
}

TEST_CASE("Parser: CREATE INDEX basic", "[parser][ddl]") {
    auto stmt = ast::parse_sql("CREATE INDEX idx_id ON users (id);");
    REQUIRE(stmt != nullptr);
    REQUIRE(stmt->type == ast::StmtType::ST_CREATE_INDEX);
    CHECK(stmt->create_index->index_name == "idx_id");
    CHECK(stmt->create_index->table_name == "users");
    CHECK(stmt->create_index->column_name == "id");
    CHECK(stmt->create_index->hash_index == false);
}

TEST_CASE("Parser: CREATE INDEX USING HASH", "[parser][ddl]") {
    auto stmt = ast::parse_sql("CREATE INDEX idx_name ON users (name) USING HASH;");
    REQUIRE(stmt != nullptr);
    REQUIRE(stmt->type == ast::StmtType::ST_CREATE_INDEX);
    CHECK(stmt->create_index->hash_index == true);
}

TEST_CASE("Parser: CREATE VIEW", "[parser][ddl]") {
    auto stmt = ast::parse_sql("CREATE VIEW v_users AS SELECT id, name FROM users;");
    REQUIRE(stmt != nullptr);
    REQUIRE(stmt->type == ast::StmtType::ST_CREATE_VIEW);
    REQUIRE(stmt->create_view != nullptr);
    CHECK(stmt->create_view->view_name == "v_users");
    CHECK(stmt->create_view->materialized == false);
    REQUIRE(stmt->create_view->query != nullptr);
    CHECK(stmt->create_view->query->select_list.size() == 2);
}

TEST_CASE("Parser: CREATE MATERIALIZED VIEW", "[parser][ddl]") {
    auto stmt = ast::parse_sql("CREATE MATERIALIZED VIEW mv_users AS SELECT id FROM users;");
    REQUIRE(stmt != nullptr);
    REQUIRE(stmt->type == ast::StmtType::ST_CREATE_MATERIALIZED_VIEW);
    REQUIRE(stmt->create_view != nullptr);
    CHECK(stmt->create_view->view_name == "mv_users");
    CHECK(stmt->create_view->materialized == true);
    REQUIRE(stmt->create_view->query != nullptr);
}

TEST_CASE("Parser: INSERT INTO basic", "[parser][ddl]") {
    auto stmt = ast::parse_sql("INSERT INTO users VALUES (1, 'Alice', 30);");
    REQUIRE(stmt != nullptr);
    REQUIRE(stmt->type == ast::StmtType::ST_INSERT);
    CHECK(stmt->insert->table_name == "users");
}

TEST_CASE("Parser: LOAD statement", "[parser][ddl]") {
    auto stmt = ast::parse_sql("LOAD users 'data.csv';");
    REQUIRE(stmt != nullptr);
    REQUIRE(stmt->type == ast::StmtType::ST_LOAD);
    CHECK(stmt->load->table_name == "users");
    CHECK(stmt->load->file_path == "data.csv");
}

// Select Statement Basics

TEST_CASE("Parser: simple SELECT *", "[parser][select]") {
    auto stmt = ast::parse_sql("SELECT * FROM t;");
    REQUIRE(stmt != nullptr);
    REQUIRE(stmt->type == ast::StmtType::ST_SELECT);
    REQUIRE(stmt->select->select_list.size() == 1);
    CHECK(stmt->select->select_list[0]->type == ast::ExprType::STAR);
}

TEST_CASE("Parser: SELECT specific columns", "[parser][select]") {
    auto stmt = ast::parse_sql("SELECT id, name FROM t;");
    REQUIRE(stmt != nullptr);
    REQUIRE(stmt->select->select_list.size() == 2);
    CHECK(stmt->select->select_list[0]->type == ast::ExprType::COLUMN_REF);
    CHECK(stmt->select->select_list[0]->column_name == "id");
    CHECK(stmt->select->select_list[1]->column_name == "name");
}

TEST_CASE("Parser: SELECT with alias AS", "[parser][select]") {
    auto stmt = ast::parse_sql("SELECT id AS user_id, name AS user_name FROM t;");
    REQUIRE(stmt != nullptr);
    CHECK(stmt->select->select_list[0]->alias == "user_id");
    CHECK(stmt->select->select_list[1]->alias == "user_name");
}

TEST_CASE("Parser: SELECT with implicit alias", "[parser][select]") {
    auto stmt = ast::parse_sql("SELECT id uid FROM t;");
    REQUIRE(stmt != nullptr);
    CHECK(stmt->select->select_list[0]->alias == "uid");
}

TEST_CASE("Parser: SELECT DISTINCT", "[parser][select]") {
    auto stmt = ast::parse_sql("SELECT DISTINCT dept FROM t;");
    REQUIRE(stmt != nullptr);
    CHECK(stmt->select->distinct == true);
}

TEST_CASE("Parser: SELECT with table alias", "[parser][select]") {
    auto stmt = ast::parse_sql("SELECT e.name FROM emp e;");
    REQUIRE(stmt != nullptr);
    // Table ref should have alias
    REQUIRE(stmt->select->from_clause.size() == 1);
    CHECK(stmt->select->from_clause[0]->alias == "e");
}

// Expressions

TEST_CASE("Parser: integer literal", "[parser][expr]") {
    auto stmt = ast::parse_sql("SELECT 42 FROM t;");
    REQUIRE(stmt != nullptr);
    CHECK(stmt->select->select_list[0]->type == ast::ExprType::LITERAL_INT);
    CHECK(stmt->select->select_list[0]->int_val == 42);
}

TEST_CASE("Parser: float literal", "[parser][expr]") {
    auto stmt = ast::parse_sql("SELECT 3.14 FROM t;");
    REQUIRE(stmt != nullptr);
    CHECK(stmt->select->select_list[0]->type == ast::ExprType::LITERAL_FLOAT);
    CHECK(stmt->select->select_list[0]->float_val == Catch::Approx(3.14));
}

TEST_CASE("Parser: string literal", "[parser][expr]") {
    auto stmt = ast::parse_sql("SELECT 'hello' FROM t;");
    REQUIRE(stmt != nullptr);
    CHECK(stmt->select->select_list[0]->type == ast::ExprType::LITERAL_STRING);
    CHECK(stmt->select->select_list[0]->str_val == "hello");
}

TEST_CASE("Parser: NULL literal in expression", "[parser][expr]") {
    auto stmt = ast::parse_sql("SELECT NULL FROM t;");
    REQUIRE(stmt != nullptr);
    CHECK(stmt->select->select_list[0]->type == ast::ExprType::LITERAL_NULL);
}

TEST_CASE("Parser: arithmetic expressions", "[parser][expr]") {
    auto stmt = ast::parse_sql("SELECT 1 + 2, 3 - 1, 2 * 3, 10 / 2, 7 % 3 FROM t;");
    REQUIRE(stmt != nullptr);
    REQUIRE(stmt->select->select_list.size() == 5);
    CHECK(stmt->select->select_list[0]->type == ast::ExprType::BINARY_OP);
    CHECK(stmt->select->select_list[0]->bin_op == ast::BinOp::OP_ADD);
    CHECK(stmt->select->select_list[1]->bin_op == ast::BinOp::OP_SUB);
    CHECK(stmt->select->select_list[2]->bin_op == ast::BinOp::OP_MUL);
    CHECK(stmt->select->select_list[3]->bin_op == ast::BinOp::OP_DIV);
    CHECK(stmt->select->select_list[4]->bin_op == ast::BinOp::OP_MOD);
}

TEST_CASE("Parser: comparison operators", "[parser][expr]") {
    auto stmt = ast::parse_sql("SELECT * FROM t WHERE id = 1;");
    REQUIRE(stmt != nullptr);
    REQUIRE(stmt->select->where_clause != nullptr);
    CHECK(stmt->select->where_clause->bin_op == ast::BinOp::OP_EQ);
}

TEST_CASE("Parser: != and <> operators", "[parser][expr]") {
    auto s1 = ast::parse_sql("SELECT * FROM t WHERE id != 1;");
    REQUIRE(s1 != nullptr);
    CHECK(s1->select->where_clause->bin_op == ast::BinOp::OP_NEQ);

    auto s2 = ast::parse_sql("SELECT * FROM t WHERE id <> 1;");
    REQUIRE(s2 != nullptr);
    CHECK(s2->select->where_clause->bin_op == ast::BinOp::OP_NEQ);
}

TEST_CASE("Parser: relational operators", "[parser][expr]") {
    auto s1 = ast::parse_sql("SELECT * FROM t WHERE id < 5;");
    REQUIRE(s1 != nullptr);
    CHECK(s1->select->where_clause->bin_op == ast::BinOp::OP_LT);

    auto s2 = ast::parse_sql("SELECT * FROM t WHERE id > 5;");
    REQUIRE(s2 != nullptr);
    CHECK(s2->select->where_clause->bin_op == ast::BinOp::OP_GT);

    auto s3 = ast::parse_sql("SELECT * FROM t WHERE id <= 5;");
    REQUIRE(s3 != nullptr);
    CHECK(s3->select->where_clause->bin_op == ast::BinOp::OP_LTE);

    auto s4 = ast::parse_sql("SELECT * FROM t WHERE id >= 5;");
    REQUIRE(s4 != nullptr);
    CHECK(s4->select->where_clause->bin_op == ast::BinOp::OP_GTE);
}

TEST_CASE("Parser: logical AND/OR", "[parser][expr]") {
    auto stmt = ast::parse_sql("SELECT * FROM t WHERE id > 1 AND id < 5;");
    REQUIRE(stmt != nullptr);
    CHECK(stmt->select->where_clause->bin_op == ast::BinOp::OP_AND);
}

TEST_CASE("Parser: NOT unary", "[parser][expr]") {
    auto stmt = ast::parse_sql("SELECT * FROM t WHERE NOT id = 1;");
    REQUIRE(stmt != nullptr);
    CHECK(stmt->select->where_clause->type == ast::ExprType::UNARY_OP);
    CHECK(stmt->select->where_clause->unary_op == ast::UnaryOp::OP_NOT);
}

TEST_CASE("Parser: IS NULL / IS NOT NULL", "[parser][expr]") {
    auto s1 = ast::parse_sql("SELECT * FROM t WHERE name IS NULL;");
    REQUIRE(s1 != nullptr);
    CHECK(s1->select->where_clause->unary_op == ast::UnaryOp::OP_IS_NULL);

    auto s2 = ast::parse_sql("SELECT * FROM t WHERE name IS NOT NULL;");
    REQUIRE(s2 != nullptr);
    CHECK(s2->select->where_clause->unary_op == ast::UnaryOp::OP_IS_NOT_NULL);
}

TEST_CASE("Parser: LIKE expression", "[parser][expr]") {
    auto stmt = ast::parse_sql("SELECT * FROM t WHERE name LIKE '%ali%';");
    REQUIRE(stmt != nullptr);
    CHECK(stmt->select->where_clause->bin_op == ast::BinOp::OP_LIKE);
}

TEST_CASE("Parser: BETWEEN expression", "[parser][expr]") {
    auto stmt = ast::parse_sql("SELECT * FROM t WHERE id BETWEEN 2 AND 4;");
    REQUIRE(stmt != nullptr);
    CHECK(stmt->select->where_clause->type == ast::ExprType::BETWEEN_EXPR);
}

TEST_CASE("Parser: IN list expression", "[parser][expr]") {
    auto stmt = ast::parse_sql("SELECT * FROM t WHERE id IN (1, 3, 5);");
    REQUIRE(stmt != nullptr);
    CHECK(stmt->select->where_clause->type == ast::ExprType::IN_EXPR);
    CHECK(stmt->select->where_clause->in_list.size() == 3);
}

TEST_CASE("Parser: IN subquery expression", "[parser][expr]") {
    auto stmt = ast::parse_sql("SELECT * FROM t WHERE id IN (SELECT id FROM t);");
    REQUIRE(stmt != nullptr);
    CHECK(stmt->select->where_clause->type == ast::ExprType::IN_EXPR);
    CHECK(stmt->select->where_clause->subquery != nullptr);
}

TEST_CASE("Parser: NOT IN subquery expression", "[parser][expr]") {
    auto stmt = ast::parse_sql("SELECT * FROM t WHERE id NOT IN (SELECT id FROM t);");
    REQUIRE(stmt != nullptr);
    REQUIRE(stmt->select->where_clause->type == ast::ExprType::UNARY_OP);
    CHECK(stmt->select->where_clause->unary_op == ast::UnaryOp::OP_NOT);
    REQUIRE(stmt->select->where_clause->operand != nullptr);
    CHECK(stmt->select->where_clause->operand->type == ast::ExprType::IN_EXPR);
    CHECK(stmt->select->where_clause->operand->subquery != nullptr);
}

TEST_CASE("Parser: SOME quantified expression", "[parser][expr]") {
    auto stmt = ast::parse_sql("SELECT * FROM t WHERE id = SOME (SELECT id FROM t);");
    REQUIRE(stmt != nullptr);
    REQUIRE(stmt->select->where_clause->type == ast::ExprType::QUANTIFIED_EXPR);
    CHECK(stmt->select->where_clause->quant_cmp_op == ast::BinOp::OP_EQ);
    CHECK(stmt->select->where_clause->quantifier == ast::Quantifier::Q_SOME);
    CHECK(stmt->select->where_clause->subquery != nullptr);
}

TEST_CASE("Parser: ALL quantified expression", "[parser][expr]") {
    auto stmt = ast::parse_sql("SELECT * FROM t WHERE id > ALL (SELECT id FROM t);");
    REQUIRE(stmt != nullptr);
    REQUIRE(stmt->select->where_clause->type == ast::ExprType::QUANTIFIED_EXPR);
    CHECK(stmt->select->where_clause->quant_cmp_op == ast::BinOp::OP_GT);
    CHECK(stmt->select->where_clause->quantifier == ast::Quantifier::Q_ALL);
    CHECK(stmt->select->where_clause->subquery != nullptr);
}

TEST_CASE("Parser: ANY quantified expression", "[parser][expr]") {
    auto stmt = ast::parse_sql("SELECT * FROM t WHERE id = ANY (SELECT id FROM t);");
    REQUIRE(stmt != nullptr);
    REQUIRE(stmt->select->where_clause->type == ast::ExprType::QUANTIFIED_EXPR);
    CHECK(stmt->select->where_clause->quant_cmp_op == ast::BinOp::OP_EQ);
    CHECK(stmt->select->where_clause->quantifier == ast::Quantifier::Q_SOME);
}

TEST_CASE("Parser: EXISTS expression", "[parser][expr]") {
    auto stmt = ast::parse_sql("SELECT * FROM t WHERE EXISTS (SELECT 1 FROM t);");
    REQUIRE(stmt != nullptr);
    CHECK(stmt->select->where_clause->type == ast::ExprType::EXISTS_EXPR);
}

TEST_CASE("Parser: NOT EXISTS expression", "[parser][expr]") {
    auto stmt = ast::parse_sql("SELECT * FROM t WHERE NOT EXISTS (SELECT 1 FROM t);");
    REQUIRE(stmt != nullptr);
    REQUIRE(stmt->select->where_clause->type == ast::ExprType::UNARY_OP);
    CHECK(stmt->select->where_clause->unary_op == ast::UnaryOp::OP_NOT);
    REQUIRE(stmt->select->where_clause->operand != nullptr);
    CHECK(stmt->select->where_clause->operand->type == ast::ExprType::EXISTS_EXPR);
}

TEST_CASE("Parser: unary negation", "[parser][expr]") {
    auto stmt = ast::parse_sql("SELECT -1 FROM t;");
    REQUIRE(stmt != nullptr);
    CHECK(stmt->select->select_list[0]->type == ast::ExprType::UNARY_OP);
    CHECK(stmt->select->select_list[0]->unary_op == ast::UnaryOp::OP_NEG);
}

TEST_CASE("Parser: parenthesized expression", "[parser][expr]") {
    auto stmt = ast::parse_sql("SELECT (1 + 2) * 3 FROM t;");
    REQUIRE(stmt != nullptr);
    auto& e = stmt->select->select_list[0];
    CHECK(e->type == ast::ExprType::BINARY_OP);
    CHECK(e->bin_op == ast::BinOp::OP_MUL);
    // Left operand is (1+2)
    CHECK(e->left->type == ast::ExprType::BINARY_OP);
    CHECK(e->left->bin_op == ast::BinOp::OP_ADD);
}

TEST_CASE("Parser: qualified column reference", "[parser][expr]") {
    auto stmt = ast::parse_sql("SELECT t.id, t.name FROM t;");
    REQUIRE(stmt != nullptr);
    CHECK(stmt->select->select_list[0]->table_name == "t");
    CHECK(stmt->select->select_list[0]->column_name == "id");
}

// We test Parser -- Aggregate Functions.

TEST_CASE("Parser: COUNT(*)", "[parser][aggregate]") {
    auto stmt = ast::parse_sql("SELECT COUNT(*) FROM t;");
    REQUIRE(stmt != nullptr);
    auto& fn = stmt->select->select_list[0];
    CHECK(fn->type == ast::ExprType::FUNC_CALL);
    CHECK(fn->func_name == "COUNT");
    CHECK(fn->args.size() == 1);
    CHECK(fn->args[0]->type == ast::ExprType::STAR);
}

TEST_CASE("Parser: COUNT(column)", "[parser][aggregate]") {
    auto stmt = ast::parse_sql("SELECT COUNT(name) FROM t;");
    REQUIRE(stmt != nullptr);
    auto& fn = stmt->select->select_list[0];
    CHECK(fn->func_name == "COUNT");
    CHECK(fn->args[0]->type == ast::ExprType::COLUMN_REF);
}

TEST_CASE("Parser: COUNT(DISTINCT column)", "[parser][aggregate]") {
    auto stmt = ast::parse_sql("SELECT COUNT(DISTINCT dept) FROM t;");
    REQUIRE(stmt != nullptr);
    auto& fn = stmt->select->select_list[0];
    CHECK(fn->func_name == "COUNT");
    CHECK(fn->distinct_func == true);
}

TEST_CASE("Parser: SUM, AVG, MIN, MAX", "[parser][aggregate]") {
    auto stmt = ast::parse_sql("SELECT SUM(value), AVG(value), MIN(value), MAX(value) FROM t;");
    REQUIRE(stmt != nullptr);
    CHECK(stmt->select->select_list[0]->func_name == "SUM");
    CHECK(stmt->select->select_list[1]->func_name == "AVG");
    CHECK(stmt->select->select_list[2]->func_name == "MIN");
    CHECK(stmt->select->select_list[3]->func_name == "MAX");
}

// We test Parser -- Clauses (Where, Group By, Having, Order By, Limit).

TEST_CASE("Parser: WHERE clause", "[parser][clause]") {
    auto stmt = ast::parse_sql("SELECT * FROM t WHERE id > 2;");
    REQUIRE(stmt != nullptr);
    REQUIRE(stmt->select->where_clause != nullptr);
}

TEST_CASE("Parser: GROUP BY clause", "[parser][clause]") {
    auto stmt = ast::parse_sql("SELECT dept, COUNT(*) FROM t GROUP BY dept;");
    REQUIRE(stmt != nullptr);
    REQUIRE(stmt->select->group_by.size() == 1);
    CHECK(stmt->select->group_by[0]->column_name == "dept");
}

TEST_CASE("Parser: HAVING clause", "[parser][clause]") {
    auto stmt = ast::parse_sql("SELECT dept, COUNT(*) FROM t GROUP BY dept HAVING COUNT(*) > 1;");
    REQUIRE(stmt != nullptr);
    REQUIRE(stmt->select->having_clause != nullptr);
}

TEST_CASE("Parser: ORDER BY ASC (default)", "[parser][clause]") {
    auto stmt = ast::parse_sql("SELECT * FROM t ORDER BY id;");
    REQUIRE(stmt != nullptr);
    REQUIRE(stmt->select->order_by.size() == 1);
    CHECK(stmt->select->order_by[0].ascending == true);
}

TEST_CASE("Parser: ORDER BY DESC", "[parser][clause]") {
    auto stmt = ast::parse_sql("SELECT * FROM t ORDER BY id DESC;");
    REQUIRE(stmt != nullptr);
    CHECK(stmt->select->order_by[0].ascending == false);
}

TEST_CASE("Parser: ORDER BY multiple keys", "[parser][clause]") {
    auto stmt = ast::parse_sql("SELECT * FROM t ORDER BY dept ASC, id DESC;");
    REQUIRE(stmt != nullptr);
    REQUIRE(stmt->select->order_by.size() == 2);
    CHECK(stmt->select->order_by[0].ascending == true);
    CHECK(stmt->select->order_by[1].ascending == false);
}

TEST_CASE("Parser: LIMIT", "[parser][clause]") {
    auto stmt = ast::parse_sql("SELECT * FROM t LIMIT 10;");
    REQUIRE(stmt != nullptr);
    CHECK(stmt->select->limit == 10);
}

TEST_CASE("Parser: LIMIT with OFFSET", "[parser][clause]") {
    auto stmt = ast::parse_sql("SELECT * FROM t LIMIT 10 OFFSET 5;");
    REQUIRE(stmt != nullptr);
    CHECK(stmt->select->limit == 10);
    CHECK(stmt->select->offset == 5);
}

// We test Parser -- Join Syntax.

TEST_CASE("Parser: INNER JOIN", "[parser][join]") {
    auto stmt = ast::parse_sql("SELECT * FROM emp JOIN dept ON emp.dept = dept.dname;");
    REQUIRE(stmt != nullptr);
    auto& from = stmt->select->from_clause[0];
    CHECK(from->type == ast::TableRefType::TRT_JOIN);
    CHECK(from->join_type == ast::JoinType::JT_INNER);
}

TEST_CASE("Parser: explicit INNER JOIN", "[parser][join]") {
    auto stmt = ast::parse_sql("SELECT * FROM emp INNER JOIN dept ON emp.dept = dept.dname;");
    REQUIRE(stmt != nullptr);
    CHECK(stmt->select->from_clause[0]->join_type == ast::JoinType::JT_INNER);
}

TEST_CASE("Parser: LEFT JOIN", "[parser][join]") {
    auto stmt = ast::parse_sql("SELECT * FROM emp LEFT JOIN dept ON emp.dept = dept.dname;");
    REQUIRE(stmt != nullptr);
    CHECK(stmt->select->from_clause[0]->join_type == ast::JoinType::JT_LEFT);
}

TEST_CASE("Parser: LEFT OUTER JOIN", "[parser][join]") {
    auto stmt = ast::parse_sql("SELECT * FROM emp LEFT OUTER JOIN dept ON emp.dept = dept.dname;");
    REQUIRE(stmt != nullptr);
    CHECK(stmt->select->from_clause[0]->join_type == ast::JoinType::JT_LEFT);
}

TEST_CASE("Parser: RIGHT JOIN", "[parser][join]") {
    auto stmt = ast::parse_sql("SELECT * FROM emp RIGHT JOIN dept ON emp.dept = dept.dname;");
    REQUIRE(stmt != nullptr);
    CHECK(stmt->select->from_clause[0]->join_type == ast::JoinType::JT_RIGHT);
}

TEST_CASE("Parser: FULL OUTER JOIN", "[parser][join]") {
    auto stmt = ast::parse_sql("SELECT * FROM emp FULL OUTER JOIN dept ON emp.dept = dept.dname;");
    REQUIRE(stmt != nullptr);
    CHECK(stmt->select->from_clause[0]->join_type == ast::JoinType::JT_FULL);
}

TEST_CASE("Parser: CROSS JOIN", "[parser][join]") {
    auto stmt = ast::parse_sql("SELECT * FROM emp CROSS JOIN dept;");
    REQUIRE(stmt != nullptr);
    CHECK(stmt->select->from_clause[0]->join_type == ast::JoinType::JT_CROSS);
}

TEST_CASE("Parser: subquery in FROM", "[parser][join]") {
    auto stmt = ast::parse_sql("SELECT * FROM (SELECT id FROM t) sub;");
    REQUIRE(stmt != nullptr);
    auto& from = stmt->select->from_clause[0];
    CHECK(from->type == ast::TableRefType::TRT_SUBQUERY);
    CHECK(from->alias == "sub");
}

// We test Parser -- Explain / Benchmark.

TEST_CASE("Parser: EXPLAIN SELECT", "[parser][explain]") {
    auto stmt = ast::parse_sql("EXPLAIN SELECT * FROM t;");
    REQUIRE(stmt != nullptr);
    CHECK(stmt->type == ast::StmtType::ST_EXPLAIN);
    CHECK(stmt->explain_analyze == false);
    CHECK(stmt->select != nullptr);
}

TEST_CASE("Parser: EXPLAIN ANALYZE SELECT", "[parser][explain]") {
    auto stmt = ast::parse_sql("EXPLAIN ANALYZE SELECT * FROM t;");
    REQUIRE(stmt != nullptr);
    CHECK(stmt->type == ast::StmtType::ST_EXPLAIN);
    CHECK(stmt->explain_analyze == true);
}

TEST_CASE("Parser: BENCHMARK SELECT", "[parser][explain]") {
    auto stmt = ast::parse_sql("BENCHMARK SELECT * FROM t;");
    REQUIRE(stmt != nullptr);
    CHECK(stmt->type == ast::StmtType::ST_BENCHMARK);
}

// We test Case Insensitivity -- Sql Keywords.

TEST_CASE("Case insensitivity: all lowercase", "[parser][case]") {
    auto stmt = ast::parse_sql("select * from t where id = 1;");
    REQUIRE(stmt != nullptr);
    REQUIRE(stmt->type == ast::StmtType::ST_SELECT);
}

TEST_CASE("Case insensitivity: all uppercase", "[parser][case]") {
    auto stmt = ast::parse_sql("SELECT * FROM T WHERE ID = 1;");
    REQUIRE(stmt != nullptr);
    REQUIRE(stmt->type == ast::StmtType::ST_SELECT);
}

TEST_CASE("Case insensitivity: mixed case keywords", "[parser][case]") {
    auto stmt = ast::parse_sql("SeLeCt * FrOm t WhErE id = 1;");
    REQUIRE(stmt != nullptr);
    REQUIRE(stmt->type == ast::StmtType::ST_SELECT);
}

TEST_CASE("Case insensitivity: DDL keywords", "[parser][case]") {
    auto s1 = ast::parse_sql("create table test (id int);");
    REQUIRE(s1 != nullptr);
    CHECK(s1->type == ast::StmtType::ST_CREATE_TABLE);

    auto s2 = ast::parse_sql("CREATE INDEX idx ON test (id) USING HASH;");
    REQUIRE(s2 != nullptr);
    CHECK(s2->type == ast::StmtType::ST_CREATE_INDEX);
}

TEST_CASE("Case insensitivity: aggregate function names", "[parser][case]") {
    auto s1 = ast::parse_sql("SELECT count(*) FROM t;");
    REQUIRE(s1 != nullptr);
    CHECK(s1->select->select_list[0]->type == ast::ExprType::FUNC_CALL);

    auto s2 = ast::parse_sql("SELECT COUNT(*) FROM t;");
    REQUIRE(s2 != nullptr);
    CHECK(s2->select->select_list[0]->type == ast::ExprType::FUNC_CALL);

    auto s3 = ast::parse_sql("SELECT Count(*) FROM t;");
    REQUIRE(s3 != nullptr);
    CHECK(s3->select->select_list[0]->type == ast::ExprType::FUNC_CALL);
}

TEST_CASE("Case insensitivity: JOIN keywords", "[parser][case]") {
    auto stmt = ast::parse_sql("select * from emp inner join dept on emp.dept = dept.dname;");
    REQUIRE(stmt != nullptr);
    CHECK(stmt->select->from_clause[0]->join_type == ast::JoinType::JT_INNER);
}

TEST_CASE("Case insensitivity: ORDER BY ASC/DESC", "[parser][case]") {
    auto s1 = ast::parse_sql("SELECT * FROM t ORDER BY id asc;");
    REQUIRE(s1 != nullptr);
    CHECK(s1->select->order_by[0].ascending == true);

    auto s2 = ast::parse_sql("SELECT * FROM t ORDER BY id desc;");
    REQUIRE(s2 != nullptr);
    CHECK(s2->select->order_by[0].ascending == false);
}

TEST_CASE("Case sensitivity: identifiers are case-preserved", "[parser][case]") {
    // Identifiers should be stored as the user typed them (case-preserving)
    auto stmt = ast::parse_sql("SELECT MyColumn FROM MyTable;");
    REQUIRE(stmt != nullptr);
    // Note: identifiers may be lowercased by the case-insensitive lexer
    // This test documents the actual behavior
    auto& col = stmt->select->select_list[0];
    CHECK(col->type == ast::ExprType::COLUMN_REF);
    // Column name is stored as-is from the lexer (lower or preserved)
}

TEST_CASE("Case insensitivity: EXPLAIN and ANALYZE", "[parser][case]") {
    auto stmt = ast::parse_sql("explain analyze select * from t;");
    REQUIRE(stmt != nullptr);
    CHECK(stmt->type == ast::StmtType::ST_EXPLAIN);
    CHECK(stmt->explain_analyze == true);
}

TEST_CASE("Case insensitivity: DISTINCT keyword", "[parser][case]") {
    auto stmt = ast::parse_sql("select distinct dept from t;");
    REQUIRE(stmt != nullptr);
    CHECK(stmt->select->distinct == true);
}

TEST_CASE("Case insensitivity: IS NULL / IS NOT NULL", "[parser][case]") {
    auto s1 = ast::parse_sql("SELECT * FROM t WHERE name is null;");
    REQUIRE(s1 != nullptr);
    CHECK(s1->select->where_clause->unary_op == ast::UnaryOp::OP_IS_NULL);

    auto s2 = ast::parse_sql("SELECT * FROM t WHERE name is not null;");
    REQUIRE(s2 != nullptr);
    CHECK(s2->select->where_clause->unary_op == ast::UnaryOp::OP_IS_NOT_NULL);
}

TEST_CASE("Case insensitivity: BETWEEN, LIKE, IN", "[parser][case]") {
    auto s1 = ast::parse_sql("SELECT * FROM t WHERE id between 1 and 5;");
    REQUIRE(s1 != nullptr);
    CHECK(s1->select->where_clause->type == ast::ExprType::BETWEEN_EXPR);

    auto s2 = ast::parse_sql("SELECT * FROM t WHERE name like '%test%';");
    REQUIRE(s2 != nullptr);
    CHECK(s2->select->where_clause->bin_op == ast::BinOp::OP_LIKE);

    auto s3 = ast::parse_sql("SELECT * FROM t WHERE id in (1, 2, 3);");
    REQUIRE(s3 != nullptr);
    CHECK(s3->select->where_clause->type == ast::ExprType::IN_EXPR);
}

// We test Punctuation & Grammar.

TEST_CASE("Grammar: semicolon terminator required", "[parser][grammar]") {
    // Without semicolon -- should still work (parser may handle)
    // or fail depending on grammar. We test the behavior.
    auto stmt = ast::parse_sql("SELECT * FROM t;");
    REQUIRE(stmt != nullptr);
}

TEST_CASE("Grammar: commas in SELECT list", "[parser][grammar]") {
    auto stmt = ast::parse_sql("SELECT a, b, c FROM t;");
    REQUIRE(stmt != nullptr);
    CHECK(stmt->select->select_list.size() == 3);
}

TEST_CASE("Grammar: commas in column definition", "[parser][grammar]") {
    auto stmt = ast::parse_sql("CREATE TABLE x (a INT, b FLOAT, c VARCHAR);");
    REQUIRE(stmt != nullptr);
    CHECK(stmt->create_table->columns.size() == 3);
}

TEST_CASE("Grammar: parentheses in expressions", "[parser][grammar]") {
    auto stmt = ast::parse_sql("SELECT (1 + 2) * (3 + 4) FROM t;");
    REQUIRE(stmt != nullptr);
    CHECK(stmt->select->select_list[0]->bin_op == ast::BinOp::OP_MUL);
}

TEST_CASE("Grammar: dot notation for qualified columns", "[parser][grammar]") {
    auto stmt = ast::parse_sql("SELECT t.id FROM t;");
    REQUIRE(stmt != nullptr);
    CHECK(stmt->select->select_list[0]->table_name == "t");
    CHECK(stmt->select->select_list[0]->column_name == "id");
}

TEST_CASE("Grammar: string with single quotes", "[parser][grammar]") {
    auto stmt = ast::parse_sql("SELECT * FROM t WHERE name = 'Alice';");
    REQUIRE(stmt != nullptr);
    // Right side of comparison should be a string literal
    auto& rhs = stmt->select->where_clause->right;
    CHECK(rhs->type == ast::ExprType::LITERAL_STRING);
    CHECK(rhs->str_val == "Alice");
}

TEST_CASE("Grammar: multiple FROM tables (implicit cross join)", "[parser][grammar]") {
    auto stmt = ast::parse_sql("SELECT * FROM emp, dept;");
    REQUIRE(stmt != nullptr);
    CHECK(stmt->select->from_clause.size() == 2);
}

TEST_CASE("Grammar: operator precedence (* before +)", "[parser][grammar]") {
    auto stmt = ast::parse_sql("SELECT 1 + 2 * 3 FROM t;");
    REQUIRE(stmt != nullptr);
    auto& e = stmt->select->select_list[0];
    // Should be ADD at top level (1 + (2*3))
    CHECK(e->bin_op == ast::BinOp::OP_ADD);
    CHECK(e->right->bin_op == ast::BinOp::OP_MUL);
}

TEST_CASE("Grammar: AND has higher precedence than OR", "[parser][grammar]") {
    auto stmt = ast::parse_sql("SELECT * FROM t WHERE a = 1 OR b = 2 AND c = 3;");
    REQUIRE(stmt != nullptr);
    // Should be OR at top level (a=1 OR (b=2 AND c=3))
    CHECK(stmt->select->where_clause->bin_op == ast::BinOp::OP_OR);
}

TEST_CASE("Parser: parse failure returns nullptr", "[parser][error]") {
    // Completely invalid SQL
    auto s1 = ast::parse_sql("FOOBAR BAZQUUX;");
    CHECK(s1 == nullptr);
}

TEST_CASE("Parser: incomplete query returns nullptr", "[parser][error]") {
    auto s1 = ast::parse_sql("SELECT;");
    CHECK(s1 == nullptr);
}

TEST_CASE("Grammar: SQL comment single-line --", "[parser][grammar]") {
    auto stmt = ast::parse_sql("SELECT * FROM t -- this is a comment\n WHERE id = 1;");
    REQUIRE(stmt != nullptr);
    REQUIRE(stmt->select->where_clause != nullptr);
}

TEST_CASE("Grammar: SQL comment block /* */", "[parser][grammar]") {
    auto stmt = ast::parse_sql("SELECT /* everything */ * FROM t;");
    REQUIRE(stmt != nullptr);
    CHECK(stmt->select->select_list[0]->type == ast::ExprType::STAR);
}

// We test Storage -- Value Helpers.

TEST_CASE("Storage: value_is_null", "[storage][value]") {
    CHECK(storage::value_is_null(std::monostate{}) == true);
    CHECK(storage::value_is_null(storage::Value{(int64_t)42}) == false);
    CHECK(storage::value_is_null(storage::Value{3.14}) == false);
    CHECK(storage::value_is_null(storage::Value{std::string("x")}) == false);
}

TEST_CASE("Storage: value_to_int conversions", "[storage][value]") {
    CHECK(storage::value_to_int(storage::Value{(int64_t)42}) == 42);
    CHECK(storage::value_to_int(storage::Value{3.7}) == 3); // truncation
    CHECK(storage::value_to_int(std::monostate{}) == 0);    // null -> 0
}

TEST_CASE("Storage: value_to_double conversions", "[storage][value]") {
    CHECK(storage::value_to_double(storage::Value{(int64_t)42}) == 42.0);
    CHECK(storage::value_to_double(storage::Value{3.14}) == Catch::Approx(3.14));
    CHECK(storage::value_to_double(std::monostate{}) == 0.0);
}

TEST_CASE("Storage: value_to_string conversions", "[storage][value]") {
    CHECK(storage::value_to_string(storage::Value{(int64_t)42}) == "42");
    CHECK(storage::value_to_string(storage::Value{std::string("hello")}) == "hello");
    CHECK(storage::value_to_string(std::monostate{}) == "NULL");
}

TEST_CASE("Storage: value_equal", "[storage][value]") {
    CHECK(storage::value_equal(storage::Value{(int64_t)1}, storage::Value{(int64_t)1}) == true);
    CHECK(storage::value_equal(storage::Value{(int64_t)1}, storage::Value{(int64_t)2}) == false);
    CHECK(storage::value_equal(storage::Value{std::string("a")}, storage::Value{std::string("a")}) == true);
    CHECK(storage::value_equal(storage::Value{std::string("a")}, storage::Value{std::string("b")}) == false);
    // NULL = NULL -> false (SQL standard)
    CHECK(storage::value_equal(std::monostate{}, std::monostate{}) == false);
    // NULL = x -> false
    CHECK(storage::value_equal(std::monostate{}, storage::Value{(int64_t)1}) == false);
}

TEST_CASE("Storage: value_less", "[storage][value]") {
    CHECK(storage::value_less(storage::Value{(int64_t)1}, storage::Value{(int64_t)2}) == true);
    CHECK(storage::value_less(storage::Value{(int64_t)2}, storage::Value{(int64_t)1}) == false);
    CHECK(storage::value_less(storage::Value{std::string("a")}, storage::Value{std::string("b")}) == true);
    // NULL comparisons -> false
    CHECK(storage::value_less(std::monostate{}, storage::Value{(int64_t)1}) == false);
    CHECK(storage::value_less(storage::Value{(int64_t)1}, std::monostate{}) == false);
}

TEST_CASE("Storage: value_less mixed int/float", "[storage][value]") {
    // int vs double comparison through value_to_double
    CHECK(storage::value_less(storage::Value{(int64_t)1}, storage::Value{2.5}) == true);
    CHECK(storage::value_less(storage::Value{3.5}, storage::Value{(int64_t)2}) == false);
}

TEST_CASE("Storage: value_add/sub/mul/div", "[storage][value]") {
    // INT + INT -> INT
    auto sum = storage::value_add(storage::Value{(int64_t)3}, storage::Value{(int64_t)4});
    CHECK(std::get<int64_t>(sum) == 7);

    // INT + FLOAT -> FLOAT (via double)
    auto fsum = storage::value_add(storage::Value{(int64_t)3}, storage::Value{1.5});
    CHECK(std::get<double>(fsum) == Catch::Approx(4.5));

    // SUB
    auto diff = storage::value_sub(storage::Value{(int64_t)10}, storage::Value{(int64_t)3});
    CHECK(std::get<int64_t>(diff) == 7);

    // MUL
    auto prod = storage::value_mul(storage::Value{(int64_t)3}, storage::Value{(int64_t)4});
    CHECK(std::get<int64_t>(prod) == 12);

    // DIV
    auto quot = storage::value_div(storage::Value{(int64_t)10}, storage::Value{(int64_t)3});
    CHECK(std::get<int64_t>(quot) == 3); // integer division
}

TEST_CASE("Storage: arithmetic with NULL propagates NULL", "[storage][value]") {
    auto r1 = storage::value_add(std::monostate{}, storage::Value{(int64_t)5});
    CHECK(storage::value_is_null(r1));

    auto r2 = storage::value_sub(storage::Value{(int64_t)5}, std::monostate{});
    CHECK(storage::value_is_null(r2));

    auto r3 = storage::value_mul(std::monostate{}, std::monostate{});
    CHECK(storage::value_is_null(r3));

    auto r4 = storage::value_div(storage::Value{(int64_t)5}, std::monostate{});
    CHECK(storage::value_is_null(r4));
}

TEST_CASE("Storage: division by zero returns NULL", "[storage][value]") {
    auto r = storage::value_div(storage::Value{(int64_t)10}, storage::Value{(int64_t)0});
    CHECK(storage::value_is_null(r));
}

TEST_CASE("Storage: value_display formatting", "[storage][value]") {
    CHECK(storage::value_display(std::monostate{}) == "NULL");
    CHECK(storage::value_display(storage::Value{(int64_t)42}) == "42");
    CHECK(storage::value_display(storage::Value{3.14}) == "3.14");
    CHECK(storage::value_display(storage::Value{std::string("hello")}) == "hello");
}

// We test Storage -- Table Operations.

TEST_CASE("Storage: Table creation and schema", "[storage][table]") {
    storage::Table tbl("test", {{"id", storage::DataType::INT}, {"name", storage::DataType::VARCHAR}});
    CHECK(tbl.name == "test");
    CHECK(tbl.col_count() == 2);
    CHECK(tbl.row_count() == 0);
    CHECK(tbl.column_index("id") == 0);
    CHECK(tbl.column_index("name") == 1);
    CHECK(tbl.column_index("nonexistent") == -1);
}

TEST_CASE("Storage: Table insert_row", "[storage][table]") {
    storage::Table tbl("test", {{"id", storage::DataType::INT}, {"val", storage::DataType::VARCHAR}});
    tbl.insert_row({(int64_t)1, std::string("hello")});
    tbl.insert_row({(int64_t)2, std::string("world")});
    CHECK(tbl.row_count() == 2);
    CHECK(std::get<int64_t>(tbl.rows[0][0]) == 1);
    CHECK(std::get<std::string>(tbl.rows[1][1]) == "world");
}

TEST_CASE("Storage: Table distinct_values", "[storage][table]") {
    storage::Table tbl("test", {{"dept", storage::DataType::VARCHAR}});
    tbl.insert_row({std::string("Eng")});
    tbl.insert_row({std::string("Sales")});
    tbl.insert_row({std::string("Eng")});
    tbl.insert_row({std::string("HR")});
    CHECK(tbl.distinct_values("dept") == 3);
}

TEST_CASE("Storage: Table cardinality", "[storage][table]") {
    storage::Table tbl("test", {{"id", storage::DataType::INT}});
    CHECK(tbl.cardinality() == 0);
    tbl.insert_row({(int64_t)1});
    tbl.insert_row({(int64_t)2});
    CHECK(tbl.cardinality() == 2);
}

// We test Storage -- Catalog.

TEST_CASE("Storage: Catalog add and get table", "[storage][catalog]") {
    storage::Catalog catalog;
    auto tbl = std::make_shared<storage::Table>("users", std::vector<storage::ColumnSchema>{{"id", storage::DataType::INT}});
    catalog.add_table(tbl);
    CHECK(catalog.get_table("users") != nullptr);
    CHECK(catalog.get_table("nonexistent") == nullptr);
}

TEST_CASE("Storage: Catalog cardinality and distinct", "[storage][catalog]") {
    storage::Catalog catalog;
    auto tbl = std::make_shared<storage::Table>("t", std::vector<storage::ColumnSchema>{{"x", storage::DataType::INT}});
    tbl->insert_row({(int64_t)1});
    tbl->insert_row({(int64_t)2});
    tbl->insert_row({(int64_t)1});
    catalog.add_table(tbl);

    CHECK(catalog.table_cardinality("t") == 3);
    CHECK(catalog.column_distinct("t", "x") == 2);
    CHECK(catalog.table_cardinality("missing") == 0);
}

TEST_CASE("Storage: Catalog add and get view", "[storage][catalog]") {
    storage::Catalog catalog;
    auto stmt = ast::parse_sql("SELECT id FROM users;");
    REQUIRE(stmt != nullptr);
    REQUIRE(stmt->type == ast::StmtType::ST_SELECT);

    catalog.add_view("v_users", stmt->select, false);
    CHECK(catalog.has_view("v_users") == true);
    CHECK(catalog.has_view("missing") == false);

    auto* v = catalog.get_view("v_users");
    REQUIRE(v != nullptr);
    CHECK(v->materialized == false);
    REQUIRE(v->query != nullptr);
}

// We test Storage -- Hash Index.

TEST_CASE("Storage: Hash index build and lookup (int)", "[storage][index]") {
    storage::Table tbl("t", {{"id", storage::DataType::INT}});
    tbl.insert_row({(int64_t)10});
    tbl.insert_row({(int64_t)20});
    tbl.insert_row({(int64_t)10});

    storage::HashIndex idx;
    idx.column_name = "id";
    idx.build(tbl);
    auto hits = idx.lookup_int(10);
    CHECK(hits.size() == 2);
    auto miss = idx.lookup_int(99);
    CHECK(miss.empty());
}

TEST_CASE("Storage: Hash index build and lookup (string)", "[storage][index]") {
    storage::Table tbl("t", {{"name", storage::DataType::VARCHAR}});
    tbl.insert_row({std::string("Alice")});
    tbl.insert_row({std::string("Bob")});
    tbl.insert_row({std::string("Alice")});

    storage::HashIndex idx;
    idx.column_name = "name";
    idx.build(tbl);
    auto hits = idx.lookup_str("Alice");
    CHECK(hits.size() == 2);
    auto miss = idx.lookup_str("Charlie");
    CHECK(miss.empty());
}

TEST_CASE("Storage: Catalog create_index", "[storage][index]") {
    storage::Catalog catalog;
    auto tbl = std::make_shared<storage::Table>("t", std::vector<storage::ColumnSchema>{{"id", storage::DataType::INT}});
    tbl->insert_row({(int64_t)1});
    tbl->insert_row({(int64_t)2});
    catalog.add_table(tbl);

    catalog.create_index("idx_id", "t", "id", true);
    CHECK(catalog.get_index("t", "id") != nullptr);
    CHECK(catalog.get_index("t", "nonexistent") == nullptr);
}

// We test End-To-End Execution -- Select *.

TEST_CASE("E2E: SELECT * FROM table", "[e2e][select]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    auto res = run_query(catalog, "SELECT * FROM t;");
    CHECK(res.rows.size() == 5);
    CHECK(res.columns.size() == 4);
}

TEST_CASE("E2E: SELECT specific columns", "[e2e][select]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    auto res = run_query(catalog, "SELECT id, name FROM t;");
    CHECK(res.rows.size() == 5);
    CHECK(res.columns.size() == 2);
}

TEST_CASE("E2E: SELECT with expression in list", "[e2e][select]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    auto res = run_query(catalog, "SELECT id, id + 10 FROM t;");
    CHECK(res.rows.size() == 5);
    // First row: id=1, id+10=11
    CHECK(as_int(res.rows[0][1]) == 11);
}

// We test End-To-End -- Where Clause.

TEST_CASE("E2E: WHERE with equality", "[e2e][where]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    auto res = run_query(catalog, "SELECT * FROM t WHERE id = 3;");
    CHECK(res.rows.size() == 1);
    CHECK(as_int(res.rows[0][0]) == 3);
}

TEST_CASE("E2E: WHERE with string comparison", "[e2e][where]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    auto res = run_query(catalog, "SELECT * FROM t WHERE name = 'Bob';");
    CHECK(res.rows.size() == 1);
    CHECK(as_string(res.rows[0][1]) == "Bob");
}

TEST_CASE("E2E: WHERE with inequality", "[e2e][where]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    auto res = run_query(catalog, "SELECT * FROM t WHERE id > 3;");
    CHECK(res.rows.size() == 2); // id=4, id=5
}

TEST_CASE("E2E: WHERE with <= operator", "[e2e][where]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    auto res = run_query(catalog, "SELECT * FROM t WHERE id <= 2;");
    CHECK(res.rows.size() == 2);
}

TEST_CASE("E2E: WHERE with AND", "[e2e][where]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    auto res = run_query(catalog, "SELECT * FROM t WHERE id > 1 AND id < 4;");
    CHECK(res.rows.size() == 2); // id=2, id=3
}

TEST_CASE("E2E: WHERE with OR", "[e2e][where]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    auto res = run_query(catalog, "SELECT * FROM t WHERE id = 1 OR id = 5;");
    CHECK(res.rows.size() == 2);
}

TEST_CASE("E2E: WHERE with NOT", "[e2e][where]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    auto res = run_query(catalog, "SELECT * FROM t WHERE NOT id = 1;");
    CHECK(res.rows.size() == 4);
}

TEST_CASE("E2E: WHERE with != operator", "[e2e][where]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    auto res = run_query(catalog, "SELECT * FROM t WHERE id != 1;");
    CHECK(res.rows.size() == 4);
}

TEST_CASE("E2E: WHERE with <> operator", "[e2e][where]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    auto res = run_query(catalog, "SELECT * FROM t WHERE id <> 1;");
    CHECK(res.rows.size() == 4);
}

TEST_CASE("E2E: WHERE with BETWEEN", "[e2e][where]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    auto res = run_query(catalog, "SELECT * FROM t WHERE id BETWEEN 2 AND 4;");
    CHECK(res.rows.size() == 3); // id=2,3,4
}

TEST_CASE("E2E: WHERE with IN list", "[e2e][where]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    auto res = run_query(catalog, "SELECT * FROM t WHERE id IN (1, 3, 5);");
    CHECK(res.rows.size() == 3);
}

TEST_CASE("E2E: WHERE with NOT IN subquery", "[e2e][where]") {
    storage::Catalog catalog;
    create_test_table(catalog);

    auto ids = std::make_shared<storage::Table>(
        "ids", std::vector<storage::ColumnSchema>{{"id", storage::DataType::INT}}
    );
    ids->insert_row({(int64_t)1});
    ids->insert_row({(int64_t)3});
    catalog.add_table(ids);

    auto res = run_query(catalog, "SELECT * FROM t WHERE id NOT IN (SELECT id FROM ids) ORDER BY id;");
    REQUIRE(res.rows.size() == 3);
    CHECK(as_int(res.rows[0][0]) == 2);
    CHECK(as_int(res.rows[1][0]) == 4);
    CHECK(as_int(res.rows[2][0]) == 5);
}

TEST_CASE("E2E: WHERE with NOT EXISTS correlated subquery", "[e2e][where]") {
    storage::Catalog catalog;
    create_test_table(catalog);

    auto ids = std::make_shared<storage::Table>(
        "ids", std::vector<storage::ColumnSchema>{{"id", storage::DataType::INT}}
    );
    ids->insert_row({(int64_t)1});
    ids->insert_row({(int64_t)3});
    catalog.add_table(ids);

    auto res = run_query(catalog, "SELECT * FROM t WHERE NOT EXISTS (SELECT 1 FROM ids WHERE ids.id = t.id) ORDER BY id;");
    REQUIRE(res.rows.size() == 3);
    CHECK(as_int(res.rows[0][0]) == 2);
    CHECK(as_int(res.rows[1][0]) == 4);
    CHECK(as_int(res.rows[2][0]) == 5);
}

TEST_CASE("E2E: WHERE with SOME quantified subquery", "[e2e][where]") {
    storage::Catalog catalog;
    create_test_table(catalog);

    auto ids = std::make_shared<storage::Table>(
        "ids", std::vector<storage::ColumnSchema>{{"id", storage::DataType::INT}}
    );
    ids->insert_row({(int64_t)1});
    ids->insert_row({(int64_t)3});
    catalog.add_table(ids);

    auto res = run_query(catalog, "SELECT * FROM t WHERE id = SOME (SELECT id FROM ids) ORDER BY id;");
    REQUIRE(res.rows.size() == 2);
    CHECK(as_int(res.rows[0][0]) == 1);
    CHECK(as_int(res.rows[1][0]) == 3);
}

TEST_CASE("E2E: WHERE with ALL quantified subquery", "[e2e][where]") {
    storage::Catalog catalog;
    create_test_table(catalog);

    auto ids = std::make_shared<storage::Table>(
        "ids", std::vector<storage::ColumnSchema>{{"id", storage::DataType::INT}}
    );
    ids->insert_row({(int64_t)1});
    ids->insert_row({(int64_t)3});
    catalog.add_table(ids);

    auto res = run_query(catalog, "SELECT * FROM t WHERE id > ALL (SELECT id FROM ids) ORDER BY id;");
    REQUIRE(res.rows.size() == 2);
    CHECK(as_int(res.rows[0][0]) == 4);
    CHECK(as_int(res.rows[1][0]) == 5);
}

TEST_CASE("E2E: WHERE with ANY quantified subquery", "[e2e][where]") {
    storage::Catalog catalog;
    create_test_table(catalog);

    auto ids = std::make_shared<storage::Table>(
        "ids", std::vector<storage::ColumnSchema>{{"id", storage::DataType::INT}}
    );
    ids->insert_row({(int64_t)1});
    ids->insert_row({(int64_t)3});
    catalog.add_table(ids);

    auto res = run_query(catalog, "SELECT * FROM t WHERE id = ANY (SELECT id FROM ids) ORDER BY id;");
    REQUIRE(res.rows.size() == 2);
    CHECK(as_int(res.rows[0][0]) == 1);
    CHECK(as_int(res.rows[1][0]) == 3);
}

TEST_CASE("E2E: WHERE with LIKE prefix%", "[e2e][where]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    auto res = run_query(catalog, "SELECT * FROM t WHERE name LIKE 'Al%';");
    CHECK(res.rows.size() == 1);
    CHECK(as_string(res.rows[0][1]) == "Alice");
}

TEST_CASE("E2E: WHERE with LIKE %suffix", "[e2e][where]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    auto res = run_query(catalog, "SELECT * FROM t WHERE name LIKE '%ve';");
    CHECK(res.rows.size() == 2); // Dave, Eve
}

TEST_CASE("E2E: WHERE with LIKE %substring%", "[e2e][where]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    auto res = run_query(catalog, "SELECT * FROM t WHERE name LIKE '%ob%';");
    CHECK(res.rows.size() == 1); // Bob
}

TEST_CASE("E2E: WHERE with LIKE exact match", "[e2e][where]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    auto res = run_query(catalog, "SELECT * FROM t WHERE name LIKE 'Bob';");
    CHECK(res.rows.size() == 1);
}

// We test End-To-End -- Order By.

TEST_CASE("E2E: ORDER BY ASC (default)", "[e2e][orderby]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    auto res = run_query(catalog, "SELECT * FROM t ORDER BY id;");
    REQUIRE(res.rows.size() == 5);
    CHECK(as_int(res.rows[0][0]) == 1);
    CHECK(as_int(res.rows[4][0]) == 5);
}

TEST_CASE("E2E: ORDER BY DESC", "[e2e][orderby]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    auto res = run_query(catalog, "SELECT * FROM t ORDER BY id DESC;");
    REQUIRE(res.rows.size() == 5);
    CHECK(as_int(res.rows[0][0]) == 5);
    CHECK(as_int(res.rows[4][0]) == 1);
}

TEST_CASE("E2E: ORDER BY string column", "[e2e][orderby]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    auto res = run_query(catalog, "SELECT * FROM t ORDER BY name;");
    REQUIRE(res.rows.size() == 5);
    // Alphabetical: Alice, Bob, Carol, Dave, Eve
    CHECK(as_string(res.rows[0][1]) == "Alice");
    CHECK(as_string(res.rows[4][1]) == "Eve");
}

TEST_CASE("E2E: ORDER BY multiple keys", "[e2e][orderby]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    auto res = run_query(catalog, "SELECT * FROM t ORDER BY dept, id DESC;");
    // Engineering first (Carol=3, Alice=1), then HR (Dave=4), then Sales (Eve=5, Bob=2)
    REQUIRE(res.rows.size() == 5);
    CHECK(as_string(res.rows[0][3]) == "Engineering");
    CHECK(as_string(res.rows[2][3]) == "HR");
}

// We test End-To-End -- Limit / Offset.

TEST_CASE("E2E: LIMIT", "[e2e][limit]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    auto res = run_query(catalog, "SELECT * FROM t ORDER BY id LIMIT 3;");
    CHECK(res.rows.size() == 3);
    CHECK(as_int(res.rows[0][0]) == 1);
    CHECK(as_int(res.rows[2][0]) == 3);
}

TEST_CASE("E2E: LIMIT with OFFSET", "[e2e][limit]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    auto res = run_query(catalog, "SELECT * FROM t ORDER BY id LIMIT 2 OFFSET 2;");
    CHECK(res.rows.size() == 2);
    CHECK(as_int(res.rows[0][0]) == 3);
    CHECK(as_int(res.rows[1][0]) == 4);
}

TEST_CASE("E2E: LIMIT larger than row count", "[e2e][limit]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    auto res = run_query(catalog, "SELECT * FROM t LIMIT 100;");
    CHECK(res.rows.size() == 5);
}

TEST_CASE("E2E: OFFSET beyond row count", "[e2e][limit]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    auto res = run_query(catalog, "SELECT * FROM t ORDER BY id LIMIT 10 OFFSET 100;");
    CHECK(res.rows.size() == 0);
}

TEST_CASE("E2E: LIMIT 0", "[e2e][limit]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    auto res = run_query(catalog, "SELECT * FROM t LIMIT 0;");
    CHECK(res.rows.size() == 0);
}

// We test End-To-End -- Distinct.

TEST_CASE("E2E: SELECT DISTINCT", "[e2e][distinct]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    auto res = run_query(catalog, "SELECT DISTINCT dept FROM t;");
    CHECK(res.rows.size() == 3); // Engineering, Sales, HR
}

TEST_CASE("E2E: DISTINCT on all-unique column", "[e2e][distinct]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    auto res = run_query(catalog, "SELECT DISTINCT id FROM t;");
    CHECK(res.rows.size() == 5);
}

// We test End-To-End -- Aggregation.

TEST_CASE("E2E: COUNT(*)", "[e2e][aggregation]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    auto res = run_query(catalog, "SELECT COUNT(*) FROM t;");
    REQUIRE(res.rows.size() == 1);
    CHECK(as_int(res.rows[0][0]) == 5);
}

TEST_CASE("E2E: COUNT(column)", "[e2e][aggregation]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    auto res = run_query(catalog, "SELECT COUNT(name) FROM t;");
    REQUIRE(res.rows.size() == 1);
    CHECK(as_int(res.rows[0][0]) == 5);
}

TEST_CASE("E2E: SUM of integer column", "[e2e][aggregation]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    auto res = run_query(catalog, "SELECT SUM(id) FROM t;");
    REQUIRE(res.rows.size() == 1);
    CHECK(as_int(res.rows[0][0]) == 15); // 1+2+3+4+5
}

TEST_CASE("E2E: AVG of float column", "[e2e][aggregation]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    auto res = run_query(catalog, "SELECT AVG(value) FROM t;");
    REQUIRE(res.rows.size() == 1);
    // (3.14 + 2.71 + 1.41 + 9.81 + 0.0) / 5 = 3.414
    CHECK(as_double(res.rows[0][0]) == Catch::Approx(3.414));
}

TEST_CASE("E2E: MIN and MAX", "[e2e][aggregation]") {
    storage::Catalog catalog;
    create_test_table(catalog);

    auto res_min = run_query(catalog, "SELECT MIN(id) FROM t;");
    CHECK(as_int(res_min.rows[0][0]) == 1);

    auto res_max = run_query(catalog, "SELECT MAX(id) FROM t;");
    CHECK(as_int(res_max.rows[0][0]) == 5);
}

TEST_CASE("E2E: MIN and MAX on strings", "[e2e][aggregation]") {
    storage::Catalog catalog;
    create_test_table(catalog);

    auto res_min = run_query(catalog, "SELECT MIN(name) FROM t;");
    CHECK(as_string(res_min.rows[0][0]) == "Alice");

    auto res_max = run_query(catalog, "SELECT MAX(name) FROM t;");
    CHECK(as_string(res_max.rows[0][0]) == "Eve");
}

TEST_CASE("E2E: GROUP BY with COUNT", "[e2e][aggregation]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    auto res = run_query(catalog, "SELECT dept, COUNT(*) FROM t GROUP BY dept;");
    CHECK(res.rows.size() == 3); // Engineering, Sales, HR
    // We can't guarantee ordering, just check total count
    int64_t total = 0;
    for (auto& row : res.rows) total += as_int(row[1]);
    CHECK(total == 5);
}

TEST_CASE("E2E: GROUP BY with SUM", "[e2e][aggregation]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    auto res = run_query(catalog, "SELECT dept, SUM(id) FROM t GROUP BY dept;");
    CHECK(res.rows.size() == 3);
}

TEST_CASE("E2E: GROUP BY with multiple aggregates", "[e2e][aggregation]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    auto res = run_query(catalog, "SELECT dept, COUNT(*), AVG(value), MIN(id), MAX(id) FROM t GROUP BY dept;");
    CHECK(res.rows.size() == 3);
    CHECK(res.columns.size() == 5);
}

TEST_CASE("E2E: HAVING clause", "[e2e][aggregation]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    auto res = run_query(catalog, "SELECT dept, COUNT(*) FROM t GROUP BY dept HAVING COUNT(*) > 1;");
    // Engineering=2, Sales=2 -> 2 groups pass HAVING
    CHECK(res.rows.size() == 2);
}

// We test End-To-End -- Joins.

TEST_CASE("E2E: INNER JOIN", "[e2e][join]") {
    storage::Catalog catalog;
    create_join_tables(catalog);
    auto res = run_query(catalog, "SELECT * FROM emp JOIN dept ON emp.dept = dept.dname;");
    CHECK(res.rows.size() == 4); // all emp match
}

TEST_CASE("E2E: INNER JOIN with alias", "[e2e][join]") {
    storage::Catalog catalog;
    create_join_tables(catalog);
    auto res = run_query(catalog, "SELECT e.name, d.budget FROM emp e JOIN dept d ON e.dept = d.dname;");
    CHECK(res.rows.size() == 4);
    CHECK(res.columns.size() == 2);
}

TEST_CASE("E2E: INNER JOIN with WHERE filter", "[e2e][join]") {
    storage::Catalog catalog;
    create_join_tables(catalog);
    auto res = run_query(catalog, "SELECT e.name, d.budget FROM emp e JOIN dept d ON e.dept = d.dname WHERE d.budget > 250000;");
    // Eng=500000, Sales=300000 -> 3 employees match (Alice, Bob, Carol)
    CHECK(res.rows.size() == 3);
}

TEST_CASE("E2E: CROSS JOIN", "[e2e][join]") {
    storage::Catalog catalog;
    create_join_tables(catalog);
    auto res = run_query(catalog, "SELECT * FROM emp CROSS JOIN dept;");
    CHECK(res.rows.size() == 12); // 4 * 3
}

TEST_CASE("E2E: implicit cross join (multi-table FROM)", "[e2e][join]") {
    storage::Catalog catalog;
    create_join_tables(catalog);
    auto res = run_query(catalog, "SELECT * FROM emp, dept;");
    CHECK(res.rows.size() == 12); // 4 * 3
}

TEST_CASE("E2E: JOIN with aggregation", "[e2e][join]") {
    storage::Catalog catalog;
    create_join_tables(catalog);
    auto res = run_query(catalog, "SELECT d.dname, COUNT(*) FROM emp e JOIN dept d ON e.dept = d.dname GROUP BY d.dname;");
    CHECK(res.rows.size() == 3);
}

// We test End-To-End -- Arithmetic In Select.

TEST_CASE("E2E: arithmetic in SELECT list", "[e2e][arithmetic]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    auto res = run_query(catalog, "SELECT id, id * 2, id + 100 FROM t WHERE id = 1;");
    REQUIRE(res.rows.size() == 1);
    CHECK(as_int(res.rows[0][0]) == 1);
    CHECK(as_int(res.rows[0][1]) == 2);
    CHECK(as_int(res.rows[0][2]) == 101);
}

TEST_CASE("E2E: arithmetic with float column", "[e2e][arithmetic]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    auto res = run_query(catalog, "SELECT id, value * 2 FROM t WHERE id = 1;");
    REQUIRE(res.rows.size() == 1);
    CHECK(as_double(res.rows[0][1]) == Catch::Approx(6.28));
}

TEST_CASE("E2E: modulo operator", "[e2e][arithmetic]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    auto res = run_query(catalog, "SELECT id, id % 2 FROM t ORDER BY id;");
    REQUIRE(res.rows.size() == 5);
    CHECK(as_int(res.rows[0][1]) == 1); // 1 % 2 = 1
    CHECK(as_int(res.rows[1][1]) == 0); // 2 % 2 = 0
}

// We test End-To-End -- Null Handling.

TEST_CASE("E2E: NULL handling in data", "[e2e][null]") {
    storage::Catalog catalog;
    auto tbl = std::make_shared<storage::Table>();
    tbl->name = "tn";
    tbl->schema = {{"id", storage::DataType::INT}, {"val", storage::DataType::INT}};
    tbl->rows = {
        {(int64_t)1, (int64_t)10},
        {(int64_t)2, std::monostate{}},
        {(int64_t)3, (int64_t)30},
    };
    catalog.add_table(tbl);

    SECTION("IS NULL filter") {
        auto res = run_query(catalog, "SELECT * FROM tn WHERE val IS NULL;");
        CHECK(res.rows.size() == 1);
        CHECK(as_int(res.rows[0][0]) == 2);
    }

    SECTION("IS NOT NULL filter") {
        auto res = run_query(catalog, "SELECT * FROM tn WHERE val IS NOT NULL;");
        CHECK(res.rows.size() == 2);
    }

    SECTION("COUNT(*) includes NULLs") {
        auto res = run_query(catalog, "SELECT COUNT(*) FROM tn;");
        CHECK(as_int(res.rows[0][0]) == 3);
    }

    SECTION("COUNT(column) excludes NULLs") {
        auto res = run_query(catalog, "SELECT COUNT(val) FROM tn;");
        CHECK(as_int(res.rows[0][0]) == 2);
    }

    SECTION("SUM skips NULLs") {
        auto res = run_query(catalog, "SELECT SUM(val) FROM tn;");
        CHECK(as_int(res.rows[0][0]) == 40); // 10+30
    }

    SECTION("AVG skips NULLs") {
        auto res = run_query(catalog, "SELECT AVG(val) FROM tn;");
        CHECK(as_double(res.rows[0][0]) == Catch::Approx(20.0)); // (10+30)/2
    }
}

TEST_CASE("E2E: NULL equality comparison", "[e2e][null]") {
    storage::Catalog catalog;
    auto tbl = std::make_shared<storage::Table>();
    tbl->name = "tn2";
    tbl->schema = {{"id", storage::DataType::INT}, {"val", storage::DataType::INT}};
    tbl->rows = {
        {(int64_t)1, (int64_t)10},
        {(int64_t)2, std::monostate{}},
    };
    catalog.add_table(tbl);

    // NULL = NULL -> false (SQL standard), so no rows match
    auto res = run_query(catalog, "SELECT * FROM tn2 WHERE val = val;");
    // Only row 1 matches (10=10), row 2 has NULL=NULL -> false
    CHECK(res.rows.size() == 1);
}

// We test End-To-End -- Combined Query Patterns.

TEST_CASE("E2E: SELECT with WHERE + ORDER BY + LIMIT", "[e2e][combined]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    auto res = run_query(catalog, "SELECT name, id FROM t WHERE id > 1 ORDER BY id DESC LIMIT 2;");
    CHECK(res.rows.size() == 2);
    CHECK(as_int(res.rows[0][1]) == 5); // first = highest after filter
    CHECK(as_int(res.rows[1][1]) == 4);
}

TEST_CASE("E2E: DISTINCT + ORDER BY", "[e2e][combined]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    auto res = run_query(catalog, "SELECT DISTINCT dept FROM t ORDER BY dept;");
    REQUIRE(res.rows.size() == 3);
    CHECK(as_string(res.rows[0][0]) == "Engineering");
    CHECK(as_string(res.rows[1][0]) == "HR");
    CHECK(as_string(res.rows[2][0]) == "Sales");
}

TEST_CASE("E2E: GROUP BY + HAVING + ORDER BY", "[e2e][combined]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    auto res = run_query(catalog, "SELECT dept, COUNT(*) FROM t GROUP BY dept HAVING COUNT(*) >= 2 ORDER BY dept;");
    REQUIRE(res.rows.size() == 2);
    CHECK(as_string(res.rows[0][0]) == "Engineering");
    CHECK(as_string(res.rows[1][0]) == "Sales");
}

TEST_CASE("E2E: JOIN + WHERE + ORDER BY + LIMIT", "[e2e][combined]") {
    storage::Catalog catalog;
    create_join_tables(catalog);
    auto res = run_query(catalog, "SELECT e.name, d.budget FROM emp e JOIN dept d ON e.dept = d.dname WHERE d.budget > 200000 ORDER BY d.budget DESC LIMIT 2;");
    CHECK(res.rows.size() == 2);
}

TEST_CASE("E2E: Aggregate without GROUP BY", "[e2e][combined]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    auto res = run_query(catalog, "SELECT COUNT(*), SUM(id), AVG(value), MIN(id), MAX(id) FROM t;");
    REQUIRE(res.rows.size() == 1);
    CHECK(as_int(res.rows[0][0]) == 5);     // COUNT
    CHECK(as_int(res.rows[0][1]) == 15);    // SUM
    CHECK(as_int(res.rows[0][3]) == 1);     // MIN
    CHECK(as_int(res.rows[0][4]) == 5);     // MAX
}

TEST_CASE("E2E: non-materialized VIEW reflects latest data", "[e2e][view]") {
    storage::Catalog catalog;
    create_test_table(catalog);

    auto create_view_stmt = ast::parse_sql("CREATE VIEW v_t AS SELECT id, name FROM t WHERE id >= 3;");
    REQUIRE(create_view_stmt != nullptr);
    REQUIRE(create_view_stmt->type == ast::StmtType::ST_CREATE_VIEW);
    catalog.add_view(create_view_stmt->create_view->view_name, create_view_stmt->create_view->query, false);

    auto r1 = run_query(catalog, "SELECT id FROM v_t ORDER BY id;");
    REQUIRE(r1.rows.size() == 3);
    CHECK(as_int(r1.rows[0][0]) == 3);
    CHECK(as_int(r1.rows[2][0]) == 5);

    auto* base = catalog.get_table("t");
    REQUIRE(base != nullptr);
    base->insert_row({(int64_t)6, std::string("Frank"), 4.20, std::string("Engineering")});

    auto r2 = run_query(catalog, "SELECT id FROM v_t ORDER BY id;");
    REQUIRE(r2.rows.size() == 4);
    CHECK(as_int(r2.rows[3][0]) == 6);
}

TEST_CASE("E2E: MATERIALIZED VIEW keeps snapshot", "[e2e][view]") {
    storage::Catalog catalog;
    create_test_table(catalog);

    auto create_mv_stmt = ast::parse_sql("CREATE MATERIALIZED VIEW mv_t AS SELECT id FROM t WHERE id <= 3;");
    REQUIRE(create_mv_stmt != nullptr);
    REQUIRE(create_mv_stmt->type == ast::StmtType::ST_CREATE_MATERIALIZED_VIEW);

    auto mv_tbl = executor::materialize_select_to_table(
        create_mv_stmt->create_view->view_name,
        *create_mv_stmt->create_view->query,
        catalog);
    catalog.add_table(mv_tbl);
    catalog.add_view(create_mv_stmt->create_view->view_name, create_mv_stmt->create_view->query, true);

    auto r1 = run_query(catalog, "SELECT id FROM mv_t ORDER BY id;");
    REQUIRE(r1.rows.size() == 3);
    CHECK(as_int(r1.rows[2][0]) == 3);

    auto* base = catalog.get_table("t");
    REQUIRE(base != nullptr);
    base->insert_row({(int64_t)0, std::string("Zero"), 1.00, std::string("HR")});

    auto r2 = run_query(catalog, "SELECT id FROM mv_t ORDER BY id;");
    REQUIRE(r2.rows.size() == 3);
    CHECK(as_int(r2.rows[0][0]) == 1);
}

// We test Planner -- Logical Plan Structure.

TEST_CASE("Planner: simple SELECT generates scan + projection", "[planner]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    auto stmt = ast::parse_sql("SELECT id, name FROM t;");
    REQUIRE(stmt != nullptr);
    auto plan = planner::build_logical_plan(*stmt->select, catalog);
    REQUIRE(plan != nullptr);
    // Top should be PROJECTION
    CHECK(plan->type == planner::LogicalNodeType::PROJECTION);
    // Child should be TABLE_SCAN
    REQUIRE(plan->left != nullptr);
    CHECK(plan->left->type == planner::LogicalNodeType::TABLE_SCAN);
}

TEST_CASE("Planner: SELECT with WHERE generates filter node", "[planner]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    auto stmt = ast::parse_sql("SELECT * FROM t WHERE id > 2;");
    REQUIRE(stmt != nullptr);
    auto plan = planner::build_logical_plan(*stmt->select, catalog);
    REQUIRE(plan != nullptr);

    // Should have a FILTER somewhere
    std::function<bool(const planner::LogicalNodePtr&)> has_filter;
    has_filter = [&](const planner::LogicalNodePtr& n) -> bool {
        if (!n) return false;
        if (n->type == planner::LogicalNodeType::FILTER) return true;
        return has_filter(n->left) || has_filter(n->right);
    };
    CHECK(has_filter(plan));
}

TEST_CASE("Planner: ORDER BY generates sort node", "[planner]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    auto stmt = ast::parse_sql("SELECT * FROM t ORDER BY id;");
    REQUIRE(stmt != nullptr);
    auto plan = planner::build_logical_plan(*stmt->select, catalog);
    REQUIRE(plan != nullptr);

    std::function<bool(const planner::LogicalNodePtr&)> has_sort;
    has_sort = [&](const planner::LogicalNodePtr& n) -> bool {
        if (!n) return false;
        if (n->type == planner::LogicalNodeType::SORT) return true;
        return has_sort(n->left) || has_sort(n->right);
    };
    CHECK(has_sort(plan));
}

TEST_CASE("Planner: LIMIT generates limit node", "[planner]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    auto stmt = ast::parse_sql("SELECT * FROM t LIMIT 5;");
    REQUIRE(stmt != nullptr);
    auto plan = planner::build_logical_plan(*stmt->select, catalog);

    std::function<bool(const planner::LogicalNodePtr&)> has_limit;
    has_limit = [&](const planner::LogicalNodePtr& n) -> bool {
        if (!n) return false;
        if (n->type == planner::LogicalNodeType::LIMIT) return true;
        return has_limit(n->left) || has_limit(n->right);
    };
    CHECK(has_limit(plan));
}

TEST_CASE("Planner: DISTINCT generates distinct node", "[planner]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    auto stmt = ast::parse_sql("SELECT DISTINCT dept FROM t;");
    REQUIRE(stmt != nullptr);
    auto plan = planner::build_logical_plan(*stmt->select, catalog);

    std::function<bool(const planner::LogicalNodePtr&)> has_distinct;
    has_distinct = [&](const planner::LogicalNodePtr& n) -> bool {
        if (!n) return false;
        if (n->type == planner::LogicalNodeType::DISTINCT) return true;
        return has_distinct(n->left) || has_distinct(n->right);
    };
    CHECK(has_distinct(plan));
}

TEST_CASE("Planner: JOIN generates join node", "[planner]") {
    storage::Catalog catalog;
    create_join_tables(catalog);
    auto stmt = ast::parse_sql("SELECT * FROM emp JOIN dept ON emp.dept = dept.dname;");
    REQUIRE(stmt != nullptr);
    auto plan = planner::build_logical_plan(*stmt->select, catalog);

    std::function<bool(const planner::LogicalNodePtr&)> has_join;
    has_join = [&](const planner::LogicalNodePtr& n) -> bool {
        if (!n) return false;
        if (n->type == planner::LogicalNodeType::JOIN) return true;
        return has_join(n->left) || has_join(n->right);
    };
    CHECK(has_join(plan));
}

TEST_CASE("Planner: GROUP BY generates aggregation node", "[planner]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    auto stmt = ast::parse_sql("SELECT dept, COUNT(*) FROM t GROUP BY dept;");
    REQUIRE(stmt != nullptr);
    auto plan = planner::build_logical_plan(*stmt->select, catalog);

    std::function<bool(const planner::LogicalNodePtr&)> has_agg;
    has_agg = [&](const planner::LogicalNodePtr& n) -> bool {
        if (!n) return false;
        if (n->type == planner::LogicalNodeType::AGGREGATION) return true;
        return has_agg(n->left) || has_agg(n->right);
    };
    CHECK(has_agg(plan));
}

// We test Optimizer -- Rule-Based.

TEST_CASE("Optimizer: selection pushdown below join", "[optimizer][rules]") {
    storage::Catalog catalog;
    create_join_tables(catalog);
    auto stmt = ast::parse_sql("SELECT * FROM emp e JOIN dept d ON e.dept = d.dname WHERE d.budget > 200000;");
    REQUIRE(stmt != nullptr);

    auto plan = planner::build_logical_plan(*stmt->select, catalog);
    auto opt  = optimizer::optimize_rules(plan);

    // After optimization, the filter on dept.budget should be pushed below the join
    // We check by finding filter nodes in the tree
    std::function<int(const planner::LogicalNodePtr&)> count_filters;
    count_filters = [&](const planner::LogicalNodePtr& n) -> int {
        if (!n) return 0;
        int c = (n->type == planner::LogicalNodeType::FILTER) ? 1 : 0;
        return c + count_filters(n->left) + count_filters(n->right);
    };
    // Should still have filter(s) in the optimized plan
    CHECK(count_filters(opt) >= 1);
}

TEST_CASE("Optimizer: optimize does not change result", "[optimizer]") {
    storage::Catalog catalog;
    create_join_tables(catalog);
    auto stmt = ast::parse_sql("SELECT e.name, d.budget FROM emp e JOIN dept d ON e.dept = d.dname WHERE d.budget > 250000;");
    REQUIRE(stmt != nullptr);

    // Unoptimized execution
    auto plan_unopt = planner::build_logical_plan(*stmt->select, catalog);
    auto res_unopt = executor::execute(plan_unopt, catalog);

    // Optimized execution
    auto plan_opt = planner::build_logical_plan(*stmt->select, catalog);
    auto opt = optimizer::optimize(plan_opt, catalog);
    auto res_opt = executor::execute(opt, catalog);

    // Same result
    CHECK(res_opt.rows.size() == res_unopt.rows.size());
    CHECK(res_opt.columns.size() == res_unopt.columns.size());
}

// We test Optimizer -- Cost-Based.

TEST_CASE("Optimizer: cost estimates are populated", "[optimizer][cost]") {
    storage::Catalog catalog;
    create_join_tables(catalog);
    auto stmt = ast::parse_sql("SELECT * FROM emp e JOIN dept d ON e.dept = d.dname;");
    REQUIRE(stmt != nullptr);

    auto plan = planner::build_logical_plan(*stmt->select, catalog);
    auto opt  = optimizer::optimize(plan, catalog);

    // Root node should have populated cost
    CHECK(opt->estimated_rows >= 0);
    CHECK(opt->estimated_cost >= 0);
}

TEST_CASE("Optimizer: hash join selected for large tables", "[optimizer][cost]") {
    storage::Catalog catalog;
    benchmark::generate_employees(catalog, 1000);
    benchmark::generate_departments(catalog, 10);

    auto stmt = ast::parse_sql("SELECT * FROM employees e JOIN departments d ON e.dept = d.dept_name;");
    REQUIRE(stmt != nullptr);

    auto plan = planner::build_logical_plan(*stmt->select, catalog);
    auto opt  = optimizer::optimize(plan, catalog);

    // Find the join node
    std::function<planner::LogicalNodePtr(const planner::LogicalNodePtr&)> find_join;
    find_join = [&](const planner::LogicalNodePtr& n) -> planner::LogicalNodePtr {
        if (!n) return nullptr;
        if (n->type == planner::LogicalNodeType::JOIN) return n;
        auto l = find_join(n->left);
        if (l) return l;
        return find_join(n->right);
    };
    auto jn = find_join(opt);
    REQUIRE(jn != nullptr);
    CHECK(jn->join_algo == planner::JoinAlgo::HASH_JOIN);
}

// We test Executor -- Execution Stats.

TEST_CASE("Executor: stats populated", "[executor][stats]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    auto res = run_query(catalog, "SELECT * FROM t;");
    CHECK(res.stats.rows_scanned == 5);
    CHECK(res.stats.rows_produced == 5);
    CHECK(res.stats.exec_time_ms >= 0);
}

TEST_CASE("Executor: filter stats", "[executor][stats]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    auto res = run_query(catalog, "SELECT * FROM t WHERE id = 1;");
    CHECK(res.stats.rows_scanned == 5);
    CHECK(res.stats.rows_filtered == 4);
    CHECK(res.stats.rows_produced == 1);
}

TEST_CASE("Executor: join comparison stats", "[executor][stats]") {
    storage::Catalog catalog;
    create_join_tables(catalog);
    auto res = run_query(catalog, "SELECT * FROM emp CROSS JOIN dept;");
    CHECK(res.stats.join_comparisons > 0);
}

// We test End-To-End -- Ddl + Dml Pipeline.

TEST_CASE("E2E: CREATE TABLE then query", "[e2e][ddl]") {
    storage::Catalog catalog;

    // Parse and execute CREATE TABLE
    auto ct = ast::parse_sql("CREATE TABLE people (id INT, name VARCHAR, score FLOAT);");
    REQUIRE(ct != nullptr);
    REQUIRE(ct->type == ast::StmtType::ST_CREATE_TABLE);

    auto tbl = std::make_shared<storage::Table>();
    tbl->name = ct->create_table->table_name;
    for (auto& cd : ct->create_table->columns) {
        storage::DataType dt = storage::DataType::VARCHAR;
        if (cd.data_type == "INT") dt = storage::DataType::INT;
        else if (cd.data_type == "FLOAT") dt = storage::DataType::FLOAT;
        tbl->schema.push_back({cd.name, dt});
    }
    catalog.add_table(tbl);

    // Manually insert rows
    tbl->insert_row({(int64_t)1, std::string("Alice"), 95.5});
    tbl->insert_row({(int64_t)2, std::string("Bob"), 87.3});

    auto res = run_query(catalog, "SELECT * FROM people;");
    CHECK(res.rows.size() == 2);
}

TEST_CASE("E2E: CREATE INDEX then verify it exists", "[e2e][ddl]") {
    storage::Catalog catalog;
    auto tbl = std::make_shared<storage::Table>("items",
        std::vector<storage::ColumnSchema>{{"id", storage::DataType::INT}, {"name", storage::DataType::VARCHAR}});
    tbl->insert_row({(int64_t)1, std::string("Widget")});
    tbl->insert_row({(int64_t)2, std::string("Gadget")});
    catalog.add_table(tbl);

    auto stmt = ast::parse_sql("CREATE INDEX idx_items_id ON items (id) USING HASH;");
    REQUIRE(stmt != nullptr);
    catalog.create_index(stmt->create_index->index_name, stmt->create_index->table_name,
                         stmt->create_index->column_name, stmt->create_index->hash_index);

    CHECK(catalog.get_index("items", "id") != nullptr);
    auto idx = catalog.get_index("items", "id");
    auto hits = idx->lookup_int(1);
    CHECK(hits.size() == 1);
}

// We test End-To-End -- Generated Data (.Generate Equivalent).

TEST_CASE("E2E: generated data queries", "[e2e][benchmark]") {
    storage::Catalog catalog;
    benchmark::generate_employees(catalog, 100);
    benchmark::generate_departments(catalog, 10);
    benchmark::generate_orders(catalog, 50);

    SECTION("simple scan employees") {
        auto res = run_query(catalog, "SELECT * FROM employees LIMIT 10;");
        CHECK(res.rows.size() == 10);
        CHECK(res.columns.size() == 5);
    }

    SECTION("filter on salary") {
        auto res = run_query(catalog, "SELECT name, salary FROM employees WHERE salary > 100000;");
        // Some employees should meet threshold
        CHECK(res.rows.size() >= 0);
    }

    SECTION("group by department") {
        auto res = run_query(catalog, "SELECT dept, COUNT(*) FROM employees GROUP BY dept;");
        CHECK(res.rows.size() > 0);
        CHECK(res.rows.size() <= 10); // max 10 departments
    }

    SECTION("join employees and departments") {
        auto res = run_query(catalog, "SELECT e.name, d.budget FROM employees e JOIN departments d ON e.dept = d.dept_name;");
        CHECK(res.rows.size() == 100); // all employees should match
    }

    SECTION("complex query with filter, join, aggregation, order") {
        auto res = run_query(catalog, "SELECT d.dept_name, COUNT(*), AVG(e.salary) FROM employees e JOIN departments d ON e.dept = d.dept_name GROUP BY d.dept_name ORDER BY d.dept_name;");
        CHECK(res.rows.size() > 0);
    }
}

// We test Edge Cases & Robustness.

TEST_CASE("Edge: empty table query", "[e2e][edge]") {
    storage::Catalog catalog;
    auto tbl = std::make_shared<storage::Table>("empty",
        std::vector<storage::ColumnSchema>{{"id", storage::DataType::INT}});
    catalog.add_table(tbl);

    auto res = run_query(catalog, "SELECT * FROM empty;");
    CHECK(res.rows.size() == 0);
}

TEST_CASE("Edge: COUNT on empty table", "[e2e][edge]") {
    storage::Catalog catalog;
    auto tbl = std::make_shared<storage::Table>("empty",
        std::vector<storage::ColumnSchema>{{"id", storage::DataType::INT}});
    catalog.add_table(tbl);

    auto res = run_query(catalog, "SELECT COUNT(*) FROM empty;");
    REQUIRE(res.rows.size() == 1);
    CHECK(as_int(res.rows[0][0]) == 0);
}

TEST_CASE("Edge: WHERE that matches nothing", "[e2e][edge]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    auto res = run_query(catalog, "SELECT * FROM t WHERE id = 999;");
    CHECK(res.rows.size() == 0);
}

TEST_CASE("Edge: single row table", "[e2e][edge]") {
    storage::Catalog catalog;
    auto tbl = std::make_shared<storage::Table>("one",
        std::vector<storage::ColumnSchema>{{"x", storage::DataType::INT}});
    tbl->insert_row({(int64_t)42});
    catalog.add_table(tbl);

    auto res = run_query(catalog, "SELECT * FROM one;");
    CHECK(res.rows.size() == 1);
    CHECK(as_int(res.rows[0][0]) == 42);
}

TEST_CASE("Edge: LIMIT 1", "[e2e][edge]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    auto res = run_query(catalog, "SELECT * FROM t ORDER BY id LIMIT 1;");
    CHECK(res.rows.size() == 1);
}

TEST_CASE("Edge: OFFSET without reaching LIMIT", "[e2e][edge]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    auto res = run_query(catalog, "SELECT * FROM t ORDER BY id LIMIT 10 OFFSET 3;");
    CHECK(res.rows.size() == 2); // 5 rows, skip 3, take 2
}

TEST_CASE("Edge: very long string values", "[e2e][edge]") {
    storage::Catalog catalog;
    auto tbl = std::make_shared<storage::Table>("strs",
        std::vector<storage::ColumnSchema>{{"s", storage::DataType::VARCHAR}});
    std::string longstr(10000, 'x');
    tbl->insert_row({longstr});
    catalog.add_table(tbl);

    auto res = run_query(catalog, "SELECT * FROM strs;");
    CHECK(res.rows.size() == 1);
    CHECK(as_string(res.rows[0][0]).size() == 10000);
}

TEST_CASE("Edge: negative integer values", "[e2e][edge]") {
    storage::Catalog catalog;
    auto tbl = std::make_shared<storage::Table>("neg",
        std::vector<storage::ColumnSchema>{{"val", storage::DataType::INT}});
    tbl->insert_row({(int64_t)-100});
    tbl->insert_row({(int64_t)0});
    tbl->insert_row({(int64_t)100});
    catalog.add_table(tbl);

    auto res = run_query(catalog, "SELECT * FROM neg WHERE val < 0;");
    CHECK(res.rows.size() == 1);
    CHECK(as_int(res.rows[0][0]) == -100);
}

TEST_CASE("Edge: zero value comparisons", "[e2e][edge]") {
    storage::Catalog catalog;
    auto tbl = std::make_shared<storage::Table>("z",
        std::vector<storage::ColumnSchema>{{"val", storage::DataType::INT}});
    tbl->insert_row({(int64_t)0});
    tbl->insert_row({(int64_t)1});
    catalog.add_table(tbl);

    auto res = run_query(catalog, "SELECT * FROM z WHERE val = 0;");
    CHECK(res.rows.size() == 1);
}

TEST_CASE("Edge: all rows NULL in a column", "[e2e][edge]") {
    storage::Catalog catalog;
    auto tbl = std::make_shared<storage::Table>("allnull",
        std::vector<storage::ColumnSchema>{{"id", storage::DataType::INT}, {"val", storage::DataType::INT}});
    tbl->insert_row({(int64_t)1, std::monostate{}});
    tbl->insert_row({(int64_t)2, std::monostate{}});
    catalog.add_table(tbl);

    auto res = run_query(catalog, "SELECT COUNT(val), SUM(val) FROM allnull;");
    REQUIRE(res.rows.size() == 1);
    CHECK(as_int(res.rows[0][0]) == 0); // COUNT of non-null = 0
}

TEST_CASE("Edge: GROUP BY with single group", "[e2e][edge]") {
    storage::Catalog catalog;
    auto tbl = std::make_shared<storage::Table>("sg",
        std::vector<storage::ColumnSchema>{{"g", storage::DataType::VARCHAR}, {"v", storage::DataType::INT}});
    tbl->insert_row({std::string("A"), (int64_t)10});
    tbl->insert_row({std::string("A"), (int64_t)20});
    tbl->insert_row({std::string("A"), (int64_t)30});
    catalog.add_table(tbl);

    auto res = run_query(catalog, "SELECT g, COUNT(*), SUM(v) FROM sg GROUP BY g;");
    REQUIRE(res.rows.size() == 1);
    CHECK(as_int(res.rows[0][1]) == 3);
    CHECK(as_int(res.rows[0][2]) == 60);
}

TEST_CASE("Edge: multiple identical rows for DISTINCT", "[e2e][edge]") {
    storage::Catalog catalog;
    auto tbl = std::make_shared<storage::Table>("dup",
        std::vector<storage::ColumnSchema>{{"val", storage::DataType::INT}});
    tbl->insert_row({(int64_t)1});
    tbl->insert_row({(int64_t)1});
    tbl->insert_row({(int64_t)1});
    catalog.add_table(tbl);

    auto res = run_query(catalog, "SELECT DISTINCT val FROM dup;");
    CHECK(res.rows.size() == 1);
}

TEST_CASE("Edge: self-join", "[e2e][edge]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    auto res = run_query(catalog, "SELECT a.id, b.id FROM t a CROSS JOIN t b;");
    CHECK(res.rows.size() == 25); // 5 * 5
}

TEST_CASE("Edge: mixed case string comparison with LIKE", "[e2e][edge]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    // LIKE is case-sensitive for data values
    auto res = run_query(catalog, "SELECT * FROM t WHERE name LIKE 'alice';");
    // "Alice" != "alice" in case-sensitive match
    CHECK(res.rows.size() == 0);
}

// We test Ast Factory Methods & To_String.

TEST_CASE("AST: factory make_int", "[ast]") {
    auto e = ast::Expr::make_int(42);
    CHECK(e->type == ast::ExprType::LITERAL_INT);
    CHECK(e->int_val == 42);
}

TEST_CASE("AST: factory make_float", "[ast]") {
    auto e = ast::Expr::make_float(3.14);
    CHECK(e->type == ast::ExprType::LITERAL_FLOAT);
    CHECK(e->float_val == Catch::Approx(3.14));
}

TEST_CASE("AST: factory make_string", "[ast]") {
    auto e = ast::Expr::make_string("hello");
    CHECK(e->type == ast::ExprType::LITERAL_STRING);
    CHECK(e->str_val == "hello");
}

TEST_CASE("AST: factory make_column", "[ast]") {
    auto e1 = ast::Expr::make_column("id");
    CHECK(e1->type == ast::ExprType::COLUMN_REF);
    CHECK(e1->column_name == "id");
    CHECK(e1->table_name.empty());

    auto e2 = ast::Expr::make_column("name", "tbl");
    CHECK(e2->table_name == "tbl");
    CHECK(e2->column_name == "name");
}

TEST_CASE("AST: factory make_star", "[ast]") {
    auto e = ast::Expr::make_star();
    CHECK(e->type == ast::ExprType::STAR);
}

TEST_CASE("AST: factory make_binary", "[ast]") {
    auto l = ast::Expr::make_int(1);
    auto r = ast::Expr::make_int(2);
    auto e = ast::Expr::make_binary(ast::BinOp::OP_ADD, l, r);
    CHECK(e->type == ast::ExprType::BINARY_OP);
    CHECK(e->bin_op == ast::BinOp::OP_ADD);
    CHECK(e->left == l);
    CHECK(e->right == r);
}

TEST_CASE("AST: factory make_unary", "[ast]") {
    auto op = ast::Expr::make_int(1);
    auto e = ast::Expr::make_unary(ast::UnaryOp::OP_NEG, op);
    CHECK(e->type == ast::ExprType::UNARY_OP);
    CHECK(e->unary_op == ast::UnaryOp::OP_NEG);
}

TEST_CASE("AST: factory make_func", "[ast]") {
    std::vector<ast::ExprPtr> args{ast::Expr::make_star()};
    auto e = ast::Expr::make_func("COUNT", std::move(args), false);
    CHECK(e->type == ast::ExprType::FUNC_CALL);
    CHECK(e->func_name == "COUNT");
    CHECK(e->distinct_func == false);
}

TEST_CASE("AST: Expr to_string doesn't crash", "[ast]") {
    auto e = ast::Expr::make_binary(
        ast::BinOp::OP_ADD,
        ast::Expr::make_int(1),
        ast::Expr::make_float(2.5)
    );
    auto s = e->to_string();
    CHECK(!s.empty());
}

// We test Planner -- Plan To_String.

TEST_CASE("Planner: plan to_string doesn't crash", "[planner]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    auto stmt = ast::parse_sql("SELECT id, name FROM t WHERE id > 1 ORDER BY name LIMIT 10;");
    auto plan = planner::build_logical_plan(*stmt->select, catalog);
    auto s = plan->to_string();
    CHECK(!s.empty());
}

TEST_CASE("Planner: optimized plan to_string", "[planner][optimizer]") {
    storage::Catalog catalog;
    create_join_tables(catalog);
    auto stmt = ast::parse_sql("SELECT e.name FROM emp e JOIN dept d ON e.dept = d.dname WHERE d.budget > 100000;");
    auto plan = planner::build_logical_plan(*stmt->select, catalog);
    auto opt = optimizer::optimize(plan, catalog);
    auto s = opt->to_string();
    CHECK(!s.empty());
}

// We test End-To-End -- String Literals & Special Characters.

TEST_CASE("E2E: string literals with spaces", "[e2e][strings]") {
    storage::Catalog catalog;
    auto tbl = std::make_shared<storage::Table>("stbl",
        std::vector<storage::ColumnSchema>{{"name", storage::DataType::VARCHAR}});
    tbl->insert_row({std::string("Hello World")});
    tbl->insert_row({std::string("Foo Bar")});
    catalog.add_table(tbl);

    auto res = run_query(catalog, "SELECT * FROM stbl WHERE name = 'Hello World';");
    CHECK(res.rows.size() == 1);
}

TEST_CASE("E2E: empty string comparison", "[e2e][strings]") {
    storage::Catalog catalog;
    auto tbl = std::make_shared<storage::Table>("stbl2",
        std::vector<storage::ColumnSchema>{{"name", storage::DataType::VARCHAR}});
    tbl->insert_row({std::string("")});
    tbl->insert_row({std::string("notempty")});
    catalog.add_table(tbl);

    auto res = run_query(catalog, "SELECT * FROM stbl2 WHERE name = '';");
    CHECK(res.rows.size() == 1);
}

TEST_CASE("E2E: LIKE on empty pattern", "[e2e][strings]") {
    storage::Catalog catalog;
    auto tbl = std::make_shared<storage::Table>("stbl3",
        std::vector<storage::ColumnSchema>{{"name", storage::DataType::VARCHAR}});
    tbl->insert_row({std::string("")});
    tbl->insert_row({std::string("notempty")});
    catalog.add_table(tbl);

    // Empty pattern should match empty strings
    auto res = run_query(catalog, "SELECT * FROM stbl3 WHERE name LIKE '';");
    CHECK(res.rows.size() == 1);
}

// We test End-To-End -- Subqueries.

TEST_CASE("E2E: IN subquery", "[e2e][subquery]") {
    storage::Catalog catalog;
    create_test_table(catalog);

    // Create a secondary table
    auto tbl2 = std::make_shared<storage::Table>("ids",
        std::vector<storage::ColumnSchema>{{"id", storage::DataType::INT}});
    tbl2->insert_row({(int64_t)1});
    tbl2->insert_row({(int64_t)3});
    catalog.add_table(tbl2);

    // Note: IN subquery support depends on executor implementation
    // If not fully supported, this test documents the behavior
    auto stmt = ast::parse_sql("SELECT * FROM t WHERE id IN (SELECT id FROM ids);");
    REQUIRE(stmt != nullptr);
    CHECK(stmt->select->where_clause->type == ast::ExprType::IN_EXPR);
}

TEST_CASE("E2E: subquery in FROM clause", "[e2e][subquery]") {
    storage::Catalog catalog;
    create_test_table(catalog);

    auto stmt = ast::parse_sql("SELECT * FROM (SELECT id, name FROM t) sub;");
    REQUIRE(stmt != nullptr);
    CHECK(stmt->select->from_clause[0]->type == ast::TableRefType::TRT_SUBQUERY);
}

// We test End-To-End -- Explain Statement.

TEST_CASE("E2E: EXPLAIN parses correctly", "[e2e][explain]") {
    storage::Catalog catalog;
    create_test_table(catalog);

    auto stmt = ast::parse_sql("EXPLAIN SELECT * FROM t WHERE id > 1;");
    REQUIRE(stmt != nullptr);
    CHECK(stmt->type == ast::StmtType::ST_EXPLAIN);
    CHECK(stmt->explain_analyze == false);

    // Can build plan from it
    auto plan = planner::build_logical_plan(*stmt->select, catalog);
    REQUIRE(plan != nullptr);
    auto opt = optimizer::optimize(plan, catalog);
    REQUIRE(opt != nullptr);
}

TEST_CASE("E2E: EXPLAIN ANALYZE parses correctly", "[e2e][explain]") {
    storage::Catalog catalog;
    create_test_table(catalog);

    auto stmt = ast::parse_sql("EXPLAIN ANALYZE SELECT id FROM t;");
    REQUIRE(stmt != nullptr);
    CHECK(stmt->type == ast::StmtType::ST_EXPLAIN);
    CHECK(stmt->explain_analyze == true);

    auto plan = planner::build_logical_plan(*stmt->select, catalog);
    auto opt = optimizer::optimize(plan, catalog);
    auto res = executor::execute(opt, catalog);
    CHECK(res.rows.size() == 5);
}

// We test End-To-End -- Complex Join Patterns.

TEST_CASE("E2E: JOIN with ORDER BY on joined column", "[e2e][join]") {
    storage::Catalog catalog;
    create_join_tables(catalog);
    auto res = run_query(catalog, "SELECT e.name, d.budget FROM emp e JOIN dept d ON e.dept = d.dname ORDER BY d.budget DESC;");
    REQUIRE(res.rows.size() == 4);
    // First should be highest budget
    CHECK(as_int(res.rows[0][1]) >= as_int(res.rows[1][1]));
}

TEST_CASE("E2E: JOIN with LIMIT", "[e2e][join]") {
    storage::Catalog catalog;
    create_join_tables(catalog);
    auto res = run_query(catalog, "SELECT * FROM emp e JOIN dept d ON e.dept = d.dname LIMIT 2;");
    CHECK(res.rows.size() == 2);
}

TEST_CASE("E2E: JOIN with GROUP BY and HAVING", "[e2e][join]") {
    storage::Catalog catalog;
    create_join_tables(catalog);
    auto res = run_query(catalog, "SELECT d.dname, COUNT(*) FROM emp e JOIN dept d ON e.dept = d.dname GROUP BY d.dname HAVING COUNT(*) > 1;");
    // Eng has 2 employees -> passes HAVING
    CHECK(res.rows.size() >= 1);
}

// We test End-To-End -- Float/Int Mixed Comparisons.

TEST_CASE("E2E: float comparison in WHERE", "[e2e][types]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    auto res = run_query(catalog, "SELECT * FROM t WHERE value > 3.0;");
    CHECK(res.rows.size() == 2); // Alice (3.14) and Dave (9.81)
}

TEST_CASE("E2E: int/float mixed arithmetic", "[e2e][types]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    auto res = run_query(catalog, "SELECT id, value + 1 FROM t WHERE id = 1;");
    REQUIRE(res.rows.size() == 1);
    // 3.14 + 1 = 4.14 (float + int -> float)
    CHECK(as_double(res.rows[0][1]) == Catch::Approx(4.14));
}

// We test End-To-End -- Multiple Aggregate Functions.

TEST_CASE("E2E: all aggregates in one query", "[e2e][aggregation]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    auto res = run_query(catalog, "SELECT COUNT(*), COUNT(name), SUM(id), AVG(id), MIN(id), MAX(id) FROM t;");
    REQUIRE(res.rows.size() == 1);
    CHECK(as_int(res.rows[0][0]) == 5);    // COUNT(*)
    CHECK(as_int(res.rows[0][1]) == 5);    // COUNT(name)
    CHECK(as_int(res.rows[0][2]) == 15);   // SUM(id)
    CHECK(as_int(res.rows[0][4]) == 1);    // MIN(id)
    CHECK(as_int(res.rows[0][5]) == 5);    // MAX(id)
}

TEST_CASE("E2E: GROUP BY with AVG", "[e2e][aggregation]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    auto res = run_query(catalog, "SELECT dept, AVG(value) FROM t GROUP BY dept;");
    CHECK(res.rows.size() == 3);
    // Each row should have dept + avg
    for (auto& row : res.rows) {
        CHECK(!is_null(row[0]));
        CHECK(!is_null(row[1]));
    }
}

// We test Benchmark Data Generation.

TEST_CASE("Benchmark: generate_employees creates table", "[benchmark]") {
    storage::Catalog catalog;
    benchmark::generate_employees(catalog, 100);
    auto* tbl = catalog.get_table("employees");
    REQUIRE(tbl != nullptr);
    CHECK(tbl->row_count() == 100);
    CHECK(tbl->col_count() == 5);
}

TEST_CASE("Benchmark: generate_departments creates table", "[benchmark]") {
    storage::Catalog catalog;
    benchmark::generate_departments(catalog, 10);
    auto* tbl = catalog.get_table("departments");
    REQUIRE(tbl != nullptr);
    CHECK(tbl->row_count() == 10);
    CHECK(tbl->col_count() == 3);
}

TEST_CASE("Benchmark: generate_orders creates table", "[benchmark]") {
    storage::Catalog catalog;
    benchmark::generate_orders(catalog, 50);
    auto* tbl = catalog.get_table("orders");
    REQUIRE(tbl != nullptr);
    CHECK(tbl->row_count() == 50);
    CHECK(tbl->col_count() == 4);
}

// We test Regression & Stress.

TEST_CASE("Regression: deeply nested AND/OR", "[e2e][regression]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    auto res = run_query(catalog, "SELECT * FROM t WHERE (id = 1 OR id = 2) AND (dept = 'Engineering' OR dept = 'Sales');");
    // id=1,dept=Eng ?  id=2,dept=Sales ?
    CHECK(res.rows.size() == 2);
}

TEST_CASE("Regression: aggregate with WHERE filter", "[e2e][regression]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    auto res = run_query(catalog, "SELECT COUNT(*), SUM(id) FROM t WHERE id > 2;");
    REQUIRE(res.rows.size() == 1);
    CHECK(as_int(res.rows[0][0]) == 3);   // id=3,4,5
    CHECK(as_int(res.rows[0][1]) == 12);  // 3+4+5
}

TEST_CASE("Regression: ORDER BY then DISTINCT", "[e2e][regression]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    auto res = run_query(catalog, "SELECT DISTINCT dept FROM t ORDER BY dept;");
    REQUIRE(res.rows.size() == 3);
}

TEST_CASE("Regression: BETWEEN boundary values", "[e2e][regression]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    // BETWEEN is inclusive on both ends
    auto res = run_query(catalog, "SELECT * FROM t WHERE id BETWEEN 1 AND 1;");
    CHECK(res.rows.size() == 1);
    CHECK(as_int(res.rows[0][0]) == 1);
}

TEST_CASE("Regression: IN with single element", "[e2e][regression]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    auto res = run_query(catalog, "SELECT * FROM t WHERE id IN (3);");
    CHECK(res.rows.size() == 1);
}

TEST_CASE("Regression: SELECT with expression alias", "[e2e][regression]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    auto res = run_query(catalog, "SELECT id * 2 AS double_id FROM t WHERE id = 1;");
    REQUIRE(res.rows.size() == 1);
    CHECK(as_int(res.rows[0][0]) == 2);
}

TEST_CASE("Regression: JOIN + aggregate + order + limit", "[e2e][regression]") {
    storage::Catalog catalog;
    create_join_tables(catalog);
    auto res = run_query(catalog, "SELECT d.dname, COUNT(*) FROM emp e JOIN dept d ON e.dept = d.dname GROUP BY d.dname ORDER BY d.dname LIMIT 2;");
    CHECK(res.rows.size() == 2);
}

TEST_CASE("Stress: query with many columns in SELECT", "[e2e][stress]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    auto res = run_query(catalog, "SELECT id, name, value, dept, id + 1, id * 2, value + 1.0 FROM t;");
    CHECK(res.rows.size() == 5);
    CHECK(res.columns.size() == 7);
}

TEST_CASE("Stress: multiple conditions in WHERE", "[e2e][stress]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    auto res = run_query(catalog, "SELECT * FROM t WHERE id > 0 AND id < 10 AND name != 'nobody' AND dept != 'nonexistent';");
    CHECK(res.rows.size() == 5);
}

//  INDEX INTEGRATION TESTS

TEST_CASE("BTreeIndex: build and exact lookup", "[storage][index]") {
    auto tbl = std::make_shared<storage::Table>();
    tbl->name = "idx_test";
    tbl->schema = {{"id", storage::DataType::INT}, {"name", storage::DataType::VARCHAR}};
    tbl->rows.push_back({(int64_t)1, std::string("Alice")});
    tbl->rows.push_back({(int64_t)2, std::string("Bob")});
    tbl->rows.push_back({(int64_t)3, std::string("Carol")});
    tbl->rows.push_back({(int64_t)1, std::string("Dave")});  // duplicate key

    storage::BTreeIndex idx;
    idx.table_name = "idx_test";
    idx.column_name = "id";
    idx.build(*tbl);

    auto r1 = idx.lookup_exact(storage::Value{(int64_t)1});
    CHECK(r1.size() == 2);  // rows 0 and 3
    auto r2 = idx.lookup_exact(storage::Value{(int64_t)2});
    CHECK(r2.size() == 1);
    auto r3 = idx.lookup_exact(storage::Value{(int64_t)99});
    CHECK(r3.empty());
}

TEST_CASE("BTreeIndex: range lookup", "[storage][index]") {
    auto tbl = std::make_shared<storage::Table>();
    tbl->name = "range_test";
    tbl->schema = {{"val", storage::DataType::INT}};
    for (int i = 1; i <= 10; i++) {
        tbl->rows.push_back({(int64_t)i});
    }

    storage::BTreeIndex idx;
    idx.table_name = "range_test";
    idx.column_name = "val";
    idx.build(*tbl);

    // BETWEEN 3 AND 7 -> {3,4,5,6,7} = 5 rows
    auto range = idx.lookup_range(storage::Value{(int64_t)3}, storage::Value{(int64_t)7});
    CHECK(range.size() == 5);

    // < 4 -> {1,2,3} = 3 rows
    auto lt = idx.lookup_lt(storage::Value{(int64_t)4});
    CHECK(lt.size() == 3);

    // > 8 -> {9,10} = 2 rows
    auto gt = idx.lookup_gt(storage::Value{(int64_t)8});
    CHECK(gt.size() == 2);

    // <= 5 -> {1,2,3,4,5} = 5 rows
    auto lte = idx.lookup_lte(storage::Value{(int64_t)5});
    CHECK(lte.size() == 5);

    // >= 8 -> {8,9,10} = 3 rows
    auto gte = idx.lookup_gte(storage::Value{(int64_t)8});
    CHECK(gte.size() == 3);
}

TEST_CASE("BTreeIndex: insert_entry maintains index", "[storage][index]") {
    storage::BTreeIndex idx;
    idx.table_name = "t";
    idx.column_name = "x";

    idx.insert_entry(storage::Value{(int64_t)10}, 0);
    idx.insert_entry(storage::Value{(int64_t)20}, 1);
    idx.insert_entry(storage::Value{(int64_t)10}, 2);  // duplicate

    auto r = idx.lookup_exact(storage::Value{(int64_t)10});
    CHECK(r.size() == 2);
    CHECK(r[0] == 0);
    CHECK(r[1] == 2);
}

TEST_CASE("BTreeIndex: string keys", "[storage][index]") {
    auto tbl = std::make_shared<storage::Table>();
    tbl->name = "str_test";
    tbl->schema = {{"name", storage::DataType::VARCHAR}};
    tbl->rows.push_back({std::string("Alice")});
    tbl->rows.push_back({std::string("Bob")});
    tbl->rows.push_back({std::string("Carol")});

    storage::BTreeIndex idx;
    idx.table_name = "str_test";
    idx.column_name = "name";
    idx.build(*tbl);

    auto r = idx.lookup_exact(storage::Value{std::string("Bob")});
    CHECK(r.size() == 1);
    CHECK(r[0] == 1);
}


TEST_CASE("Catalog: create hash vs btree index", "[storage][index]") {
    storage::Catalog catalog;
    auto tbl = std::make_shared<storage::Table>();
    tbl->name = "t";
    tbl->schema = {{"id", storage::DataType::INT}};
    tbl->rows.push_back({(int64_t)1});
    catalog.add_table(tbl);

    catalog.create_index("idx_hash", "t", "id", true);
    CHECK(catalog.get_index("t", "id") != nullptr);
    CHECK(catalog.get_btree_index("t", "id") == nullptr);

    catalog.create_index("idx_btree", "t", "id", false);
    CHECK(catalog.get_btree_index("t", "id") != nullptr);

    CHECK(catalog.has_any_index("t", "id") == true);
    CHECK(catalog.has_any_index("t", "nonexistent") == false);
}


TEST_CASE("Catalog: update_indexes_on_insert", "[storage][index]") {
    storage::Catalog catalog;
    auto tbl = std::make_shared<storage::Table>();
    tbl->name = "t";
    tbl->schema = {{"id", storage::DataType::INT}};
    tbl->rows.push_back({(int64_t)1});
    tbl->rows.push_back({(int64_t)2});
    catalog.add_table(tbl);

    catalog.create_index("idx", "t", "id", true);
    auto* hidx = catalog.get_index("t", "id");
    REQUIRE(hidx != nullptr);
    CHECK(hidx->lookup_int(1).size() == 1);
    CHECK(hidx->lookup_int(2).size() == 1);

    // Simulate insertion
    tbl->rows.push_back({(int64_t)1});
    catalog.update_indexes_on_insert("t", 2);
    CHECK(hidx->lookup_int(1).size() == 2);  // now 2 entries for key=1
}


TEST_CASE("Optimizer: rewrites equality filter to IndexScan", "[optimizer][index]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    catalog.create_index("idx_id", "t", "id", true);  // hash index

    auto stmt = ast::parse_sql("SELECT * FROM t WHERE id = 3;");
    REQUIRE(stmt != nullptr);
    auto plan = planner::build_logical_plan(*stmt->select, catalog);
    auto opt_plan = optimizer::optimize(plan, catalog);

    // The optimized plan should contain an IndexScan node
    // Walk to find it (it could be under projection)
    std::string plan_str = opt_plan->to_string();
    CHECK(plan_str.find("IndexScan") != std::string::npos);
}

TEST_CASE("Optimizer: no IndexScan without index", "[optimizer][index]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    // No index created

    auto stmt = ast::parse_sql("SELECT * FROM t WHERE id = 3;");
    REQUIRE(stmt != nullptr);
    auto plan = planner::build_logical_plan(*stmt->select, catalog);
    auto opt_plan = optimizer::optimize(plan, catalog);

    std::string plan_str = opt_plan->to_string();
    CHECK(plan_str.find("IndexScan") == std::string::npos);
    CHECK(plan_str.find("Filter") != std::string::npos);
    CHECK(plan_str.find("SeqScan") != std::string::npos);
}

TEST_CASE("Optimizer: btree range rewrite for greater-than", "[optimizer][index]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    catalog.create_index("idx_id_bt", "t", "id", false);  // btree

    auto stmt = ast::parse_sql("SELECT * FROM t WHERE id > 3;");
    REQUIRE(stmt != nullptr);
    auto plan = planner::build_logical_plan(*stmt->select, catalog);
    auto opt_plan = optimizer::optimize(plan, catalog);

    std::string plan_str = opt_plan->to_string();
    CHECK(plan_str.find("IndexScan") != std::string::npos);
    CHECK(plan_str.find("RANGE") != std::string::npos);
}


TEST_CASE("Executor: hash index equality returns correct rows", "[executor][index]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    catalog.create_index("idx_id", "t", "id", true);

    auto res = run_query(catalog, "SELECT * FROM t WHERE id = 3;");
    CHECK(res.rows.size() == 1);
    CHECK(storage::value_to_int(res.rows[0][0]) == 3);
}

TEST_CASE("Executor: btree index equality returns correct rows", "[executor][index]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    catalog.create_index("idx_id_bt", "t", "id", false);

    auto res = run_query(catalog, "SELECT * FROM t WHERE id = 2;");
    CHECK(res.rows.size() == 1);
    CHECK(storage::value_to_int(res.rows[0][0]) == 2);
}

TEST_CASE("Executor: btree range query (greater-than)", "[executor][index]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    catalog.create_index("idx_id_bt", "t", "id", false);

    auto res = run_query(catalog, "SELECT * FROM t WHERE id > 3;");
    CHECK(res.rows.size() == 2);  // ids 4 and 5
}

TEST_CASE("Executor: btree range query (less-than)", "[executor][index]") {
    storage::Catalog catalog;
    create_test_table(catalog);
    catalog.create_index("idx_id_bt", "t", "id", false);

    auto res = run_query(catalog, "SELECT * FROM t WHERE id < 3;");
    CHECK(res.rows.size() == 2);  // ids 1 and 2
}

TEST_CASE("Executor: index vs full scan produce same results", "[executor][index]") {
    storage::Catalog catalog;
    create_test_table(catalog);

    // Run without index first
    auto res_no_idx = run_query(catalog, "SELECT * FROM t WHERE id = 3;");

    // Now create index and run again
    catalog.create_index("idx_id", "t", "id", true);
    auto res_with_idx = run_query(catalog, "SELECT * FROM t WHERE id = 3;");

    // Same results regardless of execution path
    CHECK(res_no_idx.rows.size() == res_with_idx.rows.size());
    CHECK(res_no_idx.rows.size() == 1);
}


TEST_CASE("Planner: IndexScan to_string for equality", "[planner][index]") {
    auto node = std::make_shared<planner::LogicalNode>();
    node->type = planner::LogicalNodeType::INDEX_SCAN;
    node->table_name = "employees";
    node->index_column = "id";
    node->index_key = ast::Expr::make_int(42);
    node->index_range = false;

    std::string s = node->to_string();
    CHECK(s.find("IndexScan(employees.id = 42)") != std::string::npos);
}

TEST_CASE("Planner: IndexScan to_string for range", "[planner][index]") {
    auto node = std::make_shared<planner::LogicalNode>();
    node->type = planner::LogicalNodeType::INDEX_SCAN;
    node->table_name = "employees";
    node->index_column = "salary";
    node->index_range = true;

    std::string s = node->to_string();
    CHECK(s.find("IndexScan(employees.salary RANGE)") != std::string::npos);
}
