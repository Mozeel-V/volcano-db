#ifndef PARSER_TYPES_H
#define PARSER_TYPES_H

#include "ast/ast.h"

/* Dynamic array helpers used by Bison union - must be visible in the
   generated header so the lexer can include it. */
struct RawExprList   { ast::Expr** items; int count; int cap; };
struct RawTRefList   { ast::TableRef** items; int count; int cap; };
struct RawOrderList  { int count; int cap; ast::Expr** exprs; int* ascs; };
struct RawColDefList {
    int count; int cap;
    char** names; char** types;
    int* not_null;
    int* primary_key;
    int* is_unique;
    ast::Expr** defaults;
    ast::Expr** checks;
    char** fk_tables;
    char** fk_columns;
    int* fk_on_delete;
};

/* INSERT rows: array of RawExprList (each is one row of values) */
struct RawRowList    { RawExprList* rows; int count; int cap; };

/* UPDATE SET assignments: parallel arrays of column names and value exprs */
struct RawAssignList { char** cols; ast::Expr** vals; int count; int cap; };

/* CREATE FUNCTION parameters: parallel arrays of names and data types */
struct RawFuncParamList { char** names; char** types; int count; int cap; };

/* Window spec for function OVER (...) clauses */
struct RawWindowSpec {
    int has_over;
    RawExprList partition_by;
    RawOrderList order_by;
};

/* Column constraint flags - accumulated during parsing */
struct RawConstraints {
    int not_null;
    int primary_key;
    int is_unique;
    ast::Expr* default_val;
    ast::Expr* check_expr;
    char* fk_table;
    char* fk_column;
    int fk_on_delete;
};

/* List of string literals (for multi-statement trigger bodies) */
struct RawStrList { char** items; int count; int cap; };

/* CASE arms: parallel WHEN condition/value and THEN result expressions */
struct RawCaseList {
    ast::Expr** whens;
    ast::Expr** thens;
    int count;
    int cap;
};

/* Procedural statement lists (BEGIN...END, WHILE...END WHILE) */
struct RawStmtList {
    ast::Statement** items;
    int count;
    int cap;
};

#endif // PARSER_TYPES_H
