#ifndef PARSER_TYPES_H
#define PARSER_TYPES_H

#include "ast/ast.h"

/* Dynamic array helpers used by Bison union – must be visible in the
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
    char** fk_tables;    // REFERENCES table (nullptr if none)
    char** fk_columns;   // REFERENCES column (nullptr if none)
};

/* INSERT rows: array of RawExprList (each is one row of values) */
struct RawRowList    { RawExprList* rows; int count; int cap; };

/* UPDATE SET assignments: parallel arrays of column names and value exprs */
struct RawAssignList { char** cols; ast::Expr** vals; int count; int cap; };

/* Column constraint flags – accumulated during parsing */
struct RawConstraints {
    int not_null;
    int primary_key;
    int is_unique;
    ast::Expr* default_val;
    ast::Expr* check_expr;
    char* fk_table;      // REFERENCES table (nullptr if none)
    char* fk_column;     // REFERENCES column (nullptr if none)
};

#endif // PARSER_TYPES_H
