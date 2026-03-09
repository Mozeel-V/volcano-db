#ifndef PARSER_TYPES_H
#define PARSER_TYPES_H

#include "ast/ast.h"

/* Dynamic array helpers used by Bison union – must be visible in the
   generated header so the lexer can include it. */
struct RawExprList   { ast::Expr** items; int count; int cap; };
struct RawTRefList   { ast::TableRef** items; int count; int cap; };
struct RawOrderList  { int count; int cap; ast::Expr** exprs; int* ascs; };
struct RawColDefList { int count; int cap; char** names; char** types; };

#endif // PARSER_TYPES_H
