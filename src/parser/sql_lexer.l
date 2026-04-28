%{
#include <string>
#include <cstdlib>
#include "ast/ast.h"
#include "parser/parser_types.h"
#include "sql_parser.tab.h"

/* Avoid unused function warnings */
#define YY_NO_INPUT
#define YY_NO_UNPUT

extern int yylineno;
%}

%option noyywrap
%option case-insensitive
%option yylineno

%%

[ \t\r]+            { /* skip whitespace */ }
\n                  { /* skip newlines */ }
"--"[^\n]*          { /* single-line comment */ }
"/*"([^*]|\*+[^*/])*\*+"/"  { /* multi-line comment */ }

"SELECT"            { return SELECT; }
"DISTINCT"          { return DISTINCT; }
"FROM"              { return FROM; }
"WHERE"             { return WHERE; }
"AND"               { return AND; }
"OR"                { return OR; }
"NOT"               { return NOT; }
"IN"                { return IN; }
"EXISTS"            { return EXISTS; }
"BETWEEN"           { return BETWEEN; }
"LIKE"              { return LIKE; }
"IS"                { return IS; }
"NULL"              { return NULL_KW; }
"AS"                { return AS; }
"ON"                { return ON; }
"JOIN"              { return JOIN; }
"INNER"             { return INNER; }
"LEFT"              { return LEFT; }
"RIGHT"             { return RIGHT; }
"FULL"              { return FULL; }
"OUTER"             { return OUTER; }
"CROSS"             { return CROSS; }
"GROUP"             { return GROUP; }
"BY"                { return BY; }
"HAVING"            { return HAVING; }
"ORDER"             { return ORDER; }
"ASC"               { return ASC; }
"DESC"              { return DESC; }
"LIMIT"             { return LIMIT; }
"OFFSET"            { return OFFSET; }
"UNION"             { return UNION; }
"INTERSECT"         { return INTERSECT; }
"EXCEPT"            { return EXCEPT; }
"ALL"               { return ALL; }
"SOME"              { return SOME; }
"ANY"               { return ANY; }
"WITH"              { return WITH; }
"OVER"              { return OVER; }
"PARTITION"         { return PARTITION; }
"CREATE"            { return CREATE; }
"USER"              { return USER; }
"IDENTIFIED"        { return IDENTIFIED; }
"GRANT"             { return GRANT; }
"REVOKE"            { return REVOKE; }
"TABLE"             { return TABLE; }
"VIEW"              { return VIEW; }
"MATERIALIZED"      { return MATERIALIZED; }
"FUNCTION"          { return FUNCTION; }
"RETURNS"           { return RETURNS; }
"INDEX"             { return INDEX; }
"USING"             { return USING; }
"HASH"              { return HASH; }
"BTREE"             { return BTREE; }
"INSERT"            { return INSERT; }
"INTO"              { return INTO; }
"VALUES"            { return VALUES; }
"UPDATE"            { return UPDATE; }
"SET"               { return SET; }
"DELETE"            { return DELETE_KW; }
"LOAD"              { return LOAD; }
"EXPLAIN"           { return EXPLAIN; }
"ANALYZE"           { return ANALYZE; }
"BENCHMARK"         { return BENCHMARK_KW; }
"COMMIT"            { return COMMIT_KW; }
"ROLLBACK"          { return ROLLBACK_KW; }
"TRANSACTION"       { return TRANSACTION_KW; }
"COUNT"             { return COUNT; }
"SUM"               { return SUM; }
"AVG"               { return AVG; }
"MIN"               { return MIN; }
"MAX"               { return MAX; }
"INT"               { return TYPE_INT; }
"INTEGER"           { return TYPE_INT; }
"FLOAT"             { return TYPE_FLOAT; }
"DOUBLE"            { return TYPE_FLOAT; }
"VARCHAR"           { return TYPE_VARCHAR; }
"TEXT"              { return TYPE_VARCHAR; }
"CASE"              { return CASE; }
"WHEN"              { return WHEN; }
"THEN"              { return THEN; }
"ELSE"              { return ELSE; }
"END"               { return END; }
"TRUE"              { return TRUE_KW; }
"FALSE"             { return FALSE_KW; }
"ALTER"             { return ALTER; }
"ADD"               { return ADD; }
"COLUMN"            { return COLUMN; }
"RENAME"            { return RENAME; }
"TO"                { return TO; }
"DROP"              { return DROP; }
"TRUNCATE"          { return TRUNCATE_KW; }
"MERGE"             { return MERGE; }
"MATCHED"           { return MATCHED; }
"FORMAT"            { return FORMAT; }
"DOT"               { return DOT; }
"TRIGGER"           { return TRIGGER; }
"BEFORE"            { return BEFORE; }
"AFTER"             { return AFTER; }
"FOR"               { return FOR; }
"EACH"              { return EACH; }
"ROW"               { return ROW_KW; }
"EXECUTE"           { return EXECUTE; }
"DEFAULT"           { return DEFAULT; }
"PRIMARY"           { return PRIMARY; }
"KEY"               { return KEY; }
"UNIQUE"            { return UNIQUE; }
"CHECK"             { return CHECK_KW; }
"REFERENCES"        { return REFERENCES; }
"CASCADE"           { return CASCADE; }
"RESTRICT"          { return RESTRICT; }
"BEGIN"             { return BEGIN_KW; }
"WHILE"             { return WHILE; }
"DO"                { return DO; }
"LEAVE"             { return LEAVE; }
"ITERATE"           { return ITERATE; }

"<="                { return LEQ; }
">="                { return GEQ; }
"!="                { return NEQ; }
"<>"                { return NEQ; }
"="                 { return '='; }
"<"                 { return '<'; }
">"                 { return '>'; }
"+"                 { return '+'; }
"-"                 { return '-'; }
"*"                 { return '*'; }
"/"                 { return '/'; }
"%"                 { return '%'; }
"("                 { return '('; }
")"                 { return ')'; }
","                 { return ','; }
";"                 { return ';'; }
"."                 { return '.'; }
":"                 { return ':'; }

[0-9]+\.[0-9]+      {
    yylval.float_val = atof(yytext);
    return FLOAT_LITERAL;
}

[0-9]+              {
    yylval.int_val = atoll(yytext);
    return INT_LITERAL;
}

'([^'\\]|\\.)*'     {
    /* String literal: strip quotes */
    std::string s(yytext + 1, strlen(yytext) - 2);
    yylval.str_val = strdup(s.c_str());
    return STRING_LITERAL;
}

[a-zA-Z_][a-zA-Z0-9_]* {
    yylval.str_val = strdup(yytext);
    return IDENTIFIER;
}

.                   {
    fprintf(stderr, "Lexer error: unrecognized character '%s' at line %d\n", yytext, yylineno);
}

%%
