%{
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include <memory>
#include "ast/ast.h"

extern int yylex();
extern int yylineno;
extern char* yytext;
void yyerror(const char* s);

/* Parse result */
static ast::StmtPtr g_parsed_stmt;
ast::StmtPtr get_parsed_stmt() { return g_parsed_stmt; }

using namespace ast;
static std::string take_str(char* s) { std::string r(s); free(s); return r; }

#include "parser/parser_types.h"

static RawExprList make_elist() { RawExprList l = {nullptr,0,0}; return l; }
static RawTRefList make_tlist() { RawTRefList l = {nullptr,0,0}; return l; }
static RawOrderList make_olist(){ RawOrderList l = {0,0,nullptr,nullptr}; return l; }
static RawColDefList make_cdlist(){ RawColDefList l = {0,0,nullptr,nullptr,nullptr,nullptr,nullptr,nullptr,nullptr,nullptr,nullptr}; return l; }

static RawStrList make_slist() { RawStrList l = {nullptr,0,0}; return l; }
static void slist_push(RawStrList& l, char* s) {
    if (l.count >= l.cap) { l.cap = l.cap ? l.cap*2 : 4; l.items = (char**)realloc(l.items, l.cap*sizeof(char*)); }
    l.items[l.count++] = s;
}

static void elist_push(RawExprList& l, Expr* e) {
    if (l.count >= l.cap) { l.cap = l.cap ? l.cap*2 : 4; l.items = (Expr**)realloc(l.items, l.cap*sizeof(Expr*)); }
    l.items[l.count++] = e;
}
static void tlist_push(RawTRefList& l, TableRef* t) {
    if (l.count >= l.cap) { l.cap = l.cap ? l.cap*2 : 4; l.items = (TableRef**)realloc(l.items, l.cap*sizeof(TableRef*)); }
    l.items[l.count++] = t;
}
static void olist_push(RawOrderList& l, Expr* e, int asc) {
    if (l.count >= l.cap) { l.cap = l.cap ? l.cap*2 : 4; l.exprs = (Expr**)realloc(l.exprs, l.cap*sizeof(Expr*)); l.ascs = (int*)realloc(l.ascs, l.cap*sizeof(int)); }
    l.exprs[l.count] = e; l.ascs[l.count] = asc; l.count++;
}
static void cdlist_push(RawColDefList& l, char* n, char* t, RawConstraints c) {
    if (l.count >= l.cap) {
        l.cap = l.cap ? l.cap*2 : 4;
        l.names = (char**)realloc(l.names, l.cap*sizeof(char*));
        l.types = (char**)realloc(l.types, l.cap*sizeof(char*));
        l.not_null = (int*)realloc(l.not_null, l.cap*sizeof(int));
        l.primary_key = (int*)realloc(l.primary_key, l.cap*sizeof(int));
        l.is_unique = (int*)realloc(l.is_unique, l.cap*sizeof(int));
        l.defaults = (ast::Expr**)realloc(l.defaults, l.cap*sizeof(ast::Expr*));
        l.checks = (ast::Expr**)realloc(l.checks, l.cap*sizeof(ast::Expr*));
        l.fk_tables = (char**)realloc(l.fk_tables, l.cap*sizeof(char*));
        l.fk_columns = (char**)realloc(l.fk_columns, l.cap*sizeof(char*));
    }
    l.names[l.count] = n; l.types[l.count] = t;
    l.not_null[l.count] = c.not_null || c.primary_key;
    l.primary_key[l.count] = c.primary_key;
    l.is_unique[l.count] = c.is_unique || c.primary_key;
    l.defaults[l.count] = c.default_val;
    l.checks[l.count] = c.check_expr;
    l.fk_tables[l.count] = c.fk_table;
    l.fk_columns[l.count] = c.fk_column;
    l.count++;
}

static RawRowList make_rlist() { RawRowList l = {nullptr,0,0}; return l; }
static void rlist_push(RawRowList& l, RawExprList row) {
    if (l.count >= l.cap) { l.cap = l.cap ? l.cap*2 : 4; l.rows = (RawExprList*)realloc(l.rows, l.cap*sizeof(RawExprList)); }
    l.rows[l.count++] = row;
}

static RawAssignList make_alist() { RawAssignList l = {nullptr,nullptr,0,0}; return l; }
static void alist_push(RawAssignList& l, char* col, Expr* val) {
    if (l.count >= l.cap) { l.cap = l.cap ? l.cap*2 : 4; l.cols = (char**)realloc(l.cols, l.cap*sizeof(char*)); l.vals = (Expr**)realloc(l.vals, l.cap*sizeof(Expr*)); }
    l.cols[l.count] = col; l.vals[l.count] = val; l.count++;
}

/* Wrap raw Expr* into ExprPtr (takes ownership) */
static ExprPtr wrap(Expr* e) { return std::shared_ptr<Expr>(e); }
static TableRefPtr twrap(TableRef* t) { return std::shared_ptr<TableRef>(t); }

static Expr* make_binop(BinOp op, Expr* l, Expr* r) {
    auto e = new Expr(); e->type = ExprType::BINARY_OP; e->bin_op = op;
    e->left = wrap(l); e->right = wrap(r); return e;
}
%}

%union {
    int64_t int_val;
    double  float_val;
    char*   str_val;
    ast::Expr*       expr_raw;
    ast::TableRef*   tref_raw;
    ast::SelectStmt* sel_raw;
    ast::Statement*  stmt_raw;
    RawExprList      elist;
    RawTRefList      tlist;
    RawOrderList     olist;
    RawColDefList    cdlist;
    RawRowList       rowlist;
    RawAssignList    assignlist;
    RawConstraints   constraints;
    RawStrList       slist;
    int              ival;
}

/* Tokens */
%token SELECT DISTINCT FROM WHERE AND OR NOT IN EXISTS BETWEEN LIKE IS NULL_KW
%token AS ON JOIN INNER LEFT RIGHT FULL OUTER CROSS
%token GROUP BY HAVING ORDER ASC DESC LIMIT OFFSET
%token UNION INTERSECT EXCEPT ALL SOME ANY
%token WITH CREATE TABLE VIEW MATERIALIZED INDEX USING HASH BTREE
%token INSERT INTO VALUES LOAD EXPLAIN ANALYZE BENCHMARK_KW
%token UPDATE SET DELETE_KW
%token ALTER ADD COLUMN RENAME TO DROP TRUNCATE_KW
%token MERGE MATCHED
%token FORMAT DOT
%token TRIGGER BEFORE AFTER FOR EACH ROW_KW EXECUTE
%token DEFAULT PRIMARY KEY UNIQUE CHECK_KW REFERENCES
%token BEGIN_KW
%token COUNT SUM AVG MIN MAX
%token TYPE_INT TYPE_FLOAT TYPE_VARCHAR
%token CASE WHEN THEN ELSE END
%token TRUE_KW FALSE_KW
%token LEQ GEQ NEQ

%token <int_val>   INT_LITERAL
%token <float_val> FLOAT_LITERAL
%token <str_val>   STRING_LITERAL IDENTIFIER

/* Non-terminals */
%type <stmt_raw>  statement
%type <sel_raw>   select_stmt select_body
%type <expr_raw>  expr expr_or expr_and expr_not expr_cmp expr_add expr_mul expr_unary expr_primary
%type <expr_raw>  select_item opt_where opt_having
%type <tref_raw>  table_ref table_primary
%type <elist>     select_list expr_list opt_group_by
%type <tlist>     from_list
%type <olist>     opt_order_by order_list
%type <cdlist>    column_def_list
%type <str_val>   opt_alias data_type agg_name
%type <ival>      opt_distinct opt_asc_desc join_kind before_after trigger_event
%type <constraints> col_constraints
%type <slist>     trigger_body trigger_stmts
%type <int_val>   opt_limit opt_offset
%type <rowlist>   insert_rows
%type <assignlist> set_list

%nonassoc UMINUS

%start input

%%

input:
      statement ';'  { g_parsed_stmt = std::shared_ptr<Statement>($1); }
    | statement      { g_parsed_stmt = std::shared_ptr<Statement>($1); }
    ;

/* ═══════ Statements ═══════ */

statement:
      select_stmt {
          auto st = new Statement(); st->type = StmtType::ST_SELECT;
          st->select.reset($1); $$ = st;
      }
    | EXPLAIN select_stmt {
          auto st = new Statement(); st->type = StmtType::ST_EXPLAIN;
          st->select.reset($2); $$ = st;
      }
    | EXPLAIN ANALYZE select_stmt {
          auto st = new Statement(); st->type = StmtType::ST_EXPLAIN;
          st->select.reset($3); st->explain_analyze = true; $$ = st;
      }
    | EXPLAIN FORMAT DOT select_stmt {
          auto st = new Statement(); st->type = StmtType::ST_EXPLAIN;
          st->select.reset($4); st->explain_dot = true; $$ = st;
      }
    | EXPLAIN ANALYZE FORMAT DOT select_stmt {
          auto st = new Statement(); st->type = StmtType::ST_EXPLAIN;
          st->select.reset($5); st->explain_analyze = true; st->explain_dot = true; $$ = st;
      }
    | CREATE TABLE IDENTIFIER '(' column_def_list ')' {
          auto st = new Statement(); st->type = StmtType::ST_CREATE_TABLE;
          auto ct = std::make_shared<CreateTableStmt>();
          ct->table_name = take_str($3);
          for (int i = 0; i < $5.count; i++) {
              ColumnDef cd;
              cd.name = take_str($5.names[i]);
              cd.data_type = take_str($5.types[i]);
              cd.not_null = $5.not_null[i];
              cd.primary_key = $5.primary_key[i];
              cd.unique = $5.is_unique[i];
              if ($5.defaults[i]) { cd.has_default = true; cd.default_value = wrap($5.defaults[i]); }
              if ($5.checks[i]) { cd.check_expr = wrap($5.checks[i]); }
              if ($5.fk_tables[i]) {
                  cd.has_fk = true;
                  cd.fk_ref_table = take_str($5.fk_tables[i]);
                  cd.fk_ref_column = take_str($5.fk_columns[i]);
              }
              ct->columns.push_back(cd);
          }
          free($5.names); free($5.types); free($5.not_null); free($5.primary_key); free($5.is_unique); free($5.defaults); free($5.checks); free($5.fk_tables); free($5.fk_columns);
          st->create_table = ct; $$ = st;
      }
    | CREATE INDEX IDENTIFIER ON IDENTIFIER '(' IDENTIFIER ')' {
          auto st = new Statement(); st->type = StmtType::ST_CREATE_INDEX;
          auto ci = std::make_shared<CreateIndexStmt>();
          ci->index_name = take_str($3); ci->table_name = take_str($5); ci->column_name = take_str($7);
          st->create_index = ci; $$ = st;
      }
    | CREATE INDEX IDENTIFIER ON IDENTIFIER '(' IDENTIFIER ')' USING HASH {
          auto st = new Statement(); st->type = StmtType::ST_CREATE_INDEX;
          auto ci = std::make_shared<CreateIndexStmt>();
          ci->index_name = take_str($3); ci->table_name = take_str($5); ci->column_name = take_str($7);
          ci->hash_index = true; st->create_index = ci; $$ = st;
      }
    | CREATE VIEW IDENTIFIER AS select_stmt {
          auto st = new Statement(); st->type = StmtType::ST_CREATE_VIEW;
          auto cv = std::make_shared<CreateViewStmt>();
          cv->view_name = take_str($3);
          cv->query.reset($5);
          cv->materialized = false;
          st->create_view = cv; $$ = st;
      }
    | CREATE MATERIALIZED VIEW IDENTIFIER AS select_stmt {
          auto st = new Statement(); st->type = StmtType::ST_CREATE_MATERIALIZED_VIEW;
          auto cv = std::make_shared<CreateViewStmt>();
          cv->view_name = take_str($4);
          cv->query.reset($6);
          cv->materialized = true;
          st->create_view = cv; $$ = st;
      }
    | INSERT INTO IDENTIFIER VALUES insert_rows {
          auto st = new Statement(); st->type = StmtType::ST_INSERT;
          auto ins = std::make_shared<InsertStmt>();
          ins->table_name = take_str($3);
          for (int r = 0; r < $5.count; r++) {
              std::vector<ExprPtr> row;
              for (int c = 0; c < $5.rows[r].count; c++) {
                  row.push_back(wrap($5.rows[r].items[c]));
              }
              free($5.rows[r].items);
              ins->values.push_back(std::move(row));
          }
          free($5.rows);
          st->insert = ins; $$ = st;
      }
    | INSERT INTO IDENTIFIER '(' expr_list ')' VALUES insert_rows {
          auto st = new Statement(); st->type = StmtType::ST_INSERT;
          auto ins = std::make_shared<InsertStmt>();
          ins->table_name = take_str($3);
          for (int i = 0; i < $5.count; i++) {
              ins->columns.push_back(take_str($5.items[i]->column_name.empty() ?
                  strdup($5.items[i]->to_string().c_str()) : strdup($5.items[i]->column_name.c_str())));
              delete $5.items[i];
          }
          free($5.items);
          for (int r = 0; r < $8.count; r++) {
              std::vector<ExprPtr> row;
              for (int c = 0; c < $8.rows[r].count; c++) {
                  row.push_back(wrap($8.rows[r].items[c]));
              }
              free($8.rows[r].items);
              ins->values.push_back(std::move(row));
          }
          free($8.rows);
          st->insert = ins; $$ = st;
      }
    | UPDATE IDENTIFIER SET set_list opt_where {
          auto st = new Statement(); st->type = StmtType::ST_UPDATE;
          auto upd = std::make_shared<UpdateStmt>();
          upd->table_name = take_str($2);
          for (int i = 0; i < $4.count; i++) {
              upd->assignments.emplace_back(take_str($4.cols[i]), wrap($4.vals[i]));
          }
          free($4.cols); free($4.vals);
          upd->where_clause = wrap($5);
          st->update = upd; $$ = st;
      }
    | DELETE_KW FROM IDENTIFIER opt_where {
          auto st = new Statement(); st->type = StmtType::ST_DELETE;
          auto del = std::make_shared<DeleteStmt>();
          del->table_name = take_str($3);
          del->where_clause = wrap($4);
          st->del = del; $$ = st;
      }
    | LOAD IDENTIFIER STRING_LITERAL {
          auto st = new Statement(); st->type = StmtType::ST_LOAD;
          auto ld = std::make_shared<LoadStmt>();
          ld->table_name = take_str($2); ld->file_path = take_str($3);
          st->load = ld; $$ = st;
      }
    | BENCHMARK_KW select_stmt {
          auto st = new Statement(); st->type = StmtType::ST_BENCHMARK;
          st->select.reset($2); $$ = st;
      }
    | ALTER TABLE IDENTIFIER ADD COLUMN IDENTIFIER data_type {
          auto st = new Statement(); st->type = StmtType::ST_ALTER_ADD_COL;
          auto alt = std::make_shared<AlterStmt>();
          alt->table_name = take_str($3);
          alt->column_name = take_str($6);
          alt->column_type = take_str($7);
          st->alter = alt; $$ = st;
      }
    | ALTER TABLE IDENTIFIER DROP COLUMN IDENTIFIER {
          auto st = new Statement(); st->type = StmtType::ST_ALTER_DROP_COL;
          auto alt = std::make_shared<AlterStmt>();
          alt->table_name = take_str($3);
          alt->column_name = take_str($6);
          st->alter = alt; $$ = st;
      }
    | ALTER TABLE IDENTIFIER RENAME COLUMN IDENTIFIER TO IDENTIFIER {
          auto st = new Statement(); st->type = StmtType::ST_ALTER_RENAME_COL;
          auto alt = std::make_shared<AlterStmt>();
          alt->table_name = take_str($3);
          alt->column_name = take_str($6);
          alt->new_name = take_str($8);
          st->alter = alt; $$ = st;
      }
    | ALTER TABLE IDENTIFIER RENAME TO IDENTIFIER {
          auto st = new Statement(); st->type = StmtType::ST_ALTER_RENAME_TBL;
          auto alt = std::make_shared<AlterStmt>();
          alt->table_name = take_str($3);
          alt->new_name = take_str($6);
          st->alter = alt; $$ = st;
      }
    | RENAME TABLE IDENTIFIER TO IDENTIFIER {
          auto st = new Statement(); st->type = StmtType::ST_ALTER_RENAME_TBL;
          auto alt = std::make_shared<AlterStmt>();
          alt->table_name = take_str($3);
          alt->new_name = take_str($5);
          st->alter = alt; $$ = st;
      }
    | DROP TABLE IDENTIFIER {
          auto st = new Statement(); st->type = StmtType::ST_DROP_TABLE;
          st->drop_name = take_str($3); $$ = st;
      }
    | DROP INDEX IDENTIFIER {
          auto st = new Statement(); st->type = StmtType::ST_DROP_INDEX;
          st->drop_name = take_str($3); $$ = st;
      }
    | DROP VIEW IDENTIFIER {
          auto st = new Statement(); st->type = StmtType::ST_DROP_VIEW;
          st->drop_name = take_str($3); $$ = st;
      }
    | DROP TRIGGER IDENTIFIER {
          auto st = new Statement(); st->type = StmtType::ST_DROP_TRIGGER;
          st->drop_name = take_str($3); $$ = st;
      }
    | CREATE TRIGGER IDENTIFIER before_after trigger_event ON IDENTIFIER FOR EACH ROW_KW EXECUTE trigger_body {
          auto st = new Statement(); st->type = StmtType::ST_CREATE_TRIGGER;
          auto tg = std::make_shared<CreateTriggerStmt>();
          tg->trigger_name = take_str($3);
          tg->when = $4;
          tg->event = $5;
          tg->table_name = take_str($7);
          for (int i = 0; i < $12.count; i++) {
              tg->action_sqls.push_back(take_str($12.items[i]));
          }
          free($12.items);
          st->create_trigger = tg; $$ = st;
      }
    | TRUNCATE_KW TABLE IDENTIFIER {
          auto st = new Statement(); st->type = StmtType::ST_TRUNCATE;
          st->drop_name = take_str($3); $$ = st;
      }
    | TRUNCATE_KW IDENTIFIER {
          auto st = new Statement(); st->type = StmtType::ST_TRUNCATE;
          st->drop_name = take_str($2); $$ = st;
      }
    | MERGE INTO IDENTIFIER USING IDENTIFIER ON expr
      WHEN MATCHED THEN UPDATE SET set_list
      WHEN NOT MATCHED THEN INSERT VALUES insert_rows {
          auto st = new Statement(); st->type = StmtType::ST_MERGE;
          auto mg = std::make_shared<MergeStmt>();
          mg->target_table = take_str($3);
          mg->source_table = take_str($5);
          mg->on_condition = wrap($7);
          /* WHEN MATCHED → SET assignments ($13) */
          for (int i = 0; i < $13.count; i++) {
              mg->update_assignments.emplace_back(take_str($13.cols[i]), wrap($13.vals[i]));
          }
          free($13.cols); free($13.vals);
          /* WHEN NOT MATCHED → INSERT VALUES ($20) */
          for (int r = 0; r < $20.count; r++) {
              std::vector<ExprPtr> row;
              for (int c = 0; c < $20.rows[r].count; c++) {
                  row.push_back(wrap($20.rows[r].items[c]));
              }
              free($20.rows[r].items);
              mg->insert_values.push_back(std::move(row));
          }
          free($20.rows);
          st->merge = mg; $$ = st;
      }
    ;

insert_rows:
      '(' expr_list ')' {
          $$ = make_rlist();
          RawExprList row = {$2.items, $2.count, $2.cap};
          rlist_push($$, row);
      }
    | insert_rows ',' '(' expr_list ')' {
          $$ = $1;
          RawExprList row = {$4.items, $4.count, $4.cap};
          rlist_push($$, row);
      }
    ;

set_list:
      IDENTIFIER '=' expr {
          $$ = make_alist();
          alist_push($$, $1, $3);
      }
    | set_list ',' IDENTIFIER '=' expr {
          $$ = $1;
          alist_push($$, $3, $5);
      }
    ;

/* ═══════ SELECT ═══════ */

select_stmt:
      select_body         { $$ = $1; }
    | WITH cte_list select_body { $$ = $3; }
    ;

cte_list:
      IDENTIFIER AS '(' select_body ')' { free($1); delete $4; }
    | cte_list ',' IDENTIFIER AS '(' select_body ')' { free($3); delete $6; }
    ;

select_body:
      SELECT opt_distinct select_list
      FROM from_list
      opt_where
      opt_group_by
      opt_having
      opt_order_by
      opt_limit
      opt_offset
      {
          auto sel = new SelectStmt();
          sel->distinct = ($2 != 0);
          for (int i = 0; i < $3.count; i++) sel->select_list.push_back(wrap($3.items[i]));
          free($3.items);
          for (int i = 0; i < $5.count; i++) sel->from_clause.push_back(twrap($5.items[i]));
          free($5.items);
          if ($6) sel->where_clause = wrap($6);
          for (int i = 0; i < $7.count; i++) sel->group_by.push_back(wrap($7.items[i]));
          if ($7.items) free($7.items);
          if ($8) sel->having_clause = wrap($8);
          for (int i = 0; i < $9.count; i++) {
              OrderItem oi; oi.expr = wrap($9.exprs[i]); oi.ascending = ($9.ascs[i] != 0);
              sel->order_by.push_back(oi);
          }
          if ($9.exprs) { free($9.exprs); free($9.ascs); }
          sel->limit = $10;
          sel->offset = $11;
          $$ = sel;
      }
    ;

opt_distinct: /* empty */ { $$ = 0; } | DISTINCT { $$ = 1; } ;

/* ─── SELECT list ─── */
select_list:
      select_item                   { $$ = make_elist(); elist_push($$, $1); }
    | select_list ',' select_item   { $$ = $1; elist_push($$, $3); }
    ;

select_item:
      expr opt_alias { $$ = $1; if ($2) $$->alias = take_str($2); }
    ;

opt_alias:
      /* empty */   { $$ = nullptr; }
    | AS IDENTIFIER { $$ = $2; }
    | IDENTIFIER    { $$ = $1; }
    ;

/* ─── FROM ─── */
from_list:
      table_ref                 { $$ = make_tlist(); tlist_push($$, $1); }
    | from_list ',' table_ref   { $$ = $1; tlist_push($$, $3); }
    ;

table_ref:
      table_primary     { $$ = $1; }
    | table_ref join_kind JOIN table_primary ON expr {
          auto t = new TableRef(); t->type = TableRefType::TRT_JOIN;
          t->join_type = (JoinType)$2;
          t->left.reset($1); t->right.reset($4); t->join_cond = wrap($6); $$ = t;
      }
    | table_ref CROSS JOIN table_primary {
          auto t = new TableRef(); t->type = TableRefType::TRT_JOIN;
          t->join_type = JoinType::JT_CROSS;
          t->left.reset($1); t->right.reset($4); $$ = t;
      }
    ;

table_primary:
      IDENTIFIER opt_alias {
          auto t = new TableRef(); t->type = TableRefType::BASE_TABLE;
          t->table_name = take_str($1); if ($2) t->alias = take_str($2); $$ = t;
      }
    | '(' select_body ')' opt_alias {
          auto t = new TableRef(); t->type = TableRefType::TRT_SUBQUERY;
          t->subquery.reset($2); if ($4) t->alias = take_str($4); $$ = t;
      }
    ;

join_kind:
      /* empty */   { $$ = (int)JoinType::JT_INNER; }
    | INNER         { $$ = (int)JoinType::JT_INNER; }
    | LEFT          { $$ = (int)JoinType::JT_LEFT; }
    | LEFT OUTER    { $$ = (int)JoinType::JT_LEFT; }
    | RIGHT         { $$ = (int)JoinType::JT_RIGHT; }
    | RIGHT OUTER   { $$ = (int)JoinType::JT_RIGHT; }
    | FULL          { $$ = (int)JoinType::JT_FULL; }
    | FULL OUTER    { $$ = (int)JoinType::JT_FULL; }
    ;

/* ─── Clauses ─── */
opt_where:   /* empty */ { $$ = nullptr; } | WHERE expr   { $$ = $2; } ;
opt_having:  /* empty */ { $$ = nullptr; } | HAVING expr  { $$ = $2; } ;

opt_group_by:
      /* empty */       { $$ = make_elist(); }
    | GROUP BY expr_list { $$ = $3; }
    ;

opt_order_by:
      /* empty */           { $$ = make_olist(); }
    | ORDER BY order_list   { $$ = $3; }
    ;

order_list:
      expr opt_asc_desc                 { $$ = make_olist(); olist_push($$, $1, $2); }
    | order_list ',' expr opt_asc_desc  { $$ = $1; olist_push($$, $3, $4); }
    ;

opt_asc_desc: /* empty */ { $$ = 1; } | ASC { $$ = 1; } | DESC { $$ = 0; } ;

opt_limit:  /* empty */ { $$ = -1; } | LIMIT INT_LITERAL  { $$ = $2; } ;
opt_offset: /* empty */ { $$ = 0; }  | OFFSET INT_LITERAL { $$ = $2; } ;

/* ─── Column definitions ─── */
column_def_list:
      IDENTIFIER data_type col_constraints {
          $$ = make_cdlist(); cdlist_push($$, $1, $2, $3);
      }
    | column_def_list ',' IDENTIFIER data_type col_constraints {
          $$ = $1; cdlist_push($$, $3, $4, $5);
      }
    ;

col_constraints:
      /* empty */ {
          $$.not_null = 0; $$.primary_key = 0; $$.is_unique = 0;
          $$.default_val = nullptr; $$.check_expr = nullptr;
          $$.fk_table = nullptr; $$.fk_column = nullptr;
      }
    | col_constraints NOT NULL_KW {
          $$ = $1; $$.not_null = 1;
      }
    | col_constraints DEFAULT expr_primary {
          $$ = $1; $$.default_val = $3;
      }
    | col_constraints PRIMARY KEY {
          $$ = $1; $$.primary_key = 1;
      }
    | col_constraints UNIQUE {
          $$ = $1; $$.is_unique = 1;
      }
    | col_constraints CHECK_KW '(' expr ')' {
          $$ = $1; $$.check_expr = $4;
      }
    | col_constraints REFERENCES IDENTIFIER '(' IDENTIFIER ')' {
          $$ = $1; $$.fk_table = $3; $$.fk_column = $5;
      }
    ;

data_type:
      TYPE_INT                         { $$ = strdup("INT"); }
    | TYPE_FLOAT                       { $$ = strdup("FLOAT"); }
    | TYPE_VARCHAR                     { $$ = strdup("VARCHAR"); }
    | TYPE_VARCHAR '(' INT_LITERAL ')' { $$ = strdup("VARCHAR"); }
    ;

/* ─── expr list ─── */
expr_list:
      expr                  { $$ = make_elist(); elist_push($$, $1); }
    | expr_list ',' expr    { $$ = $1; elist_push($$, $3); }
    ;

/* ═══════ Expressions ═══════ */

expr: expr_or ;

expr_or:
      expr_and              { $$ = $1; }
    | expr_or OR expr_and   { $$ = make_binop(BinOp::OP_OR, $1, $3); }
    ;

expr_and:
      expr_not              { $$ = $1; }
    | expr_and AND expr_not { $$ = make_binop(BinOp::OP_AND, $1, $3); }
    ;

expr_not:
      expr_cmp      { $$ = $1; }
    | NOT expr_not  {
          auto e = new Expr(); e->type = ExprType::UNARY_OP;
          e->unary_op = UnaryOp::OP_NOT; e->operand = wrap($2); $$ = e;
      }
    ;

expr_cmp:
      expr_add                      { $$ = $1; }
    | expr_add '=' expr_add         { $$ = make_binop(BinOp::OP_EQ, $1, $3); }
    | expr_add NEQ expr_add         { $$ = make_binop(BinOp::OP_NEQ, $1, $3); }
    | expr_add '<' expr_add         { $$ = make_binop(BinOp::OP_LT, $1, $3); }
    | expr_add '>' expr_add         { $$ = make_binop(BinOp::OP_GT, $1, $3); }
    | expr_add LEQ expr_add         { $$ = make_binop(BinOp::OP_LTE, $1, $3); }
    | expr_add GEQ expr_add         { $$ = make_binop(BinOp::OP_GTE, $1, $3); }
    | expr_add LIKE expr_add        { $$ = make_binop(BinOp::OP_LIKE, $1, $3); }
    | expr_add IS NULL_KW {
          auto e = new Expr(); e->type = ExprType::UNARY_OP;
          e->unary_op = UnaryOp::OP_IS_NULL; e->operand = wrap($1); $$ = e;
      }
    | expr_add IS NOT NULL_KW {
          auto e = new Expr(); e->type = ExprType::UNARY_OP;
          e->unary_op = UnaryOp::OP_IS_NOT_NULL; e->operand = wrap($1); $$ = e;
      }
    | expr_add IN '(' expr_list ')' {
          auto e = new Expr(); e->type = ExprType::IN_EXPR;
          e->left = wrap($1);
          for (int i = 0; i < $4.count; i++) e->in_list.push_back(wrap($4.items[i]));
          free($4.items); $$ = e;
      }
    | expr_add IN '(' select_body ')' {
          auto e = new Expr(); e->type = ExprType::IN_EXPR;
          e->left = wrap($1); e->subquery.reset($4); $$ = e;
      }
    | expr_add NOT IN '(' expr_list ')' {
          auto in_e = new Expr(); in_e->type = ExprType::IN_EXPR;
          in_e->left = wrap($1);
          for (int i = 0; i < $5.count; i++) in_e->in_list.push_back(wrap($5.items[i]));
          free($5.items);
          auto e = new Expr(); e->type = ExprType::UNARY_OP; e->unary_op = UnaryOp::OP_NOT;
          e->operand = wrap(in_e); $$ = e;
      }
    | expr_add BETWEEN expr_add AND expr_add {
          auto e = new Expr(); e->type = ExprType::BETWEEN_EXPR;
          e->operand = wrap($1); e->between_low = wrap($3); e->between_high = wrap($5); $$ = e;
      }
    | EXISTS '(' select_body ')' {
          auto e = new Expr(); e->type = ExprType::EXISTS_EXPR;
          e->subquery.reset($3); $$ = e;
      }
    ;

expr_add:
      expr_mul                  { $$ = $1; }
    | expr_add '+' expr_mul     { $$ = make_binop(BinOp::OP_ADD, $1, $3); }
    | expr_add '-' expr_mul     { $$ = make_binop(BinOp::OP_SUB, $1, $3); }
    ;

expr_mul:
      expr_unary                    { $$ = $1; }
    | expr_mul '*' expr_unary       { $$ = make_binop(BinOp::OP_MUL, $1, $3); }
    | expr_mul '/' expr_unary       { $$ = make_binop(BinOp::OP_DIV, $1, $3); }
    | expr_mul '%' expr_unary       { $$ = make_binop(BinOp::OP_MOD, $1, $3); }
    ;

expr_unary:
      expr_primary          { $$ = $1; }
    | '-' expr_unary %prec UMINUS {
          auto e = new Expr(); e->type = ExprType::UNARY_OP;
          e->unary_op = UnaryOp::OP_NEG; e->operand = wrap($2); $$ = e;
      }
    ;

expr_primary:
      INT_LITERAL {
          auto e = new Expr(); e->type = ExprType::LITERAL_INT; e->int_val = $1; $$ = e;
      }
    | FLOAT_LITERAL {
          auto e = new Expr(); e->type = ExprType::LITERAL_FLOAT; e->float_val = $1; $$ = e;
      }
    | STRING_LITERAL {
          auto e = new Expr(); e->type = ExprType::LITERAL_STRING; e->str_val = take_str($1); $$ = e;
      }
    | NULL_KW {
          auto e = new Expr(); e->type = ExprType::LITERAL_NULL; $$ = e;
      }
    | TRUE_KW {
          auto e = new Expr(); e->type = ExprType::LITERAL_INT; e->int_val = 1; $$ = e;
      }
    | FALSE_KW {
          auto e = new Expr(); e->type = ExprType::LITERAL_INT; e->int_val = 0; $$ = e;
      }
    | '*' {
          auto e = new Expr(); e->type = ExprType::STAR; $$ = e;
      }
    | IDENTIFIER '.' IDENTIFIER {
          auto e = new Expr(); e->type = ExprType::COLUMN_REF;
          e->table_name = take_str($1); e->column_name = take_str($3); $$ = e;
      }
    | IDENTIFIER '.' '*' {
          auto e = new Expr(); e->type = ExprType::STAR;
          e->table_name = take_str($1); $$ = e;
      }
    | IDENTIFIER {
          auto e = new Expr(); e->type = ExprType::COLUMN_REF;
          e->column_name = take_str($1); $$ = e;
      }
    | agg_name '(' expr ')' {
          auto e = new Expr(); e->type = ExprType::FUNC_CALL;
          e->func_name = take_str($1); e->args.push_back(wrap($3)); $$ = e;
      }
    | agg_name '(' '*' ')' {
          auto e = new Expr(); e->type = ExprType::FUNC_CALL;
          e->func_name = take_str($1);
          auto star = new Expr(); star->type = ExprType::STAR;
          e->args.push_back(wrap(star)); $$ = e;
      }
    | agg_name '(' DISTINCT expr ')' {
          auto e = new Expr(); e->type = ExprType::FUNC_CALL;
          e->func_name = take_str($1); e->distinct_func = true;
          e->args.push_back(wrap($4)); $$ = e;
      }
    | '(' expr ')' { $$ = $2; }
    | '(' select_body ')' {
          auto e = new Expr(); e->type = ExprType::SUBQUERY;
          e->subquery.reset($2); $$ = e;
      }
    ;

agg_name:
      COUNT { $$ = strdup("COUNT"); }
    | SUM   { $$ = strdup("SUM"); }
    | AVG   { $$ = strdup("AVG"); }
    | MIN   { $$ = strdup("MIN"); }
    | MAX   { $$ = strdup("MAX"); }
    ;

before_after:
      BEFORE { $$ = 0; }
    | AFTER  { $$ = 1; }
    ;

trigger_event:
      INSERT    { $$ = 0; }
    | UPDATE    { $$ = 1; }
    | DELETE_KW { $$ = 2; }
    ;

trigger_body:
      STRING_LITERAL {
          $$ = make_slist(); slist_push($$, $1);
      }
    | BEGIN_KW trigger_stmts END {
          $$ = $2;
      }
    ;

trigger_stmts:
      STRING_LITERAL ';' {
          $$ = make_slist(); slist_push($$, $1);
      }
    | trigger_stmts STRING_LITERAL ';' {
          $$ = $1; slist_push($$, $2);
      }
    ;

%%

void yyerror(const char* s) {
    fprintf(stderr, "Parse error at line %d: %s\n", yylineno, s);
}
