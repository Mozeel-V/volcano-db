#include "ast/ast.h"

namespace ast {

ExprPtr Expr::make_column(const std::string& col, const std::string& tbl) {
    auto e = std::make_shared<Expr>();
    e->type = ExprType::COLUMN_REF;
    e->column_name = col;
    e->table_name = tbl;
    return e;
}

ExprPtr Expr::make_int(int64_t v) {
    auto e = std::make_shared<Expr>();
    e->type = ExprType::LITERAL_INT;
    e->int_val = v;
    return e;
}

ExprPtr Expr::make_float(double v) {
    auto e = std::make_shared<Expr>();
    e->type = ExprType::LITERAL_FLOAT;
    e->float_val = v;
    return e;
}

ExprPtr Expr::make_string(const std::string& v) {
    auto e = std::make_shared<Expr>();
    e->type = ExprType::LITERAL_STRING;
    e->str_val = v;
    return e;
}

ExprPtr Expr::make_star() {
    auto e = std::make_shared<Expr>();
    e->type = ExprType::STAR;
    return e;
}

ExprPtr Expr::make_binary(BinOp op, ExprPtr l, ExprPtr r) {
    auto e = std::make_shared<Expr>();
    e->type = ExprType::BINARY_OP;
    e->bin_op = op;
    e->left = std::move(l);
    e->right = std::move(r);
    return e;
}

ExprPtr Expr::make_unary(UnaryOp op, ExprPtr operand) {
    auto e = std::make_shared<Expr>();
    e->type = ExprType::UNARY_OP;
    e->unary_op = op;
    e->operand = std::move(operand);
    return e;
}

ExprPtr Expr::make_func(const std::string& name, std::vector<ExprPtr> args, bool dist) {
    auto e = std::make_shared<Expr>();
    e->type = ExprType::FUNC_CALL;
    e->func_name = name;
    e->args = std::move(args);
    e->distinct_func = dist;
    return e;
}

static std::string binop_str(BinOp op) {
    switch (op) {
        case BinOp::OP_ADD: return "+";
        case BinOp::OP_SUB: return "-";
        case BinOp::OP_MUL: return "*";
        case BinOp::OP_DIV: return "/";
        case BinOp::OP_MOD: return "%";
        case BinOp::OP_EQ:  return "=";
        case BinOp::OP_NEQ: return "!=";
        case BinOp::OP_LT:  return "<";
        case BinOp::OP_GT:  return ">";
        case BinOp::OP_LTE: return "<=";
        case BinOp::OP_GTE: return ">=";
        case BinOp::OP_AND: return "AND";
        case BinOp::OP_OR:  return "OR";
        case BinOp::OP_LIKE: return "LIKE";
    }
    return "?";
}

std::string Expr::to_string() const {
    switch (type) {
        case ExprType::COLUMN_REF:
            return table_name.empty() ? column_name : table_name + "." + column_name;
        case ExprType::LITERAL_INT:
            return std::to_string(int_val);
        case ExprType::LITERAL_FLOAT:
            return std::to_string(float_val);
        case ExprType::LITERAL_STRING:
            return "'" + str_val + "'";
        case ExprType::LITERAL_NULL:
            return "NULL";
        case ExprType::STAR:
            return "*";
        case ExprType::BINARY_OP:
            return "(" + left->to_string() + " " + binop_str(bin_op) + " " + right->to_string() + ")";
        case ExprType::UNARY_OP:
            if (unary_op == UnaryOp::OP_NOT) return "(NOT " + operand->to_string() + ")";
            if (unary_op == UnaryOp::OP_NEG) return "(-" + operand->to_string() + ")";
            if (unary_op == UnaryOp::OP_IS_NULL) return "(" + operand->to_string() + " IS NULL)";
            if (unary_op == UnaryOp::OP_IS_NOT_NULL) return "(" + operand->to_string() + " IS NOT NULL)";
            return "?";
        case ExprType::FUNC_CALL: {
            std::string s = func_name + "(";
            if (distinct_func) s += "DISTINCT ";
            for (size_t i = 0; i < args.size(); i++) {
                if (i) s += ", ";
                s += args[i]->to_string();
            }
            return s + ")";
        }
        case ExprType::SUBQUERY:
            return "(subquery)";
        case ExprType::IN_EXPR:
            return left->to_string() + " IN (...)";
        case ExprType::EXISTS_EXPR:
            return "EXISTS(...)";
        case ExprType::QUANTIFIED_EXPR: {
            std::string q = (quantifier == Quantifier::Q_ALL) ? "ALL" : "SOME";
            return left->to_string() + " " + binop_str(quant_cmp_op) + " " + q + " (...)";
        }
        case ExprType::BETWEEN_EXPR:
            return operand->to_string() + " BETWEEN " + between_low->to_string() + " AND " + between_high->to_string();
        default:
            return "?expr?";
    }
}

std::string SelectStmt::to_string() const {
    std::ostringstream ss;
    ss << "SELECT ";
    if (distinct) ss << "DISTINCT ";
    for (size_t i = 0; i < select_list.size(); i++) {
        if (i) ss << ", ";
        ss << select_list[i]->to_string();
        if (!select_list[i]->alias.empty()) ss << " AS " << select_list[i]->alias;
    }
    if (!from_clause.empty()) {
        ss << " FROM ";
        for (size_t i = 0; i < from_clause.size(); i++) {
            if (i) ss << ", ";
            ss << from_clause[i]->table_name;
            if (!from_clause[i]->alias.empty()) ss << " " << from_clause[i]->alias;
        }
    }
    if (where_clause) ss << " WHERE " << where_clause->to_string();
    if (!group_by.empty()) {
        ss << " GROUP BY ";
        for (size_t i = 0; i < group_by.size(); i++) {
            if (i) ss << ", ";
            ss << group_by[i]->to_string();
        }
    }
    if (having_clause) ss << " HAVING " << having_clause->to_string();
    if (!order_by.empty()) {
        ss << " ORDER BY ";
        for (size_t i = 0; i < order_by.size(); i++) {
            if (i) ss << ", ";
            ss << order_by[i].expr->to_string();
            ss << (order_by[i].ascending ? " ASC" : " DESC");
        }
    }
    if (limit >= 0) ss << " LIMIT " << limit;
    if (offset > 0) ss << " OFFSET " << offset;

    if (set_op != SetOpType::SO_NONE && set_rhs) {
        switch (set_op) {
            case SetOpType::SO_UNION:      ss << " UNION "; break;
            case SetOpType::SO_UNION_ALL:  ss << " UNION ALL "; break;
            case SetOpType::SO_INTERSECT:  ss << " INTERSECT "; break;
            case SetOpType::SO_EXCEPT:     ss << " EXCEPT "; break;
            default: break;
        }
        ss << set_rhs->to_string();
    }

    return ss.str();
}

} // namespace ast
