#pragma once
#include <string>
#include <vector>
#include <unordered_map>
#include <variant>
#include <memory>
#include <algorithm>
#include <map>
#include <stdexcept>
#include <fstream>
#include <sstream>
#include <iostream>

namespace ast {
struct SelectStmt;
struct Expr;
using ExprPtr = std::shared_ptr<Expr>;
}

namespace storage {

// ───── Value type: each cell can be int, double, string, or null ─────
using Value = std::variant<std::monostate, int64_t, double, std::string>;

enum class DataType { INT, FLOAT, VARCHAR };

struct ColumnSchema {
    std::string name;
    DataType type;
    bool not_null = false;
    bool primary_key = false;
    bool is_unique = false;
    bool has_default = false;
    Value default_value;        // used when has_default == true
};

// ───── Row = vector of Values ─────
using Row = std::vector<Value>;

// ───── Foreign Key definition ─────
struct ForeignKeyDef {
    std::string column_name;
    std::string ref_table;
    std::string ref_column;
};

// ───── Table ─────
class Table {
public:
    std::string name;
    std::vector<ColumnSchema> schema;
    std::vector<Row> rows;
    std::vector<ast::ExprPtr> check_constraints;  // CHECK expressions
    std::vector<ForeignKeyDef> foreign_keys;      // FOREIGN KEY references

    Table() = default;
    Table(const std::string& name, const std::vector<ColumnSchema>& schema)
        : name(name), schema(schema) {}

    int column_index(const std::string& col_name) const;
    size_t row_count() const { return rows.size(); }
    size_t col_count() const { return schema.size(); }

    void insert_row(const Row& row);
    void load_csv(const std::string& path);
    void print_rows(const std::vector<std::string>& columns, const std::vector<Row>& result_rows, int max_rows = -1) const;

    // Statistics
    size_t cardinality() const { return rows.size(); }
    size_t distinct_values(const std::string& col) const;
};

// ───── Hash Index ─────
class HashIndex {
public:
    std::string table_name;
    std::string column_name;
    std::unordered_map<int64_t, std::vector<size_t>> int_map;
    std::unordered_map<std::string, std::vector<size_t>> str_map;

    void build(const Table& table);
    std::vector<size_t> lookup_int(int64_t key) const;
    std::vector<size_t> lookup_str(const std::string& key) const;
};

// ───── B-Tree Index (ordered, supports range queries) ─────
class BTreeIndex {
public:
    std::string table_name;
    std::string column_name;
    std::map<Value, std::vector<size_t>> tree;  // ordered map acts as B-tree

    void build(const Table& table);
    void insert_entry(const Value& key, size_t row_idx);

    // Equality lookup
    std::vector<size_t> lookup_exact(const Value& key) const;
    // Range lookup [low, high] inclusive
    std::vector<size_t> lookup_range(const Value& low, const Value& high) const;
    // All rows with key < val
    std::vector<size_t> lookup_lt(const Value& val) const;
    // All rows with key > val
    std::vector<size_t> lookup_gt(const Value& val) const;
    // All rows with key <= val
    std::vector<size_t> lookup_lte(const Value& val) const;
    // All rows with key >= val
    std::vector<size_t> lookup_gte(const Value& val) const;
};

// ───── Index entry in catalog ─────
enum class IndexType { HASH, BTREE };

struct IndexEntry {
    std::string index_name;
    std::string table_name;
    std::string column_name;
    IndexType type;
    std::shared_ptr<HashIndex> hash_idx;
    std::shared_ptr<BTreeIndex> btree_idx;
};

// ───── Trigger definition ─────
struct TriggerDef {
    std::string name;
    std::string table_name;
    enum When { BEFORE, AFTER } when;
    enum Event { ON_INSERT, ON_UPDATE, ON_DELETE } event;
    std::vector<std::string> action_sqls;
};

// ───── Catalog ─────
class Catalog {
public:
    struct ViewDef {
        std::shared_ptr<ast::SelectStmt> query;
        bool materialized = false;
    };

    std::unordered_map<std::string, std::shared_ptr<Table>> tables;
    std::unordered_map<std::string, std::shared_ptr<HashIndex>> indexes; // key: "table.column"
    std::unordered_map<std::string, std::shared_ptr<BTreeIndex>> btree_indexes; // key: "table.column"
    std::unordered_map<std::string, std::shared_ptr<ViewDef>> views;

    void add_table(std::shared_ptr<Table> table);
    Table* get_table(const std::string& name);
    void add_view(const std::string& name, std::shared_ptr<ast::SelectStmt> query, bool materialized);
    ViewDef* get_view(const std::string& name);
    bool has_view(const std::string& name) const;
    void create_index(const std::string& idx_name, const std::string& table_name,
                      const std::string& column_name, bool hash);
    HashIndex* get_index(const std::string& table_name, const std::string& column_name);
    BTreeIndex* get_btree_index(const std::string& table_name, const std::string& column_name);
    bool has_any_index(const std::string& table_name, const std::string& column_name);

    // Index maintenance: update all indexes when a row is inserted
    void update_indexes_on_insert(const std::string& table_name, size_t row_idx);

    // Drop helpers
    void remove_indexes_for_table(const std::string& table_name);
    bool drop_index_by_name(const std::string& index_name);

    // statistics
    size_t table_cardinality(const std::string& name) const;
    size_t column_distinct(const std::string& table, const std::string& col) const;

    // Trigger management
    std::vector<std::shared_ptr<TriggerDef>> triggers;
    void add_trigger(std::shared_ptr<TriggerDef> t);
    bool drop_trigger(const std::string& name);
    std::vector<TriggerDef*> get_triggers(const std::string& table, TriggerDef::Event event);
};

// ───── Value helpers ─────
bool value_is_null(const Value& v);
int64_t value_to_int(const Value& v);
double value_to_double(const Value& v);
std::string value_to_string(const Value& v);
bool value_less(const Value& a, const Value& b);
bool value_equal(const Value& a, const Value& b);
Value value_add(const Value& a, const Value& b);
Value value_sub(const Value& a, const Value& b);
Value value_mul(const Value& a, const Value& b);
Value value_div(const Value& a, const Value& b);
std::string value_display(const Value& v);

} // namespace storage
