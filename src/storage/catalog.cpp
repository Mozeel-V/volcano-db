#include "storage/storage.h"

#include <cctype>
#include <functional>
#include <unordered_map>

namespace storage {

static std::string normalize_identifier_key(const std::string& name) {
    std::string key = name;
    std::transform(key.begin(), key.end(), key.begin(),
                   [](unsigned char c) { return static_cast<char>(std::toupper(c)); });
    return key;
}

void Catalog::add_table(std::shared_ptr<Table> table) {
    tables[table->name] = table;
}

Table* Catalog::get_table(const std::string& name) {
    auto it = tables.find(name);
    if (it == tables.end()) return nullptr;
    return it->second.get();
}

void Catalog::add_view(const std::string& name, std::shared_ptr<ast::SelectStmt> query, bool materialized) {
    auto def = std::make_shared<ViewDef>();
    def->query = std::move(query);
    def->materialized = materialized;
    views[name] = def;
}

Catalog::ViewDef* Catalog::get_view(const std::string& name) {
    auto it = views.find(name);
    if (it == views.end()) return nullptr;
    return it->second.get();
}

bool Catalog::has_view(const std::string& name) const {
    return views.find(name) != views.end();
}

void Catalog::add_function(std::shared_ptr<FunctionDef> fn) {
    if (!fn) throw std::runtime_error("Function definition is null");
    std::string key = normalize_identifier_key(fn->name);
    if (functions.find(key) != functions.end()) {
        throw std::runtime_error("Function already exists: " + fn->name);
    }
    functions[key] = std::move(fn);
}

Catalog::FunctionDef* Catalog::get_function(const std::string& name) {
    auto it = functions.find(normalize_identifier_key(name));
    if (it == functions.end()) return nullptr;
    return it->second.get();
}

const Catalog::FunctionDef* Catalog::get_function(const std::string& name) const {
    auto it = functions.find(normalize_identifier_key(name));
    if (it == functions.end()) return nullptr;
    return it->second.get();
}

bool Catalog::drop_function(const std::string& name) {
    return functions.erase(normalize_identifier_key(name)) > 0;
}

void Catalog::create_index(const std::string& idx_name, const std::string& table_name,
                           const std::string& column_name, bool hash) {
    auto tbl = get_table(table_name);
    if (!tbl) throw std::runtime_error("Table not found: " + table_name);

    std::string key = table_name + "." + column_name;

    if (hash) {
        auto idx = std::make_shared<HashIndex>();
        idx->table_name = table_name;
        idx->column_name = column_name;
        idx->build(*tbl);
        indexes[key] = idx;
    } else {
        auto idx = std::make_shared<BTreeIndex>();
        idx->table_name = table_name;
        idx->column_name = column_name;
        idx->build(*tbl);
        btree_indexes[key] = idx;
    }

    std::cout << "Index '" << idx_name << "' created on " << table_name
              << "(" << column_name << ") [" << (hash ? "hash" : "btree") << "]\n";
}

HashIndex* Catalog::get_index(const std::string& table_name, const std::string& column_name) {
    std::string key = table_name + "." + column_name;
    auto it = indexes.find(key);
    if (it == indexes.end()) return nullptr;
    return it->second.get();
}

BTreeIndex* Catalog::get_btree_index(const std::string& table_name, const std::string& column_name) {
    std::string key = table_name + "." + column_name;
    auto it = btree_indexes.find(key);
    if (it == btree_indexes.end()) return nullptr;
    return it->second.get();
}

bool Catalog::has_any_index(const std::string& table_name, const std::string& column_name) {
    std::string key = table_name + "." + column_name;
    return indexes.count(key) > 0 || btree_indexes.count(key) > 0;
}

void Catalog::update_indexes_on_insert(const std::string& table_name, size_t row_idx) {
    auto tbl = get_table(table_name);
    if (!tbl || row_idx >= tbl->rows.size()) return;

    const Row& row = tbl->rows[row_idx];

    // We update hash indexes
    for (auto& [key, idx] : indexes) {
        if (idx->table_name == table_name) {
            int col_idx = tbl->column_index(idx->column_name);
            if (col_idx < 0) continue;
            const Value& v = row[col_idx];
            if (value_is_null(v)) continue;
            if (std::holds_alternative<int64_t>(v)) {
                idx->int_map[std::get<int64_t>(v)].push_back(row_idx);
            } else if (std::holds_alternative<std::string>(v)) {
                idx->str_map[std::get<std::string>(v)].push_back(row_idx);
            } else if (std::holds_alternative<double>(v)) {
                idx->int_map[(int64_t)std::get<double>(v)].push_back(row_idx);
            }
        }
    }

    // We update btree indexes
    for (auto& [key, idx] : btree_indexes) {
        if (idx->table_name == table_name) {
            int col_idx = tbl->column_index(idx->column_name);
            if (col_idx < 0) continue;
            const Value& v = row[col_idx];
            if (!value_is_null(v)) {
                idx->insert_entry(v, row_idx);
            }
        }
    }
}

void Catalog::remove_indexes_for_table(const std::string& table_name) {
    for (auto it = indexes.begin(); it != indexes.end(); ) {
        if (it->second->table_name == table_name)
            it = indexes.erase(it);
        else
            ++it;
    }
    for (auto it = btree_indexes.begin(); it != btree_indexes.end(); ) {
        if (it->second->table_name == table_name)
            it = btree_indexes.erase(it);
        else
            ++it;
    }
}

bool Catalog::drop_index_by_name(const std::string& index_name) {
    // We try hash indexes
    for (auto it = indexes.begin(); it != indexes.end(); ++it) {
        if (it->first == index_name) {
            indexes.erase(it);
            return true;
        }
    }
    // We try btree indexes
    for (auto it = btree_indexes.begin(); it != btree_indexes.end(); ++it) {
        if (it->first == index_name) {
            btree_indexes.erase(it);
            return true;
        }
    }
    
    // Also try matching as "table.column" pattern for any key containing the index name
    // Since indexes are keyed by "table.column" but created with a user-given name,
    // let's also check all entries
    for (auto it = indexes.begin(); it != indexes.end(); ++it) {
        if (it->first.find(index_name) != std::string::npos) {
            indexes.erase(it);
            return true;
        }
    }
    for (auto it = btree_indexes.begin(); it != btree_indexes.end(); ++it) {
        if (it->first.find(index_name) != std::string::npos) {
            btree_indexes.erase(it);
            return true;
        }
    }
    
    return false;
}

bool Catalog::validate_table_indexes(const std::string& table_name, std::string* error_msg) const {
    auto tbl_it = tables.find(table_name);
    if (tbl_it == tables.end() || !tbl_it->second) {
        if (error_msg) {
            *error_msg = "Table not found while validating indexes: " + table_name;
        }
        return false;
    }

    const Table& tbl = *tbl_it->second;

    for (const auto& [key, idx] : indexes) {
        if (!idx || idx->table_name != table_name) continue;

        try {
            HashIndex expected;
            expected.table_name = idx->table_name;
            expected.column_name = idx->column_name;
            expected.build(tbl);

            if (expected.int_map != idx->int_map || expected.str_map != idx->str_map) {
                if (error_msg) {
                    *error_msg = "Hash index mismatch for key '" + key + "'";
                }
                return false;
            }
        } catch (const std::exception& e) {
            if (error_msg) {
                *error_msg = "Hash index validation error for key '" + key + "': " + e.what();
            }
            return false;
        }
    }

    for (const auto& [key, idx] : btree_indexes) {
        if (!idx || idx->table_name != table_name) continue;

        try {
            BTreeIndex expected;
            expected.table_name = idx->table_name;
            expected.column_name = idx->column_name;
            expected.build(tbl);

            if (expected.tree != idx->tree) {
                if (error_msg) {
                    *error_msg = "B-tree index mismatch for key '" + key + "'";
                }
                return false;
            }
        } catch (const std::exception& e) {
            if (error_msg) {
                *error_msg = "B-tree index validation error for key '" + key + "': " + e.what();
            }
            return false;
        }
    }

    return true;
}

bool Catalog::validate_all_indexes(std::string* error_msg) const {
    std::unordered_set<std::string> referenced_tables;

    for (const auto& [key, idx] : indexes) {
        if (!idx) continue;
        referenced_tables.insert(idx->table_name);
        if (tables.find(idx->table_name) == tables.end()) {
            if (error_msg) {
                *error_msg = "Hash index '" + key + "' references missing table '" + idx->table_name + "'";
            }
            return false;
        }
    }

    for (const auto& [key, idx] : btree_indexes) {
        if (!idx) continue;
        referenced_tables.insert(idx->table_name);
        if (tables.find(idx->table_name) == tables.end()) {
            if (error_msg) {
                *error_msg = "B-tree index '" + key + "' references missing table '" + idx->table_name + "'";
            }
            return false;
        }
    }

    for (const auto& table_name : referenced_tables) {
        std::string table_error;
        if (!validate_table_indexes(table_name, &table_error)) {
            if (error_msg) {
                *error_msg = table_error;
            }
            return false;
        }
    }

    return true;
}

size_t Catalog::table_cardinality(const std::string& name) const {
    auto it = tables.find(name);
    if (it == tables.end()) return 0;
    return it->second->cardinality();
}

size_t Catalog::column_distinct(const std::string& table, const std::string& col) const {
    auto it = tables.find(table);
    if (it == tables.end()) return 0;
    return it->second->distinct_values(col);
}

bool value_is_null(const Value& v) {
    return std::holds_alternative<std::monostate>(v);
}

int64_t value_to_int(const Value& v) {
    if (std::holds_alternative<int64_t>(v)) return std::get<int64_t>(v);
    if (std::holds_alternative<double>(v)) return (int64_t)std::get<double>(v);
    if (std::holds_alternative<std::string>(v)) return std::stoll(std::get<std::string>(v));
    return 0;
}

double value_to_double(const Value& v) {
    if (std::holds_alternative<double>(v)) return std::get<double>(v);
    if (std::holds_alternative<int64_t>(v)) return (double)std::get<int64_t>(v);
    if (std::holds_alternative<std::string>(v)) return std::stod(std::get<std::string>(v));
    return 0.0;
}

std::string value_to_string(const Value& v) {
    if (std::holds_alternative<std::string>(v)) return std::get<std::string>(v);
    if (std::holds_alternative<int64_t>(v)) return std::to_string(std::get<int64_t>(v));
    if (std::holds_alternative<double>(v)) return std::to_string(std::get<double>(v));
    return "NULL";
}

bool value_less(const Value& a, const Value& b) {
    if (value_is_null(a) || value_is_null(b)) return false;
    if (std::holds_alternative<int64_t>(a) && std::holds_alternative<int64_t>(b))
        return std::get<int64_t>(a) < std::get<int64_t>(b);
    if (std::holds_alternative<std::string>(a) && std::holds_alternative<std::string>(b))
        return std::get<std::string>(a) < std::get<std::string>(b);
    return value_to_double(a) < value_to_double(b);
}

bool value_equal(const Value& a, const Value& b) {
    if (value_is_null(a) || value_is_null(b)) return false;
    if (std::holds_alternative<int64_t>(a) && std::holds_alternative<int64_t>(b))
        return std::get<int64_t>(a) == std::get<int64_t>(b);
    if (std::holds_alternative<std::string>(a) && std::holds_alternative<std::string>(b))
        return std::get<std::string>(a) == std::get<std::string>(b);
    return value_to_double(a) == value_to_double(b);
}

Value value_add(const Value& a, const Value& b) {
    if (value_is_null(a) || value_is_null(b)) return std::monostate{};
    if (std::holds_alternative<int64_t>(a) && std::holds_alternative<int64_t>(b))
        return std::get<int64_t>(a) + std::get<int64_t>(b);
    return value_to_double(a) + value_to_double(b);
}

Value value_sub(const Value& a, const Value& b) {
    if (value_is_null(a) || value_is_null(b)) return std::monostate{};
    if (std::holds_alternative<int64_t>(a) && std::holds_alternative<int64_t>(b))
        return std::get<int64_t>(a) - std::get<int64_t>(b);
    return value_to_double(a) - value_to_double(b);
}

Value value_mul(const Value& a, const Value& b) {
    if (value_is_null(a) || value_is_null(b)) return std::monostate{};
    if (std::holds_alternative<int64_t>(a) && std::holds_alternative<int64_t>(b))
        return std::get<int64_t>(a) * std::get<int64_t>(b);
    return value_to_double(a) * value_to_double(b);
}

Value value_div(const Value& a, const Value& b) {
    if (value_is_null(a) || value_is_null(b)) return std::monostate{};
    double denom = value_to_double(b);
    if (denom == 0.0) return std::monostate{};
    if (std::holds_alternative<int64_t>(a) && std::holds_alternative<int64_t>(b))
        return std::get<int64_t>(a) / std::get<int64_t>(b);
    return value_to_double(a) / denom;
}

bool like_pattern_match(const std::string& text, const std::string& pattern, char escape) {
    std::unordered_map<uint64_t, bool> memo;

    std::function<bool(size_t, size_t)> match = [&](size_t ti, size_t pi) -> bool {
        uint64_t key = (static_cast<uint64_t>(ti) << 32) | static_cast<uint64_t>(pi);
        auto it = memo.find(key);
        if (it != memo.end()) return it->second;

        bool result = false;
        if (pi == pattern.size()) {
            result = (ti == text.size());
        } else {
            char pc = pattern[pi];
            if (pc == escape) {
                if (pi + 1 < pattern.size()) {
                    result = (ti < text.size() && text[ti] == pattern[pi + 1] && match(ti + 1, pi + 2));
                } else {
                    // Trailing escape character is treated as a literal escape.
                    result = (ti < text.size() && text[ti] == escape && match(ti + 1, pi + 1));
                }
            } else if (pc == '%') {
                size_t next_pi = pi;
                while (next_pi + 1 < pattern.size() && pattern[next_pi + 1] == '%') {
                    next_pi++;
                }
                next_pi++;

                if (next_pi == pattern.size()) {
                    result = true;
                } else {
                    for (size_t k = ti; k <= text.size(); k++) {
                        if (match(k, next_pi)) {
                            result = true;
                            break;
                        }
                    }
                }
            } else if (pc == '_') {
                result = (ti < text.size() && match(ti + 1, pi + 1));
            } else {
                result = (ti < text.size() && text[ti] == pc && match(ti + 1, pi + 1));
            }
        }

        memo[key] = result;
        return result;
    };

    return match(0, 0);
}

bool value_like(const Value& text, const Value& pattern, char escape) {
    if (value_is_null(text) || value_is_null(pattern)) return false;
    return like_pattern_match(value_to_string(text), value_to_string(pattern), escape);
}

std::string value_display(const Value& v) {
    if (std::holds_alternative<std::monostate>(v)) return "NULL";
    if (std::holds_alternative<int64_t>(v)) return std::to_string(std::get<int64_t>(v));
    if (std::holds_alternative<double>(v)) {
        char buf[64];
        snprintf(buf, sizeof(buf), "%.2f", std::get<double>(v));
        return buf;
    }
    return std::get<std::string>(v);
}

void Catalog::add_trigger(std::shared_ptr<TriggerDef> t) {
    triggers.push_back(std::move(t));
}

bool Catalog::drop_trigger(const std::string& name) {
    for (auto it = triggers.begin(); it != triggers.end(); ++it) {
        if ((*it)->name == name) {
            triggers.erase(it);
            return true;
        }
    }
    return false;
}

std::vector<TriggerDef*> Catalog::get_triggers(const std::string& table, TriggerDef::Event event) {
    std::vector<TriggerDef*> result;
    for (auto& t : triggers) {
        if (t->table_name == table && t->event == event) {
            result.push_back(t.get());
        }
    }
    return result;
}

} // namespace storage
