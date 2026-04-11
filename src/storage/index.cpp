#include "storage/storage.h"

namespace storage {

void HashIndex::build(const Table& table) {
    int col_idx = table.column_index(column_name);
    if (col_idx < 0) throw std::runtime_error("Column not found for index: " + column_name);

    int_map.clear();
    str_map.clear();

    for (size_t i = 0; i < table.rows.size(); i++) {
        const Value& v = table.rows[i][col_idx];
        if (value_is_null(v)) continue;
        if (std::holds_alternative<int64_t>(v)) {
            int_map[std::get<int64_t>(v)].push_back(i);
        } else if (std::holds_alternative<std::string>(v)) {
            str_map[std::get<std::string>(v)].push_back(i);
        } else if (std::holds_alternative<double>(v)) {
            int_map[(int64_t)std::get<double>(v)].push_back(i);
        }
    }
}

std::vector<size_t> HashIndex::lookup_int(int64_t key) const {
    auto it = int_map.find(key);
    if (it == int_map.end()) return {};
    return it->second;
}

std::vector<size_t> HashIndex::lookup_str(const std::string& key) const {
    auto it = str_map.find(key);
    if (it == str_map.end()) return {};
    return it->second;
}

// ───── BTreeIndex implementation ─────

void BTreeIndex::build(const Table& table) {
    int col_idx = table.column_index(column_name);
    if (col_idx < 0) throw std::runtime_error("Column not found for index: " + column_name);

    tree.clear();
    for (size_t i = 0; i < table.rows.size(); i++) {
        const Value& v = table.rows[i][col_idx];
        if (value_is_null(v)) continue;
        tree[v].push_back(i);
    }
}

void BTreeIndex::insert_entry(const Value& key, size_t row_idx) {
    tree[key].push_back(row_idx);
}

std::vector<size_t> BTreeIndex::lookup_exact(const Value& key) const {
    auto it = tree.find(key);
    if (it == tree.end()) return {};
    return it->second;
}

std::vector<size_t> BTreeIndex::lookup_range(const Value& low, const Value& high) const {
    std::vector<size_t> result;
    auto lo_it = tree.lower_bound(low);
    for (auto it = lo_it; it != tree.end(); ++it) {
        if (value_less(high, it->first)) break;
        result.insert(result.end(), it->second.begin(), it->second.end());
    }
    return result;
}

std::vector<size_t> BTreeIndex::lookup_lt(const Value& val) const {
    std::vector<size_t> result;
    for (auto it = tree.begin(); it != tree.end(); ++it) {
        if (!value_less(it->first, val)) break;
        result.insert(result.end(), it->second.begin(), it->second.end());
    }
    return result;
}

std::vector<size_t> BTreeIndex::lookup_gt(const Value& val) const {
    std::vector<size_t> result;
    auto it = tree.upper_bound(val);
    for (; it != tree.end(); ++it) {
        result.insert(result.end(), it->second.begin(), it->second.end());
    }
    return result;
}

std::vector<size_t> BTreeIndex::lookup_lte(const Value& val) const {
    std::vector<size_t> result;
    for (auto it = tree.begin(); it != tree.end(); ++it) {
        if (value_less(val, it->first)) break;
        result.insert(result.end(), it->second.begin(), it->second.end());
    }
    return result;
}

std::vector<size_t> BTreeIndex::lookup_gte(const Value& val) const {
    std::vector<size_t> result;
    auto it = tree.lower_bound(val);
    for (; it != tree.end(); ++it) {
        result.insert(result.end(), it->second.begin(), it->second.end());
    }
    return result;
}

} // namespace storage
