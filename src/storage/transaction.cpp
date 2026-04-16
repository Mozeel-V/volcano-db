#include "storage/transaction.h"

#include <stdexcept>
#include <unordered_set>

namespace storage {

bool TransactionManager::in_transaction() const {
    return ctx_.active;
}

uint64_t TransactionManager::current_txn_id() const {
    return ctx_.txn_id;
}

void TransactionManager::begin() {
    if (ctx_.active) {
        throw std::runtime_error("Transaction already active");
    }
    ctx_.active = true;
    ctx_.txn_id = next_txn_id_++;
    ctx_.undo_log.clear();
}

void TransactionManager::commit() {
    if (!ctx_.active) {
        throw std::runtime_error("No active transaction");
    }
    ctx_.undo_log.clear();
    ctx_.active = false;
}

void TransactionManager::rollback(Catalog& catalog) {
    if (!ctx_.active) {
        throw std::runtime_error("No active transaction");
    }

    std::unordered_set<std::string> touched_tables;

    for (auto it = ctx_.undo_log.rbegin(); it != ctx_.undo_log.rend(); ++it) {
        Table* tbl = catalog.get_table(it->table_name);
        if (!tbl) {
            continue;
        }

        touched_tables.insert(it->table_name);

        switch (it->type) {
            case UndoType::INSERT_ROW: {
                if (it->row_index < tbl->rows.size()) {
                    tbl->rows.erase(tbl->rows.begin() + static_cast<std::ptrdiff_t>(it->row_index));
                } else if (!tbl->rows.empty()) {
                    tbl->rows.pop_back();
                }
                break;
            }
            case UndoType::DELETE_ROW: {
                if (it->row_index <= tbl->rows.size()) {
                    tbl->rows.insert(tbl->rows.begin() + static_cast<std::ptrdiff_t>(it->row_index), it->before_row);
                } else {
                    tbl->rows.push_back(it->before_row);
                }
                break;
            }
            case UndoType::UPDATE_ROW: {
                if (it->row_index < tbl->rows.size()) {
                    tbl->rows[it->row_index] = it->before_row;
                }
                break;
            }
        }
    }

    for (const auto& table_name : touched_tables) {
        rebuild_indexes_for_table(catalog, table_name);

        std::string index_error;
        if (!catalog.validate_table_indexes(table_name, &index_error)) {
            throw std::runtime_error("Index consistency check failed after rollback: " + index_error);
        }
    }

    ctx_.undo_log.clear();
    ctx_.active = false;
}

void TransactionManager::log_insert(const std::string& table_name, size_t row_index) {
    if (!ctx_.active) {
        return;
    }
    UndoRecord rec;
    rec.type = UndoType::INSERT_ROW;
    rec.table_name = table_name;
    rec.row_index = row_index;
    ctx_.undo_log.push_back(std::move(rec));
}

void TransactionManager::log_delete(const std::string& table_name, size_t row_index, const Row& deleted_row) {
    if (!ctx_.active) {
        return;
    }
    UndoRecord rec;
    rec.type = UndoType::DELETE_ROW;
    rec.table_name = table_name;
    rec.row_index = row_index;
    rec.before_row = deleted_row;
    ctx_.undo_log.push_back(std::move(rec));
}

void TransactionManager::log_update(const std::string& table_name, size_t row_index, const Row& old_row) {
    if (!ctx_.active) {
        return;
    }
    UndoRecord rec;
    rec.type = UndoType::UPDATE_ROW;
    rec.table_name = table_name;
    rec.row_index = row_index;
    rec.before_row = old_row;
    ctx_.undo_log.push_back(std::move(rec));
}

void TransactionManager::rebuild_indexes_for_table(Catalog& catalog, const std::string& table_name) {
    Table* tbl = catalog.get_table(table_name);
    if (!tbl) {
        return;
    }

    for (auto& [_, idx] : catalog.indexes) {
        if (idx->table_name == table_name) {
            idx->build(*tbl);
        }
    }
    for (auto& [_, idx] : catalog.btree_indexes) {
        if (idx->table_name == table_name) {
            idx->build(*tbl);
        }
    }
}

}  // namespace storage
