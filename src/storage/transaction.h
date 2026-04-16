#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "storage/storage.h"

namespace storage {

enum class UndoType {
    INSERT_ROW,
    DELETE_ROW,
    UPDATE_ROW,
};

struct UndoRecord {
    UndoType type;
    std::string table_name;
    size_t row_index = 0;
    Row before_row;
};

struct TransactionContext {
    uint64_t txn_id = 0;
    bool active = false;
    std::vector<UndoRecord> undo_log;
};

class TransactionManager {
public:
    bool in_transaction() const;
    uint64_t current_txn_id() const;

    void begin();
    void commit();
    void rollback(Catalog& catalog);

    void log_insert(const std::string& table_name, size_t row_index);
    void log_delete(const std::string& table_name, size_t row_index, const Row& deleted_row);
    void log_update(const std::string& table_name, size_t row_index, const Row& old_row);

private:
    void rebuild_indexes_for_table(Catalog& catalog, const std::string& table_name);

    TransactionContext ctx_;
    uint64_t next_txn_id_ = 1;
};

}  // namespace storage
