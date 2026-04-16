#pragma once

#include <cstdint>
#include <string>

#include "storage/storage.h"

namespace storage {

struct RecoveryStats {
    bool checkpoint_loaded = false;
    uint64_t checkpoint_lsn = 0;
    size_t records_scanned = 0;
    size_t transactions_committed = 0;
    size_t transactions_ignored = 0;
    size_t redo_records = 0;
};

class WalManager {
public:
    WalManager(const std::string& wal_path = "sqp.wal",
               const std::string& checkpoint_path = "sqp.checkpoint");

    void log_begin(uint64_t txn_id);
    void log_insert(uint64_t txn_id, const std::string& table_name, size_t row_index, const Row& after_row);
    void log_update(uint64_t txn_id, const std::string& table_name, size_t row_index,
                    const Row& before_row, const Row& after_row);
    void log_delete(uint64_t txn_id, const std::string& table_name, size_t row_index, const Row& before_row);
    void log_commit(uint64_t txn_id);
    void log_rollback(uint64_t txn_id);

    void flush();

    void checkpoint(const Catalog& catalog);
    RecoveryStats recover(Catalog& catalog);

private:
    std::string wal_path_;
    std::string checkpoint_path_;
    uint64_t next_lsn_ = 1;

    void load_next_lsn();
};

}  // namespace storage
