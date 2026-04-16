#include "storage/wal.h"

#include <algorithm>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace storage {

namespace {

enum class TxnRecordType {
    INSERT_ROW,
    UPDATE_ROW,
    DELETE_ROW,
};

struct TxnRecord {
    TxnRecordType type;
    std::string table_name;
    size_t row_index = 0;
    Row before_row;
    Row after_row;
};

std::string encode_value(const Value& v) {
    if (std::holds_alternative<std::monostate>(v)) return "N";
    if (std::holds_alternative<int64_t>(v)) return "I:" + std::to_string(std::get<int64_t>(v));
    if (std::holds_alternative<double>(v)) {
        std::ostringstream oss;
        oss << "F:" << std::setprecision(17) << std::get<double>(v);
        return oss.str();
    }
    return "S:" + std::get<std::string>(v);
}

Value decode_value(const std::string& token) {
    if (token == "N") return std::monostate{};
    if (token.rfind("I:", 0) == 0) return static_cast<int64_t>(std::stoll(token.substr(2)));
    if (token.rfind("F:", 0) == 0) return std::stod(token.substr(2));
    if (token.rfind("S:", 0) == 0) return token.substr(2);
    return std::monostate{};
}

void write_row(std::ostream& os, const Row& row) {
    os << row.size();
    for (const auto& v : row) {
        os << ' ' << std::quoted(encode_value(v));
    }
}

bool read_row(std::istream& is, Row& out_row) {
    size_t nvals = 0;
    if (!(is >> nvals)) return false;
    out_row.clear();
    out_row.reserve(nvals);
    for (size_t i = 0; i < nvals; i++) {
        std::string tok;
        if (!(is >> std::quoted(tok))) return false;
        out_row.push_back(decode_value(tok));
    }
    return true;
}

void rebuild_indexes_for_table(Catalog& catalog, const std::string& table_name) {
    Table* tbl = catalog.get_table(table_name);
    if (!tbl) return;

    for (auto& [_, idx] : catalog.indexes) {
        if (idx->table_name == table_name) idx->build(*tbl);
    }
    for (auto& [_, idx] : catalog.btree_indexes) {
        if (idx->table_name == table_name) idx->build(*tbl);
    }
}

void save_checkpoint_file(const std::string& checkpoint_path,
                         const Catalog& catalog,
                         uint64_t checkpoint_lsn) {
    std::ofstream out(checkpoint_path, std::ios::out | std::ios::trunc);
    if (!out.is_open()) {
        throw std::runtime_error("Could not write checkpoint file: " + checkpoint_path);
    }

    out << "SNP1 " << checkpoint_lsn << "\n";
    out << catalog.tables.size() << "\n";

    std::vector<std::string> table_names;
    table_names.reserve(catalog.tables.size());
    for (const auto& [name, _] : catalog.tables) table_names.push_back(name);
    std::sort(table_names.begin(), table_names.end());

    for (const auto& table_name : table_names) {
        const auto& tbl = *catalog.tables.at(table_name);
        out << "TABLE " << std::quoted(tbl.name) << ' '
            << tbl.schema.size() << ' '
            << tbl.rows.size() << ' '
            << tbl.foreign_keys.size() << "\n";

        for (const auto& col : tbl.schema) {
            out << "COL " << std::quoted(col.name) << ' '
                << static_cast<int>(col.type) << ' '
                << static_cast<int>(col.not_null) << ' '
                << static_cast<int>(col.primary_key) << ' '
                << static_cast<int>(col.is_unique) << ' '
                << static_cast<int>(col.has_default) << ' '
                << std::quoted(encode_value(col.default_value)) << "\n";
        }

        for (const auto& fk : tbl.foreign_keys) {
            out << "FK " << std::quoted(fk.column_name) << ' '
                << std::quoted(fk.ref_table) << ' '
                << std::quoted(fk.ref_column) << ' '
                << static_cast<int>(fk.on_delete) << "\n";
        }

        for (const auto& row : tbl.rows) {
            out << "ROW ";
            write_row(out, row);
            out << "\n";
        }

        out << "END_TABLE\n";
    }

    out << "HASH_INDEXES " << catalog.indexes.size() << "\n";
    for (const auto& [_, idx] : catalog.indexes) {
        out << "HASH " << std::quoted(idx->table_name) << ' '
            << std::quoted(idx->column_name) << "\n";
    }

    out << "BTREE_INDEXES " << catalog.btree_indexes.size() << "\n";
    for (const auto& [_, idx] : catalog.btree_indexes) {
        out << "BTREE " << std::quoted(idx->table_name) << ' '
            << std::quoted(idx->column_name) << "\n";
    }
}

bool load_checkpoint_file(const std::string& checkpoint_path,
                         Catalog& catalog,
                         uint64_t& checkpoint_lsn) {
    std::ifstream in(checkpoint_path);
    if (!in.is_open()) return false;

    std::string magic;
    if (!(in >> magic >> checkpoint_lsn) || magic != "SNP1") return false;

    size_t table_count = 0;
    if (!(in >> table_count)) return false;

    catalog.tables.clear();
    catalog.indexes.clear();
    catalog.btree_indexes.clear();
    catalog.views.clear();
    catalog.triggers.clear();

    for (size_t t = 0; t < table_count; t++) {
        std::string tok;
        if (!(in >> tok) || tok != "TABLE") return false;

        auto tbl = std::make_shared<Table>();
        size_t ncols = 0, nrows = 0, nfks = 0;
        if (!(in >> std::quoted(tbl->name) >> ncols >> nrows >> nfks)) return false;

        tbl->schema.reserve(ncols);
        for (size_t c = 0; c < ncols; c++) {
            std::string col_tok;
            if (!(in >> col_tok) || col_tok != "COL") return false;

            ColumnSchema col;
            int type_i = 0;
            int not_null_i = 0, pk_i = 0, uniq_i = 0, has_default_i = 0;
            std::string def_tok;
            if (!(in >> std::quoted(col.name)
                    >> type_i
                    >> not_null_i
                    >> pk_i
                    >> uniq_i
                    >> has_default_i
                    >> std::quoted(def_tok))) {
                return false;
            }

            col.type = static_cast<DataType>(type_i);
            col.not_null = (not_null_i != 0);
            col.primary_key = (pk_i != 0);
            col.is_unique = (uniq_i != 0);
            col.has_default = (has_default_i != 0);
            if (col.has_default) {
                col.default_value = decode_value(def_tok);
            }
            tbl->schema.push_back(col);
        }

        tbl->foreign_keys.reserve(nfks);
        for (size_t f = 0; f < nfks; f++) {
            std::string fk_tok;
            if (!(in >> fk_tok) || fk_tok != "FK") return false;

            ForeignKeyDef fk;
            int on_delete_i = 0;
            if (!(in >> std::quoted(fk.column_name)
                    >> std::quoted(fk.ref_table)
                    >> std::quoted(fk.ref_column)
                    >> on_delete_i)) {
                return false;
            }
            fk.on_delete = static_cast<FkDeleteAction>(on_delete_i);
            tbl->foreign_keys.push_back(fk);
        }

        tbl->rows.reserve(nrows);
        for (size_t r = 0; r < nrows; r++) {
            std::string row_tok;
            if (!(in >> row_tok) || row_tok != "ROW") return false;
            Row row;
            if (!read_row(in, row)) return false;
            tbl->rows.push_back(std::move(row));
        }

        std::string end_tok;
        if (!(in >> end_tok) || end_tok != "END_TABLE") return false;

        catalog.add_table(tbl);
    }

    size_t hash_count = 0;
    std::string hash_hdr;
    if (!(in >> hash_hdr >> hash_count)) return false;
    if (hash_hdr != "HASH_INDEXES") return false;

    for (size_t i = 0; i < hash_count; i++) {
        std::string tok;
        std::string table_name;
        std::string column_name;
        if (!(in >> tok >> std::quoted(table_name) >> std::quoted(column_name))) return false;
        if (tok != "HASH") return false;

        auto* tbl = catalog.get_table(table_name);
        if (!tbl) continue;

        auto idx = std::make_shared<HashIndex>();
        idx->table_name = table_name;
        idx->column_name = column_name;
        idx->build(*tbl);
        catalog.indexes[table_name + "." + column_name] = idx;
    }

    size_t btree_count = 0;
    std::string btree_hdr;
    if (!(in >> btree_hdr >> btree_count)) return false;
    if (btree_hdr != "BTREE_INDEXES") return false;

    for (size_t i = 0; i < btree_count; i++) {
        std::string tok;
        std::string table_name;
        std::string column_name;
        if (!(in >> tok >> std::quoted(table_name) >> std::quoted(column_name))) return false;
        if (tok != "BTREE") return false;

        auto* tbl = catalog.get_table(table_name);
        if (!tbl) continue;

        auto idx = std::make_shared<BTreeIndex>();
        idx->table_name = table_name;
        idx->column_name = column_name;
        idx->build(*tbl);
        catalog.btree_indexes[table_name + "." + column_name] = idx;
    }

    return true;
}

void apply_txn_record(const TxnRecord& rec, Catalog& catalog) {
    Table* tbl = catalog.get_table(rec.table_name);
    if (!tbl) return;

    switch (rec.type) {
        case TxnRecordType::INSERT_ROW:
            if (rec.row_index <= tbl->rows.size()) {
                tbl->rows.insert(tbl->rows.begin() + static_cast<std::ptrdiff_t>(rec.row_index), rec.after_row);
            } else {
                tbl->rows.push_back(rec.after_row);
            }
            break;
        case TxnRecordType::UPDATE_ROW:
            if (rec.row_index < tbl->rows.size()) {
                tbl->rows[rec.row_index] = rec.after_row;
            }
            break;
        case TxnRecordType::DELETE_ROW:
            if (rec.row_index < tbl->rows.size()) {
                tbl->rows.erase(tbl->rows.begin() + static_cast<std::ptrdiff_t>(rec.row_index));
            }
            break;
    }
}

}  // namespace

WalManager::WalManager(const std::string& wal_path, const std::string& checkpoint_path)
    : wal_path_(wal_path), checkpoint_path_(checkpoint_path) {
    load_next_lsn();
}

void WalManager::load_next_lsn() {
    std::ifstream in(wal_path_);
    if (!in.is_open()) {
        next_lsn_ = 1;
        return;
    }

    uint64_t max_lsn = 0;
    std::string line;
    while (std::getline(in, line)) {
        if (line.empty()) continue;
        std::istringstream iss(line);
        uint64_t lsn = 0;
        if (iss >> lsn) {
            if (lsn > max_lsn) max_lsn = lsn;
        }
    }
    next_lsn_ = max_lsn + 1;
}

void WalManager::log_begin(uint64_t txn_id) {
    std::ofstream out(wal_path_, std::ios::out | std::ios::app);
    if (!out.is_open()) throw std::runtime_error("Could not open WAL for append");
    out << next_lsn_++ << " BEGIN " << txn_id << "\n";
}

void WalManager::log_insert(uint64_t txn_id,
                            const std::string& table_name,
                            size_t row_index,
                            const Row& after_row) {
    std::ofstream out(wal_path_, std::ios::out | std::ios::app);
    if (!out.is_open()) throw std::runtime_error("Could not open WAL for append");
    out << next_lsn_++ << " INSERT " << txn_id << ' ' << std::quoted(table_name) << ' ' << row_index << ' ';
    write_row(out, after_row);
    out << "\n";
}

void WalManager::log_update(uint64_t txn_id,
                            const std::string& table_name,
                            size_t row_index,
                            const Row& before_row,
                            const Row& after_row) {
    std::ofstream out(wal_path_, std::ios::out | std::ios::app);
    if (!out.is_open()) throw std::runtime_error("Could not open WAL for append");
    out << next_lsn_++ << " UPDATE " << txn_id << ' ' << std::quoted(table_name) << ' ' << row_index << ' ';
    write_row(out, before_row);
    out << ' ';
    write_row(out, after_row);
    out << "\n";
}

void WalManager::log_delete(uint64_t txn_id,
                            const std::string& table_name,
                            size_t row_index,
                            const Row& before_row) {
    std::ofstream out(wal_path_, std::ios::out | std::ios::app);
    if (!out.is_open()) throw std::runtime_error("Could not open WAL for append");
    out << next_lsn_++ << " DELETE " << txn_id << ' ' << std::quoted(table_name) << ' ' << row_index << ' ';
    write_row(out, before_row);
    out << "\n";
}

void WalManager::log_commit(uint64_t txn_id) {
    std::ofstream out(wal_path_, std::ios::out | std::ios::app);
    if (!out.is_open()) throw std::runtime_error("Could not open WAL for append");
    out << next_lsn_++ << " COMMIT " << txn_id << "\n";
}

void WalManager::log_rollback(uint64_t txn_id) {
    std::ofstream out(wal_path_, std::ios::out | std::ios::app);
    if (!out.is_open()) throw std::runtime_error("Could not open WAL for append");
    out << next_lsn_++ << " ROLLBACK " << txn_id << "\n";
}

void WalManager::flush() {
    std::ofstream out(wal_path_, std::ios::out | std::ios::app);
    if (!out.is_open()) throw std::runtime_error("Could not open WAL for flush");
    out.flush();
}

void WalManager::checkpoint(const Catalog& catalog) {
    uint64_t checkpoint_lsn = (next_lsn_ > 0) ? (next_lsn_ - 1) : 0;
    save_checkpoint_file(checkpoint_path_, catalog, checkpoint_lsn);

    std::ofstream out(wal_path_, std::ios::out | std::ios::app);
    if (!out.is_open()) throw std::runtime_error("Could not open WAL for append");
    out << next_lsn_++ << " CHECKPOINT " << checkpoint_lsn << "\n";
}

RecoveryStats WalManager::recover(Catalog& catalog) {
    RecoveryStats stats;

    uint64_t checkpoint_lsn = 0;
    if (load_checkpoint_file(checkpoint_path_, catalog, checkpoint_lsn)) {
        stats.checkpoint_loaded = true;
        stats.checkpoint_lsn = checkpoint_lsn;
    }

    std::ifstream in(wal_path_);
    if (!in.is_open()) {
        load_next_lsn();
        return stats;
    }

    std::unordered_map<uint64_t, std::vector<TxnRecord>> txn_records;
    std::unordered_set<uint64_t> rolled_back;
    std::vector<uint64_t> committed_order;

    std::string line;
    while (std::getline(in, line)) {
        if (line.empty()) continue;

        std::istringstream iss(line);
        uint64_t lsn = 0;
        std::string type;
        if (!(iss >> lsn >> type)) continue;

        if (lsn <= checkpoint_lsn) continue;
        stats.records_scanned++;

        if (type == "BEGIN") {
            uint64_t txn_id = 0;
            if (iss >> txn_id) {
                txn_records[txn_id];
            }
            continue;
        }

        if (type == "COMMIT") {
            uint64_t txn_id = 0;
            if (iss >> txn_id) {
                committed_order.push_back(txn_id);
            }
            continue;
        }

        if (type == "ROLLBACK") {
            uint64_t txn_id = 0;
            if (iss >> txn_id) {
                rolled_back.insert(txn_id);
            }
            continue;
        }

        if (type == "CHECKPOINT") {
            continue;
        }

        uint64_t txn_id = 0;
        std::string table_name;
        size_t row_index = 0;
        if (!(iss >> txn_id >> std::quoted(table_name) >> row_index)) continue;

        TxnRecord rec;
        rec.table_name = table_name;
        rec.row_index = row_index;

        if (type == "INSERT") {
            rec.type = TxnRecordType::INSERT_ROW;
            if (!read_row(iss, rec.after_row)) continue;
            txn_records[txn_id].push_back(std::move(rec));
            continue;
        }

        if (type == "UPDATE") {
            rec.type = TxnRecordType::UPDATE_ROW;
            if (!read_row(iss, rec.before_row)) continue;
            if (!read_row(iss, rec.after_row)) continue;
            txn_records[txn_id].push_back(std::move(rec));
            continue;
        }

        if (type == "DELETE") {
            rec.type = TxnRecordType::DELETE_ROW;
            if (!read_row(iss, rec.before_row)) continue;
            txn_records[txn_id].push_back(std::move(rec));
            continue;
        }
    }

    for (uint64_t txn_id : committed_order) {
        auto it = txn_records.find(txn_id);
        if (it == txn_records.end()) continue;
        if (rolled_back.count(txn_id) > 0) {
            stats.transactions_ignored++;
            continue;
        }

        std::unordered_set<std::string> touched_tables;
        for (const auto& rec : it->second) {
            apply_txn_record(rec, catalog);
            touched_tables.insert(rec.table_name);
            stats.redo_records++;
        }
        for (const auto& table_name : touched_tables) {
            rebuild_indexes_for_table(catalog, table_name);

            std::string index_error;
            if (!catalog.validate_table_indexes(table_name, &index_error)) {
                throw std::runtime_error("Index consistency check failed during recovery: " + index_error);
            }
        }

        stats.transactions_committed++;
    }

    std::string full_validation_error;
    if (!catalog.validate_all_indexes(&full_validation_error)) {
        throw std::runtime_error("Global index consistency check failed after recovery: " + full_validation_error);
    }

    load_next_lsn();
    return stats;
}

}  // namespace storage
