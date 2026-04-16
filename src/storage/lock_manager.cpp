#include "storage/lock_manager.h"

#include <stdexcept>

namespace storage {

void LockManager::acquire_shared(const std::string& table_name, uint64_t txn_id) {
    auto& state = table_locks_[table_name];

    if (state.exclusive_holder.has_value() && state.exclusive_holder.value() != txn_id) {
        throw std::runtime_error("Lock conflict on table '" + table_name + "': held exclusively by another transaction");
    }

    state.shared_holders.insert(txn_id);
}

void LockManager::acquire_exclusive(const std::string& table_name, uint64_t txn_id) {
    auto& state = table_locks_[table_name];

    if (state.exclusive_holder.has_value() && state.exclusive_holder.value() != txn_id) {
        throw std::runtime_error("Lock conflict on table '" + table_name + "': held exclusively by another transaction");
    }

    for (uint64_t holder : state.shared_holders) {
        if (holder != txn_id) {
            throw std::runtime_error("Lock conflict on table '" + table_name + "': held shared by another transaction");
        }
    }

    state.exclusive_holder = txn_id;
    state.shared_holders.erase(txn_id);
}

void LockManager::release_shared(const std::string& table_name, uint64_t txn_id) {
    auto it = table_locks_.find(table_name);
    if (it == table_locks_.end()) {
        return;
    }

    it->second.shared_holders.erase(txn_id);
    cleanup_empty_lock(table_name);
}

void LockManager::release_all(uint64_t txn_id) {
    for (auto& [table_name, state] : table_locks_) {
        state.shared_holders.erase(txn_id);
        if (state.exclusive_holder.has_value() && state.exclusive_holder.value() == txn_id) {
            state.exclusive_holder.reset();
        }
    }

    for (auto it = table_locks_.begin(); it != table_locks_.end();) {
        if (it->second.shared_holders.empty() && !it->second.exclusive_holder.has_value()) {
            it = table_locks_.erase(it);
        } else {
            ++it;
        }
    }
}

void LockManager::cleanup_empty_lock(const std::string& table_name) {
    auto it = table_locks_.find(table_name);
    if (it == table_locks_.end()) {
        return;
    }
    if (it->second.shared_holders.empty() && !it->second.exclusive_holder.has_value()) {
        table_locks_.erase(it);
    }
}

}  // namespace storage
