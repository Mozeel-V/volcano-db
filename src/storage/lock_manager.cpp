#include "storage/lock_manager.h"

#include <stdexcept>

namespace storage {

std::string LockManager::build_conflict_message(const std::string& table_name,
                                                LockMode requested_mode,
                                                uint64_t requester_txn,
                                                const LockState& state) const {
    std::ostringstream oss;
    oss << "Lock conflict on table '" << table_name << "': txn " << requester_txn
        << " requested " << (requested_mode == LockMode::SHARED ? "SHARED" : "EXCLUSIVE");

    if (state.exclusive_holder.has_value() && state.exclusive_holder.value() != requester_txn) {
        oss << " but EXCLUSIVE is held by txn " << state.exclusive_holder.value();
    } else {
        bool found_blocker = false;
        uint64_t blocker_txn = 0;
        for (uint64_t holder : state.shared_holders) {
            if (holder == requester_txn) continue;
            if (!found_blocker || holder < blocker_txn) {
                blocker_txn = holder;
                found_blocker = true;
            }
        }
        if (found_blocker) {
            oss << " but SHARED is held by txn " << blocker_txn;
        } else {
            oss << " but lock is held by another transaction";
        }
    }

    oss << " (policy: immediate abort, no wait)";
    return oss.str();
}

void LockManager::acquire_shared(const std::string& table_name, uint64_t txn_id) {
    auto& state = table_locks_[table_name];

    if (state.exclusive_holder.has_value() && state.exclusive_holder.value() != txn_id) {
        throw std::runtime_error(build_conflict_message(table_name, LockMode::SHARED, txn_id, state));
    }

    state.shared_holders.insert(txn_id);
}

void LockManager::acquire_exclusive(const std::string& table_name, uint64_t txn_id) {
    auto& state = table_locks_[table_name];

    if (state.exclusive_holder.has_value() && state.exclusive_holder.value() != txn_id) {
        throw std::runtime_error(build_conflict_message(table_name, LockMode::EXCLUSIVE, txn_id, state));
    }

    for (uint64_t holder : state.shared_holders) {
        if (holder != txn_id) {
            throw std::runtime_error(build_conflict_message(table_name, LockMode::EXCLUSIVE, txn_id, state));
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
