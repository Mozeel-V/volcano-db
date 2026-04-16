#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>

namespace storage {

enum class LockMode {
    SHARED,
    EXCLUSIVE,
};

class LockManager {
public:
    void acquire_shared(const std::string& table_name, uint64_t txn_id);
    void acquire_exclusive(const std::string& table_name, uint64_t txn_id);

    void release_shared(const std::string& table_name, uint64_t txn_id);
    void release_all(uint64_t txn_id);

private:
    struct LockState {
        std::unordered_set<uint64_t> shared_holders;
        std::optional<uint64_t> exclusive_holder;
    };

    void cleanup_empty_lock(const std::string& table_name);

    std::unordered_map<std::string, LockState> table_locks_;
};

}  // namespace storage
