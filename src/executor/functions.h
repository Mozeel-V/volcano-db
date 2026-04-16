#pragma once

#include <string>
#include <vector>

#include "storage/storage.h"

namespace executor {

std::string normalize_function_name(const std::string& name);
bool is_aggregate_function_name(const std::string& name);
std::vector<std::string> list_builtin_scalar_function_names();

// Returns true if function name is recognized as a built-in scalar function.
// Throws std::runtime_error on arity/type validation errors for known functions.
bool try_eval_builtin_function(const std::string& name,
                               const std::vector<storage::Value>& args,
                               storage::Value& out);

} // namespace executor
