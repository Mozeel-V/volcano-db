#include "executor/functions.h"

#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstdlib>
#include <limits>
#include <stdexcept>

namespace executor {

using storage::Value;

std::string normalize_function_name(const std::string& name) {
    std::string normalized = name;
    std::transform(normalized.begin(), normalized.end(), normalized.begin(),
                   [](unsigned char c) { return static_cast<char>(std::toupper(c)); });
    return normalized;
}

bool is_aggregate_function_name(const std::string& name) {
    const std::string fn = normalize_function_name(name);
    return fn == "COUNT" || fn == "SUM" || fn == "AVG" || fn == "MIN" || fn == "MAX";
}

std::vector<std::string> list_builtin_scalar_function_names() {
    return {
        "ABS",
        "CEIL",
        "CEILING",
        "COALESCE",
        "FLOOR",
        "LENGTH",
        "LOWER",
        "NULLIF",
        "ROUND",
        "SUBSTR",
        "TRIM",
        "UPPER",
    };
}

static void require_arity(const std::string& fn, size_t got, size_t min_arity, size_t max_arity) {
    if (got < min_arity || got > max_arity) {
        if (min_arity == max_arity) {
            throw std::runtime_error("Function " + fn + " expects " + std::to_string(min_arity) +
                                     " argument(s), got " + std::to_string(got));
        }
        throw std::runtime_error("Function " + fn + " expects between " + std::to_string(min_arity) +
                                 " and " + std::to_string(max_arity) + " argument(s), got " +
                                 std::to_string(got));
    }
}

static std::string to_lower_ascii(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return s;
}

static std::string to_upper_ascii(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(),
                   [](unsigned char c) { return static_cast<char>(std::toupper(c)); });
    return s;
}

static std::string trim_ascii(const std::string& s) {
    size_t start = 0;
    while (start < s.size() && std::isspace(static_cast<unsigned char>(s[start]))) start++;
    size_t end = s.size();
    while (end > start && std::isspace(static_cast<unsigned char>(s[end - 1]))) end--;
    return s.substr(start, end - start);
}

bool try_eval_builtin_function(const std::string& name,
                               const std::vector<Value>& args,
                               Value& out) {
    const std::string fn = normalize_function_name(name);

    if (fn == "LOWER") {
        require_arity(fn, args.size(), 1, 1);
        if (storage::value_is_null(args[0])) {
            out = std::monostate{};
        } else {
            out = to_lower_ascii(storage::value_to_string(args[0]));
        }
        return true;
    }

    if (fn == "UPPER") {
        require_arity(fn, args.size(), 1, 1);
        if (storage::value_is_null(args[0])) {
            out = std::monostate{};
        } else {
            out = to_upper_ascii(storage::value_to_string(args[0]));
        }
        return true;
    }

    if (fn == "LENGTH") {
        require_arity(fn, args.size(), 1, 1);
        if (storage::value_is_null(args[0])) {
            out = std::monostate{};
        } else {
            out = static_cast<int64_t>(storage::value_to_string(args[0]).size());
        }
        return true;
    }

    if (fn == "TRIM") {
        require_arity(fn, args.size(), 1, 1);
        if (storage::value_is_null(args[0])) {
            out = std::monostate{};
        } else {
            out = trim_ascii(storage::value_to_string(args[0]));
        }
        return true;
    }

    if (fn == "SUBSTR") {
        require_arity(fn, args.size(), 2, 3);
        if (storage::value_is_null(args[0]) || storage::value_is_null(args[1])) {
            out = std::monostate{};
            return true;
        }

        std::string s = storage::value_to_string(args[0]);
        int64_t start = storage::value_to_int(args[1]);
        int64_t length = std::numeric_limits<int64_t>::max();

        if (args.size() == 3) {
            if (storage::value_is_null(args[2])) {
                out = std::monostate{};
                return true;
            }
            length = storage::value_to_int(args[2]);
            if (length <= 0) {
                out = std::string("");
                return true;
            }
        }

        int64_t idx = 0;
        if (start > 0) {
            idx = start - 1;
        } else if (start < 0) {
            idx = static_cast<int64_t>(s.size()) + start;
        }

        if (idx < 0) idx = 0;
        if (idx >= static_cast<int64_t>(s.size())) {
            out = std::string("");
            return true;
        }

        size_t uidx = static_cast<size_t>(idx);
        size_t max_len = s.size() - uidx;
        size_t ulen = static_cast<size_t>(std::min<int64_t>(length, static_cast<int64_t>(max_len)));
        out = s.substr(uidx, ulen);
        return true;
    }

    if (fn == "ABS") {
        require_arity(fn, args.size(), 1, 1);
        if (storage::value_is_null(args[0])) {
            out = std::monostate{};
        } else if (std::holds_alternative<int64_t>(args[0])) {
            out = std::llabs(std::get<int64_t>(args[0]));
        } else {
            out = std::fabs(storage::value_to_double(args[0]));
        }
        return true;
    }

    if (fn == "ROUND") {
        require_arity(fn, args.size(), 1, 2);
        if (storage::value_is_null(args[0])) {
            out = std::monostate{};
            return true;
        }

        double v = storage::value_to_double(args[0]);
        if (args.size() == 1) {
            out = static_cast<int64_t>(std::llround(v));
        } else {
            if (storage::value_is_null(args[1])) {
                out = std::monostate{};
                return true;
            }
            int digits = static_cast<int>(storage::value_to_int(args[1]));
            double scale = std::pow(10.0, digits);
            out = std::round(v * scale) / scale;
        }
        return true;
    }

    if (fn == "CEIL" || fn == "CEILING") {
        require_arity(fn, args.size(), 1, 1);
        if (storage::value_is_null(args[0])) {
            out = std::monostate{};
        } else if (std::holds_alternative<int64_t>(args[0])) {
            out = std::get<int64_t>(args[0]);
        } else {
            out = static_cast<int64_t>(std::ceil(storage::value_to_double(args[0])));
        }
        return true;
    }

    if (fn == "FLOOR") {
        require_arity(fn, args.size(), 1, 1);
        if (storage::value_is_null(args[0])) {
            out = std::monostate{};
        } else if (std::holds_alternative<int64_t>(args[0])) {
            out = std::get<int64_t>(args[0]);
        } else {
            out = static_cast<int64_t>(std::floor(storage::value_to_double(args[0])));
        }
        return true;
    }

    if (fn == "COALESCE") {
        if (args.empty()) {
            throw std::runtime_error("Function COALESCE expects at least 1 argument");
        }
        out = std::monostate{};
        for (const auto& v : args) {
            if (!storage::value_is_null(v)) {
                out = v;
                break;
            }
        }
        return true;
    }

    if (fn == "NULLIF") {
        require_arity(fn, args.size(), 2, 2);
        if (storage::value_is_null(args[0]) || storage::value_is_null(args[1])) {
            out = args[0];
        } else {
            out = storage::value_equal(args[0], args[1]) ? Value(std::monostate{}) : args[0];
        }
        return true;
    }

    return false;
}

} // namespace executor
