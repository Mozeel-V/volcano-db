#include "storage/storage.h"

namespace storage {

int Table::column_index(const std::string& col_name) const {
    for (size_t i = 0; i < schema.size(); i++) {
        if (schema[i].name == col_name) return (int)i;
    }
    return -1;
}

void Table::insert_row(const Row& row) {
    if (row.size() != schema.size()) {
        throw std::runtime_error("Row size mismatch for table " + name);
    }
    rows.push_back(row);
}

void Table::load_csv(const std::string& path) {
    std::ifstream file(path);
    if (!file.is_open()) {
        throw std::runtime_error("Cannot open file: " + path);
    }
    std::string line;
    // Skip header line
    if (!std::getline(file, line)) return;

    while (std::getline(file, line)) {
        if (line.empty()) continue;
        std::istringstream ss(line);
        Row row;
        std::string cell;
        size_t col = 0;
        while (std::getline(ss, cell, ',') && col < schema.size()) {
            // Trim whitespace
            size_t start = cell.find_first_not_of(" \t\r\n");
            size_t end = cell.find_last_not_of(" \t\r\n");
            if (start == std::string::npos) {
                row.push_back(std::monostate{});
            } else {
                cell = cell.substr(start, end - start + 1);
                if (cell == "NULL" || cell == "null" || cell.empty()) {
                    row.push_back(std::monostate{});
                } else if (schema[col].type == DataType::INT) {
                    row.push_back((int64_t)std::stoll(cell));
                } else if (schema[col].type == DataType::FLOAT) {
                    row.push_back(std::stod(cell));
                } else {
                    row.push_back(cell);
                }
            }
            col++;
        }
        while (row.size() < schema.size()) row.push_back(std::monostate{});
        rows.push_back(std::move(row));
    }
}

size_t Table::distinct_values(const std::string& col) const {
    int idx = column_index(col);
    if (idx < 0) return 0;
    std::vector<std::string> vals;
    for (auto& row : rows) {
        vals.push_back(value_display(row[idx]));
    }
    std::sort(vals.begin(), vals.end());
    vals.erase(std::unique(vals.begin(), vals.end()), vals.end());
    return vals.size();
}

void Table::print_rows(const std::vector<std::string>& columns, const std::vector<Row>& result_rows, int max_rows) const {
    // Print header
    for (size_t i = 0; i < columns.size(); i++) {
        if (i) std::cout << " | ";
        std::cout << columns[i];
    }
    std::cout << "\n";
    for (size_t i = 0; i < columns.size(); i++) {
        if (i) std::cout << "-+-";
        std::cout << "----------";
    }
    std::cout << "\n";
    int count = 0;
    for (auto& row : result_rows) {
        if (max_rows >= 0 && count >= max_rows) {
            std::cout << "... (" << result_rows.size() - max_rows << " more rows)\n";
            break;
        }
        for (size_t i = 0; i < row.size() && i < columns.size(); i++) {
            if (i) std::cout << " | ";
            std::cout << value_display(row[i]);
        }
        std::cout << "\n";
        count++;
    }
    std::cout << "(" << result_rows.size() << " rows)\n";
}

} // namespace storage
