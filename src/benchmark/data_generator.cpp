#include "benchmark/benchmark.h"
#include <random>
#include <iostream>

namespace benchmark {

using namespace storage;

static std::mt19937 rng(42);

static std::string random_name() {
    static const char* first[] = {"Alice","Bob","Carol","Dave","Eve","Frank","Grace","Heidi","Ivan","Judy",
                                  "Karl","Liam","Mia","Noah","Olivia","Pete","Quinn","Ruth","Sam","Tina"};
    static const char* last[] = {"Smith","Jones","Brown","Taylor","Wilson","Davis","Clark","Hall","Allen","Young"};
    return std::string(first[rng() % 20]) + " " + last[rng() % 10];
}

static std::string random_dept() {
    static const char* depts[] = {"Engineering","Sales","Marketing","HR","Finance","Support","Legal","R&D","Ops","Admin"};
    return depts[rng() % 10];
}

void generate_employees(Catalog& catalog, size_t num_rows) {
    auto tbl = std::make_shared<Table>();
    tbl->name = "employees";
    tbl->schema = {
        {"id", DataType::INT},
        {"name", DataType::VARCHAR},
        {"dept", DataType::VARCHAR},
        {"salary", DataType::INT},
        {"age", DataType::INT},
    };
    tbl->rows.reserve(num_rows);
    for (size_t i = 0; i < num_rows; i++) {
        Row row;
        row.push_back((int64_t)(i + 1));
        row.push_back(random_name());
        row.push_back(random_dept());
        row.push_back((int64_t)(30000 + rng() % 120000));
        row.push_back((int64_t)(22 + rng() % 43));
        tbl->rows.push_back(std::move(row));
    }
    catalog.add_table(tbl);
    std::cout << "Generated 'employees' table with " << num_rows << " rows\n";
}

void generate_departments(Catalog& catalog, size_t num_depts) {
    auto tbl = std::make_shared<Table>();
    tbl->name = "departments";
    tbl->schema = {
        {"dept_name", DataType::VARCHAR},
        {"budget", DataType::INT},
        {"location", DataType::VARCHAR},
    };
    static const char* depts[] = {"Engineering","Sales","Marketing","HR","Finance","Support","Legal","R&D","Ops","Admin"};
    static const char* locs[] = {"NYC","SF","London","Berlin","Tokyo","Sydney","Toronto","Paris","Mumbai","Seoul"};
    for (size_t i = 0; i < num_depts && i < 10; i++) {
        Row row;
        row.push_back(std::string(depts[i]));
        row.push_back((int64_t)(100000 + rng() % 900000));
        row.push_back(std::string(locs[i]));
        tbl->rows.push_back(std::move(row));
    }
    catalog.add_table(tbl);
    std::cout << "Generated 'departments' table with " << tbl->rows.size() << " rows\n";
}

void generate_orders(Catalog& catalog, size_t num_rows) {
    auto tbl = std::make_shared<Table>();
    tbl->name = "orders";
    tbl->schema = {
        {"order_id", DataType::INT},
        {"employee_id", DataType::INT},
        {"amount", DataType::FLOAT},
        {"product", DataType::VARCHAR},
    };
    static const char* products[] = {"Laptop","Phone","Tablet","Monitor","Keyboard","Mouse","Headset","Camera"};
    tbl->rows.reserve(num_rows);
    for (size_t i = 0; i < num_rows; i++) {
        Row row;
        row.push_back((int64_t)(i + 1));
        row.push_back((int64_t)(1 + rng() % 1000));
        row.push_back((double)(10 + rng() % 5000) + (rng() % 100) / 100.0);
        row.push_back(std::string(products[rng() % 8]));
        tbl->rows.push_back(std::move(row));
    }
    catalog.add_table(tbl);
    std::cout << "Generated 'orders' table with " << num_rows << " rows\n";
}

} // namespace benchmark
