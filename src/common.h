#pragma once

#include <cstdint>
#include <vector>

using namespace std;

#define ASSERT(condition, message) \
    do { \
        if (! (condition)) { \
            std::cerr << "Assertion `" #condition "` failed in " << __FILE__ \
                      << " line " << __LINE__ << ": " << message << std::endl; \
            std::terminate(); \
        } \
    } while (false)

using TID = uint64_t;

enum type {
    NT4,
    NT16,
    NT48,
    NT256,
};

enum status {
    UNLOCK = 0b00,
    DELETE = 0b01,
    LOCK = 0b10,
};

struct IndexMeta {
    vector<uint16_t>  col_ids;
    struct TableMeta* table;
};

struct TableMeta {
    vector<IndexMeta*> index_metas;
    vector<uint16_t>   attr_size_;
};
