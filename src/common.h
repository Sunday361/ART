#pragma once

#include <cstdint>
#include <vector>

using namespace std;

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
