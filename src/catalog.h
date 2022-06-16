#pragma once

#include <unordered_map>
#include <vector>

#include "common.h"

using namespace std;

class CatalogManager {
    unordered_map<uint64_t, TableMeta*> tables_;
public:
    bool InsertTable(uint64_t tid, TableMeta* meta) {
        if (!tables_.count(tid)) {
            tables_[tid] = meta;
            return true;
        }
        return false;
    }

    TableMeta* GetTable(uint64_t tid) {
        if (!tables_.count(tid)) {
            return nullptr;
        }
        return tables_[tid];
    }
};

class DB {
public:
    static CatalogManager& GetManager() {
        static CatalogManager m;
        return m;
    }

    DB(DB const&)              = delete;
    void operator=(DB const&)  = delete;

private:
    DB() {}
};
