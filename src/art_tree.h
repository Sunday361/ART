#pragma once
#include <cstdint>
#include <vector>
#include <iostream>

#include "sched.h"
#include "emmintrin.h"

#include "art_key.h"
#include "common.h"
#include "art_node.h"
#include "catalog.h"

using namespace std;

template<uint16_t KeyLen>
class ART {
    using Key = KEY<KeyLen>;

    enum class Result : uint8_t {
        PARTIAL_MATCH,
        FULL_MATCH,
        NO_MATCH,
    };

    N* root_ = nullptr;
public:
    ART();

    void yield(int count) const;

    bool checkPrefix(N* n, const Key& k, uint16_t& level) const {
        for (int i = 0; i < n->getPrefixLen(); i++) {
            if (level >= k.getKeyLen() || k[level] != n->getPrefix(i)) {
                return false;
            }
            level++;
        }
        return true;
    }

    Result checkPrefixInsert(N* n, const Key& k, uint16_t& level) {
        int i;
        for (i = 0; i < n->getPrefixLen(); i++) {
            if (k[level] != n->getPrefix(i)) {
                return Result::NO_MATCH;
            }
        }


    }

    bool lookup(const Key& key, TID& tid) const ;
    bool lookup_range(const Key& k1, const Key& k2, vector<TID>& res);
    void insert(const Key& key, TID tid);
};

extern template class ART<64>;
extern template class ART<128>;
extern template class ART<256>;