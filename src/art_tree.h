#pragma once
#include <cstdint>
#include <vector>
#include <iostream>
#include <algorithm>

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

    static uint16_t min(uint16_t a, uint16_t b) {
        return a > b ? b : a;
    }

    N* root_ = nullptr;
public:
    ART();

    void yield(int count) const;

    bool checkPrefix(N* n, const Key& k, uint16_t& level) const {
        for (int i = 0; i < n->getPrefixLen(); i++) {
            if (level >= k.getKeyLen() || k[level] != n->getPrefix()[i]) {
                return false;
            }
            level++;
        }
        return true;
    }

    bool checkPrefix(N* n, const Key& k, uint16_t& level, uint8_t& no_match_key) const {
        for (int i = 0; i < n->getPrefixLen(); i++) {
            if (k[level] != n->getPrefix()[i]) {
                no_match_key = n->getPrefix()[i];
                return false;
            }
            level++;
        }
        return true;
    }

    N* GenNewNode(const Key& key, uint16_t &level, TID tid) {
        N *n;
        if (level == key.getKeyLen() - 1) {
            return (N*)tid;
        } else {
            uint16_t p_len;
            if (level < key.getKeyLen()) {
                n = new N4();
                p_len = min(MAX_PREFIX_LEN, key.getKeyLen() - level - 1);
                level += p_len;
                n->setPrefix((uint8_t*)&key, p_len);
                N::setChild(n, key[level], GenNewNode(key, level, tid));
            }
        }
        return n;
    }

    bool lookup(const Key& key, TID& tid) const ;
    bool lookup_range(const Key& k1, const Key& k2, vector<TID>& res);
    void insert(const Key& key, TID tid);
};

extern template class ART<64>;
extern template class ART<128>;
extern template class ART<256>;