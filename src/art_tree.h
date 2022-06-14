#pragma once
#include <cstdint>
#include <vector>

#include "sched.h"
#include "emmintrin.h"

#include "art_key.h"
#include "common.h"
#include "art_node.h"

using namespace std;

template<uint16_t KeyLen>
class ART {
    using Key = KEY<KeyLen>;

    N* root_ = nullptr;
public:
    ART();

    void yield(int count) {
        if (count>3)
            sched_yield();
        else
            _mm_pause();
    }

    bool checkPrefix(N* n, const Key& k, uint16_t& level) {
        for (int i = 0; i < n->getPrefixLen(); i++) {
            if (level >= k.getKeyLen() || k[level] != n->getPrefix(i)) {
                return false;
            }
            level++;
        }
        return true;
    }

    bool lookup(const Key& key, TID& tid);
    bool lookup_range(const Key& k1, const Key& k2, vector<TID>& res);
    void insert(const Key& key, TID tid);
};
