#pragma once
#include <cstdint>
#include <vector>
#include <iostream>
#include <algorithm>

#include "sched.h"
#include "emmintrin.h"

#include "art_key.h"
#include "common/common.h"
#include "art_node.h"
#include "art_obj_pool.h"
#include "common/catalog.h"

namespace Index {

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

        N *root_ = nullptr;

        Index::ArtObjPool *art_obj_pool_ = nullptr;

    public:
        ART(Index::ArtObjPool *art_obj_pool);

        ~ART();

        void yield(int count) const;

        bool CheckPrefix(N *n, const Key &k, uint16_t &level) const {
            for (int i = 0; i < n->GetPrefixLen(); i++) {
                if (level >= k.GetKeyLen() || k[level] != n->GetPrefix()[i]) {
                    return false;
                }
                level++;
            }
            return true;
        }

        bool CheckPrefix(N *n, const Key &k, uint16_t &level,
                         uint8_t &no_match_key, uint8_t *remain, uint8_t &remain_len) const {
            for (int i = 0; i < n->GetPrefixLen(); i++) {
                if (k[level] != n->GetPrefix()[i]) {
                    no_match_key = n->GetPrefix()[i];
                    if (i + 1 < n->GetPrefixLen()) {
                        memcpy(remain, &n->GetPrefix()[i + 1], n->GetPrefixLen() - i - 1);
                        remain_len = n->GetPrefixLen() - i - 1;
                    }
                    return false;
                }
                level++;
            }
            return true;
        }

        N *GenNewNode(const Key &key, uint16_t level, TID tid) {
            N *n;
            uint8_t p_len;

            if (level < key.GetKeyLen()) {
                n = new N4();
                p_len = min(MAX_PREFIX_LEN, key.GetKeyLen() - level - 1);
                n->SetPrefix((uint8_t *) &key[level], p_len);
                level += p_len;

                N::SetChild(n, key[level], GenNewNode(key, level + 1, tid));
            } else {
                return (N *) N::ConvertToLeaf(tid);
            }
            return n;
        }

        bool Lookup(const Key &key, TID &tid) const;

        bool LookupRange(const Key &k1, const Key &k2, vector<TID> &res);

        void Insert(const Key &key, TID tid);
    };
}
extern template class Index::ART<32>;
extern template class Index::ART<64>;
extern template class Index::ART<128>;
extern template class Index::ART<256>;