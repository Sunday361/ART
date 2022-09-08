#pragma once
#include <cstdint>
#include <vector>
#include <iostream>
#include <algorithm>
#include <shared_mutex>

#include "sched.h"
#include "emmintrin.h"

#include "art_key.h"
#include "common/common.h"
#include "art_node.h"
#include "art_obj_pool.h"
#include "catalog.h"

namespace Index {

    template<uint16_t KeyLen>
    class ART_Lock {
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

        mutable std::shared_mutex stx_;

        void GC(N* n);

    public:
        ART_Lock(Index::ArtObjPool *art_obj_pool);

        ~ART_Lock();

        bool checkPrefix(const N *n, const Key &k, uint16_t &level) const {
            for (int i = 0; i < n->getPrefixLen(); i++) {
                if (level >= k.getKeyLen() || k[level] != n->getPrefix()[i]) {
                    return false;
                }
                level++;
            }
            return true;
        }

        bool checkPrefix(const N* n, const Key &start, const Key &end, uint16_t &level,
                         uint16_t& k1, uint16_t& k2) const {
            uint8_t start_key, end_key;
            k1 = k2 = -1;
            for (int i = 0; i < n->getPrefixLen(); i++) {
                start_key = start[level];
                end_key = end[level];

                if (start_key > n->getPrefix()[i] || end_key < n->getPrefix()[i]) { // no contained
                    return false;
                } else if (start_key < n->getPrefix()[i]) { // lower is open
                    k1 = 0;
                } else if (end_key > n->getPrefix()[i]) { // higher is open
                    k2 = 255;
                }
                level++;
            }
            if (k1 == -1) k1 = start[level];
            if (k2 == -1) k2 = end[level];
            return true;
        }

        bool checkPrefix(const N *n, const Key &k, uint16_t &level,
                         uint8_t &no_match_key, uint8_t *remain, uint8_t &remain_len) const {
            for (int i = 0; i < n->getPrefixLen(); i++) {
                if (k[level] != n->getPrefix()[i]) {
                    no_match_key = n->getPrefix()[i];
                    if (i + 1 < n->getPrefixLen()) {
                        memcpy(remain, &n->getPrefix()[i + 1], n->getPrefixLen() - i - 1);
                        remain_len = n->getPrefixLen() - i - 1;
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

            if (level < key.getKeyLen()) {
                n = new N4();
                p_len = min(MAX_PREFIX_LEN, key.getKeyLen() - level - 1);
                n->setPrefix((uint8_t *) &key[level], p_len);
                level += p_len;

                N::setChild(n, key[level], GenNewNode(key, level + 1, tid));
            } else {
                return (N *) N::convertToLeaf(tid);
            }
            return n;
        }

        bool lookup(const Key &key, TID &tid) const;

        bool lookupRange(const Key &k1, const Key &k2, vector<TID> &res) const ;

        void insert(const Key &key, TID tid);
    };
}
extern template class Index::ART_Lock<32>;
extern template class Index::ART_Lock<64>;
extern template class Index::ART_Lock<128>;
extern template class Index::ART_Lock<256>;