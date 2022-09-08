//
// Created by panrenhua on 2022/6/15.
//

#include "art_tree_lock.h"

namespace Index {

    template<uint16_t KeyLen>
    ART_Lock<KeyLen>::ART_Lock(Index::ArtObjPool *art_obj_pool) {
        root_ = new N256();
        art_obj_pool_ = art_obj_pool;
    }

    template<uint16_t KeyLen>
    ART_Lock<KeyLen>::~ART_Lock() {
        //GC(root_);
    }

    template<uint16_t KeyLen>
    void ART_Lock<KeyLen>::GC(N* n) {
        if (N::isLeaf(n)) return ;
        std::tuple<uint8_t, N*> children[256];
        uint16_t len;

        N::getChildren(n, 0, 255, children, len);
        for (uint16_t i = 0; i < len; i++) {
            auto node = std::get<1>(children[i]);
            if (!N::isLeaf(node)) {
                GC(node);
            }
        }
        delete n;
    }

    template<uint16_t KeyLen>
    bool ART_Lock<KeyLen>::lookup(const Key &key, TID &tid) const {
        std::shared_lock<std::shared_mutex> rlk(stx_);

        N *cur;
        N *parent = nullptr;
        uint16_t level = 0;

        cur = root_;
        while (key.getKeyLen() > level) {
            if (checkPrefix(cur, key, level)) { // MATCH
                parent = cur;
                cur = N::getChild(parent, key[level]);
                if (cur == nullptr) {
                    return false;
                }
                if (N::isLeaf(cur) && level == key.getKeyLen() - 1) {
                    tid = N::getLeaf(cur);
                    return true;
                }
            } else {   // NO MATCH
                return false;
            }
            level++;
        }
        return false;
    }

    template<uint16_t KeyLen>
    bool ART_Lock<KeyLen>::lookupRange(const Key &k1, const Key &k2, vector<TID> &res) const {

        std::function<void(const N*)> copy = [&](const N* cur) {
            if (N::isLeaf(cur)) {
                res.push_back(N::getLeaf(cur));
            } else {
                std::tuple<uint8_t, N*> children[256];
                uint16_t len;

                N::getChildren(cur, 0, 255, children, len);
                for (uint16_t i = 0; i < len; i++) {
                    const N* n = std::get<1>(children[i]);
                    copy(n);
                }
            }
        };

        std::function<void(const N*, uint16_t&)> find_start = [&](const N* cur, uint16_t& level) {
            int res = checkPrefix(cur, k1, level);
            if (res > 0) {
                return ;
            } else if (res < 0) {
                copy(cur);
            } else {
                std::tuple<uint8_t, N*> children[256];
                uint16_t len;

                N::getChildren(cur, k1[level], 255, children, len);
                for (int i = 0; i < len; i++) {
                    const N* child = std::get<1>(children[i]);
                    if (i == 0) {
                        find_start(child, level);
                    } else {
                        copy(child);
                    }
                }
            }
        };

        std::function<void(const N*, uint16_t&)> find_end = [&](const N* cur, uint16_t& level) {
            int res = checkPrefix(cur, k1, level);
            if (res < 0) {
                return ;
            } else if (res > 0) {
                copy(cur);
            } else {
                std::tuple<uint8_t, N*> children[256];
                uint16_t len;

                N::getChildren(cur, 0, k1[level], children, len);
                for (int i = 0; i < len; i++) {
                    const N* child = std::get<1>(children[i]);
                    if (i == len - 1) {
                        find_end(child, level);
                    } else {
                        copy(child);
                    }
                }
            }
        };

        uint16_t level = 0;
        N* cur = root_;
        N* parent;
        uint16_t start_key, end_key;

        while (true) {
            if (checkPrefix(cur, k1, k2, level, start_key, end_key)) {
                std::tuple<uint8_t, N*> children[256];
                uint16_t len;

                N::getChildren(cur, start_key, end_key, children, len);

                if (len >= 2) {
                    for (int i = 0; i < len; ++i) {
                        const N *n = std::get<1>(children[i]);
                        if (i == 0) {
                            uint16_t level_copy = level;
                            find_start(n, level_copy);
                        } else if (i == len - 1) {
                            uint16_t level_copy = level;
                            find_end(n, level_copy);
                        } else {
                            copy(n);
                        }
                    }
                } else if (len == 1) {
                    parent = cur;
                    cur = N::getChild(parent, start_key);
                } else {
                    return false;
                }
            } else {
                return false;
            }
            level++;
        }
    }

    template<uint16_t KeyLen>
    void ART_Lock<KeyLen>::insert(const Key &key, TID tid) {
        std::unique_lock<std::shared_mutex> rlk(stx_);

        N *cur = nullptr;
        N *next = root_;
        N *parent;
        uint8_t pk = 0, k = 0;
        uint16_t level = 0;
        uint8_t remainPrefix[MAX_PREFIX_LEN];
        uint8_t no_match_key = 0, remain_prefix_len = 0;

        while (level < key.getKeyLen()) {
            parent = cur;
            pk = k;

            cur = next;

            uint16_t nextLevel = level;
            if (!checkPrefix(cur, key, nextLevel, no_match_key, remainPrefix, remain_prefix_len)) { /* No Match */
                N *newNode = art_obj_pool_->newNode(NT4);
                N *nextNode = GenNewNode(key, nextLevel + 1, tid);
                newNode->setPrefix(cur->getPrefix(), nextLevel - level);
                cur->setPrefix(remainPrefix, remain_prefix_len);

                N::setChild(newNode, no_match_key, cur);
                N::setChild(newNode, key[nextLevel], nextNode);
                N::changeChild(parent, pk, newNode);
                return;
            }

            k = key[nextLevel];
            if (!N::getChild(cur, k)) { /* Specific Slot is NULL */
                if (cur->isFull()) {
                    N::insertAndGrow(cur, parent, pk, k, GenNewNode(key, nextLevel + 1, tid), art_obj_pool_);
                    art_obj_pool_->gcNode(cur);
                } else {
                    N::setChild(cur, k, GenNewNode(key, nextLevel + 1, tid));
                }
                return;
            } else {
                next = N::getChild(cur, k);

                if (N::isLeaf(next)) { /* The Same Key is thought as Update */
                    N::changeChild(cur, k, (N *) N::convertToLeaf(tid));
                    return;
                }
            }
            level = nextLevel + 1;
        }
    }

}

template class Index::ART_Lock<32>;
template class Index::ART_Lock<64>;
template class Index::ART_Lock<128>;
template class Index::ART_Lock<256>;


