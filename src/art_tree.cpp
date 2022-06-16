//
// Created by panrenhua on 2022/6/15.
//
#include "art_tree.h"

#define CHECK(needRestart) \
        if (needRestart) goto restart

#define READ_LOCK(node, v, needRestart) \
        v = (node)->readLockOrRestart(needRestart); \
        if (needRestart) goto restart

#define WRITE_LOCK(node, v, needRestart) \
        v = (node)->readLockOrRestart(needRestart); \
        if (needRestart) goto restart

#define UPGRADE_LOCK(node, v, needRestart) \
        (node)->upgradeToWriteLockOrRestart(v, needRestart); \
        if (needRestart) goto restart

#define READ_UNLOCK(node, v, needRestart) \
        (node)->readUnlockOrRestart(v, needRestart); \
        if (needRestart) goto restart

#define WRITE_UNLOCK(node) \
        (node)->writeUnlock()

template<uint16_t KeyLen>
ART<KeyLen>::ART() {
    root_ = new N256();
}

template<uint16_t KeyLen>
void ART<KeyLen>::yield(int count) const {
    if (count>3)
        sched_yield();
    else
        _mm_pause();
}

template<uint16_t KeyLen>
bool ART<KeyLen>::lookup(const Key& key, TID& tid) const {
    int restartCount = 0;
restart:
    if (restartCount++) {
        yield(restartCount);
    }
    bool needRestart = false;

    N* cur;
    N* parent = nullptr;
    uint64_t v, nv;
    uint16_t level = 0;

    cur = root_;
    READ_LOCK(cur, v, needRestart);
    while (key.getKeyLen() > level) {
        if (checkPrefix(cur, key, level)) { // MATCH
            parent = cur;
            cur = cur->getChild(key[level]);
            READ_LOCK(parent, v, needRestart);

            if (cur == nullptr) {
                return false;
            }
            if (N::isLeaf(cur) && level == key.getKeyLen() - 1) {
                READ_UNLOCK(parent, v, needRestart);
                tid = N::getLeaf(cur);
                return true;
            }
        } else {   // NO MATCH
            READ_UNLOCK(cur, v, needRestart);
            return false;
        }
        level++;

        READ_LOCK(cur, nv, needRestart);
        READ_UNLOCK(parent, v, needRestart);
        v = nv;
    }
    return false;
}

template<uint16_t KeyLen>
bool ART<KeyLen>::lookup_range(const Key &k1, const Key &k2, vector<TID> &res) {
    return true;
}

template<uint16_t KeyLen>
void ART<KeyLen>::insert(const Key &key, TID tid) {
    int restartCount = 0;
    restart:
    if (restartCount++) {
        yield(restartCount);
    }
    bool needRestart = false;

    N* cur;
    N* next = root_;
    N* parent = nullptr;
    uint8_t pk, k = 0;
    uint16_t level = 0;
    uint64_t v, nv;

    while (true) {
        parent = cur;
        pk = k;
        cur = next;
        READ_LOCK(cur, v, needRestart);

        uint16_t nextLevel = level;

    }
}

template class ART<64>;
template class ART<128>;
template class ART<256>;

