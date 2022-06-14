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

}

template<uint16_t KeyLen>
bool ART<KeyLen>::lookup(const Key& key, TID& tid) {
    int restartCount = 0;
restart:
    if (restartCount > 3) {
        yield();
    }
    bool needRestart = false;

    N* cur;
    N* parent = nullptr;
    uint64_t v;
    uint16_t level;

    cur = root_;
    READ_LOCK(cur, v, needRestart);
    while (true) {
        switch () {

        }
    }
}

template<uint16_t KeyLen>
bool ART<KeyLen>::lookup_range(const Key &k1, const Key &k2, vector<TID> &res) {
    return true;
}

template<uint16_t KeyLen>
void ART<KeyLen>::insert(const Key &key, TID tid) {

}

