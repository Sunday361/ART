//
// Created by panrenhua on 2022/6/15.
//

/**
 * ART 的数据结构， 为什么在现在这般的实现中取消路径压缩
 * 1. 路径压缩需要回表去数据
 * 2. 现有的底层存储结构为 PAX 会表cache miss
 * 3. 需要考虑垃圾回收
 * 4. 额外的TBB 进行分布式或者集中的GC
 *
 *
 * trans方面 采取的是 DAF 最后集中是的提交或者回滚
 * 1. 带研究
 * 2. 读一下DAF的论文
 * 3. 争取把乐观锁和乐观事务结合起来
 *
 * 论文结构
 * 1.1 现代计算及结构
 * 1.2 数据库面临的问题
 * 1.3 内存数据库的主要开销
 *
 * 2.1 ART
 * 2.2 ART Sync
 * 2.3 Optimistic Coupling
 *
 * 2.4 realization of Concurrent ART
 *
 *  Include ( Innovation of my ART )
 *
 * Why do not use PATH Compress
 * How different Data Structure be stored
 *
 * How we integrate this ART into Transaction and DataStorage
 *
 * DAF and block in memory
 *
 * The benchmark of this ART
 *
 * */

#include "art_tree.h"

#define CHECK(needRestart) \
        if (needRestart) goto restart;

#define READ_LOCK(node, v, needRestart) \
        v = (node)->readLockOrRestart(needRestart); \
        if (needRestart) goto restart;

#define WRITE_LOCK(node, v, needRestart) \
        v = (node)->readLockOrRestart(needRestart); \
        if (needRestart) goto restart;

#define UPGRADE_LOCK(node, v, needRestart) \
        (node)->upgradeToWriteLockOrRestart(v, needRestart); \
        if (needRestart) goto restart;

#define READ_UNLOCK(node, v, needRestart) \
        (node)->readUnlockOrRestart(v, needRestart); \
        if (needRestart) goto restart;

#define WRITE_UNLOCK(node) \
        (node)->writeUnlock();

#define COUPLING_LOCK(cur, parent, pv, v, needRestart) \
        UPGRADE_LOCK(parent, pv, needRestart) \
        (cur)->upgradeToWriteLockOrRestart(v, needRestart); \
        if (needRestart) {                             \
            (parent)->writeUnlock();                   \
            goto restart;                              \
        }


template<uint16_t KeyLen>
ART<KeyLen>::ART() {
    root_ = new N256();
}

template<uint16_t KeyLen>
void ART<KeyLen>::yield(int count) const {
    if (count > 3)
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
    READ_LOCK(cur, v, needRestart)
    while (key.getKeyLen() > level) {
        if (checkPrefix(cur, key, level)) { // MATCH
            parent = cur;
            cur = N::getChild(cur, key[level]);
            READ_LOCK(parent, v, needRestart)

            if (cur == nullptr) {
                return false;
            }
            if (N::isLeaf(cur) && level == key.getKeyLen() - 1) {
                READ_UNLOCK(parent, v, needRestart)
                tid = N::getLeaf(cur);
                return true;
            }
        } else {   // NO MATCH
            READ_UNLOCK(cur, v, needRestart)
            return false;
        }
        level++;

        READ_LOCK(cur, nv, needRestart)
        READ_UNLOCK(parent, v, needRestart)
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
    uint8_t pk, k = 0, no_match_key;
    uint16_t level = 0;
    uint64_t v, pv;

    while (true) {
        parent = cur;
        pk = k;
        cur = next;
        READ_LOCK(cur, v, needRestart)

        uint16_t nextLevel = level;
        if (!checkPrefix(cur, key, nextLevel, no_match_key)) { /* No Match */
            COUPLING_LOCK(cur, parent, pv, v, needRestart)
            N* newNode = new N4();
            N* nextNode = GenNewNode(key, nextLevel, tid);
            cur->setPrefixLen(nextLevel - level);
            newNode->setPrefix(cur->getPrefix(), nextLevel - level);
            N::setChild(newNode, no_match_key, cur);
            N::setChild(newNode, key[nextLevel], nextNode);

            N::setChild(parent, pk, newNode);
            WRITE_UNLOCK(cur)
            WRITE_UNLOCK(parent)
            return ;
        }
        if (!N::getChild(cur, key[nextLevel + 1])) { /* Specific Slot is NULL */
            if (cur->isFull()) {
                COUPLING_LOCK(cur, parent, pv, v, needRestart)
                N::insertAndGrow(cur, parent, pk, k, GenNewNode(key, nextLevel, tid));
                WRITE_UNLOCK(cur)
                WRITE_UNLOCK(parent)
            } else {
                UPGRADE_LOCK(cur, v, needRestart)
                N::setChild(cur,key[nextLevel + 1], GenNewNode(key, nextLevel, tid));
                WRITE_UNLOCK(cur)
            }
            return;
        }
        level = nextLevel + 1;
    }
}

template class ART<64>;
template class ART<128>;
template class ART<256>;

