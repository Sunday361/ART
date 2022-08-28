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
 * %%% 还需要考虑一下 数据的存储格式， 大小端还有 ieee 754
 * 其他诸多数据格式
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

namespace Index {

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

#define DELETE_UNLOCK(node) \
        (node)->writeUnlockObsolete();

    template<uint16_t KeyLen>
    ART<KeyLen>::ART(Index::ArtObjPool *art_obj_pool) {
        root_ = new N256();
        art_obj_pool_ = art_obj_pool;
    }

    template<uint16_t KeyLen>
    void ART<KeyLen>::yield(int count) const {
        if (count > 3)
            sched_yield();
        else
            _mm_pause();
    }

    template<uint16_t KeyLen>
    bool ART<KeyLen>::lookup(const Key &key, TID &tid) const {
        int restartCount = 0;
        restart:
        if (restartCount++) {
            yield(restartCount);
        }
        bool needRestart = false;

        N *cur;
        N *parent = nullptr;
        uint64_t v, nv;
        uint16_t level = 0;

        cur = root_;
        READ_LOCK(cur, v, needRestart)
        while (key.getKeyLen() > level) {
            if (checkPrefix(cur, key, level)) { // MATCH
                parent = cur;
                cur = N::getChild(cur, key[level]);
                READ_UNLOCK(parent, v, needRestart)

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
    bool ART<KeyLen>::lookupRange(const Key &k1, const Key &k2, vector<TID> &res) {

        std::function<void(const N*)> copy = [&](const N* cur) {
            if (N::isLeaf(cur)) {
                res.push_back(N::getLeaf(cur));
            } else {
                std::tuple<uint8_t, N*> children[256];
                uint16_t len;

                N::GetChildren(cur, 0, 255, children, len);
                for (uint16_t i = 0; i < len; i++) {
                    const N* n = std::get<1>(children[i]);
                    copy(n);
                }
            }
        };

        std::function<void(const N*, uint16_t&)> find_start = [&](const N* cur, uint16_t& level) {
            int res = CheckPrefix(cur, k1, level);
            if (res > 0) {
                return ;
            } else if (res < 0) {
                copy(cur);
            } else {
                std::tuple<uint8_t, N*> children[256];
                uint16_t len;

                N::GetChildren(cur, k1[level], 255, children, len);
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
            int res = CheckPrefix(cur, k1, level);
            if (res < 0) {
                return ;
            } else if (res > 0) {
                copy(cur);
            } else {
                std::tuple<uint8_t, N*> children[256];
                uint16_t len;

                N::GetChildren(cur, 0, k1[level], children, len);
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

        int restartCount = 0;
        restart:
        if (restartCount++) {
            yield(restartCount);
        }
        bool needRestart = false;

        uint16_t level = 0;
        N* cur = root_;
        N* parent = nullptr;
        uint64_t v = 0;
        uint16_t start_key, end_key;

        while (true) {
            READ_LOCK(cur, v, needRestart)
            if (CheckPrefix(cur, k1, k2, level, start_key, end_key)) {
                std::tuple<uint8_t, N*> children[256];
                uint16_t len;

                N::GetChildren(cur, start_key, end_key, children, len);

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
                    READ_CHECK(cur, v, needRestart)
                    parent = cur;
                    cur = N::GetChild(cur, start_key);
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
    void ART<KeyLen>::insert(const Key &key, TID tid) {
        int restartCount = 0;
        restart:
        if (restartCount++) {
            yield(restartCount);
        }
        bool needRestart = false;

        N *cur = nullptr;
        N *next = root_;
        N *parent;
        uint8_t pk = 0, k = 0;
        uint16_t level = 0;
        uint64_t v, pv;
        uint8_t remainPrefix[MAX_PREFIX_LEN];
        uint8_t no_match_key = 0, remain_prefix_len = 0;

        while (level < key.getKeyLen()) {
            parent = cur;
            pk = k;
            pv = v;
            cur = next;
            READ_LOCK(cur, v, needRestart)

            uint16_t nextLevel = level;
            if (!checkPrefix(cur, key, nextLevel, no_match_key, remainPrefix, remain_prefix_len)) { /* No Match */
                COUPLING_LOCK(cur, parent, pv, v, needRestart)
                N *newNode = art_obj_pool_->newNode(NT4);
                N *nextNode = GenNewNode(key, nextLevel + 1, tid);
                newNode->setPrefix(cur->getPrefix(), nextLevel - level);
                cur->setPrefix(remainPrefix, remain_prefix_len);

                N::setChild(newNode, no_match_key, cur);
                N::setChild(newNode, key[nextLevel], nextNode);
                N::changeChild(parent, pk, newNode);

                WRITE_UNLOCK(cur)
                WRITE_UNLOCK(parent)
                return;
            }
            k = key[nextLevel];
            if (!N::getChild(cur, k)) { /* Specific Slot is NULL */
                if (cur->isFull()) {
                    COUPLING_LOCK(cur, parent, pv, v, needRestart)
                    N::insertAndGrow(cur, parent, pk, k, GenNewNode(key, nextLevel + 1, tid), art_obj_pool_);
                    DELETE_UNLOCK(cur)
                    WRITE_UNLOCK(parent)
                    art_obj_pool_->gcNode(cur);
                } else {
                    UPGRADE_LOCK(cur, v, needRestart)
                    N::setChild(cur, k, GenNewNode(key, nextLevel + 1, tid));
                    WRITE_UNLOCK(cur)
                }
                return;
            } else {
                next = N::getChild(cur, k);

                if (N::isLeaf(next)) { /* The Same Key is thought as Update */
                    UPGRADE_LOCK(cur, v, needRestart)
                    N::changeChild(cur, k, (N *) N::convertToLeaf(tid));
                    WRITE_UNLOCK(cur)
                    return;
                }

                if (parent != nullptr) {
                    READ_UNLOCK(parent, pv, needRestart)
                }
            }
            level = nextLevel + 1;
        }
    }

}

template class Index::ART<32>;
template class Index::ART<64>;
template class Index::ART<128>;
template class Index::ART<256>;


