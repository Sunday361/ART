#pragma once

#include <cstdint>
#include <atomic>
#include <cstring>
#include <tuple>
#include <emmintrin.h>
#include <iostream>

#include "common/common.h"
#include "index_defs.h"

namespace Index {

    class ArtObjPool;

    const uint64_t LEAF = (1UL << 63);

    const uint16_t MAX_PREFIX_LEN = 8;

    class N {
    protected:
        uint8_t pCount_ = 0;
        uint8_t type_;
        uint16_t count_ = 0; // child count
        uint8_t prefix[8];
        atomic<uint64_t> lock_{0b100};

    public:
        static bool isLeaf(const N *ptr) {
            uint64_t d = uint64_t(ptr);
            return (d & LEAF) == LEAF;
        }

        static uint64_t getLeaf(const N *ptr) {
            uint64_t d = uint64_t(ptr);
            return (d & ~LEAF);
        }

        static TID convertToLeaf(TID tid) {
            return tid | LEAF;
        }

        void setType(uint8_t type) { this->type_ = type; }

        uint8_t getType() const { return this->type_; }

        uint16_t getCount() const {
            return count_;
        }

        uint16_t getPrefixLen() const {
            return pCount_;
        }

        uint8_t *getPrefix() {
            return &prefix[0];
        }

        void setPrefix(uint8_t *c, uint8_t len) {
            memcpy(this->prefix, c, len);
            this->pCount_ = len;
        }

        void setPrefixLen(uint8_t len) {
            this->pCount_ = len;
        }

        bool isLocked(uint64_t version) const { return (version & LOCK) == LOCK; }

        void writeLockOrRestart(bool &needRestart) {
            uint64_t version = readLockOrRestart(needRestart);
            if (needRestart) return;
            upgradeToWriteLockOrRestart(version, needRestart);
        }

        void upgradeToWriteLockOrRestart(uint64_t &version, bool &needRestart) {
            if (lock_.compare_exchange_strong(version, version + 0b10)) {
                version = version + 0b10;
            } else {
                needRestart = true;
            }
        }

        void writeUnlock() { lock_.fetch_add(0b10); }

        uint64_t readLockOrRestart(bool &needRestart) const {
            uint64_t version = lock_.load();
            if (isLocked(version) || isObsolete(version)) {
                needRestart = true;
            }
            return version;
        }

        void checkOrRestart(uint64_t startRead, bool &needRestart) const {
            readUnlockOrRestart(startRead, needRestart);
        }

        void readUnlockOrRestart(uint64_t startRead, bool &needRestart) const {
            needRestart = (startRead != lock_.load());
        }

        static bool isObsolete(uint64_t version) { return (version & 1) == 1; }

        void writeUnlockObsolete() { lock_.fetch_add(0b11); }

        static void insertAndGrow(N *n, N *parent, uint8_t pk, uint8_t key, N *new_node, ArtObjPool *pool);

        template<typename Small, typename Big>
        static void insertGrow(Small *small, Big *big, N *parent, uint8_t pk, uint8_t key, N *new_node);

        /* Node Common Interface */
        static N *getChild(N *n, const uint8_t k);

        static void setChild(N *n, const uint8_t k, N *child);

        static bool changeChild(N *n, const uint8_t k, N *child);

        template<typename Node>
        void copyTo(Node *n);

        bool isFull() const;

        bool isUnderFull() const;
    };

    class N4 : public N {
        uint8_t keys_[4];
        N *children_[4];
    public:
        N4() {
            this->type_ = NT4;
            std::memset(keys_, 0, 4);
            std::memset(children_, 0, sizeof(N *) * 4);
        }

        N *getChild(const uint8_t k) const {
            for (int i = 0; i < count_; i++) {
                if (keys_[i] == k) {
                    return children_[i];
                }
            }
            return nullptr;
        }

        bool changeChild(const uint8_t k, N *child) {
            for (int i = 0; i < count_; i++) {
                if (keys_[i] == k) {
                    children_[i] = child;
                    return true;
                }
            }
            return false;
        }

        void setChild(const uint8_t k, N *child) {
            uint8_t i;
            for (i = 0; (i < count_) && (keys_[i] < k); i++);
            memmove(keys_ + i + 1, keys_ + i, count_ - i);
            memmove(children_ + i + 1, children_ + i, (count_ - i) * sizeof(N *));
            keys_[i] = k;
            children_[i] = child;
            count_++;
        }

        template<typename N>
        void copyTo(N *n) {
            for (int i = 0; i < count_; i++) {
                n->setChild(keys_[i], children_[i]);
            }
        }
    };

    class N16 : public N {
        uint8_t keys_[16];
        N *children_[16];

        static uint8_t flipSign(uint8_t keyByte) {
            // Flip the sign bit, enables signed SSE comparison of unsigned values, used by Node16
            return keyByte ^ 128;
        }

        static inline unsigned ctz(uint16_t x) {
            // Count trailing zeros, only defined for x>0
#ifdef __GNUC__
            return __builtin_ctz(x);
#else
            // Adapted from Hacker's Delight
       unsigned n=1;
       if ((x&0xFF)==0) {n+=8; x=x>>8;}
       if ((x&0x0F)==0) {n+=4; x=x>>4;}
       if ((x&0x03)==0) {n+=2; x=x>>2;}
       return n-(x&1);
#endif
        }

    public:
        N16() {
            this->type_ = NT16;
            std::memset(keys_, 0, 16);
            std::memset(children_, 0, sizeof(N *) * 16);
        }

        N *getChild(const uint8_t k) const {
            N *const *childPos = getChildPos(k);
            if (childPos == nullptr) {
                return nullptr;
            } else {
                return *childPos;
            }
        }

        bool changeChild(const uint8_t k, N *child) {
            N **childPos = const_cast<N **>(getChildPos(k));
            if (childPos == nullptr) {
                return false;
            } else {
                *childPos = child;
                return true;
            }
        }

        void setChild(const uint8_t k, N *child) {
            uint8_t keyByteFlipped = flipSign(k);
            __m128i cmp = _mm_cmplt_epi8(_mm_set1_epi8(keyByteFlipped),
                                         _mm_loadu_si128(reinterpret_cast<__m128i *>(keys_)));
            uint16_t bitfield = _mm_movemask_epi8(cmp) & (0xFFFF >> (16 - count_));
            unsigned pos = bitfield ? ctz(bitfield) : count_;
            memmove(keys_ + pos + 1, keys_ + pos, count_ - pos);
            memmove(children_ + pos + 1, children_ + pos, (count_ - pos) * sizeof(N *));
            keys_[pos] = keyByteFlipped;
            children_[pos] = child;
            count_++;
        }

        N *const *getChildPos(const uint8_t k) const {
            __m128i cmp = _mm_cmpeq_epi8(_mm_set1_epi8(flipSign(k)),
                                         _mm_loadu_si128(reinterpret_cast<const __m128i *>(keys_)));
            unsigned bitfield = _mm_movemask_epi8(cmp) & ((1 << count_) - 1);
            if (bitfield) {
                return &children_[ctz(bitfield)];
            } else {
                return nullptr;
            }
        }

        template<typename N>
        void copyTo(N *n) {
            for (int i = 0; i < count_; i++) {
                n->setChild(flipSign(keys_[i]), children_[i]);
            }
        }
    };

    class N48 : public N {
        uint8_t keys_[256];
        N *children_[48];
    public:
        static const uint8_t emptyMarker = 48;

        N48() {
            this->type_ = NT48;
            std::memset(keys_, emptyMarker, 256);
            std::memset(children_, 0, sizeof(N *) * 48);
        }

        N *getChild(const uint8_t k) const {
            if (keys_[k] == emptyMarker) return nullptr;
            else return children_[keys_[k]];
        }

        bool changeChild(const uint8_t k, N *child) {
            if (keys_[k] == emptyMarker) return false;
            children_[keys_[k]] = child;
            return true;
        }

        void setChild(const uint8_t k, N *child) {
            keys_[k] = count_;
            children_[count_] = child;
            count_++;
        }

        template<typename N>
        void copyTo(N *n) {
            for (int i = 0; i < 256; i++) {
                if (keys_[i] != emptyMarker)
                    n->setChild(i, children_[keys_[i]]);
            }
        }
    };

    class N256 : public N {
        N *children_[256];
    public:
        N256() {
            this->type_ = NT256;
            std::memset(children_, 0, sizeof(N *) * 256);
        }

        N *getChild(const uint8_t k) const {
            return children_[k];
        }

        bool changeChild(const uint8_t k, N *child) {
            children_[k] = child;
            return true;
        }

        void setChild(const uint8_t k, N *child) {
            children_[k] = child;
            count_++;
        }

        template<typename N>
        void copyTo(N *n) {
            for (int i = 0; i < 256; i++) {
                n->setChild(i, children_[i]);
            }
        }
    };
}