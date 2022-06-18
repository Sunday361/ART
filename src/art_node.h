#pragma once

#include <cstdint>
#include <atomic>
#include <cstring>
#include <tuple>

#include "common.h"

using namespace std;

const uint64_t LEAF = (1UL << 63);

const uint16_t MAX_PREFIX_LEN = 8;

class N {
protected:
    uint8_t  pCount_ = 0;
    uint8_t  type_;
    uint16_t count_; // child count
    uint8_t  prefix[8];
    atomic<uint64_t> lock_{0b100};

public:
    static bool isLeaf(void* ptr) {
        uint64_t d = *reinterpret_cast<uint64_t*>(ptr);
        return (d & LEAF) == LEAF;
    }

    static uint64_t getLeaf(void* ptr) {
        uint64_t d = *reinterpret_cast<uint64_t*>(ptr);
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

    uint8_t* getPrefix() {
        return &prefix[0];
    }

    void setPrefix(uint8_t* c, uint8_t len) {
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

    void checkOrRestart(uint64_t startRead, bool &needRestart) const { readUnlockOrRestart(startRead, needRestart); }

    void readUnlockOrRestart(uint64_t startRead, bool &needRestart) const {
        needRestart = (startRead != lock_.load());
    }

    static bool isObsolete(uint64_t version) { return (version & 1) == 1; }

    void writeUnlockObsolete() { lock_.fetch_add(0b11); }

    static void insertAndGrow(N* n, N* parent, uint8_t pk, uint8_t key, N* new_node);

    template<typename Small, typename Big>
    static void insertGrow(Small* n, N* parent, uint8_t pk, uint8_t key, N* new_node);

    /* Node Common Interface */
    static N* getChild(N* n, const uint8_t k);

    static void setChild(N* n, const uint8_t k, N* child);

    template<typename Node>
    void copyTo(Node* n);

    bool isFull() const;

    bool isUnderFull() const;
};

class N4: public N {
    uint8_t keys_[4];
    N* children_[4];
public:
    N4() {
        memset(keys_, 0, 4);
        memset(children_, 0, sizeof(N*) * 4);
    }

    N* getChild(const uint8_t k) const {
        for (int i = 0; i < 4; i++) {
            if (keys_[i] == k) {
                return children_[i];
            }
        }
        return nullptr;
    }

    void setChild(const uint8_t k, N* child) {

    }

    template<typename N>
    void copyTo(N* n) {
        for (int i = 0; i < 4; i++) {
            n->setChild(keys_[i], children_[i]);
        }
    }
};

class N16: public N {
    uint8_t keys_[16];
    N* children_[16];
public:
    N16() {
        memset(keys_, 0, 16);
        memset(children_, 0, sizeof(N*) * 16);
    }

    N* getChild(const uint8_t k) const {
        for (int i = 0; i < 16; i++) {
            if (keys_[i] == k) {
                return children_[i];
            }
        }
        return nullptr;
    }

    void setChild(const uint8_t k, N* child) {

    }

    template<typename N>
    void copyTo(N* n) {
        for (int i = 0; i < 16; i++) {
            n->setChild(keys_[i], children_[i]);
        }
    }
};

class N48: public N {
    uint8_t keys_[256];
    N* children_[48];
public:
    static const uint8_t emptyMarker = 48;

    N48() {
        memset(keys_, emptyMarker, 256);
        memset(children_, 0, sizeof(N*) * 48);
    }

    N* getChild(const uint8_t k) const {
        return children_[keys_[k]];
    }

    void setChild(const uint8_t k, N* child) {

    }

    template<typename N>
    void copyTo(N* n) {
        for (int i = 0; i < 256; i++) {
            if (keys_[i] != emptyMarker)
                n->setChild(keys_[i], children_[i]);
        }
    }
};

class N256: public N {
    N* children_[256];
public:
    N256() {
        memset(children_, 0, sizeof(N*) * 256);
    }

    N* getChild(const uint8_t k) const {
        return children_[k];
    }

    void setChild(const uint8_t k, N* child) {

    }

    template<typename N>
    void copyTo(N* n) {
        for (int i = 0; i < 256; i++) {
            n->setChild(i, children_[i]);
        }
    }

};