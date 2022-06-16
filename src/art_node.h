#pragma once

#include <cstdint>
#include <atomic>
#include <cstring>

#include "common.h"

using namespace std;

const uint64_t LEAF = (1UL << 63);

class N {
protected:
    uint8_t  pCount_ = 0;
    uint8_t  type_;
    uint8_t  count_; // child count
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

    void setType(uint8_t type) { this->type_ = type; }

    uint8_t getType() const { return this->type_; }

    uint16_t getCount() const {
        return count_;
    }

    uint16_t getPrefixLen() const {
        return pCount_;
    }

    uint8_t getPrefix(uint16_t i) const {
        return prefix[i];
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

    /* Node Common Interface */
    virtual N* getChild(const uint8_t k) const = 0;
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
};

class N48: public N {
    uint8_t keys_[256];
    N* children_[48];
public:
    N48() {
        memset(keys_, 0, 256);
        memset(children_, 0, sizeof(N*) * 48);
    }

    N* getChild(const uint8_t k) const {
        return children_[keys_[k]];
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
};