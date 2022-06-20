#pragma once

#include <cstdint>
#include "common/common.h"

namespace Index {

    template<uint16_t KeyLen>
    class KEY {
    private:
        uint8_t keys_[KeyLen];

    public:
        KEY(uint8_t *ptr, uint16_t len) {
            std::memcpy(keys_, ptr, len);
        }

        KEY(const KEY &key) {
            std::memcpy(keys_, key.keys_, KeyLen);
        }

        KEY() {
            std::memset(keys_, 0, KeyLen);
        }

        bool operator==(const KEY &key) const {
            if (sizeof(key) != KeyLen) return false;

            for (int i = 0; i < KeyLen; i++) {
                if (keys_[i] != key.keys_[i]) return false;
            }

            return true;
        }

        bool operator!=(const KEY &key) const {
            if (sizeof(key) == KeyLen) return true;

            for (int i = 0; i < KeyLen; i++) {
                if (keys_[i] != key.keys_[i]) return true;
            }

            return false;
        }

        bool operator<(const KEY &key) const {
            for (int i = 0; i < KeyLen; i++) {
                if (keys_[i] < key.keys_[i]) {
                    return true;
                }
            }
            return false;
        }

        uint8_t &operator[](uint16_t i) {
            ASSERT(i >= 0 && i < KeyLen, "idx over range %d" << i);
            return keys_[i];
        }

        const uint8_t &operator[](uint16_t i) const {
            ASSERT(i >= 0 && i < KeyLen, "idx over range %d" << i);
            return keys_[i];
        }

        uint16_t getKeyLen() const {
            return KeyLen;
        }
    };
}