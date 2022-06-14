#pragma once

#include <cstdint>

using namespace std;

template<uint16_t KeyLen>
class KEY {
private:
    uint8_t keys_[KeyLen];

public:
    KEY(uint8_t *ptr, uint16_t len) {
        memcpy(keys_, ptr, len);
    }

    KEY(const KEY& key) {
        memcpy(keys_, key.keys_, KeyLen);
    }

    bool operator==(const KEY &key) const {
        if (sizeof (key) != KeyLen) return false;

        for (int i = 0; i < KeyLen; i++) {
            if (keys_[i] != key.keys_[i]) return false;
        }

        return true;
    }

    bool operator!=(const KEY &key) const {
        if (sizeof (key) == KeyLen) return true;

        for (int i = 0; i < KeyLen; i++) {
            if (keys_[i] != key.keys_[i]) return true;
        }

        return false;
    }

    uint8_t& operator[](uint16_t i){
        return keys_[i];
    }

    const uint8_t& operator[](uint16_t i) const {
        return keys_[i];
    }

    uint16_t getKeyLen() const {
        return KeyLen;
    }
};