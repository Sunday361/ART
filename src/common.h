#pragma once

#include "cstdint"

using namespace std;

using TID = uint64_t;

enum type {
    N4,
    N16,
    N48,
    N256,
};

enum status {
    UNLOCK = 0b00,
    DELETE = 0b01,
    LOCK = 0b10,
};
