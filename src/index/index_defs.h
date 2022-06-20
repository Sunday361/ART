#pragma once

namespace Index {

    enum type {
        NT4,
        NT16,
        NT48,
        NT256,
    };

    enum status {
        UNLOCK = 0b00,
        DELETE = 0b01,
        LOCK = 0b10,
    };
}