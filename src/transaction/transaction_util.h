#pragma once

#include <transaction/transaction_defs.h>

namespace transaction {

    class TransactionUtil {
    public:
        TransactionUtil() = delete;

        static bool NewerThan(const timestamp_t a, const timestamp_t b) {
            return a > b;
        }

        static bool Committed(const timestamp_t timestamp) {
            return static_cast<int64_t>(timestamp) >= 0;
        }


        static void EmptyCallback(void * /*unused*/) {}
    };
}