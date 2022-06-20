#pragma once

#include <atomic>

#include "transaction_defs.h"

namespace transaction {

    class TransactionContext {
        timestamp_t start_time_;
        std::atomic<timestamp_t> finish_time_;


    public:
        TransactionContext()
                :
    };

}