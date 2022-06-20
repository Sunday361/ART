#pragma once

#include <algorithm>
#include <vector>
#include <set>

#include "common/common.h"
#include "common/spin_lock.h"

namespace transaction {
    class TimestampManager {
    public:
        ~TimestampManager() {
            ASSERT(curr_running_txns_.empty(),
                   "Destroying the TimestampManager while txns are still running. That seems wrong.");
        }

        timestamp_t checkOutTimestamp() { return time_++; }

        timestamp_t currentTime() const { return time_.load(); }

        timestamp_t oldestTransactionStartTime();

        timestamp_t cachedOldestTransactionStartTime();

    private:

        friend class TransactionManager;

        friend class storage::LogSerializerTask;

        timestamp_t beginTransaction() {
            timestamp_t start_time;
            {
                ScopedSpinLatch running_guard(&curr_running_txns_latch_);
                start_time = time_++;

                const auto ret
                UNUSED_ATTRIBUTE = curr_running_txns_.emplace(start_time);
                ASSERT(ret.second, "commit start time should be globally unique");
            }  // Release latch on current running transactions
            return start_time;
        }

        void removeTransaction(timestamp_t timestamp);

        bool removeTransactions(const std::vector<timestamp_t> &timestamps);

        std::atomic<timestamp_t> time_{0};
        // We cache the oldest txn start time
        std::atomic<timestamp_t> cached_oldest_txn_start_time_{0};

        std::unordered_set<timestamp_t> curr_running_txns_;
        mutable SpinLatch curr_running_txns_latch_;
    };
}
