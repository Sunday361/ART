#pragma once

#include <queue>
#include <pair>

#include "timestamp_manager.h"
#include "transaction_defs.h"

namespace transaction {
    class DeferredActionManager {
        TimestampManager *timestamp_manager_;

        std::queue<pair<timestamp_t, DeferredAction>> new_deferred_actions_, back_log_;

        SpinLatch spin_latch_;

        uint32_t clearBacklog(timestamp_t oldest_txn) {
            uint32_t processed = 0;
            while (!back_log_.empty() && oldest_txn >= back_log_.front().first) {
                back_log_.front().second(oldest_txn);
                processed++;
                back_log_.pop();
            }
            return processed;
        }

        uint32_t processNewAction(timestamp_t oldest_txn) {
            uint32_t processed = 0;
            std::queue<std::pair<timestamp_t, DeferredAction>> new_actions_local;
            {
                ScopedSpinLatch guard(spin_latch_);
                new_actions_local = std::move(new_deferred_actions_);
            }

            while (!new_actions_local.empty() && oldest_txn >= new_actions_local.front().first) {
                new_actions_local.front().second(oldest_txn);
                processed++;
                new_actions_local.pop();
            }

            while (!new_actions_local.empty()) {
                back_log_.push(new_actions_local.front());
                new_actions_local.pop();
            }
            return processed;
        }

    public:
        explicit DeferredActionManager(TimestampManager *timestamp_manager) :

        :
        timestamp_manager_(timestamp_manager) {}

        timestamp_t registerDeferredAction(const DeferredAction &a) {
            ScopedSpinLatch guard(spin_latch_);
            timestamp_t result = timestamp_manager_->CurrentTime();
            new_deferred_actions.emplace(result, a);
            return result;
        }

        timestamp_t registerDeferredAction(const std::function<void *()> &a) {
            return registerDeferredAction([=](timestamp_t /*unused*/) { a(); });
        }

        uint32_t process(timestamp_t oldest_txn) {
            const auto backlog_size = static_cast<uint32_t>(back_log_.size());
            uint32_t processed = clearBacklog(oldest_txn);

            if (backlog_size != processed)
                return processed;

            processed += processNewAction(oldest_txn);

            return processed;
        }
    };
} // namespace transaction

