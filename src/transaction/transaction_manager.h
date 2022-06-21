#pragma once

#include "timestamp_manager.h"
#include "deferred_action_manager.h"
#include "transaction_context.h"
#include "transaction_util.h"
#include "transaction_defs.h"
#include "storage/record_buffer.h"
#include "storage/tuple_access_strategy.h"
#include "common/spin_lock.h"

namespace transaction {
    class TransactionManager {
        TimestampManager* timestamp_manager_;

        DeferredActionManager* deferred_action_manager_;

        storage::RecordBufferSegmentPool* buffer_pool_;

        storage::LogManager log_manager_;

        const bool gc_enabled_ = false;

        TransactionQueue completed_txns_;

        TransactionPolicy default_txn_policy_{DurabilityPolicy::SYNC, ReplicationPolicy::DISABLE};

        timestamp_t UpdatingCommitCriticalSection(TransactionContext *txn);

        void LogCommit(TransactionContext *txn, timestamp_t commit_time, transaction::callback_fn commit_callback,
                       void *commit_callback_arg, timestamp_t oldest_active_txn);

        void LogAbort(TransactionContext *txn);

        void Rollback(TransactionContext *txn, const storage::UndoRecord &record) const;

        void DeallocateColumnUpdateIfVarlen(TransactionContext *txn, storage::UndoRecord *undo,
                                            uint16_t projection_list_index,
                                            const storage::TupleAccessStrategy &accessor) const;

        void DeallocateInsertedTupleIfVarlen(TransactionContext *txn, storage::UndoRecord *undo,
                                             const storage::TupleAccessStrategy &accessor) const;
        void GCLastUpdateOnAbort(TransactionContext *txn);
    public:
        TransactionManager(const TimestampManager* timestamp_manager,
                           const DeferredActionManager* deferred_action_manager,
                           const storage::RecordBufferSegmentPool* buffer_pool, const bool gc_enabled,
                           const bool wal_async_commit_enable, const storage::LogManager* log_manager)
                : timestamp_manager_(timestamp_manager),
                  deferred_action_manager_(deferred_action_manager),
                  buffer_pool_(buffer_pool),
                  gc_enabled_(gc_enabled),
                  log_manager_(log_manager) {
            ASSERT(timestamp_manager_ != DISABLED, "transaction manager cannot function without a timestamp manager");
            ASSERT(!wal_async_commit_enable || (wal_async_commit_enable && log_manager_ != DISABLED),
                             "Doesn't make sense to enable async commit without enabling logging.");
            if (wal_async_commit_enable) {
                SetDefaultTransactionDurabilityPolicy(transaction::DurabilityPolicy::ASYNC);
            }
        }

        TransactionContext *BeginTransaction();

        timestamp_t Commit(TransactionContext *txn, transaction::callback_fn callback, void *callback_arg);

        timestamp_t Abort(TransactionContext *txn);

        bool GCEnabled() const { return gc_enabled_; }

        TransactionQueue CompletedTransactionsForGC();

        void SetDefaultTransactionDurabilityPolicy(const DurabilityPolicy &policy) {
            default_txn_policy_.durability_ = policy;
        }

        void SetDefaultTransactionReplicationPolicy(const ReplicationPolicy &policy) {
            ASSERT(default_txn_policy_.durability_ != DurabilityPolicy::DISABLE, "Replication relies on logs!");
            ASSERT(!(default_txn_policy_.durability_ == DurabilityPolicy::ASYNC && policy == ReplicationPolicy::SYNC),
                             "Weird configuration that we don't support; this would require a new approach that isn't "
                             "swap-the-commit-callback.");
            default_txn_policy_.replication_ = policy;
        }

        const TransactionPolicy &GetDefaultTransactionPolicy() const { return default_txn_policy_; }

        timestamp_t GetCurrentTimestamp() const { return timestamp_manager_->CurrentTime(); }
    };
}
