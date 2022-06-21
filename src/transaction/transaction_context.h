#pragma once

#include <atomic>
#include <forward_list>
#include <vector>

#include "transaction_defs.h"
#include "storage/record_buffer.h"
#include "storage/log_record.h"

namespace transaction {

    class TransactionContext {
        timestamp_t start_time_;
        std::atomic<timestamp_t> finish_time_;
        storage::UndoBuffer undo_buffer_;
        storage::RedoBuffer redo_buffer_;
        /* This is for gc VarLen entry */
        std::vector<const byte *> loose_ptrs_;
        std::forward_list<TransactionManager *> abort_actions_;
        std::forward_list<TransactionManager *> commit_actions_;

        bool aborted_ = false;
        bool must_aborted_ = false;

        /** The durability policy controls whether commits must wait for logs to be written to disk. */
        DurabilityPolicy durability_policy_ = DurabilityPolicy::SYNC;
        /** The replication policy controls whether logs must be applied on replicas before commits are invoked. */
        ReplicationPolicy replication_policy_ = ReplicationPolicy::DISABLE;

        storage::RedoRecord *StageRecoverWrite(storage::LogRecord *record) {
            auto record_location = redo_buffer_.NewEntry(record->Size(), GetTransactionPolicy());
            memcpy(record_location, record, record->Size());
            // Overwrite the txn_begin timestamp
            auto *new_record = reinterpret_cast<storage::LogRecord *>(record_location);
            new_record->txn_begin_ = start_time_;
            return new_record->GetUnderlyingRecordBodyAs<storage::RedoRecord>();
        }

    public:
        TransactionContext(const timestamp_t start, const timestamp_t finish,
                           const storage::RecordBufferSegmentPool *buffer_pool,
                           const storage::LogManager (log_manager))
                : start_time_(start),
                  finish_time_(finish),
                  undo_buffer_(buffer_pool),
                  redo_buffer_(log_manager, buffer_pool) {}

        ~TransactionContext() {
            for (const byte *ptr : loose_ptrs_) delete[] ptr;
        }

        bool Aborted() const { return aborted_; }

        timestamp_t StartTime() const { return start_time_; }

        timestamp_t FinishTime() const { return finish_time_.load(); }

        storage::UndoRecord *UndoRecordForUpdate(storage::DataTable *const table, const storage::TupleSlot slot,
                                                 const storage::ProjectedRow &redo) {
            const uint32_t size = storage::UndoRecord::Size(redo);
            return storage::UndoRecord::InitializeUpdate(undo_buffer_.NewEntry(size), finish_time_.load(), slot, table, redo);
        }

        storage::UndoRecord *UndoRecordForInsert(storage::DataTable *const table, const storage::TupleSlot slot) {
            byte *const result = undo_buffer_.NewEntry(sizeof(storage::UndoRecord));
            return storage::UndoRecord::InitializeInsert(result, finish_time_.load(), slot, table);
        }

        storage::UndoRecord *UndoRecordForDelete(storage::DataTable *const table, const storage::TupleSlot slot) {
            byte *const result = undo_buffer_.NewEntry(sizeof(storage::UndoRecord));
            return storage::UndoRecord::InitializeDelete(result, finish_time_.load(), slot, table);
        }

        storage::RedoRecord *StageWrite(const catalog::db_oid_t db_oid, const catalog::table_oid_t table_oid,
                                        const storage::ProjectedRowInitializer &initializer) {
            const uint32_t size = storage::RedoRecord::Size(initializer);
            auto *const log_record = storage::RedoRecord::Initialize(redo_buffer_.NewEntry(size, GetTransactionPolicy()),
                                                                     start_time_, db_oid, table_oid, initializer);
            return log_record->GetUnderlyingRecordBodyAs<storage::RedoRecord>();
        }

        void StageDelete(const catalog::db_oid_t db_oid, const catalog::table_oid_t table_oid,
                         const storage::TupleSlot slot) {
            const uint32_t size = storage::DeleteRecord::Size();
            storage::DeleteRecord::Initialize(redo_buffer_.NewEntry(size, GetTransactionPolicy()), start_time_, db_oid,
                                              table_oid, slot);
        }

        bool IsReadOnly() const { return undo_buffer_.Empty() && loose_ptrs_.empty(); }

        void RegisterAbortAction(const TransactionEndAction &a) { abort_actions_.push_front(a); }

        void RegisterAbortAction(const std::function<void()> &a) {
            RegisterAbortAction([=](transaction::DeferredActionManager * /*unused*/) { a(); });
        }

        void RegisterCommitAction(const TransactionEndAction &a) { commit_actions_.push_front(a); }

        void RegisterCommitAction(const std::function<void()> &a) {
            RegisterCommitAction([=](transaction::DeferredActionManager * /*unused*/) { a(); });
        }

        bool MustAbort() { return must_abort_; }

        void SetMustAbort() { must_abort_ = true; }

        void SetDurabilityPolicy(DurabilityPolicy durability_policy) { durability_policy_ = durability_policy; }

        DurabilityPolicy GetDurabilityPolicy() const { return durability_policy_; }

        void SetReplicationPolicy(ReplicationPolicy replication_policy) {
            ASSERT(
                    replication_policy == ReplicationPolicy::DISABLE || durability_policy_ != DurabilityPolicy::DISABLE,
                    "Replication needs durability enabled.");
            replication_policy_ = replication_policy;
        }

        /** @return The replication policy of the entire transaction. */
        ReplicationPolicy GetReplicationPolicy() const { return replication_policy_; }

        /** @return The transaction-wide policies for this transaction. */
        TransactionPolicy GetTransactionPolicy() const { return {durability_policy_, replication_policy_}; }
    };

}