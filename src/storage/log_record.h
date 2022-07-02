#pragma once

#include "common/common.h"
#include "transaction/transaction_context.h"
#include "transaction/timestamp_manager.h"

namespace storage {

    class LogRecord {
    public:
        MEM_REINTERPRETATION_ONLY(LogRecord)

        LogRecordType RecordType() const { return type_; }

        uint32_t Size() const { return size_; }

        transaction::timestamp_t TxnBegin() const { return txn_begin_; }

        template<class UnderlyingType>
        UnderlyingType *GetUnderlyingRecordBodyAs() {
            ASSERT(UnderlyingType::RecordType() == type_,
                             "Attempting to access incompatible log record types");
            return reinterpret_cast<UnderlyingType *>(varlen_contents_);
        }

        template<class UnderlyingType>
        const UnderlyingType *GetUnderlyingRecordBodyAs() const {
            ASSERT(UnderlyingType::RecordType() == type_,
                             "Attempting to access incompatible log record types");
            return reinterpret_cast<const UnderlyingType *>(varlen_contents_);
        }

        static LogRecord *InitializeHeader(byte *const head, const LogRecordType type, const uint32_t size,
                                           const transaction::timestamp_t txn_begin) {
            auto *result = reinterpret_cast<LogRecord *>(head);
            result->type_ = type;
            result->size_ = size;
            result->txn_begin_ = txn_begin;
            return result;
        }

    private:
        friend class transaction::TransactionContext;

        /* Header common to all log records */
        LogRecordType type_;
        uint32_t size_;
        transaction::timestamp_t txn_begin_;
        // This needs to be aligned to 8 bytes to ensure the real size of RedoRecord (plus actual ProjectedRow) is also
        // a multiple of 8.
        uint64_t varlen_contents_[0];
    };

    class RedoRecord {
    public:
        MEM_REINTERPRETATION_ONLY(RedoRecord)

        TupleSlot GetTupleSlot() const { return tuple_slot_; }

        db_oid_t GetDatabaseOid() const { return db_oid_; }

        table_oid_t GetTableOid() const { return table_oid_; }

        void SetTupleSlot(const TupleSlot tuple_slot) { tuple_slot_ = tuple_slot; }

        ProjectedRow *Delta() { return reinterpret_cast<ProjectedRow *>(varlen_contents_); }

        const ProjectedRow *Delta() const { return reinterpret_cast<const ProjectedRow *>(varlen_contents_); }

        static constexpr LogRecordType RecordType() { return LogRecordType::REDO; }

        static uint32_t Size(const ProjectedRowInitializer &initializer) {
            return static_cast<uint32_t>(sizeof(LogRecord) + sizeof(RedoRecord) + initializer.ProjectedRowSize());
        }

        static LogRecord *Initialize(byte *const head, const transaction::timestamp_t txn_begin,
                                     const db_oid_t db_oid, const table_oid_t table_oid,
                                     const ProjectedRowInitializer &initializer) {
            LogRecord *result = LogRecord::InitializeHeader(head, LogRecordType::REDO, Size(initializer), txn_begin);
            auto *body = result->GetUnderlyingRecordBodyAs<RedoRecord>();
            body->db_oid_ = db_oid;
            body->table_oid_ = table_oid;
            body->tuple_slot_ = TupleSlot(nullptr, 0);
            initializer.InitializeRow(body->Delta());
            return result;
        }

        static LogRecord *
        PartialInitialize(byte *const head, const uint32_t size, const transaction::timestamp_t txn_begin,
                          const db_oid_t db_oid, const table_oid_t table_oid,
                          const TupleSlot tuple_slot) {
            LogRecord *result = LogRecord::InitializeHeader(head, LogRecordType::REDO, size, txn_begin);
            auto *body = result->GetUnderlyingRecordBodyAs<RedoRecord>();
            body->db_oid_ = db_oid;
            body->table_oid_ = table_oid;
            body->tuple_slot_ = tuple_slot;
            return result;
        }

    private:
        // TODO(Tianyu): We will eventually need to consult the DataTable to determine how to serialize a given column
        // (varlen? compressed? from an outdated schema?) For now we just assume we can serialize everything out as-is,
        // and the reader still have access to the layout on recovery and can deserialize. This is why we are not
        // just taking an oid.
        db_oid_t db_oid_;
        table_oid_t table_oid_;
        TupleSlot tuple_slot_;
        // This needs to be aligned to 8 bytes to ensure the real size of RedoRecord (plus actual ProjectedRow) is also
        // a multiple of 8.
        uint64_t varlen_contents_[0];
    };

    class DeleteRecord {
    public:
        MEM_REINTERPRETATION_ONLY(DeleteRecord)

        static constexpr LogRecordType RecordType() { return LogRecordType::DELETE; }

        static uint32_t Size() { return static_cast<uint32_t>(sizeof(LogRecord) + sizeof(DeleteRecord)); }

        static LogRecord *Initialize(byte *const head, const transaction::timestamp_t txn_begin,
                                     const db_oid_t db_oid, const table_oid_t table_oid,
                                     const TupleSlot slot) {
            auto *result = LogRecord::InitializeHeader(head, LogRecordType::DELETE, Size(), txn_begin);
            auto *body = result->GetUnderlyingRecordBodyAs<DeleteRecord>();
            body->db_oid_ = db_oid;
            body->table_oid_ = table_oid;
            body->tuple_slot_ = slot;
            return result;
        }

        TupleSlot GetTupleSlot() const { return tuple_slot_; }

        db_oid_t GetDatabaseOid() const { return db_oid_; }

        table_oid_t GetTableOid() const { return table_oid_; }

    private:
        db_oid_t db_oid_;
        table_oid_t table_oid_;
        TupleSlot tuple_slot_;
    };


    class CommitRecord {
    public:
        MEM_REINTERPRETATION_ONLY(CommitRecord)

        static constexpr LogRecordType RecordType() { return LogRecordType::COMMIT; }

        static uint32_t Size() { return static_cast<uint32_t>(sizeof(LogRecord) + sizeof(CommitRecord)); }


        // TODO(Tianyu): txn should contain a lot of the information here. Maybe we can simplify the function.
        // Note that however when reading log records back in we will not have a proper transaction.
        static LogRecord *Initialize(byte *const head, const transaction::timestamp_t txn_begin,
                                     const transaction::timestamp_t txn_commit, transaction::callback_fn commit_callback,
                                     void *commit_callback_arg, const transaction::timestamp_t oldest_active_txn,
                                     const bool is_read_only, transaction::TransactionContext *const txn,
                                     transaction::TimestampManager *const timestamp_manager) {
            auto *result = LogRecord::InitializeHeader(head, LogRecordType::COMMIT, Size(), txn_begin);
            auto *body = result->GetUnderlyingRecordBodyAs<CommitRecord>();
            body->txn_commit_ = txn_commit;
            body->commit_callback_ = commit_callback;
            body->commit_callback_arg_ = commit_callback_arg;
            body->oldest_active_txn_ = oldest_active_txn;
            body->timestamp_manager_ = timestamp_manager;
            body->txn_ = txn;
            body->is_read_only_ = is_read_only;
            return result;
        }

        transaction::timestamp_t CommitTime() const { return txn_commit_; }

        transaction::timestamp_t OldestActiveTxn() const { return oldest_active_txn_; }

        transaction::callback_fn CommitCallback() const { return commit_callback_; }

        transaction::TimestampManager *TimestampManager() const { return timestamp_manager_; }

        transaction::TransactionContext *Txn() const { return txn_; }

        void *CommitCallbackArg() const { return commit_callback_arg_; }

        bool IsReadOnly() const { return is_read_only_; }

    private:
        transaction::timestamp_t txn_commit_;
        transaction::callback_fn commit_callback_;
        void *commit_callback_arg_;
        transaction::timestamp_t oldest_active_txn_;
        // TODO(TIanyu): Can replace the other arguments
        // More specifically, commit timestamp and read_only can be inferred from looking inside the transaction context
        transaction::TransactionContext *txn_;
        transaction::TimestampManager *timestamp_manager_;
        bool is_read_only_;
    };

/**
 * Record body of an Abort. The header is stored in the LogRecord class that would presumably return this
 * object. An AbortRecord is only generated if an aborted transaction previously handed off a log buffer to the
 * log manager
 */
    class AbortRecord {
    public:
        MEM_REINTERPRETATION_ONLY(AbortRecord)

        static constexpr LogRecordType RecordType() { return LogRecordType::ABORT; }

        static uint32_t Size() { return static_cast<uint32_t>(sizeof(LogRecord) + sizeof(AbortRecord)); }

        static LogRecord *Initialize(byte *const head, const transaction::timestamp_t txn_begin,
                                     transaction::TransactionContext *txn,
                                     transaction::TimestampManager *const timestamp_manager) {
            auto *result = LogRecord::InitializeHeader(head, LogRecordType::ABORT, Size(), txn_begin);
            auto *body = result->GetUnderlyingRecordBodyAs<AbortRecord>();
            body->timestamp_manager_ = timestamp_manager;
            body->txn_ = txn;
            return result;
        }

        transaction::TransactionContext *Txn() const { return txn_; }

        transaction::TimestampManager *TimestampManager() const { return timestamp_manager_; }

    private:
        transaction::TimestampManager *timestamp_manager_;
        transaction::TransactionContext *txn_;
    };
}