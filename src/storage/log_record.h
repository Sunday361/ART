#pragma once

#include "common/common.h"
#include "transaction/transaction_context.h"
#include "transaction/timestamp_manager.h"

namespace storage {

    class RedoRecord {

    };

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
}