#pragma once

#include <vector>

#include "common/common.h"
#include "storage_defs.h"
#include "projectd_row.h"

namespace storage {
    class DataTable;

/**
 * Extension of a ProjectedRow that adds relevant information to be able to traverse the version chain and find the
 * relevant tuple version:
 * pointer to the next record, timestamp of the transaction that created this record, pointer to the data table, and the
 * tuple slot.
 */
    class UndoRecord {
    public:
        MEM_REINTERPRETATION_ONLY(UndoRecord)

        std::atomic<UndoRecord *> &Next() { return next_; }

        const std::atomic<UndoRecord *> &Next() const { return next_; }

        std::atomic<transaction::timestamp_t> &Timestamp() { return timestamp_; }

        const std::atomic<transaction::timestamp_t> &Timestamp() const { return timestamp_; }

        DeltaRecordType Type() const { return type_; }

        DataTable *&Table() { return table_; }

        DataTable *Table() const { return table_; }

        TupleSlot Slot() const { return slot_; }

        ProjectedRow *Delta() { return reinterpret_cast<ProjectedRow *>(varlen_contents_); }

        const ProjectedRow *Delta() const { return reinterpret_cast<const ProjectedRow *>(varlen_contents_); }

        uint32_t Size() const {
            return static_cast<uint32_t>(sizeof(UndoRecord) + (type_ == DeltaRecordType::UPDATE ? Delta()->Size() : 0));
        }

        static uint32_t Size(const ProjectedRow &redo) {
            return static_cast<uint32_t>(sizeof(UndoRecord)) + redo.Size();
        }

        static uint32_t Size(const ProjectedRowInitializer &initializer) {
            return static_cast<uint32_t>(sizeof(UndoRecord)) + initializer.ProjectedRowSize();
        }

        static UndoRecord *
        InitializeInsert(byte *const head, const transaction::timestamp_t timestamp, const TupleSlot slot,
                         DataTable *const table) {
            auto *result = reinterpret_cast<UndoRecord *>(head);
            result->type_ = DeltaRecordType::INSERT;
            result->next_ = nullptr;
            result->timestamp_.store(timestamp);
            result->table_ = table;
            result->slot_ = slot;
            return result;
        }

        static UndoRecord *
        InitializeDelete(byte *const head, const transaction::timestamp_t timestamp, const TupleSlot slot,
                         DataTable *const table) {
            auto *result = reinterpret_cast<UndoRecord *>(head);
            result->type_ = DeltaRecordType::DELETE;
            result->next_ = nullptr;
            result->timestamp_.store(timestamp);
            result->table_ = table;
            result->slot_ = slot;
            return result;
        }

        static UndoRecord *
        InitializeUpdate(byte *const head, const transaction::timestamp_t timestamp, const TupleSlot slot,
                         DataTable *const table, const ProjectedRowInitializer &initializer) {
            auto *result = reinterpret_cast<UndoRecord *>(head);

            result->type_ = DeltaRecordType::UPDATE;
            result->next_ = nullptr;
            result->timestamp_.store(timestamp);
            result->table_ = table;
            result->slot_ = slot;

            initializer.InitializeRow(result->varlen_contents_);

            return result;
        }

        static UndoRecord *
        InitializeUpdate(byte *const head, const transaction::timestamp_t timestamp, const TupleSlot slot,
                         DataTable *const table, const storage::ProjectedRow &redo) {
            auto *result = reinterpret_cast<UndoRecord *>(head);

            result->type_ = DeltaRecordType::UPDATE;
            result->next_ = nullptr;
            result->timestamp_.store(timestamp);
            result->table_ = table;
            result->slot_ = slot;

            ProjectedRow::CopyProjectedRowLayout(result->varlen_contents_, redo);

            return result;
        }

    private:
        DeltaRecordType type_;
        std::atomic<UndoRecord *> next_;
        std::atomic<transaction::timestamp_t> timestamp_;
        DataTable *table_;
        TupleSlot slot_;
        uint64_t varlen_contents_[0];
    };
}

static_assert(sizeof(storage::UndoRecord) % 8 == 0,
                  "a projected row inside the undo record needs to be aligned to 8 bytes"
                  "to ensure true atomicity");


