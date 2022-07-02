#pragma once

#include <common/shared_lock.h>

#include <storage/block_layout.h>
#include <storage/tuple_access_strategy.h>
#include <transaction/transaction_context.h>

namespace storage {

    class DataTable {
    public:
        class SlotIterator {
        public:
            const TupleSlot &operator*() const { return current_slot_; }

            const TupleSlot *operator->() const { return &current_slot_; }

            SlotIterator &operator++() {
                RawBlock *b = current_slot_.GetBlock();
                slot_num_++;

                if (slot_num_ < max_slot_num_) {
                    current_slot_ = {b, slot_num_};
                } else {
                    ASSERT(block_index_ <= end_index_, "block_index_ must always stay in range of table's size");
                    block_index_++;
                    UpdateFromNextBlock();
                }
                return *this;
            }

            SlotIterator operator++(int) {
                SlotIterator copy = *this;
                operator++();
                return copy;
            }

            bool operator==(const SlotIterator &other) const { return current_slot_ == other.current_slot_; }
        private:
            friend class DataTable;

            // constructor for DataTable::end()
            SlotIterator() = default;

            SlotIterator(const DataTable *table) : table_(table), block_index_(0) {  // NOLINT
                end_index_ = table_->blocks_size_;
                ASSERT(end_index_ >= 1, "there should always be at least one block");
                UpdateFromNextBlock();
            }

            void UpdateFromNextBlock() {
                ASSERT(end_index_ >= 1, "there should always be at least one block");

                while (true) {
                    if (block_index_ == end_index_) {
                        max_slot_num_ = 0;
                        current_slot_ = InvalidTupleSlot();
                        return;
                    }

                    RawBlock *b;
                    {
                        common::SharedLatch::ScopedSharedLatch latch(&table_->blocks_latch_);
                        b = table_->blocks_[block_index_];
                    }
                    slot_num_ = 0;
                    max_slot_num_ = b->GetInsertHead();
                    current_slot_ = {b, slot_num_};

                    if (max_slot_num_ != 0) return;
                    block_index_++;
                }
            }

            static auto InvalidTupleSlot() -> TupleSlot { return {nullptr, 0}; }
            const DataTable *table_ = nullptr;
            uint64_t block_index_ = 0, end_index_ = 0;
            TupleSlot current_slot_ = InvalidTupleSlot();
            uint32_t slot_num_ = 0, max_slot_num_ = 0;
        };

        DataTable(BlockStore *store, const BlockLayout &layout, layout_version_t v);

        ~DataTable();

        bool Select(transaction::TransactionContext *txn, TupleSlot slot, ProjectedRow *out_buffer) const;

        void Scan(transaction::TransactionContext* txn, SlotIterator *start_pos,
                  ProjectedColumns *out_buffer) const;

        void Scan(transaction::TransactionContext* txn, SlotIterator *start_pos,
                  execution::sql::VectorProjection *out_buffer) const;

        bool Update(transaction::TransactionContext* txn,
                    TupleSlot slot, const ProjectedRow &redo);

        TupleSlot Insert(transaction::TransactionContext* txn, const ProjectedRow &redo);

        bool Delete(transaction::TransactionContext* txn, TupleSlot slot);

        SlotIterator begin() const {  // NOLINT for STL name compability
            return {this};
        }

        SlotIterator end() const {  // NOLINT for STL name compability
            return SlotIterator();
        }

        SlotIterator GetBlockedSlotIterator(uint32_t start, uint32_t end) const {
            ASSERT(start <= end && end <= blocks_size_, "must have valid index for start and end");
            SlotIterator it(this);
            it.end_index_ = std::min<uint64_t>(it.end_index_, end);
            it.block_index_ = start;

            it.UpdateFromNextBlock();
            return it;
        }

        std::vector<RawBlock *> GetBlocks() const {
            common::SharedLatch::ScopedSharedLatch latch(&blocks_latch_);
            return std::vector<RawBlock *>(blocks_.begin(), blocks_.end());
        }

        const BlockLayout &GetBlockLayout() const { return accessor_.GetBlockLayout(); }

        uint32_t GetNumBlocks() const { return blocks_size_; }

        /** @return Maximum number of blocks in the data table. */
        static uint32_t GetMaxBlocks() { return std::numeric_limits<uint32_t>::max(); }

        void Reset();

        uint64_t GetNumTuple() const { return GetBlockLayout().NumSlots() * blocks_size_; }

        size_t EstimateHeapUsage() const {
            // This is a back-of-the-envelope calculation that could be innacurate. It does not account for the delta chain
            // elements that are actually owned by TransactionContext
            return blocks_size_ * BLOCK_SIZE;
        }

    private:
        const TupleAccessStrategy accessor_;

        std::atomic<uint64_t> blocks_size_ = 0;
        std::atomic<uint64_t> insert_index_ = 0;
        BlockStore* const block_store_;

        // protected by blocks_latch_
        std::vector<RawBlock *> blocks_;
        mutable common::SharedLatch blocks_latch_;
        const layout_version_t layout_version_;

        // A templatized version for select, so that we can use the same code for both row and column access.
        // the method is explicitly instantiated for ProjectedRow and ProjectedColumns::RowView
        template <class RowType>
        bool SelectIntoBuffer(transaction::TransactionContext *txn, TupleSlot slot,
                              RowType *out_buffer) const;

        void InsertInto(transaction::TransactionContext* txn, const ProjectedRow &redo,
                        TupleSlot dest);
        // Atomically read out the version pointer value.
        UndoRecord *AtomicallyReadVersionPtr(TupleSlot slot, const TupleAccessStrategy &accessor) const;

        // Atomically write the version pointer value. Should only be used by Insert where there is guaranteed to be no
        // contention
        void AtomicallyWriteVersionPtr(TupleSlot slot, const TupleAccessStrategy &accessor, UndoRecord *desired);

        // Checks for Snapshot Isolation conflicts, used by Update
        bool HasConflict(const transaction::TransactionContext &txn, UndoRecord *version_ptr) const;

        // Wrapper around the other HasConflict for indexes to call (they only have tuple slot, not the version pointer)
        bool HasConflict(const transaction::TransactionContext &txn, TupleSlot slot) const;

        // Performs a visibility check on the designated TupleSlot. Note that this does not traverse a version chain, so this
        // information alone is not enough to determine visibility of a tuple to a transaction. This should be used along with
        // a version chain traversal to determine if a tuple's versions are actually visible to a txn.
        // The criteria for visibility of a slot are presence (slot is occupied) and not deleted
        // (logical delete bitmap is non-NULL).
        bool Visible(TupleSlot slot, const TupleAccessStrategy &accessor) const;

        // Compares and swaps the version pointer to be the undo record, only if its value is equal to the expected one.
        bool CompareAndSwapVersionPtr(TupleSlot slot, const TupleAccessStrategy &accessor, UndoRecord *expected,
                                      UndoRecord *desired);

        // Allocates a new block to be used as insertion head.
        RawBlock *NewBlock();

        bool IsVisible(const transaction::TransactionContext &txn, TupleSlot slot) const;
    };
}
