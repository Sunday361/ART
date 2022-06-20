#pragma once

#include "data_table.h"

namespace storage {
    class DataTable;

    class TupleAccessStrategy {
    private:
        /*
         * A mini block stores individual columns. Mini block layout:
         * ----------------------------------------------------
         * | null-bitmap (pad up to 8 bytes) | val1 | val2 | ... |
         * ----------------------------------------------------
         * Warning, 0 means null
         */
        struct MiniBlock {
            MEM_REINTERPRETATION_ONLY(MiniBlock)
            // return a pointer to the start of the column. (use as an array)
            byte *ColumnStart(const BlockLayout &layout, const col_id_t col_id) {
                return StorageUtil::AlignedPtr(sizeof(uint64_t),  // always padded up to 8 bytes
                                               varlen_contents_ + common::RawBitmap::SizeInBytes(layout.NumSlots()));
            }

            // return The null-bitmap of this column
            RawConcurrentBitmap *NullBitmap() {
                return reinterpret_cast<common::RawConcurrentBitmap *>(varlen_contents_);
            }

            byte varlen_contents_[0];
        };
    };
}