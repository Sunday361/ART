#pragma once

#include "common/common.h"

namespace storage {
    class PACKED ProjectedRow {
        uint32_t size_;
        uint16_t num_cols_;
        byte varlen_contents_[0];

        uint32_t *AttrValueOffsets() { return StorageUtil::AlignedPtr<uint32_t>(ColumnIds() + num_cols_); }

        const uint32_t *AttrValueOffsets() const { return StorageUtil::AlignedPtr<const uint32_t>(ColumnIds() + num_cols_); }

        RawBitmap &Bitmap() { return *reinterpret_cast<RawBitmap *>(AttrValueOffsets() + num_cols_); }

        const RawBitmap &Bitmap() const {
            return *reinterpret_cast<const RawBitmap *>(AttrValueOffsets() + num_cols_);
        }

    public:
        MEM_REINTERPRETATION_ONLY(ProjectedRow)

        static ProjectedRow *copyProjectedRowLayout(void *head, const ProjectedRow &other);

        uint32_t size() const { return size_; }

        uint16_t numColumns() const { return num_cols_; }

        col_id_t *ColumnIds() { return reinterpret_cast<col_id_t *>(varlen_contents_); }

        const col_id_t *ColumnIds() const { return reinterpret_cast<const col_id_t *>(varlen_contents_); }

        byte *AccessWithNullCheck(const uint16_t offset) {
            if (!Bitmap().Test(offset)) return nullptr;
            return reinterpret_cast<byte *>(this) + AttrValueOffsets()[offset];
        }

        const byte *AccessWithNullCheck(const uint16_t offset) const {
            NOISEPAGE_ASSERT(offset < num_cols_, "Column offset out of bounds.");
            if (!Bitmap().Test(offset)) return nullptr;
            return reinterpret_cast<const byte *>(this) + AttrValueOffsets()[offset];
        }

        byte *AccessForceNotNull(const uint16_t offset) {
            NOISEPAGE_ASSERT(offset < num_cols_, "Column offset out of bounds.");
            if (!Bitmap().Test(offset)) Bitmap().Flip(offset);
            return reinterpret_cast<byte *>(this) + AttrValueOffsets()[offset];
        }

        void SetNull(const uint16_t offset) {
            NOISEPAGE_ASSERT(offset < num_cols_, "Column offset out of bounds.");
            Bitmap().Set(offset, false);
        }

        void SetNotNull(const uint16_t offset) {
            NOISEPAGE_ASSERT(offset < num_cols_, "Column offset out of bounds.");
            Bitmap().Set(offset, true);
        }

        bool IsNull(const uint16_t offset) const {
            NOISEPAGE_ASSERT(offset < num_cols_, "Column offset out of bounds.");
            return !Bitmap().Test(offset);
        }

        template <typename T, bool Nullable>
        const T *Get(const uint16_t col_idx, bool *const null) const {
            const auto *result = reinterpret_cast<const T *>(AccessWithNullCheck(col_idx));
            // NOLINTNEXTLINE: bugprone-suspicious-semicolon: seems like a false positive because of constexpr
            if constexpr (Nullable) {
                NOISEPAGE_ASSERT(null != nullptr, "Missing output variable for NULL indicator");
                if (result == nullptr) {
                    *null = true;
                    return result;
                }
                *null = false;
                return result;
            }
            return result;
        }

        template <typename T, bool Nullable>
        void Set(const uint16_t col_idx, const T &value, const bool null) {
            if constexpr (Nullable) {
                if (null) {
                    SetNull(static_cast<uint16_t>(col_idx));
                } else {
                    *reinterpret_cast<T *>(AccessForceNotNull(col_idx)) = value;
                }
            } else {  // NOLINT
                *reinterpret_cast<T *>(AccessForceNotNull(col_idx)) = value;
            }
        }
    };

    class ProjectedRowInitializer {
    public:
        ProjectedRow *InitializeRow(void *head) const;

        uint32_t ProjectedRowSize() const { return size_; }

        uint16_t NumColumns() const { return static_cast<uint16_t>(col_ids_.size()); }

        col_id_t ColId(uint16_t i) const { return col_ids_.at(i); }

        static ProjectedRowInitializer Create(const BlockLayout &layout, std::vector<col_id_t> col_ids);

        static ProjectedRowInitializer Create(std::vector<uint16_t> real_attr_sizes, const std::vector<uint16_t> &pr_offsets);

    private:

        static ProjectedRowInitializer Create(const std::vector<uint16_t> &real_attr_sizes,
                                              const std::vector<col_id_t> &col_ids);

        ProjectedRowInitializer(const std::vector<uint16_t> &attr_sizes, std::vector<col_id_t> col_ids);

        ProjectedRowInitializer() = default;

        uint32_t size_ = 0;
        std::vector<col_id_t> col_ids_;
        std::vector<uint32_t> offsets_;
    };
}
