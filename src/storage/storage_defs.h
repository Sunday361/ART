#pragma once

#include <cstdint>

namespace storage {

    static const uint32_t BLOCK_SIZE = 1 << 20;

    static const uint32_t BUFFER_SEGMENT_SIZE = 1 << 12;

    enum class DeltaRecordType : uint8_t {
        UPDATE = 0, INSERT, DELETE
    };

    enum class LogRecordType : uint8_t {
        REDO = 1, DELETE, COMMIT, ABORT
    };

    class alignas(BLOCK_SIZE) RawBlock {
    };

    class TupleSlot {
        uintptr_t bytes_;
    public:
        TupleSlot() = default;
        TupleSlot(const RawBlock* const block, const uint32_t offset)
            : bytes_(reinterpret_cast<uintptr_t>(block) | offset) {}

        RawBlock *GetBlock() const {
            // Get the first 44 bits as the ptr
            return reinterpret_cast<RawBlock *>(bytes_ & ~(static_cast<uintptr_t>(common::Constants::BLOCK_SIZE) - 1));
        }

        uint32_t GetOffset() const {
            return static_cast<uint32_t>(bytes_ & (static_cast<uintptr_t>(common::Constants::BLOCK_SIZE) - 1));
        }

        bool operator==(const TupleSlot &other) const { return bytes_ == other.bytes_; }

        bool operator!=(const TupleSlot &other) const { return bytes_ != other.bytes_; }

        friend std::ostream &operator<<(std::ostream &os, const TupleSlot &slot) {
            return os << "block: " << slot.GetBlock() << ", offset: " << slot.GetOffset();
        }
    };
}