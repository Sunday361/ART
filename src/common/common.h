#pragma once

#include <cstdint>
#include <vector>
#include <cassert>
#include <stdexcept>
#include <cstring>
#include <atomic>

using namespace std;

#define ASSERT(condition, message) \
    do { \
        if (! (condition)) { \
            std::cerr << "Assertion `" #condition "` failed in " << __FILE__ \
                      << " line " << __LINE__ << ": " << message << std::endl; \
            std::terminate(); \
        } \
    } while (false)

#define DISALLOW_COPY(cname)     \
  /* Delete copy constructor. */ \
  cname(const cname &) = delete; \
  /* Delete copy assignment. */  \
  cname &operator=(const cname &) = delete;

#define DISALLOW_MOVE(cname)     \
  /* Delete move constructor. */ \
  cname(cname &&) = delete;      \
  /* Delete move assignment. */  \
  cname &operator=(cname &&) = delete;

#define DISALLOW_COPY_AND_MOVE(cname) \
  DISALLOW_COPY(cname);               \
  DISALLOW_MOVE(cname);

#define MEM_REINTERPRETATION_ONLY(cname) \
  cname() = delete;                      \
  DISALLOW_COPY_AND_MOVE(cname)          \
  ~cname() = delete;

#define UNUSED_ATTRIBUTE __attribute__((unused))
#define RESTRICT __restrict__
#define NEVER_INLINE __attribute__((noinline))
#define PACKED __attribute__((packed))
// NOLINTNEXTLINE
#define FALLTHROUGH [[fallthrough]]
#define NORETURN __attribute((noreturn))

#ifdef __clang__
#define NO_CLONE
#else
#define NO_CLONE __attribute__((noclone))
#endif

/**
 * Above are Macro definitions used in this project
 * */

using TID = uint64_t;

using col_id_t = uint16_t;

#define BYTE_SIZE 8u
#define LSB_ONE_HOT_MASK(n) (1U << n)
#define LSB_ONE_COLD_MASK(n) (0xFF - LSB_ONE_HOT_MASK(n))

struct AllocationUtil {
    AllocationUtil() = delete;

    static byte *AllocateAligned(uint64_t byte_size) {
        return reinterpret_cast<byte *>(new uint64_t[(byte_size + 7) / 8]);
    }

    template <class T>
    static T *AllocateAligned(uint32_t size) {
        return reinterpret_cast<T *>(AllocateAligned(size * sizeof(T)));
    }
};


class RawBitmap {
    uint8_t bits_[0];
public:
    static constexpr uint32_t SizeInBytes(uint32_t n) { return n % BYTE_SIZE == 0 ? n / BYTE_SIZE : n / BYTE_SIZE + 1; }

    static RawBitmap *Allocate(const uint32_t num_bits) {
        auto size = SizeInBytes(num_bits);
        auto *result = new uint8_t[size];
        std::memset(result, 0, size);
        return reinterpret_cast<RawBitmap *>(result);
    }

    static void Deallocate(RawBitmap *const map) { delete[] reinterpret_cast<uint8_t *>(map); }

    bool Test(const uint32_t pos) const {
        return static_cast<bool>(bits_[pos / BYTE_SIZE] & LSB_ONE_HOT_MASK(pos % BYTE_SIZE));
    }

    bool operator[](const uint32_t pos) const { return Test(pos); }

    RawBitmap &Set(const uint32_t pos, const bool val) {
        if (val)
            bits_[pos / BYTE_SIZE] |= static_cast<uint8_t>(LSB_ONE_HOT_MASK(pos % BYTE_SIZE));
        else
            bits_[pos / BYTE_SIZE] &= static_cast<uint8_t>(LSB_ONE_COLD_MASK(pos % BYTE_SIZE));
        return *this;
    }

    RawBitmap &Flip(const uint32_t pos) {
        bits_[pos / BYTE_SIZE] ^= static_cast<uint8_t>(LSB_ONE_HOT_MASK(pos % BYTE_SIZE));
        return *this;
    }

    void Clear(const uint32_t num_bits) {
        auto size = SizeInBytes(num_bits);
        std::memset(bits_, 0, size);
    }
};

class RawConcurrentBitmap {
public:
    MEM_REINTERPRETATION_ONLY(RawConcurrentBitmap)

    static RawConcurrentBitmap *Allocate(const uint32_t num_bits) {
        uint32_t num_bytes = RawBitmap::SizeInBytes(num_bits);
        auto *result = AllocationUtil::AllocateAligned(num_bytes);
        ASSERT(reinterpret_cast<uintptr_t>(result) % sizeof(uint64_t) == 0, "Allocate should be 64-bit aligned.");
        std::memset(result, 0, num_bytes);
        return reinterpret_cast<RawConcurrentBitmap *>(result);
    }

    static void Deallocate(RawConcurrentBitmap *const map) { delete[] reinterpret_cast<uint8_t *>(map); }

    bool Test(const uint32_t pos) const {
        return static_cast<bool>(bits_[pos / BYTE_SIZE].load() & LSB_ONE_HOT_MASK(pos % BYTE_SIZE));
    }

    bool operator[](const uint32_t pos) const { return Test(pos); }

    bool Flip(const uint32_t pos, const bool expected_val) {
        const uint32_t element = pos / BYTE_SIZE;
        auto mask = static_cast<uint8_t>(LSB_ONE_HOT_MASK(pos % BYTE_SIZE));
        for (uint8_t old_val = bits_[element]; static_cast<bool>(old_val & mask) == expected_val;
             old_val = bits_[element]) {
            uint8_t new_val = old_val ^ mask;
            if (bits_[element].compare_exchange_strong(old_val, new_val)) return true;
        }
        return false;
    }

    bool FirstUnsetPos(const uint32_t bitmap_num_bits, const uint32_t start_pos, uint32_t *const out_pos) const {
        // invalid starting position.
        if (start_pos >= bitmap_num_bits) {
            return false;
        }

        ASSERT(reinterpret_cast<uintptr_t>(bits_) % sizeof(uint64_t) == 0, "bits_ should be 64-bit aligned.");

        const uint32_t num_bytes = RawBitmap::SizeInBytes(bitmap_num_bits);  // maximum number of bytes in the bitmap
        uint32_t byte_pos = start_pos / BYTE_SIZE;                           // current byte position
        uint32_t bits_left = bitmap_num_bits - start_pos;                    // number of bits remaining
        bool found_unset_bit = false;                                        // whether we found an unset bit previously

        while (byte_pos < num_bytes && bits_left > 0) {
            if (!found_unset_bit && IsAlignedAndFits<uint64_t>(bits_left, byte_pos)) {
                found_unset_bit = FindUnsetBit<int64_t>(&byte_pos, &bits_left);
            } else if (!found_unset_bit && IsAlignedAndFits<uint32_t>(bits_left, byte_pos)) {
                found_unset_bit = FindUnsetBit<int32_t>(&byte_pos, &bits_left);
            } else if (!found_unset_bit && IsAlignedAndFits<uint16_t>(bits_left, byte_pos)) {
                found_unset_bit = FindUnsetBit<int16_t>(&byte_pos, &bits_left);
            } else {
                uint8_t bits = bits_[byte_pos].load();
                if (static_cast<std::atomic<int8_t>>(bits) != -1) {
                    for (uint32_t pos = 0; pos < BYTE_SIZE; pos++) {
                        uint32_t current_pos = pos + byte_pos * BYTE_SIZE;
                        // we are always padded to a byte, but we don't want to use the padding.
                        if (current_pos >= bitmap_num_bits) return false;
                        // we want to make sure it is after the start pos.
                        if (current_pos < start_pos) {
                            continue;
                        }
                        // if we're here, we have a valid position.
                        // if it locates an unset bit, return it.
                        auto is_set = static_cast<bool>(bits & LSB_ONE_HOT_MASK(pos));
                        if (!is_set) {
                            *out_pos = current_pos;
                            return true;
                        }
                    }
                }
                byte_pos += 1;
                bits_left -= BYTE_SIZE - (start_pos % BYTE_SIZE);
            }
        }
        return false;
    }

    void UnsafeClear(const uint32_t num_bits) {
        auto size = RawBitmap::SizeInBytes(num_bits);
        std::memset(static_cast<void *>(bits_), 0, size);
    }

    // TODO(Tianyu): We will eventually need optimization for bulk checks and
    // bulk flips. This thing is embarrassingly easy to vectorize.

private:
    std::atomic<uint8_t> bits_[0];

    template <class T>
    bool FindUnsetBit(uint32_t *const byte_pos, uint32_t *const bits_left) const {
        ASSERT(*bits_left >= sizeof(T) * BYTE_SIZE,
                         "Need to check that there are enough bits left before calling");
        T bits = reinterpret_cast<const std::atomic<T> *>(&bits_[*byte_pos])->load();
        if (bits == static_cast<T>(-1)) {
            *byte_pos += static_cast<uint32_t>(sizeof(T));
            *bits_left = *bits_left - static_cast<uint32_t>(sizeof(T) * BYTE_SIZE);
            return false;
        }
        return true;
    }

    template <class T>
    bool IsAlignedAndFits(const uint32_t bits_left, const uint32_t byte_pos) const {
        return bits_left >= sizeof(T) * BYTE_SIZE && byte_pos % sizeof(T) == 0;
    }
};


