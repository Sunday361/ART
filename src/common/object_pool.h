#pragma once

#include <queue>
#include <exception>

#include "common.h"
#include "spin_lock.h"

class NoMoreObjectException : public std::exception {
public:
    explicit NoMoreObjectException(uint64_t limit)
            : message_("Object Pool have no object to hand out. Exceed size limit " + std::to_string(limit) + ".\n") {}

    const char *what() const noexcept override { return message_.c_str(); }

private:
    std::string message_;
};

class AllocatorFailureException : public std::exception {
public:
    const char *what() const noexcept override { return "Allocator fails to allocate memory.\n"; }
};

template<typename T>
class ByteAlignedAllocator {
public:
    T *New() {
        auto *result = reinterpret_cast<T *>(AllocationUtil::AllocateAligned(sizeof(T)));
        Reuse(result);
        return result;
    }

    void Reuse(T *const reused) {}

    void Delete(T *const ptr) { delete[] reinterpret_cast<byte *>(ptr); }
};

template<typename T, class Allocator = ByteAlignedAllocator<T>>
class ObjectPool {
    Allocator alloc_;
    SpinLatch latch_;
    // TODO(yangjuns): We don't need to reuse objects in a FIFO pattern. We could potentially pass a second template
    // parameter to define the backing container for the std::queue. That way we can measure each backing container.
    std::queue<T *> reuse_queue_;
    uint64_t size_limit_;   // the maximum number of objects a object pool can have
    uint64_t reuse_limit_;  // the maximum number of reusable objects in reuse_queue
    // current_size_ represents the number of objects the object pool has allocated,
    // including objects that have been given out to callers and those reside in reuse_queue
    uint64_t current_size_;
public:
    ObjectPool(uint64_t size_limit, uint64_t reuse_limit)
            : size_limit_(size_limit), reuse_limit_(reuse_limit), current_size_(0) {}

    ~ObjectPool() {
        T *result = nullptr;
        while (!reuse_queue_.empty()) {
            result = reuse_queue_.front();
            alloc_.Delete(result);
            reuse_queue_.pop();
        }
    }

    T *Get() {
        SpinLatch::ScopedSpinLatch guard(&latch_);
        if (reuse_queue_.empty() && current_size_ >= size_limit_) throw NoMoreObjectException(size_limit_);
        T *result = nullptr;
        if (reuse_queue_.empty()) {
            result = alloc_.New();  // result could be null because the allocator may not find enough memory space
            if (result != nullptr) current_size_++;
        } else {
            result = reuse_queue_.front();
            reuse_queue_.pop();
            alloc_.Reuse(result);
        }
        if (result == nullptr) throw AllocatorFailureException();
        ASSERT(current_size_ <= size_limit_, "Object pool has exceeded its size limit.");
        return result;
    }

    bool SetSizeLimit(uint64_t new_size) {
        SpinLatch::ScopedSpinLatch guard(&latch_);
        if (new_size >= current_size_) {
            // current_size_ might increase and become > new_size if we don't use lock
            size_limit_ = new_size;
            ASSERT(current_size_ <= size_limit_, "object pool size exceed its size limit");
            return true;
        }
        return false;
    }

    void SetReuseLimit(uint64_t new_reuse_limit) {
        SpinLatch::ScopedSpinLatch guard(&latch_);
        reuse_limit_ = new_reuse_limit;
        T *obj = nullptr;
        while (reuse_queue_.size() > reuse_limit_) {
            obj = reuse_queue_.front();
            alloc_.Delete(obj);
            reuse_queue_.pop();
            current_size_--;
        }
    }

    void Release(T *obj) {
        ASSERT(obj != nullptr, "releasing a null pointer");
        SpinLatch::ScopedSpinLatch guard(&latch_);
        if (reuse_queue_.size() >= reuse_limit_) {
            alloc_.Delete(obj);
            current_size_--;
        } else {
            reuse_queue_.push(obj);
        }
    }

    uint64_t GetSizeLimit() const { return size_limit_; }
};
