#pragma once

#include <atomic>
#include <array>
#include "tbb/enumerable_thread_specific.h"
#include "tbb/combinable.h"

namespace Index {

    struct LabelDelete {
        std::array<void*, 32> nodes;
        uint64_t epoch;
        std::size_t nodesCount;
        LabelDelete* next;
    };

    class DeletionList {
        LabelDelete* head_ = nullptr;
        LabelDelete* free_ = nullptr;
        std::size_t deletionListCount = 0;

    public:
        std::atomic<uint64_t> localEpoch;
        size_t threshold = 0;

        ~DeletionList();
        LabelDelete* head();

        void add(void* n, uint64_t globalEpoch);

        void remove(LabelDelete* label, LabelDelete* prev);

        std::size_t size();

        std::size_t deleted = 0;
        std::size_t added = 0;
    };

    class Epoch;
    class EpochGuard;

    class ThreadInfo {
        friend class Epoch;
        friend class EpochGuard;

        Epoch& epoch;
        DeletionList& deletionList;

        DeletionList& getDeletionList() const;

    public:
        ThreadInfo(Epoch& e);
        ThreadInfo(const ThreadInfo& ti) : epoch(ti.epoch), deletionList(ti.deletionList) {}
        ~ThreadInfo();

        Epoch& getEpoch() const;
    };

    class Epoch {
        friend class ThreadInfo;
        std::atomic<uint64_t> currentEpoch{0};

        tbb::enumerable_thread_specific<DeletionList> deletionLists;

        size_t startGCThreshold;

    public:
        Epoch(size_t startGCThreshold) : startGCThreshold(startGCThreshold) {}
        ~Epoch();

        void enterEpoch(ThreadInfo& ti);

        void markNodeForDeletion(void *n, ThreadInfo &ti);

        void exitEpochAndCleanup(ThreadInfo &ti);

        void showDeleteRatio();
    };

    class EpochGuard {
        ThreadInfo& ti;
    public:
        EpochGuard(ThreadInfo& ti) : ti(ti) {
            ti.getEpoch().enterEpoch(ti);
        }

        ~EpochGuard() {
            ti.getEpoch().exitEpochAndCleanup(ti);
        }
    };

    inline ThreadInfo::~ThreadInfo() {
        deletionList.localEpoch.store(std::numeric_limits<uint64_t>::max());
    }

}
