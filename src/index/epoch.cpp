#include "epoch.h"
#include <iostream>

namespace Index {
    inline DeletionList::~DeletionList() {
        LabelDelete* cur = nullptr, *next = free_;
        while (next != nullptr) {
            cur = next;
            next = cur->next;
            delete cur;
        }
        free_ = nullptr;
    }

    inline std::size_t DeletionList::size() {
        return deletionListCount;
    }

    inline void DeletionList::remove(LabelDelete *label, LabelDelete *prev) {
        if (prev == nullptr)
            head_ = label->next;
        else
            prev->next = label->next;

        deletionListCount -= label->nodesCount;

        label->next = free_;
        free_ = label;
        deleted += label->nodesCount;
    }

    inline void DeletionList::add(void *n, uint64_t globalEpoch) {
        deletionListCount++;
        LabelDelete* label;
        if (head_ != nullptr && head_->nodesCount < head_->nodes.size()) {
            label = head_;
        } else {
            if (free_ != nullptr) {
                label = free_;
                free_ = free_->next;
            } else {
                label = new LabelDelete();
            }
            label->nodesCount = 0;
            label->next = head_;
            head_ = label;
        }
        label->nodes[label->nodesCount++] = n;
        label->epoch = globalEpoch;

        added++;
    }

    inline LabelDelete* DeletionList::head() {
        return head_;
    }

    inline void Epoch::enterEpoch(ThreadInfo &ti) {
        uint64_t current = currentEpoch.load(std::memory_order_relaxed);
        ti.getDeletionList().localEpoch.store(current, std::memory_order_release);
    }

    inline void Epoch::markNodeForDeletion(void *n, ThreadInfo &ti) {
        ti.getDeletionList().add(n, currentEpoch.load());
        ti.getDeletionList().threshold++;
    }

    inline void Epoch::exitEpochAndCleanup(ThreadInfo &ti) {
        DeletionList &deletionList = ti.getDeletionList();
        if ((deletionList.threshold & (64 - 1)) == 1) {
            currentEpoch++;
        }
        if (deletionList.threshold > startGCThreshold) {
            if (deletionList.size() == 0) {
                deletionList.threshold = 0;
                return ;
            }
            deletionList.localEpoch.store(std::numeric_limits<uint64_t>::max());

            uint64_t oldestEpoch = std::numeric_limits<uint64_t>::max();
            for (auto& deletion : deletionLists) {
                auto e = deletion.localEpoch.load();
                if (e < oldestEpoch) {
                    oldestEpoch = e;
                }
            }

            LabelDelete* cur = deletionList.head(), *next, *prev = nullptr;
            while (cur != nullptr) {
                next = cur->next;

                if (cur->epoch < oldestEpoch) {
                    for (std::size_t i = 0; i < cur->nodesCount; ++i) {
                        operator delete(cur->nodes[i]);
                    }
                    deletionList.remove(cur, prev);
                } else {
                    prev = cur;
                }
                cur = next;
            }
            deletionList.threshold = 0;
        }
    }

    Epoch::~Epoch() {
        uint64_t oldestEpoch = std::numeric_limits<uint64_t>::max();
        for (auto& deletion : deletionLists) {
            auto e= deletion.localEpoch.load();
            if (e < oldestEpoch) {
                oldestEpoch = e;
            }
        }
        for (auto& d : deletionLists) {
            LabelDelete *cur = d.head(), *next, *prev = nullptr;
            while (cur != nullptr) {
                next = cur->next;

                for (std::size_t i = 0; i < cur->nodesCount; ++i) {
                    operator delete(cur->nodes[i]);
                }
                d.remove(cur, prev);
                cur = next;
            }
        }
    }

    inline void Epoch::showDeleteRatio() {
        for (auto &d : deletionLists) {
            std::cout << "deleted " << d.deleted << " of " << d.added << std::endl;
        }
    }

    inline ThreadInfo::ThreadInfo(Epoch &epoch)
            : epoch(epoch), deletionList(epoch.deletionLists.local()) { }

    inline DeletionList &ThreadInfo::getDeletionList() const {
        return deletionList;
    }

    inline Epoch &ThreadInfo::getEpoch() const {
        return epoch;
    }
}