#pragma once

#include "common/enum_defs.h"

namespace transaction {

    typedef uint64 timestamp_t;

    using DeferredAction = std::function<void(timestamp_t)>;

    using TransactionEndAction = std::function<void(DeferredActionManager * )>;

    using TransactionQueue = std::forward_list<transaction::TransactionContext *>;

    using callback_fn = void (*)(void *);

#define DURABILITY_POLICY_ENUM(T)                                                  \
  /** Do not make any buffers durable. */                                          \
  T(DurabilityPolicy, DISABLE)                                                     \
  /** Synchronous: commits must wait for logs to be written to disk. */            \
  T(DurabilityPolicy, SYNC)                                                        \
  /** Asynchronous: commits do not need to wait for logs to be written to disk. */ \
  T(DurabilityPolicy, ASYNC)
/** DurabilityPolicy controls whether commits must wait for logs to be written to disk. */
    ENUM_DEFINE(DurabilityPolicy, uint8_t, DURABILITY_POLICY_ENUM);
#undef DURABILITY_POLICY_ENUM

#define REPLICATION_POLICY_ENUM(T)                                                                         \
  /** Do not replicate any logs. */                                                                        \
  T(ReplicationPolicy, DISABLE)                                                                            \
  /** Synchronous: commits must wait for logs to be replicated and applied. */                             \
  T(ReplicationPolicy, SYNC)                                                                               \
  /** Asynchronous: logs will be replicated, but commits do not need to wait for replication to happen. */ \
  T(ReplicationPolicy, ASYNC)
/**
 * ReplicationPolicy controls whether logs should be replicated over the network,
 * and whether logs must be applied on replicas before commit callbacks are invoked.
 */
    ENUM_DEFINE(ReplicationPolicy, uint8_t, REPLICATION_POLICY_ENUM);
#undef REPLICATION_POLICY_ENUM

/** Transaction-wide policies. */
    struct TransactionPolicy {
        DurabilityPolicy durability_;    ///< Durability policy for the entire transaction.
        ReplicationPolicy replication_;  ///< Replication policy for the entire transaction.

        /** @return True if the transaction policies are identical. False otherwise. */
        bool operator==(const TransactionPolicy &other) const {
            return durability_ == other.durability_ && replication_ == other.replication_;
        }
    };
}