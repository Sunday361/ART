#pragma once

namespace transaction {

    typedef uint64 timestamp_t;

    using DeferredAction = std::function<void(timestamp_t)>;
}