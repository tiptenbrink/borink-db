#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <optional>
#include <outcome/experimental/status_result.hpp>
#include <span>
#include <string_view>
#include <vector>

namespace borinkdb {

// Storage-neutral byte log API.
//
// This is intentionally below any MessagePack / reflect-cpp layer. Callers pass
// already-serialized metadata and payload bytes, and receive borrowed byte views
// back. A typed API can live above this interface and choose whatever encoding
// it wants.

namespace bytelog {

namespace outcome = OUTCOME_V2_NAMESPACE::experimental;
using byteview = std::span<const std::byte>;

struct LogicalBlockId {
    uint64_t hi = 0;
    uint64_t lo = 0;

    bool operator==(const LogicalBlockId& other) const noexcept {
        return hi == other.hi && lo == other.lo;
    }
};

struct RecordView {
    LogicalBlockId id;
    byteview meta;
    std::vector<byteview> payload_blocks;
};

enum class PutOutcome {
    Success,
    Retry,
};

struct PutResult {
    PutOutcome outcome = PutOutcome::Retry;
    LogicalBlockId id;
};

class TransactionContext {
public:
    virtual ~TransactionContext() = default;

    virtual outcome::status_result<std::optional<RecordView>> get(std::string_view key) = 0;
    virtual outcome::status_result<void> put_if(std::string_view key, byteview meta, byteview payload) = 0;
    virtual outcome::status_result<void> overwrite(std::string_view key, byteview meta, byteview payload) = 0;
};

using TransactionFunction = std::function<outcome::status_result<void>(TransactionContext&)>;

class TxHandle {
public:
    virtual ~TxHandle() = default;
    virtual outcome::status_result<PutResult> wait() = 0;
};

class Log {
public:
    virtual ~Log() = default;

    // Return the current cached/indexed value. This is intentionally a cache
    // read; callers that need to force external visibility should call refresh.
    virtual outcome::status_result<std::optional<RecordView>> get(std::string_view key) = 0;

    // Queue a side-effect-free transaction for this log's worker thread. The
    // returned handle resolves once the transaction's batch has been written and
    // this transaction's individual success/retry result is known.
    virtual outcome::status_result<std::unique_ptr<TxHandle>> tx(TransactionFunction fn) = 0;

    // Bring this instance's read-side cache/index forward without requiring a
    // lookup. For PostgreSQL this can be driven by LISTEN/NOTIFY; for the file
    // backend it scans the appended tail into the mmap-backed reader index.
    virtual outcome::status_result<void> refresh() = 0;
};

class FileLog final : public Log {
public:
    static outcome::status_result<std::unique_ptr<Log>> open(std::string_view path);

    FileLog(FileLog&&);
    FileLog& operator=(FileLog&&);
    ~FileLog() override;

    FileLog(const FileLog&) = delete;
    FileLog& operator=(const FileLog&) = delete;

    outcome::status_result<std::optional<RecordView>> get(std::string_view key) override;
    outcome::status_result<std::unique_ptr<TxHandle>> tx(TransactionFunction fn) override;
    outcome::status_result<void> refresh() override;

private:
    struct Impl;

    explicit FileLog(std::unique_ptr<Impl> impl);

    std::unique_ptr<Impl> impl_;
};

} // namespace bytelog

}
