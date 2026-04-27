#pragma once

#include <cstddef>
#include <cstdint>
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

enum class ReadOptions {
    Refresh,
    UseCached,
};

struct RecordView {
    uint64_t counter = 0;
    byteview meta;
    std::vector<byteview> payload_blocks;
};

enum class PutOutcome {
    Success,
    Retry,
};

struct PutResult {
    PutOutcome outcome = PutOutcome::Retry;
    uint64_t counter = 0;
};

class Log {
public:
    virtual ~Log() = default;

    // Append one logical value. Success means fresh reads should return this
    // value until a higher counter for the same key is committed. Retry means
    // another writer won the current counter race; callers should try again.
    virtual outcome::status_result<PutResult> put(std::string_view key, byteview meta, byteview payload) = 0;

    // Refresh the backend's view of storage as needed and return the latest
    // committed value for key. Returned spans are borrowed from the backend and
    // stay valid while the Log is alive. Payload is exposed as block spans;
    // callers can copy if they need one contiguous buffer.
    virtual outcome::status_result<std::optional<RecordView>>
        get_latest(std::string_view key, ReadOptions options = ReadOptions::Refresh) = 0;

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

    outcome::status_result<PutResult> put(std::string_view key, byteview meta, byteview payload) override;
    outcome::status_result<std::optional<RecordView>>
        get_latest(std::string_view key, ReadOptions options = ReadOptions::Refresh) override;
    outcome::status_result<void> refresh() override;

private:
    struct Impl;

    explicit FileLog(std::unique_ptr<Impl> impl);

    std::unique_ptr<Impl> impl_;
};

} // namespace bytelog

}
