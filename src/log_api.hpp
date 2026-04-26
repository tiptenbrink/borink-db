#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <outcome/experimental/status_result.hpp>
#include <span>
#include <string_view>

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
    byteview payload;
};


class Log {
public:
    virtual ~Log() = default;

    // Append one logical value. A Committed result means fresh reads should return this
    // value until a higher counter for the same key is committed.
    virtual outcome::status_result<uint64_t> put(std::string_view key, byteview meta, byteview payload) = 0;

    // Refresh the backend's view of storage as needed and return the latest
    // committed value for key. Returned spans are borrowed from the backend and
    // stay valid only until the next backend operation that can refresh or reuse
    // its internal buffers.
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

    outcome::status_result<uint64_t> put(std::string_view key, byteview meta, byteview payload) override;
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
