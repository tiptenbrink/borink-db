#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
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

enum class Status {
    Ok,
    NotFound,
    Lost,
    ReadFailed,
    WriteFailed,
    TooLarge,
    CorruptStorage,
};

enum class ReadOptions {
    Refresh,
    UseCached,
};

struct Put {
    Status status = Status::Ok;
    uint64_t counter = 0;

    explicit operator bool() { return status == Status::Ok; }

    // TODO: some convenient way to check if we have to retry, some errors are fatal others aren't
};

struct RecordViewGet {
    Status status = Status::NotFound;
    uint64_t counter = 0;
    std::span<const std::byte> meta;
    std::span<const std::byte> payload;

    explicit operator bool() { return status == Status::Ok; }
};

using byteview = std::span<const std::byte>;

class Log {
public:
    virtual ~Log() = default;

    // Append one logical value. A Committed result means fresh reads should return this
    // value until a higher counter for the same key is committed.
    virtual Put put(std::string_view key, byteview meta, byteview payload) = 0;

    // Refresh the backend's view of storage as needed and return the latest
    // committed value for key. Returned spans are borrowed from the backend and
    // stay valid only until the next backend operation that can refresh or reuse
    // its internal buffers.
    virtual RecordViewGet get_latest(std::string_view key, ReadOptions options = ReadOptions::Refresh) = 0;

    // Bring this instance's read-side cache/index forward without requiring a
    // lookup. For PostgreSQL this can be driven by LISTEN/NOTIFY; for the file
    // backend it scans the appended tail into the mmap-backed reader index.
    virtual Status refresh() = 0;
};

struct FileLogOpen {
    Status status = Status::Ok;
    std::unique_ptr<Log> log;

    explicit operator bool() { return status == Status::Ok && log != nullptr; }
};

class FileLog final : public Log {
public:
    static FileLogOpen open(std::string_view path);

    FileLog(FileLog&&);
    FileLog& operator=(FileLog&&);
    ~FileLog() override;

    FileLog(const FileLog&) = delete;
    FileLog& operator=(const FileLog&) = delete;

    Put put(std::string_view key, byteview meta, byteview payload) override;
    RecordViewGet get_latest(std::string_view key, ReadOptions options = ReadOptions::Refresh) override;
    Status refresh() override;

private:
    struct Impl;

    explicit FileLog(std::unique_ptr<Impl> impl);

    std::unique_ptr<Impl> impl_;
};

} // namespace bytelog

}
