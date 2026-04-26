#include "log_api.hpp"

#include "log.hpp"

#include <utility>

namespace borinkdb::bytelog {

struct FileLog::Impl {
    std::unique_ptr<log::file::LogWriter> writer;
    std::unique_ptr<log::file::LogReader> reader;
};

FileLogOpen FileLog::open(std::string_view path) {
    auto writer = log::file::LogWriter::open(path);
    if (!writer) {
        return FileLogOpen{.status = Status::WriteFailed, .log = nullptr};
    }
    auto reader = log::file::LogReader::open(path);
    if (!reader) {
        return FileLogOpen{.status = Status::ReadFailed, .log = nullptr};
    }

    auto impl = std::make_unique<Impl>();
    impl->writer = std::move(writer.value());
    impl->reader = std::move(reader.value());
    return FileLogOpen{
        .status = Status::Ok,
        .log = std::unique_ptr<Log>(new FileLog(std::move(impl))),
    };
}

FileLog::FileLog(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}

FileLog::FileLog(FileLog&&) = default;
FileLog& FileLog::operator=(FileLog&&) = default;
FileLog::~FileLog() = default;

Put FileLog::put(std::string_view key, byteview meta, byteview payload) {
    auto committed = impl_->writer->commit_payload(key, meta, payload);
    if (!committed) {
        return Put{.status = Status::WriteFailed};
    }

    const log::file::CommitResult& result = committed.value();
    if (result.outcome == log::file::CommitOutcome::Lost) {
        return Put{.status = Status::Lost, .counter = result.ts_counter};
    }

    if (!impl_->reader->refresh()) {
        return Put{.status = Status::ReadFailed, .counter = result.ts_counter};
    }
    return Put{.status = Status::Ok, .counter = result.ts_counter};
}

RecordViewGet FileLog::get_latest(std::string_view key, ReadOptions options) {
    log::file::LogReadMode read_mode = log::file::LogReadMode::Refresh;
    switch (options) {
        case ReadOptions::Refresh:
            read_mode = log::file::LogReadMode::Refresh;
            break;
        case ReadOptions::UseCached:
            read_mode = log::file::LogReadMode::Cached;
            break;
    }

    auto record = impl_->reader->get_record_view(key, read_mode);
    if (!record) {
        return RecordViewGet{.status = Status::ReadFailed};
    }
    if (!record.value().has_value()) {
        return RecordViewGet{.status = Status::NotFound};
    }

    const log::file::LogRecordView& value = *record.value();
    return RecordViewGet{
        .status = Status::Ok,
        .counter = value.counter,
        .meta = value.meta_bytes,
        .payload = value.payload_bytes,
    };
}

Status FileLog::refresh() {
    if (!impl_->reader->refresh()) {
        return Status::ReadFailed;
    }
    return Status::Ok;
}

}
