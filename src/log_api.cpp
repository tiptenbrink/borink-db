#include "log_api.hpp"

#include "log.hpp"

#include <outcome/try.hpp>
#include <utility>

namespace borinkdb::bytelog {

struct FileLog::Impl {
    std::unique_ptr<log::file::LogWriter> writer;
    std::unique_ptr<log::file::LogReader> reader;
};

outcome::status_result<std::unique_ptr<Log>> FileLog::open(std::string_view path) {
    OUTCOME_TRY(auto writer, log::file::LogWriter::open(path));
    OUTCOME_TRY(auto reader, log::file::LogReader::open(path));

    auto impl = std::make_unique<Impl>();
    impl->writer = std::move(writer);
    impl->reader = std::move(reader);
    return std::unique_ptr<Log>(new FileLog(std::move(impl)));
}

FileLog::FileLog(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}

FileLog::FileLog(FileLog&&) = default;
FileLog& FileLog::operator=(FileLog&&) = default;
FileLog::~FileLog() = default;

outcome::status_result<uint64_t> FileLog::put(std::string_view key, byteview meta, byteview payload) {
    OUTCOME_TRY(auto result, impl_->writer->commit_payload(key, meta, payload));

    if (result.outcome == log::file::CommitOutcome::Lost) {
        return log::file::Error::commit_lost;
    }

    OUTCOME_TRYV(impl_->reader->refresh());
    return result.ts_counter;
}

outcome::status_result<std::optional<RecordView>> FileLog::get_latest(std::string_view key, ReadOptions options) {
    log::file::LogReadMode read_mode = log::file::LogReadMode::Refresh;
    switch (options) {
        case ReadOptions::Refresh:
            read_mode = log::file::LogReadMode::Refresh;
            break;
        case ReadOptions::UseCached:
            read_mode = log::file::LogReadMode::Cached;
            break;
    }

    OUTCOME_TRY(auto record, impl_->reader->get_record_view(key, read_mode));
    if (!record.has_value()) {
        return std::optional<RecordView>{};
    }

    const log::file::LogRecordView& value = *record;
    return std::optional<RecordView>{RecordView{
        .counter = value.counter,
        .meta = value.meta_bytes,
        .payload = value.payload_bytes,
    }};
}

outcome::status_result<void> FileLog::refresh() {
    return impl_->reader->refresh();
}

}
