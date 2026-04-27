#include "log_api.hpp"

#include "log.hpp"

#include <outcome/try.hpp>
#include <utility>

namespace borinkdb::bytelog {

struct FileLog::Impl {
    std::unique_ptr<log::file::LogFile> log;
};

outcome::status_result<std::unique_ptr<Log>> FileLog::open(std::string_view path) {
    OUTCOME_TRY(auto log, log::file::LogFile::open(path));

    auto impl = std::make_unique<Impl>();
    impl->log = std::move(log);
    return std::unique_ptr<Log>(new FileLog(std::move(impl)));
}

FileLog::FileLog(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}

FileLog::FileLog(FileLog&&) = default;
FileLog& FileLog::operator=(FileLog&&) = default;
FileLog::~FileLog() = default;

outcome::status_result<PutResult> FileLog::put(std::string_view key, byteview meta, byteview payload) {
    OUTCOME_TRY(auto result, impl_->log->commit_payload(key, meta, payload));

    if (result.outcome == log::file::CommitOutcome::Lost) {
        return PutResult{
            .outcome = PutOutcome::Retry,
            .counter = result.ts_counter,
        };
    }

    return PutResult{
        .outcome = PutOutcome::Success,
        .counter = result.ts_counter,
    };
}

outcome::status_result<std::optional<RecordView>> FileLog::get_latest(std::string_view key, ReadOptions options) {
    log::file::LogReadMode read_mode = log::file::LogReadMode::Refresh;
    // TODO: make this nicer
    switch (options) {
        case ReadOptions::Refresh:
            read_mode = log::file::LogReadMode::Refresh;
            break;
        case ReadOptions::UseCached:
            read_mode = log::file::LogReadMode::Cached;
            break;
    }

    OUTCOME_TRY(auto record, impl_->log->get_record_view(key, read_mode));
    if (!record.has_value()) {
        return std::optional<RecordView>{};
    }

    const auto& value = *record;
    return std::optional<RecordView>{RecordView{
        .counter = value.counter(),
        .meta = value.meta_bytes(),
        .payload_blocks = {value.payload_blocks().begin(), value.payload_blocks().end()},
    }};
}

outcome::status_result<void> FileLog::refresh() {
    return impl_->log->refresh();
}

}
