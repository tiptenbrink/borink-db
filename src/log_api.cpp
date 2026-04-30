#include "log_api.hpp"

#include "log.hpp"

#include <condition_variable>
#include <deque>
#include <mutex>
#include <outcome/try.hpp>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>

namespace borinkdb::bytelog {

struct QueuedTransaction {
    TransactionFunction fn;
    std::promise<outcome::status_result<PutResult>> result;
};

RecordView to_record_view(const log::file::LogRecordView& value) {
    return RecordView{
        .id = LogicalBlockId{.hi = value.id_hi(), .lo = value.id_lo()},
        .meta = value.meta_bytes(),
        .payload_blocks = {value.payload_blocks().begin(), value.payload_blocks().end()},
    };
}

class FileTransactionContext final : public TransactionContext {
public:
    struct Condition {
        std::string key;
        std::optional<LogicalBlockId> id;
    };

    struct OwnedWrite {
        std::string key;
        std::vector<std::byte> meta;
        std::vector<std::byte> payload;
        bool conditional = false;
    };

    explicit FileTransactionContext(log::file::LogFile& log) : log_(log) {}

    outcome::status_result<std::optional<RecordView>> get(std::string_view key) override {
        OUTCOME_TRY(auto record, log_.get_record_view(key, log::file::LogReadMode::Cached));
        observed_[std::string{key}] = record ? std::optional<LogicalBlockId>{{.hi = record->id_hi(), .lo = record->id_lo()}} : std::nullopt;
        if (!record) {
            return std::optional<RecordView>{};
        }
        return std::optional<RecordView>{to_record_view(*record)};
    }

    outcome::status_result<void> put_if(std::string_view key, byteview meta, byteview payload) override {
        const auto owned_key = std::string{key};
        auto observed_it = observed_.find(owned_key);
        std::optional<LogicalBlockId> condition;
        if (observed_it == observed_.end()) {
            OUTCOME_TRY(auto record, log_.get_record_view(key, log::file::LogReadMode::Cached));
            condition = record ? std::optional<LogicalBlockId>{{.hi = record->id_hi(), .lo = record->id_lo()}} : std::nullopt;
            observed_[owned_key] = condition;
        } else {
            condition = observed_it->second;
        }
        conditions_.push_back(Condition{
            .key = owned_key,
            .id = condition,
        });
        writes_.push_back(OwnedWrite{
            .key = owned_key,
            .meta = {meta.begin(), meta.end()},
            .payload = {payload.begin(), payload.end()},
            .conditional = true,
        });
        return outcome::success();
    }

    outcome::status_result<void> overwrite(std::string_view key, byteview meta, byteview payload) override {
        writes_.push_back(OwnedWrite{
            .key = std::string{key},
            .meta = {meta.begin(), meta.end()},
            .payload = {payload.begin(), payload.end()},
            .conditional = false,
        });
        return outcome::success();
    }

    [[nodiscard]] const std::vector<Condition>& conditions() const noexcept { return conditions_; }
    [[nodiscard]] const std::vector<OwnedWrite>& owned_writes() const noexcept { return writes_; }

    [[nodiscard]] std::vector<log::file::TransactionEntry> write_entries() const {
        std::vector<log::file::TransactionEntry> out;
        out.reserve(writes_.size());
        for (const auto& write : writes_) {
            out.push_back(log::file::TransactionEntry{
                .key = write.key,
                .meta_bytes = write.meta,
                .payload_bytes = write.payload,
            });
        }
        return out;
    }

    [[nodiscard]] bool has_writes() const noexcept { return !writes_.empty(); }

private:
    log::file::LogFile& log_;
    std::unordered_map<std::string, std::optional<LogicalBlockId>> observed_;
    std::vector<Condition> conditions_;
    std::vector<OwnedWrite> writes_;
};

class FutureTxHandle final : public TxHandle {
public:
    explicit FutureTxHandle(std::future<outcome::status_result<PutResult>> future)
        : future_(std::move(future)) {}

    outcome::status_result<PutResult> wait() override {
        return future_.get();
    }

private:
    std::future<outcome::status_result<PutResult>> future_;
};

struct FileLog::Impl {
    explicit Impl(std::unique_ptr<log::file::LogFile> opened_log)
        : log(std::move(opened_log)),
          worker(&Impl::run, this) {}

    ~Impl() {
        {
            std::lock_guard lock(queue_mutex);
            stop = true;
        }
        queue_cv.notify_one();
        if (worker.joinable()) {
            worker.join();
        }
    }

    Impl(const Impl&) = delete;
    Impl& operator=(const Impl&) = delete;

    std::optional<LogicalBlockId> cached_id(std::string_view key) {
        auto record = log->get_record_view(key, log::file::LogReadMode::Cached);
        if (!record || !record.value()) {
            return std::nullopt;
        }
        return LogicalBlockId{.hi = record.value()->id_hi(), .lo = record.value()->id_lo()};
    }

    bool conditions_hold(const FileTransactionContext& tx,
                         const std::unordered_map<std::string, std::optional<LogicalBlockId>>& overlay) {
        std::unordered_set<std::string_view> write_keys;
        write_keys.reserve(tx.owned_writes().size());
        for (const auto& write : tx.owned_writes()) {
            if (!write_keys.insert(write.key).second) {
                return false;
            }
        }
        for (const auto& condition : tx.conditions()) {
            auto overlay_it = overlay.find(condition.key);
            const auto current = overlay_it == overlay.end() ? cached_id(condition.key) : overlay_it->second;
            if (current != condition.id) {
                return false;
            }
        }
        for (const auto& write : tx.owned_writes()) {
            if (overlay.contains(write.key)) {
                return false;
            }
        }
        return true;
    }

    void run() {
        while (true) {
            std::vector<QueuedTransaction> batch;
            {
                std::unique_lock lock(queue_mutex);
                queue_cv.wait(lock, [&] { return stop || !queue.empty(); });
                if (stop && queue.empty()) {
                    return;
                }
                while (!queue.empty()) {
                    batch.push_back(std::move(queue.front()));
                    queue.pop_front();
                }
            }

            (void) log->refresh();
            std::vector<FileTransactionContext> contexts;
            contexts.reserve(batch.size());
            std::vector<bool> should_commit(batch.size(), false);
            std::vector<log::file::TransactionEntry> entries;
            std::unordered_map<std::string, std::optional<LogicalBlockId>> overlay;

            for (std::size_t i = 0; i < batch.size(); ++i) {
                contexts.emplace_back(*log);
                auto fn_result = batch[i].fn(contexts.back());
                if (!fn_result) {
                    batch[i].result.set_value(std::move(fn_result).as_failure());
                    continue;
                }
                if (!contexts.back().has_writes()) {
                    batch[i].result.set_value(PutResult{.outcome = PutOutcome::Success, .id = {}});
                    continue;
                }
                auto refresh_result = log->refresh();
                if (!refresh_result) {
                    batch[i].result.set_value(std::move(refresh_result).as_failure());
                    continue;
                }
                if (!conditions_hold(contexts.back(), overlay)) {
                    batch[i].result.set_value(PutResult{.outcome = PutOutcome::Retry, .id = {}});
                    continue;
                }

                should_commit[i] = true;
                const std::optional<LogicalBlockId> pending_write = LogicalBlockId{};
                for (const auto& write : contexts.back().owned_writes()) {
                    overlay[write.key] = pending_write;
                    entries.push_back(log::file::TransactionEntry{
                        .key = write.key,
                        .meta_bytes = write.meta,
                        .payload_bytes = write.payload,
                    });
                }
            }

            if (entries.empty()) {
                continue;
            }

            auto commit = log->commit_transaction(entries);
            if (!commit) {
                for (std::size_t i = 0; i < batch.size(); ++i) {
                    if (should_commit[i]) {
                        batch[i].result.set_value(PutResult{.outcome = PutOutcome::Retry, .id = {}});
                    }
                }
                continue;
            }

            const auto result = PutResult{
                .outcome = PutOutcome::Success,
                .id = LogicalBlockId{.hi = commit.value().id_hi, .lo = commit.value().id_lo},
            };
            for (std::size_t i = 0; i < batch.size(); ++i) {
                if (should_commit[i]) {
                    batch[i].result.set_value(result);
                }
            }
        }
    }

    std::unique_ptr<log::file::LogFile> log;
    std::mutex queue_mutex;
    std::condition_variable queue_cv;
    std::deque<QueuedTransaction> queue;
    bool stop = false;
    std::thread worker;
};

outcome::status_result<std::unique_ptr<Log>> FileLog::open(std::string_view path) {
    OUTCOME_TRY(auto log, log::file::LogFile::open(path));
    return std::unique_ptr<Log>(new FileLog(std::make_unique<Impl>(std::move(log))));
}

FileLog::FileLog(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}

FileLog::FileLog(FileLog&&) = default;
FileLog& FileLog::operator=(FileLog&&) = default;
FileLog::~FileLog() = default;

outcome::status_result<std::optional<RecordView>> FileLog::get(std::string_view key) {
    OUTCOME_TRY(auto record, impl_->log->get_record_view(key, log::file::LogReadMode::Cached));
    if (!record.has_value()) {
        return std::optional<RecordView>{};
    }
    return std::optional<RecordView>{to_record_view(*record)};
}

outcome::status_result<std::unique_ptr<TxHandle>> FileLog::tx(TransactionFunction fn) {
    QueuedTransaction queued{
        .fn = std::move(fn),
        .result = {},
    };
    auto future = queued.result.get_future();
    {
        std::lock_guard lock(impl_->queue_mutex);
        if (impl_->stop) {
            return log::file::Error::bad_argument;
        }
        impl_->queue.push_back(std::move(queued));
    }
    impl_->queue_cv.notify_one();
    return std::unique_ptr<TxHandle>(new FutureTxHandle(std::move(future)));
}

outcome::status_result<void> FileLog::refresh() {
    return impl_->log->refresh();
}

}
