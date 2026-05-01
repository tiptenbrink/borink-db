#include <chrono>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <cstddef>
#include <cstring>
#include <future>
#include <iostream>
#include <outcome/config.hpp>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <mutex>
#include <vector>

#include <llfio/llfio.hpp>
#include <outcome/experimental/status-code/include/status-code/iostream_support.hpp>
#include <outcome/experimental/status_result.hpp>
#include <outcome/try.hpp>

#if !defined(_WIN32)
#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

#include "bench.hpp"
#include "crc32c.hpp"
#include "log.hpp"
#include "log_api.hpp"

namespace borinkdb::coordinator {
OUTCOME_V2_NAMESPACE::experimental::status_result<void> run_integration_test();
}

namespace {

namespace llfio = LLFIO_V2_NAMESPACE;
namespace outcome = OUTCOME_V2_NAMESPACE::experimental;
namespace filelog = borinkdb::log::file;
using byteview = std::span<const std::byte>;

outcome::status_result<void> test_failure(std::string_view message) {
    std::cerr << message << '\n';
    return filelog::Error::bad_argument;
}

std::vector<std::byte> bytes(std::string_view s) {
    const auto view = std::as_bytes(std::span{s.data(), s.size()});
    return {view.begin(), view.end()};
}

std::span<const std::byte> byte_view(std::string_view s) {
    return std::as_bytes(std::span{s.data(), s.size()});
}

outcome::status_result<filelog::CommitResult>
commit_one(filelog::LogFile& log, std::string_view key, byteview meta, byteview payload) {
    const filelog::TransactionEntry entry{
        .key = key,
        .meta_bytes = meta,
        .payload_bytes = payload,
    };
    return log.commit_transaction(std::span<const filelog::TransactionEntry>{&entry, 1});
}

bool bytes_equal(std::span<const std::byte> a, std::span<const std::byte> b) {
    return a.size() == b.size() && std::memcmp(a.data(), b.data(), a.size()) == 0;
}

bool bytes_equal_blocks(std::span<const byteview> blocks, byteview expected) {
    std::size_t offset = 0;
    for (const auto block : blocks) {
        if (offset > expected.size()) {
            return false;
        }
        const std::size_t remaining = expected.size() - offset;
        if (block.size() > remaining || !bytes_equal(block, expected.subspan(offset, block.size()))) {
            return false;
        }
        offset += block.size();
    }
    return offset == expected.size();
}

std::size_t payload_block_count_for(std::size_t payload_size) {
    return (payload_size + filelog::kGroupPayloadChunkBytes - 1) / filelog::kGroupPayloadChunkBytes;
}

outcome::status_result<std::string> make_temp_log_path() {
    OUTCOME_TRY(auto dir, llfio::path_discovery::storage_backed_temporary_files_directory().current_path());
    std::string path = dir.string();
    if (!path.empty() && path.back() != '/') {
        path.push_back('/');
    }
    path += "borinkdb-";
    path += std::to_string(static_cast<unsigned long long>(
        std::chrono::steady_clock::now().time_since_epoch().count()));
    path += ".log";
    return path;
}

outcome::status_result<void> unlink_path(std::string_view path) {
    OUTCOME_TRY(auto h, llfio::file_handle::file(
        {},
        llfio::path_view(path.data(), path.size(), llfio::path_view::not_zero_terminated),
        llfio::file_handle::mode::write,
        llfio::file_handle::creation::open_existing));
    OUTCOME_TRYV(h.unlink());
    return llfio::success();
}

outcome::status_result<void> append_raw(std::string_view path, byteview data) {
    OUTCOME_TRY(auto h, llfio::file_handle::file(
        {},
        llfio::path_view(path.data(), path.size(), llfio::path_view::not_zero_terminated),
        llfio::file_handle::mode::append,
        llfio::file_handle::creation::if_needed));

    llfio::file_handle::const_buffer_type wb{data.data(), data.size()};
    llfio::file_handle::const_buffers_type wbs{&wb, 1};
    OUTCOME_TRY(auto wr, h.write({wbs, 0}));

    std::size_t written = 0;
    for (const auto& b : wr) {
        written += b.size();
    }
    if (written != data.size()) {
        return filelog::Error::short_write;
    }
    return llfio::success();
}

outcome::status_result<void> crc32c_known_answer_test() {
    using namespace borinkdb;
    const uint32_t got = crc32c(byte_view("123456789"));
    if (got != 0xE3069283u) {
        return test_failure("crc32c known-answer failed");
    }
    std::cout << "crc32c OK\n";
    return LLFIO_V2_NAMESPACE::success();
}

outcome::status_result<void> block_roundtrip_test() {
    using namespace borinkdb::log::file;

    const auto meta = bytes("opaque-meta");
    const auto payload = bytes("opaque-payload");
    OUTCOME_TRY(auto encoded, encode_block({
        .id_hi = 7,
        .id_lo = 0x1122334455667788ull,
        .group = GroupInfo{.index = 0, .count = 1},
        .key = "alpha",
        .meta_bytes = meta,
        .payload_bytes = payload,
    }));

    OUTCOME_TRY(auto decoded, decode_block(encoded));
    if (
        decoded.id_hi != 7 ||
        decoded.id_lo != 0x1122334455667788ull ||
        decoded.group.index != 0 ||
        decoded.group.count != 1 ||
        decoded.total_size != encoded.size() ||
        decoded.key != "alpha" ||
        std::memcmp(decoded.meta_bytes.data(), meta.data(), meta.size()) != 0 ||
        std::memcmp(decoded.payload_bytes.data(), payload.data(), payload.size()) != 0) {
        return test_failure("decoded block mismatch");
    }

    auto corrupt = encoded;
    corrupt[12] = static_cast<std::byte>(static_cast<unsigned char>(corrupt[12]) ^ 0x55u);
    auto corrupt_decoded = decode_block(corrupt);
    if (corrupt_decoded ||
        !corrupt_decoded.error().equivalent(system_error2::make_status_code(system_error2::errc::illegal_byte_sequence))) {
        return test_failure("decode_block did not reject CRC damage");
    }

    std::cout << "block encode/decode OK\n";
    return outcome::success();
}

outcome::status_result<void> writer_reader_payload_test() {
    using namespace borinkdb::log::file;

    OUTCOME_TRY(auto path, make_temp_log_path());
    OUTCOME_TRY(auto log, LogFile::open(path));

    const auto meta = bytes("meta");
    const std::vector<std::byte> payload(2500, std::byte{0x42});
    OUTCOME_TRY(auto commit, commit_one(*log, "large-key", meta, payload));
    (void) commit;

    OUTCOME_TRY(auto got_opt, log->get_record_view("large-key", LogReadMode::Cached));
    if (!got_opt || !bytes_equal_blocks(got_opt->payload_blocks(), std::span<const std::byte>{payload.data(), payload.size()})) {
        OUTCOME_TRYV(unlink_path(path));
        return test_failure("reader payload mismatch");
    }

    std::cout << "writer/reader payload OK\n";
    OUTCOME_TRYV(unlink_path(path));
    return outcome::success();
}

outcome::status_result<void> corrupt_skip_test() {
    using namespace borinkdb::log::file;

    OUTCOME_TRY(auto path, make_temp_log_path());
    OUTCOME_TRY(auto one, encode_block({1, 11, GroupInfo{0, 1}, "k", std::span<const std::byte>{}, byte_view("one")}));
    OUTCOME_TRY(auto bad, encode_block({2, 22, GroupInfo{0, 1}, "k", std::span<const std::byte>{}, byte_view("bad")}));
    OUTCOME_TRY(auto two, encode_block({3, 33, GroupInfo{0, 1}, "k", std::span<const std::byte>{}, byte_view("two")}));
    bad[12] = static_cast<std::byte>(static_cast<unsigned char>(bad[12]) ^ 0xAAu);

    OUTCOME_TRYV(append_raw(path, one));
    OUTCOME_TRYV(append_raw(path, bad));
    OUTCOME_TRYV(append_raw(path, two));

    OUTCOME_TRY(auto reader, LogFile::open(path));
    OUTCOME_TRY(auto got_opt, reader->get_record_view("k", LogReadMode::Refresh));
    if (!got_opt || !bytes_equal_blocks(got_opt->payload_blocks(), byte_view("two"))) {
        OUTCOME_TRYV(unlink_path(path));
        return test_failure("reader did not skip corrupt block");
    }

    std::cout << "corrupt skip OK\n";
    OUTCOME_TRYV(unlink_path(path));
    return outcome::success();
}

outcome::status_result<void> interleaved_group_blocks_test() {
    using namespace borinkdb::log::file;

    OUTCOME_TRY(auto path, make_temp_log_path());
    OUTCOME_TRY(auto first, encode_block({1, 11, GroupInfo{0, 2}, "k", std::span<const std::byte>{}, byte_view("one")}));
    OUTCOME_TRY(auto other, encode_block({2, 22, GroupInfo{0, 1}, "other", std::span<const std::byte>{}, byte_view("x")}));
    OUTCOME_TRY(auto second, encode_block({1, 11, GroupInfo{1, 2}, "", std::span<const std::byte>{}, byte_view("two")}));

    OUTCOME_TRYV(append_raw(path, first));
    OUTCOME_TRYV(append_raw(path, other));
    OUTCOME_TRYV(append_raw(path, second));

    OUTCOME_TRY(auto reader, LogFile::open(path));
    OUTCOME_TRY(auto got_opt, reader->get_record_view("k", LogReadMode::Refresh));
    if (!got_opt || !bytes_equal_blocks(got_opt->payload_blocks(), byte_view("onetwo"))) {
        OUTCOME_TRYV(unlink_path(path));
        return test_failure("reader did not assemble interleaved group blocks");
    }
    OUTCOME_TRY(auto other_opt, reader->get_record_view("other", LogReadMode::Refresh));
    if (!other_opt || !bytes_equal_blocks(other_opt->payload_blocks(), byte_view("x"))) {
        OUTCOME_TRYV(unlink_path(path));
        return test_failure("interleaved group affected unrelated record");
    }

    std::cout << "interleaved group blocks OK\n";
    OUTCOME_TRYV(unlink_path(path));
    return outcome::success();
}

outcome::status_result<void> duplicate_logical_block_is_idempotent_test() {
    using namespace borinkdb::log::file;

    OUTCOME_TRY(auto path, make_temp_log_path());
    OUTCOME_TRY(auto first, encode_block({7, 11, GroupInfo{0, 1}, "k", std::span<const std::byte>{}, byte_view("first")}));
    OUTCOME_TRY(auto duplicate, encode_block({7, 11, GroupInfo{0, 1}, "k", std::span<const std::byte>{}, byte_view("duplicate")}));
    OUTCOME_TRY(auto later, encode_block({8, 22, GroupInfo{0, 1}, "k", std::span<const std::byte>{}, byte_view("later")}));

    OUTCOME_TRYV(append_raw(path, first));
    OUTCOME_TRYV(append_raw(path, duplicate));

    OUTCOME_TRY(auto reader, LogFile::open(path));
    OUTCOME_TRY(auto got_opt, reader->get_record_view("k", LogReadMode::Refresh));
    if (!got_opt || !bytes_equal_blocks(got_opt->payload_blocks(), byte_view("first"))) {
        OUTCOME_TRYV(unlink_path(path));
        return test_failure("duplicate logical block was not idempotent");
    }

    OUTCOME_TRYV(append_raw(path, later));
    OUTCOME_TRYV(reader->refresh());
    OUTCOME_TRY(auto later_opt, reader->get_record_view("k", LogReadMode::Cached));
    if (!later_opt || !bytes_equal_blocks(later_opt->payload_blocks(), byte_view("later"))) {
        OUTCOME_TRYV(unlink_path(path));
        return test_failure("later logical block did not replace earlier key value");
    }

    std::cout << "duplicate logical block idempotency OK\n";
    OUTCOME_TRYV(unlink_path(path));
    return outcome::success();
}

outcome::status_result<void> file_byte_log_cache_test() {
    using namespace borinkdb::bytelog;

    OUTCOME_TRY(auto path, make_temp_log_path());
    OUTCOME_TRY(auto opened, FileLog::open(path));

    OUTCOME_TRY(auto first_handle, opened->tx([](TransactionContext& tx) -> outcome::status_result<void> {
        OUTCOME_TRYV(tx.overwrite("k", byte_view("m1"), byte_view("one")));
        return outcome::success();
    }));
    OUTCOME_TRY(auto first_put, first_handle->wait());
    if (first_put.outcome != PutOutcome::Success) {
        OUTCOME_TRYV(unlink_path(path));
        return test_failure("file byte log first put requested retry");
    }

    OUTCOME_TRY(auto cached_opt, opened->get("k"));
    if (!cached_opt) {
        OUTCOME_TRYV(unlink_path(path));
        return test_failure("file byte log cached read missed local put");
    }
    const auto& cached = *cached_opt;
    if (cached.id != first_put.id ||
        !bytes_equal(cached.meta, byte_view("m1")) ||
        !bytes_equal_blocks(cached.payload_blocks, byte_view("one"))) {
        OUTCOME_TRYV(unlink_path(path));
        return test_failure("file byte log cached read missed local put");
    }

    OUTCOME_TRY(auto other, FileLog::open(path));
    OUTCOME_TRY(auto second_handle, other->tx([](TransactionContext& tx) -> outcome::status_result<void> {
        OUTCOME_TRYV(tx.overwrite("k", byte_view("m2"), byte_view("two")));
        return outcome::success();
    }));
    OUTCOME_TRY(auto second_put, second_handle->wait());
    if (second_put.outcome != PutOutcome::Success) {
        OUTCOME_TRYV(unlink_path(path));
        return test_failure("file byte log second put requested retry");
    }

    std::optional<RecordView> watched_cached;
    for (int attempt = 0; attempt < 30; ++attempt) {
        OUTCOME_TRY(auto cached_after_external, opened->get("k"));
        if (cached_after_external && cached_after_external->id == second_put.id) {
            watched_cached = cached_after_external;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{10});
    }
    if (!watched_cached ||
        !bytes_equal(watched_cached->meta, byte_view("m2")) ||
        !bytes_equal_blocks(watched_cached->payload_blocks, byte_view("two"))) {
        OUTCOME_TRYV(unlink_path(path));
        return test_failure("file byte log watcher did not refresh cached read");
    }
    OUTCOME_TRYV(opened->refresh());
    OUTCOME_TRY(auto fresh_opt, opened->get("k"));
    if (!fresh_opt) {
        OUTCOME_TRYV(unlink_path(path));
        return test_failure("file byte log refresh read missed external put");
    }
    const auto& fresh = *fresh_opt;
    if (fresh.id != second_put.id ||
        !bytes_equal(fresh.meta, byte_view("m2")) ||
        !bytes_equal_blocks(fresh.payload_blocks, byte_view("two"))) {
        OUTCOME_TRYV(unlink_path(path));
        return test_failure("file byte log refresh read missed external put");
    }

    std::cout << "file byte log cache/freshness OK\n";
    OUTCOME_TRYV(unlink_path(path));
    return outcome::success();
}

outcome::status_result<void> file_byte_log_transaction_test() {
    using namespace borinkdb::bytelog;

    OUTCOME_TRY(auto path, make_temp_log_path());
    OUTCOME_TRY(auto opened, FileLog::open(path));

    OUTCOME_TRY(auto put_handle, opened->tx([](TransactionContext& tx) -> outcome::status_result<void> {
        OUTCOME_TRYV(tx.overwrite("a", byte_view("ma"), byte_view("one")));
        OUTCOME_TRYV(tx.overwrite("b", byte_view("mb"), byte_view("two")));
        OUTCOME_TRYV(tx.overwrite("c", byte_view("mc"), byte_view("three")));
        return outcome::success();
    }));
    OUTCOME_TRY(auto put, put_handle->wait());
    if (put.outcome != PutOutcome::Success) {
        OUTCOME_TRYV(unlink_path(path));
        return test_failure("multi-key transaction requested retry");
    }

    OUTCOME_TRY(auto a, opened->get("a"));
    OUTCOME_TRY(auto b, opened->get("b"));
    OUTCOME_TRY(auto c, opened->get("c"));
    if (!a || !b || !c ||
        a->id != put.id ||
        b->id != put.id ||
        c->id != put.id ||
        !bytes_equal(a->meta, byte_view("ma")) ||
        !bytes_equal(b->meta, byte_view("mb")) ||
        !bytes_equal(c->meta, byte_view("mc")) ||
        !bytes_equal_blocks(a->payload_blocks, byte_view("one")) ||
        !bytes_equal_blocks(b->payload_blocks, byte_view("two")) ||
        !bytes_equal_blocks(c->payload_blocks, byte_view("three"))) {
        OUTCOME_TRYV(unlink_path(path));
        return test_failure("multi-key transaction read mismatch");
    }

    OUTCOME_TRY(auto batch_one, opened->tx([](TransactionContext& tx) -> outcome::status_result<void> {
        OUTCOME_TRYV(tx.overwrite("d", byte_view("md"), byte_view("four")));
        return outcome::success();
    }));
    OUTCOME_TRY(auto batch_two, opened->tx([](TransactionContext& tx) -> outcome::status_result<void> {
        OUTCOME_TRYV(tx.overwrite("e", byte_view("me"), byte_view("five")));
        OUTCOME_TRYV(tx.overwrite("f", byte_view("mf"), byte_view("six")));
        return outcome::success();
    }));
    OUTCOME_TRY(auto batch_one_result, batch_one->wait());
    OUTCOME_TRY(auto batch_two_result, batch_two->wait());
    if (batch_one_result.outcome != PutOutcome::Success ||
        batch_two_result.outcome != PutOutcome::Success) {
        OUTCOME_TRYV(unlink_path(path));
        return test_failure("transaction batch did not return successful per-transaction results");
    }

    OUTCOME_TRY(auto read_then_write_handle, opened->tx([&](TransactionContext& tx) -> outcome::status_result<void> {
        OUTCOME_TRY(auto got, tx.get("a"));
        if (!got || !bytes_equal_blocks(got->payload_blocks, byte_view("one"))) {
            return test_failure("transaction get did not read begin snapshot");
        }
        OUTCOME_TRYV(tx.put_if("g", byte_view("mg"), byte_view("seven")));
        return outcome::success();
    }));
    OUTCOME_TRY(auto read_then_write, read_then_write_handle->wait());
    if (read_then_write.outcome != PutOutcome::Success) {
        OUTCOME_TRYV(unlink_path(path));
        return test_failure("transaction read/write requested retry unexpectedly");
    }

    std::promise<void> stale_read_captured;
    std::promise<void> release_stale_read;
    auto stale_read_ready = stale_read_captured.get_future();
    auto release_stale_read_future = release_stale_read.get_future().share();
    OUTCOME_TRY(auto stale_read_handle, opened->tx([&](TransactionContext& tx) -> outcome::status_result<void> {
        OUTCOME_TRY(auto got, tx.get("a"));
        if (!got) {
            return test_failure("transaction stale-read setup missed key");
        }
        stale_read_captured.set_value();
        release_stale_read_future.wait();
        OUTCOME_TRYV(tx.put_if("a", byte_view("ma-stale"), byte_view("stale-one")));
        return outcome::success();
    }));
    stale_read_ready.wait();
    OUTCOME_TRY(auto conflicting, FileLog::open(path));
    OUTCOME_TRY(auto wins_handle, conflicting->tx([](TransactionContext& tx) -> outcome::status_result<void> {
        OUTCOME_TRYV(tx.overwrite("a", byte_view("ma2"), byte_view("new-one")));
        return outcome::success();
    }));
    OUTCOME_TRY(auto wins, wins_handle->wait());
    if (wins.outcome != PutOutcome::Success) {
        OUTCOME_TRYV(unlink_path(path));
        return test_failure("transaction stale-read setup first writer requested retry");
    }
    release_stale_read.set_value();
    OUTCOME_TRY(auto stale_read, stale_read_handle->wait());
    if (stale_read.outcome != PutOutcome::Retry) {
        OUTCOME_TRYV(unlink_path(path));
        return test_failure("transaction stale read did not request retry");
    }
    OUTCOME_TRY(auto after_stale, opened->get("a"));
    if (!after_stale || !bytes_equal_blocks(after_stale->payload_blocks, byte_view("new-one"))) {
        OUTCOME_TRYV(unlink_path(path));
        return test_failure("transaction stale put_if committed buffered write");
    }

    OUTCOME_TRY(auto overwrite_first, opened->tx([](TransactionContext& tx) -> outcome::status_result<void> {
        OUTCOME_TRYV(tx.overwrite("a", byte_view("ma3"), byte_view("newer-one")));
        return outcome::success();
    }));
    OUTCOME_TRY(auto overwrite_handle, opened->tx([&](TransactionContext& tx) -> outcome::status_result<void> {
        OUTCOME_TRY(auto got, tx.get("a"));
        if (!got) {
            return test_failure("transaction overwrite setup missed key");
        }
        OUTCOME_TRYV(tx.overwrite("i", byte_view("mi"), byte_view("nine")));
        return outcome::success();
    }));
    OUTCOME_TRY(auto overwrite_first_result, overwrite_first->wait());
    if (overwrite_first_result.outcome != PutOutcome::Success) {
        OUTCOME_TRYV(unlink_path(path));
        return test_failure("transaction overwrite setup first writer requested retry");
    }
    OUTCOME_TRY(auto overwrite_result, overwrite_handle->wait());
    if (overwrite_result.outcome != PutOutcome::Success) {
        OUTCOME_TRYV(unlink_path(path));
        return test_failure("transaction overwrite cared about a stale read");
    }
    OUTCOME_TRY(auto cached_i, opened->get("i"));
    if (!cached_i || !bytes_equal_blocks(cached_i->payload_blocks, byte_view("nine"))) {
        OUTCOME_TRYV(unlink_path(path));
        return test_failure("transaction get did not read cached index");
    }

    auto large_payload = bytes("tx-large:");
    large_payload.resize(filelog::kGroupPayloadChunkBytes * 2 + 123, std::byte{'L'});
    OUTCOME_TRY(auto large_handle, opened->tx([&](TransactionContext& tx) -> outcome::status_result<void> {
        OUTCOME_TRYV(tx.overwrite("large-tx", byte_view("large-meta"), large_payload));
        return outcome::success();
    }));
    OUTCOME_TRY(auto large_result, large_handle->wait());
    if (large_result.outcome != PutOutcome::Success) {
        OUTCOME_TRYV(unlink_path(path));
        return test_failure("large transaction payload requested retry");
    }
    OUTCOME_TRY(auto large_got, opened->get("large-tx"));
    if (!large_got ||
        large_got->payload_blocks.size() < 2 ||
        large_got->id != large_result.id ||
        !bytes_equal(large_got->meta, byte_view("large-meta")) ||
        !bytes_equal_blocks(large_got->payload_blocks, large_payload)) {
        OUTCOME_TRYV(unlink_path(path));
        return test_failure("large transaction payload read mismatch");
    }

    std::cout << "file byte log transactions OK\n";
    OUTCOME_TRYV(unlink_path(path));
    return outcome::success();
}

// Multiple threads share one LogFile and concurrently commit distinct keys.
// Afterwards every key is read back, so this primarily exercises the mutexes
// around one handle's mutable writer/index state.
//
// Contract checked: concurrent calls on one LogFile must not corrupt the append
// stream or lose successfully committed records.
// Not checked: true parallel disk writes, because the LogFile mutex is expected
// to serialize the actual commit path inside this process.
outcome::status_result<void> same_log_multithreaded_write_stress_test() {
    using namespace borinkdb::log::file;

    OUTCOME_TRY(auto path, make_temp_log_path());
    OUTCOME_TRY(auto log, LogFile::open(path));

    constexpr std::size_t kThreadCount = 4;
    constexpr std::size_t kRecordsPerThread = 40;
    std::atomic<bool> failed{false};
    std::mutex message_mutex;
    std::string failure_message;

    auto fail = [&](std::string message) {
        failed.store(true, std::memory_order_release);
        std::lock_guard lock(message_mutex);
        if (failure_message.empty()) {
            failure_message = std::move(message);
        }
    };

    std::vector<std::thread> threads;
    threads.reserve(kThreadCount);
    for (std::size_t thread_index = 0; thread_index < kThreadCount; ++thread_index) {
        threads.emplace_back([&, thread_index] {
            for (std::size_t record_index = 0; record_index < kRecordsPerThread; ++record_index) {
                if (failed.load(std::memory_order_acquire)) {
                    return;
                }
                const std::string key =
                    "thread-" + std::to_string(thread_index) + "-record-" + std::to_string(record_index);
                const std::string payload_text = "payload-" + key;
                const auto payload = bytes(payload_text);
                auto committed = commit_one(*log, key, byteview{}, payload);
                if (!committed) {
                    fail("same-log concurrent commit failed");
                    return;
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }
    if (failed.load(std::memory_order_acquire)) {
        OUTCOME_TRYV(unlink_path(path));
        return test_failure(failure_message.empty() ? "same-log stress failed" : failure_message);
    }

    // The real assertion is the full read-back. Thread completion alone only
    // proves the commit calls returned, not that the log remained coherent.
    for (std::size_t thread_index = 0; thread_index < kThreadCount; ++thread_index) {
        for (std::size_t record_index = 0; record_index < kRecordsPerThread; ++record_index) {
            const std::string key =
                "thread-" + std::to_string(thread_index) + "-record-" + std::to_string(record_index);
            const std::string payload_text = "payload-" + key;
            const auto expected = bytes(payload_text);
            OUTCOME_TRY(auto got, log->get_record_view(key, LogReadMode::Refresh));
            if (!got || !bytes_equal_blocks(got->payload_blocks(), expected)) {
                OUTCOME_TRYV(unlink_path(path));
                return test_failure("same-log stress verification failed");
            }
        }
    }

    std::cout << "same-log multithreaded write stress OK\n";
    OUTCOME_TRYV(unlink_path(path));
    return outcome::success();
}

// One handle appends many versions of a key while separate reader handles
// repeatedly refresh and read it. Readers check that visible values are
// well-formed and that the final value is available from a fresh reader.
//
// Contract checked: independent LogFile instances in one process can observe
// append-only growth without seeing torn/corrupt grouped values.
// Potentially timing-sensitive: readers may not observe every version; they only
// need to see valid values while the writer is active.
outcome::status_result<void> reader_writer_multithreaded_stress_test() {
    using namespace borinkdb::log::file;

    OUTCOME_TRY(auto path, make_temp_log_path());
    OUTCOME_TRY(auto writer, LogFile::open(path));

    constexpr std::size_t kReaderCount = 4;
    constexpr std::size_t kWriteCount = 80;
    std::atomic<bool> writer_done{false};
    std::atomic<bool> failed{false};
    std::mutex message_mutex;
    std::string failure_message;

    auto fail = [&](std::string message) {
        failed.store(true, std::memory_order_release);
        std::lock_guard lock(message_mutex);
        if (failure_message.empty()) {
            failure_message = std::move(message);
        }
    };

    std::thread writer_thread([&] {
        for (std::size_t index = 0; index < kWriteCount; ++index) {
            if (failed.load(std::memory_order_acquire)) {
                break;
            }
            const std::string payload_text = "value-" + std::to_string(index);
            const auto payload = bytes(payload_text);
            auto committed = commit_one(*writer, "shared", byteview{}, payload);
            if (!committed) {
                fail("reader/writer stress commit failed");
                break;
            }
        }
        writer_done.store(true, std::memory_order_release);
    });

    std::vector<std::thread> readers;
    readers.reserve(kReaderCount);
    for (std::size_t reader_index = 0; reader_index < kReaderCount; ++reader_index) {
        (void) reader_index;
        readers.emplace_back([&] {
            auto opened = LogFile::open(path);
            if (!opened) {
                fail("reader/writer stress reader open failed");
                return;
            }

            while (!writer_done.load(std::memory_order_acquire) && !failed.load(std::memory_order_acquire)) {
                // Refresh forces this handle to observe bytes appended by the
                // writer's separate handle.
                auto got = opened.value()->get_record_view("shared", LogReadMode::Refresh);
                if (!got) {
                    fail("reader/writer stress read failed");
                    return;
                }
                if (got.value().has_value()) {
                    // Any visible committed value for this key has a non-empty
                    // payload in this test. Empty would suggest a torn/corrupt
                    // group view, not just reader lag.
                    if (got.value()->payload_blocks().empty()) {
                        fail("reader/writer stress read empty payload");
                        return;
                    }
                }
                std::this_thread::yield();
            }
        });
    }

    writer_thread.join();
    for (auto& reader : readers) {
        reader.join();
    }
    if (failed.load(std::memory_order_acquire)) {
        OUTCOME_TRYV(unlink_path(path));
        return test_failure(failure_message.empty() ? "reader/writer stress failed" : failure_message);
    }

    // A fresh reader after all children stop removes watcher/poll timing from
    // the final assertion: the last committed value must be readable by refresh.
    OUTCOME_TRY(auto final_reader, LogFile::open(path));
    OUTCOME_TRY(auto got, final_reader->get_record_view("shared", LogReadMode::Refresh));
    const std::string expected_text = "value-" + std::to_string(kWriteCount - 1);
    const auto expected = bytes(expected_text);
    if (!got || !bytes_equal_blocks(got->payload_blocks(), expected)) {
        OUTCOME_TRYV(unlink_path(path));
        return test_failure("reader/writer stress final value mismatch");
    }

    std::cout << "reader/writer multithreaded stress OK\n";
    OUTCOME_TRYV(unlink_path(path));
    return outcome::success();
}

// Keep a borrowed mmap-backed view alive, then grow and refresh the same file.
// The old bytes and block addresses should remain valid while the owning
// LogFile is alive; this directly probes our append-only mmap assumption.
//
// Contract checked: LogRecordView spans borrowed from a live LogFile remain
// usable across append-only growth and refresh_index/update_map.
// Assumption sampled: LLFIO preserves old mmap addresses for this append-only
// Linux path. This is not a full cross-platform proof, especially for Windows.
outcome::status_result<void> borrowed_view_survives_append_refresh_test() {
    using namespace borinkdb::log::file;

    OUTCOME_TRY(auto path, make_temp_log_path());
    OUTCOME_TRY(auto log, LogFile::open(path));

    const auto meta = bytes("stable-meta");
    const std::vector<std::byte> stable_payload(5000, std::byte{0x5A});
    OUTCOME_TRY(auto stable_commit, commit_one(*log, "stable", meta, stable_payload));
    (void) stable_commit;

    OUTCOME_TRY(auto stable_view_opt, log->get_record_view("stable", LogReadMode::Cached));
    if (!stable_view_opt) {
        OUTCOME_TRYV(unlink_path(path));
        return test_failure("stable-view setup read failed");
    }
    auto stable_view = std::move(*stable_view_opt);
    const auto original_blocks = stable_view.payload_blocks();
    if (original_blocks.size() != payload_block_count_for(stable_payload.size()) ||
        !bytes_equal(stable_view.meta_bytes(), meta) ||
        !bytes_equal_blocks(original_blocks, stable_payload)) {
        OUTCOME_TRYV(unlink_path(path));
        return test_failure("stable-view initial bytes mismatch");
    }

    // Save mmap addresses, not copies. If update_map remaps old pages elsewhere,
    // the byte comparison might still pass but this address check should fail.
    std::vector<const std::byte*> original_block_addresses;
    original_block_addresses.reserve(original_blocks.size());
    for (const auto block : original_blocks) {
        original_block_addresses.push_back(block.data());
    }

    // Grow the file enough that refresh/update_map sees a meaningfully larger
    // file. This stays well below the reserved mapping size.
    constexpr std::size_t kAppendCount = 160;
    for (std::size_t index = 0; index < kAppendCount; ++index) {
        const std::string key = "growth-" + std::to_string(index);
        const std::vector<std::byte> payload(1800 + (index % 7), static_cast<std::byte>(index & 0xFFu));
        OUTCOME_TRY(auto commit, commit_one(*log, key, byteview{}, payload));
        (void) commit;
    }
    OUTCOME_TRYV(log->refresh());

    // The old view should still refer to the same mapped blocks after refresh.
    // If this fails, callers cannot safely hold borrowed views across refresh.
    const auto refreshed_blocks = stable_view.payload_blocks();
    if (refreshed_blocks.size() != original_block_addresses.size() ||
        !bytes_equal(stable_view.meta_bytes(), meta) ||
        !bytes_equal_blocks(refreshed_blocks, stable_payload)) {
        OUTCOME_TRYV(unlink_path(path));
        return test_failure("borrowed view bytes changed after append refresh");
    }
    for (std::size_t index = 0; index < refreshed_blocks.size(); ++index) {
        if (refreshed_blocks[index].data() != original_block_addresses[index]) {
            OUTCOME_TRYV(unlink_path(path));
            return test_failure("borrowed view block address changed after append refresh");
        }
    }

    std::cout << "borrowed view append/refresh stability OK\n";
    OUTCOME_TRYV(unlink_path(path));
    return outcome::success();
}

// The borrowed view comes from owner, not other. Closing an independent LogFile
// for the same path must not invalidate spans borrowed from owner. This does not
// claim views survive destruction of their own owning LogFile.
//
// Contract checked: each LogFile owns its own mapping lifetime. Destroying an
// unrelated handle for the same path must not invalidate this view.
outcome::status_result<void> borrowed_view_survives_other_handles_closing_test() {
    using namespace borinkdb::log::file;

    OUTCOME_TRY(auto path, make_temp_log_path());
    OUTCOME_TRY(auto owner, LogFile::open(path));
    OUTCOME_TRY(auto other, LogFile::open(path));

    const auto payload = bytes("owned-view");
    OUTCOME_TRY(auto commit, commit_one(*owner, "k", byteview{}, payload));
    (void) commit;

    OUTCOME_TRY(auto view_opt, owner->get_record_view("k", LogReadMode::Cached));
    if (!view_opt) {
        OUTCOME_TRYV(unlink_path(path));
        return test_failure("other-handle close setup read failed");
    }
    auto view = std::move(*view_opt);

    // Only the unrelated handle closes; owner still owns this view's mmap. A
    // test that destroyed owner and then read view would be undefined behavior.
    other.reset();

    if (!bytes_equal_blocks(view.payload_blocks(), payload)) {
        OUTCOME_TRYV(unlink_path(path));
        return test_failure("borrowed view changed after independent handle closed");
    }

    std::cout << "borrowed view other-handle close OK\n";
    OUTCOME_TRYV(unlink_path(path));
    return outcome::success();
}

// Use a payload large enough to split into more blocks than PayloadBlockList's
// inline capacity, forcing the spill path while still verifying the block spans
// reconstruct the original payload.
//
// Contract checked: callers can reconstruct a large payload by iterating block
// spans, including when PayloadBlockList spills from inline storage to vector.
outcome::status_result<void> split_payload_limit_stress_test() {
    using namespace borinkdb::log::file;

    OUTCOME_TRY(auto path, make_temp_log_path());
    OUTCOME_TRY(auto log, LogFile::open(path));

    constexpr std::size_t kPayloadBytes = 800 * 9 + 123;
    std::vector<std::byte> payload;
    payload.reserve(kPayloadBytes);
    // Deterministic non-uniform bytes catch block ordering and offset mistakes
    // better than a repeated fill byte would.
    for (std::size_t index = 0; index < kPayloadBytes; ++index) {
        payload.push_back(static_cast<std::byte>((index * 31u) & 0xFFu));
    }

    OUTCOME_TRY(auto commit, commit_one(*log, "split", byte_view("meta"), payload));
    (void) commit;

    OUTCOME_TRY(auto got, log->get_record_view("split", LogReadMode::Refresh));
    if (!got ||
        got->payload_blocks().size() != payload_block_count_for(payload.size()) ||
        got->payload_blocks().size() <= 4 ||
        !bytes_equal(got->meta_bytes(), byte_view("meta")) ||
        !bytes_equal_blocks(got->payload_blocks(), payload)) {
        OUTCOME_TRYV(unlink_path(path));
        return test_failure("split payload stress read mismatch");
    }

    std::cout << "split payload limit stress OK\n";
    OUTCOME_TRYV(unlink_path(path));
    return outcome::success();
}

// Directly hit encode-time field limits so oversized key, metadata, and payload
// inputs return specific errors instead of being accepted or truncated.
//
// Contract checked: encode_block validates independent field-size limits before
// writing bytes. This is deterministic and should not be flaky.
outcome::status_result<void> encode_size_limit_test() {
    using namespace borinkdb::log::file;

    const std::string too_large_key(0x10000u, 'k');
    auto key_result = encode_block({
        .id_hi = 1,
        .id_lo = 1,
        .group = GroupInfo{.index = 0, .count = 1},
        .key = too_large_key,
        .meta_bytes = byteview{},
        .payload_bytes = byteview{},
    });
    if (key_result ||
        !key_result.error().equivalent(system_error2::make_status_code(Error::key_too_large))) {
        return test_failure("oversized key was not rejected");
    }

    const std::vector<std::byte> too_large_payload(0x10000u, std::byte{0x2A});
    auto payload_result = encode_block({
        .id_hi = 1,
        .id_lo = 1,
        .group = GroupInfo{.index = 0, .count = 1},
        .key = "k",
        .meta_bytes = byteview{},
        .payload_bytes = too_large_payload,
    });
    if (payload_result ||
        !payload_result.error().equivalent(system_error2::make_status_code(Error::payload_too_large))) {
        return test_failure("oversized single block payload was not rejected");
    }

    const std::vector<std::byte> too_large_meta(0x10000u, std::byte{0x7B});
    auto meta_result = encode_block({
        .id_hi = 1,
        .id_lo = 1,
        .group = GroupInfo{.index = 0, .count = 1},
        .key = "k",
        .meta_bytes = too_large_meta,
        .payload_bytes = byteview{},
    });
    if (meta_result ||
        !meta_result.error().equivalent(system_error2::make_status_code(Error::metadata_too_large))) {
        return test_failure("oversized metadata was not rejected");
    }

    std::cout << "encode size limits OK\n";
    return outcome::success();
}

outcome::status_result<void> grouped_payload_limit_test() {
    using namespace borinkdb::log::file;

    OUTCOME_TRY(auto path, make_temp_log_path());
    OUTCOME_TRY(auto log, LogFile::open(path));

    const std::vector<std::byte> payload(kMaxGroupedPayloadBytes + 1, std::byte{0x55});
    auto result = commit_one(*log, "too-large-group", byteview{}, payload);
    if (result ||
        !result.error().equivalent(system_error2::make_status_code(Error::too_many_group_blocks))) {
        OUTCOME_TRYV(unlink_path(path));
        return test_failure("oversized grouped payload did not report group block limit");
    }

    std::cout << "grouped payload limit OK\n";
    OUTCOME_TRYV(unlink_path(path));
    return outcome::success();
}

#if defined(BORINKDB_DEBUG_TESTS) && !defined(_WIN32)
namespace child_exit {
constexpr int success = 0;
constexpr int open_failed = 2;
constexpr int commit_failed = 3;
constexpr int read_failed = 4;
constexpr int empty_payload = 5;
constexpr int stale_page_still_mapped = 6;
constexpr int barrier_read_failed = 7;
constexpr int commit_lost_after_retries = 8;
}

// Linux-only helper for the expired-owner test. mincore reports ENOMEM when
// the page address is not currently mapped in this process. We use this instead
// of deliberately reading a stale pointer and waiting for a crash.
bool page_is_unmapped(const void* address) {
    const long page_size_raw = sysconf(_SC_PAGESIZE);
    if (page_size_raw <= 0) {
        return false;
    }

    const auto page_size = static_cast<std::size_t>(page_size_raw);
    const auto raw_address = reinterpret_cast<std::uintptr_t>(address);
    const auto page_mask = ~(static_cast<std::uintptr_t>(page_size) - 1u);
    const auto page_address = raw_address & page_mask;

    unsigned char residency = 0;
    errno = 0;
    const int rc = mincore(reinterpret_cast<void*>(page_address), page_size, &residency);
    return rc != 0 && errno == ENOMEM;
}

int expired_owner_view_child(std::string_view path) {
    using namespace borinkdb::log::file;

    auto opened = LogFile::open(path);
    if (!opened) {
        return child_exit::open_failed;
    }

    const std::vector<std::byte> payload(5000, std::byte{0x33});
    auto committed = commit_one(*opened.value(), "stale", byteview{}, payload);
    if (!committed) {
        return child_exit::commit_failed;
    }

    auto view_opt = opened.value()->get_record_view("stale", LogReadMode::Cached);
    if (!view_opt || !view_opt.value()) {
        return child_exit::read_failed;
    }

    auto view = std::move(*view_opt.value());
    const auto blocks = view.payload_blocks();
    if (blocks.empty() || blocks.front().empty()) {
        return child_exit::empty_payload;
    }

    const auto* raw = blocks.front().data();

    // Destroy the owner; the remaining span is intentionally stale after this.
    // We never dereference raw again.
    opened.value().reset();

    // Expected behavior: destroying the mapped handle unmaps the old address.
    // If LLFIO keeps the mapping alive longer, this child returns failure.
    return page_is_unmapped(raw) ? child_exit::success : child_exit::stale_page_still_mapped;
}

// Contract checked: once the owning LogFile is destroyed, borrowed mmap spans
// are no longer backed by that owner. This test is debug/Linux-only because it
// inspects VM state with mincore and stale pointers are inherently delicate.
//
// Potential flakiness: low. The child process isolates the stale pointer, and
// mincore avoids a crash path. It still depends on Linux mmap semantics.
outcome::status_result<void> debug_expired_owner_invalidates_view_process_test() {
    OUTCOME_TRY(auto path, make_temp_log_path());

    // Keep this in a child because it inspects a deliberately stale pointer.
    // Parent only observes an exit code, so failure cannot poison later tests.
    const pid_t pid = fork();
    if (pid < 0) {
        return test_failure("failed to fork expired-view child");
    }
    if (pid == 0) {
        const int code = expired_owner_view_child(path);
        std::_Exit(code);
    }

    int status = 0;
    if (waitpid(pid, &status, 0) < 0) {
        OUTCOME_TRYV(unlink_path(path));
        return test_failure("failed to wait for expired-view child");
    }

    OUTCOME_TRYV(unlink_path(path));
    if (WIFEXITED(status) && WEXITSTATUS(status) == child_exit::success) {
        std::cout << "debug expired owner invalidates view OK\n";
        return outcome::success();
    }
    return test_failure("expired owner view page remained mapped or failed before invalidation");
}

int multiprocess_writer_child(std::string_view path, std::size_t child_index, std::size_t record_count) {
    using namespace borinkdb::log::file;

    auto opened = LogFile::open(path);
    if (!opened) {
        return child_exit::open_failed;
    }

    for (std::size_t record_index = 0; record_index < record_count; ++record_index) {
        const std::string key =
            "process-" + std::to_string(child_index) + "-record-" + std::to_string(record_index);
        const auto payload = bytes("payload-" + key);

        bool committed_successfully = false;
        for (int attempt = 0; attempt < 20; ++attempt) {
            auto committed = commit_one(*opened.value(), key, byteview{}, payload);
            if (!committed) {
                return child_exit::commit_failed;
            }
            if (committed.value().id_hi != 0 || committed.value().id_lo != 0) {
                committed_successfully = true;
                break;
            }

            // The loop is retained as a bounded retry point for filesystem
            // errors; the append-only logical ID model no longer reports lost
            // append races here.
            std::this_thread::yield();
        }
        if (!committed_successfully) {
            return child_exit::commit_lost_after_retries;
        }
    }
    return child_exit::success;
}

int multiprocess_open_create_child(std::string_view path, std::size_t child_index, int barrier_read_fd) {
    using namespace borinkdb::log::file;

    // Parent writes one byte per child once all children are forked. That makes
    // open/create happen close together without relying on sleeps.
    char release = 0;
    if (read(barrier_read_fd, &release, 1) != 1) {
        return child_exit::barrier_read_failed;
    }
    close(barrier_read_fd);

    auto opened = LogFile::open(path);
    if (!opened) {
        return child_exit::open_failed;
    }

    const std::string key = "open-race-" + std::to_string(child_index);
    const auto payload = bytes("payload-" + key);

    bool committed_successfully = false;
    for (int attempt = 0; attempt < 20; ++attempt) {
        auto committed = commit_one(*opened.value(), key, byteview{}, payload);
        if (!committed) {
            return child_exit::commit_failed;
        }
        if (committed.value().id_hi != 0 || committed.value().id_lo != 0) {
            committed_successfully = true;
            break;
        }

        // The open race is the point of this test; retry remains bounded even
        // though logical IDs make successful appends idempotent.
        std::this_thread::yield();
    }
    if (!committed_successfully) {
        return child_exit::commit_lost_after_retries;
    }
    return child_exit::success;
}

// Contract checked: multiple processes may race to LogFile::open a path that
// does not exist yet, and each can then commit a record to the same file.
//
// What this asserts: all children exit successfully and a fresh reader can see
// every record. What it does not assert: exact ordering between child commits.
// Potential flakiness: moderate if the filesystem has unusual create/mmap race
// behavior; the pipe barrier makes the race intentional and bounded.
outcome::status_result<void> multiprocess_open_create_same_file_test() {
    using namespace borinkdb::log::file;

    OUTCOME_TRY(auto path, make_temp_log_path());

    int barrier_fds[2] = {-1, -1};
    if (pipe(barrier_fds) != 0) {
        return test_failure("failed to create open-race barrier");
    }

    constexpr std::size_t kProcessCount = 4;
    std::vector<pid_t> children;
    children.reserve(kProcessCount);

    for (std::size_t child_index = 0; child_index < kProcessCount; ++child_index) {
        const pid_t pid = fork();
        if (pid < 0) {
            close(barrier_fds[0]);
            close(barrier_fds[1]);
            OUTCOME_TRYV(unlink_path(path));
            return test_failure("failed to fork open-race child");
        }
        if (pid == 0) {
            close(barrier_fds[1]);
            const int code = multiprocess_open_create_child(path, child_index, barrier_fds[0]);
            std::_Exit(code);
        }
        children.push_back(pid);
    }

    // Release all children at once. The pipe is the synchronization primitive;
    // there are no sleeps in this test.
    close(barrier_fds[0]);
    const std::array<char, kProcessCount> release_bytes{};
    if (write(barrier_fds[1], release_bytes.data(), release_bytes.size()) !=
        static_cast<ssize_t>(release_bytes.size())) {
        close(barrier_fds[1]);
        OUTCOME_TRYV(unlink_path(path));
        return test_failure("failed to release open-race children");
    }
    close(barrier_fds[1]);

    for (const pid_t pid : children) {
        int status = 0;
        if (waitpid(pid, &status, 0) < 0 || !WIFEXITED(status) || WEXITSTATUS(status) != child_exit::success) {
            OUTCOME_TRYV(unlink_path(path));
            return test_failure("open-race child process failed");
        }
    }

    // Verify from a brand-new reader so success does not depend on any child
    // process's in-memory index state.
    OUTCOME_TRY(auto reader, LogFile::open(path));
    for (std::size_t child_index = 0; child_index < kProcessCount; ++child_index) {
        const std::string key = "open-race-" + std::to_string(child_index);
        const auto expected = bytes("payload-" + key);
        OUTCOME_TRY(auto got, reader->get_record_view(key, LogReadMode::Refresh));
        if (!got || !bytes_equal_blocks(got->payload_blocks(), expected)) {
            OUTCOME_TRYV(unlink_path(path));
            return test_failure("multiprocess open-race verification failed");
        }
    }

    std::cout << "multiprocess open/create same-file stress OK\n";
    OUTCOME_TRYV(unlink_path(path));
    return outcome::success();
}

outcome::status_result<void> multiprocess_same_file_stress_test() {
    using namespace borinkdb::log::file;

    OUTCOME_TRY(auto path, make_temp_log_path());
    {
        // This test is about concurrent append/read on an existing file. The
        // separate open/create test above covers the first-creation race.
        OUTCOME_TRY(auto seed, LogFile::open(path));
    }

    constexpr std::size_t kProcessCount = 4;
    constexpr std::size_t kRecordsPerProcess = 20;
    std::vector<pid_t> children;
    children.reserve(kProcessCount);

    for (std::size_t child_index = 0; child_index < kProcessCount; ++child_index) {
        const pid_t pid = fork();
        if (pid < 0) {
            OUTCOME_TRYV(unlink_path(path));
            return test_failure("failed to fork writer child");
        }
        if (pid == 0) {
            const int code = multiprocess_writer_child(path, child_index, kRecordsPerProcess);
            std::_Exit(code);
        }
        children.push_back(pid);
    }

    // Every child must complete its own retry loop. A failed child means either
    // open failed, a commit errored, or Lost did not resolve within the bound.
    for (const pid_t pid : children) {
        int status = 0;
        if (waitpid(pid, &status, 0) < 0 || !WIFEXITED(status) || WEXITSTATUS(status) != child_exit::success) {
            OUTCOME_TRYV(unlink_path(path));
            return test_failure("writer child process failed");
        }
    }

    // Final assertion: all unique records from all processes are visible from a
    // fresh reader after refresh. Interleaving/order is intentionally ignored.
    OUTCOME_TRY(auto reader, LogFile::open(path));
    for (std::size_t child_index = 0; child_index < kProcessCount; ++child_index) {
        for (std::size_t record_index = 0; record_index < kRecordsPerProcess; ++record_index) {
            const std::string key =
                "process-" + std::to_string(child_index) + "-record-" + std::to_string(record_index);
            const auto expected = bytes("payload-" + key);
            OUTCOME_TRY(auto got, reader->get_record_view(key, LogReadMode::Refresh));
            if (!got || !bytes_equal_blocks(got->payload_blocks(), expected)) {
                OUTCOME_TRYV(unlink_path(path));
                return test_failure("multiprocess same-file verification failed");
            }
        }
    }

    std::cout << "multiprocess same-file stress OK\n";
    OUTCOME_TRYV(unlink_path(path));
    return outcome::success();
}
#endif

outcome::status_result<void> run_tests() {
    OUTCOME_TRYV(crc32c_known_answer_test());
    OUTCOME_TRYV(block_roundtrip_test());
    OUTCOME_TRYV(writer_reader_payload_test());
    OUTCOME_TRYV(corrupt_skip_test());
    OUTCOME_TRYV(interleaved_group_blocks_test());
    OUTCOME_TRYV(duplicate_logical_block_is_idempotent_test());
    OUTCOME_TRYV(file_byte_log_cache_test());
    OUTCOME_TRYV(file_byte_log_transaction_test());
    OUTCOME_TRYV(same_log_multithreaded_write_stress_test());
    OUTCOME_TRYV(reader_writer_multithreaded_stress_test());
    OUTCOME_TRYV(borrowed_view_survives_append_refresh_test());
    OUTCOME_TRYV(borrowed_view_survives_other_handles_closing_test());
    OUTCOME_TRYV(split_payload_limit_stress_test());
    OUTCOME_TRYV(encode_size_limit_test());
    OUTCOME_TRYV(grouped_payload_limit_test());
    OUTCOME_TRYV(borinkdb::coordinator::run_integration_test());
#if defined(BORINKDB_DEBUG_TESTS) && !defined(_WIN32)
    OUTCOME_TRYV(debug_expired_owner_invalidates_view_process_test());
    OUTCOME_TRYV(multiprocess_open_create_same_file_test());
    OUTCOME_TRYV(multiprocess_same_file_stress_test());
#endif
    return outcome::success();
}

}

namespace borinkdb::coordinator {
int run(int argc, char** argv);
OUTCOME_V2_NAMESPACE::experimental::status_result<void> run_integration_test();
}

int main(int argc, char** argv) {
    if (argc > 1 && std::string_view{argv[1]} == "--bench") {
        return borinkdb::bench::run(argc, argv);
    }
    if (argc > 1 && std::string_view{argv[1]} == "--coordinator") {
        return borinkdb::coordinator::run(argc, argv);
    }

    auto result = run_tests();
    if (!result) {
        std::cerr << "test failed: " << result.error().message() << '\n';
        return 1;
    }
    return 0;
}
