#include <chrono>
#include <atomic>
#include <cstddef>
#include <cstring>
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

#include "crc32c.hpp"
#include "log.hpp"
#include "log_api.hpp"

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
    constexpr std::size_t kChunkPayloadBytes = 800;
    return (payload_size + kChunkPayloadBytes - 1) / kChunkPayloadBytes;
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
        .kind = BlockKind::Group,
        .ts_counter = 7,
        .random_id = 0x1122334455667788ull,
        .group = GroupInfo{.index = 0, .count = 1},
        .key = "alpha",
        .meta_bytes = meta,
        .payload_bytes = payload,
    }));

    OUTCOME_TRY(auto decoded, decode_block(encoded));
    if (decoded.kind != BlockKind::Group ||
        decoded.ts_counter != 7 ||
        decoded.random_id != 0x1122334455667788ull ||
        !decoded.group ||
        decoded.group->index != 0 ||
        decoded.group->count != 1 ||
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
    OUTCOME_TRY(auto commit, log->commit_payload("large-key", meta, payload));
    if (commit.outcome != CommitOutcome::Success) {
        OUTCOME_TRYV(unlink_path(path));
        return test_failure("commit_payload failed");
    }

    OUTCOME_TRY(auto got_opt, log->get_record_view("large-key", LogReadMode::Cached));
    if (!got_opt || !bytes_equal_blocks(got_opt->payload_blocks(), std::span<const std::byte>{payload.data(), payload.size()})) {
        OUTCOME_TRYV(unlink_path(path));
        return test_failure("reader payload mismatch");
    }

    std::cout << "writer/reader payload OK\n";
    OUTCOME_TRYV(unlink_path(path));
    return outcome::success();
}

outcome::status_result<void> wait_block_test() {
    using namespace borinkdb::log::file;

    OUTCOME_TRY(auto path, make_temp_log_path());
    OUTCOME_TRY(auto log, LogFile::open(path));

    OUTCOME_TRY(auto wait, log->commit_wait());
    OUTCOME_TRY(auto put, log->commit_payload("k", std::span<const std::byte>{}, byte_view("v")));
    if (wait.outcome != CommitOutcome::Success || put.outcome != CommitOutcome::Success) {
        OUTCOME_TRYV(unlink_path(path));
        return test_failure("wait/write sequence failed");
    }

    if (log->latest_counter() != 2) {
        OUTCOME_TRYV(unlink_path(path));
        return test_failure("reader did not count wait block");
    }
    OUTCOME_TRY(auto got_opt, log->get_record_view("k", LogReadMode::Cached));
    if (!got_opt || !bytes_equal_blocks(got_opt->payload_blocks(), byte_view("v"))) {
        OUTCOME_TRYV(unlink_path(path));
        return test_failure("wait block affected value index");
    }

    std::cout << "wait block OK\n";
    OUTCOME_TRYV(unlink_path(path));

    return outcome::success();
}

outcome::status_result<void> corrupt_skip_test() {
    using namespace borinkdb::log::file;

    OUTCOME_TRY(auto path, make_temp_log_path());
    OUTCOME_TRY(auto one, encode_block({BlockKind::Group, 1, 11, GroupInfo{0, 1}, "k", std::span<const std::byte>{}, byte_view("one")}));
    OUTCOME_TRY(auto bad, encode_block({BlockKind::Group, 2, 22, GroupInfo{0, 1}, "k", std::span<const std::byte>{}, byte_view("bad")}));
    OUTCOME_TRY(auto two, encode_block({BlockKind::Group, 3, 33, GroupInfo{0, 1}, "k", std::span<const std::byte>{}, byte_view("two")}));
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

outcome::status_result<void> file_byte_log_cache_test() {
    using namespace borinkdb::bytelog;

    OUTCOME_TRY(auto path, make_temp_log_path());
    OUTCOME_TRY(auto opened, FileLog::open(path));

    OUTCOME_TRY(auto first_counter, opened->put("k", byte_view("m1"), byte_view("one")));

    OUTCOME_TRY(auto cached_opt, opened->get_latest("k", ReadOptions::UseCached));
    if (!cached_opt) {
        OUTCOME_TRYV(unlink_path(path));
        return test_failure("file byte log cached read missed local put");
    }
    const auto& cached = *cached_opt;
    if (cached.counter != first_counter ||
        !bytes_equal(cached.meta, byte_view("m1")) ||
        !bytes_equal_blocks(cached.payload_blocks, byte_view("one"))) {
        OUTCOME_TRYV(unlink_path(path));
        return test_failure("file byte log cached read missed local put");
    }

    OUTCOME_TRY(auto other, FileLog::open(path));
    OUTCOME_TRY(auto second_counter, other->put("k", byte_view("m2"), byte_view("two")));

    std::this_thread::sleep_for(std::chrono::milliseconds{150});
    std::optional<RecordView> watched_cached;
    for (int attempt = 0; attempt < 20; ++attempt) {
        OUTCOME_TRY(auto cached_after_external, opened->get_latest("k", ReadOptions::UseCached));
        if (cached_after_external && cached_after_external->counter == second_counter) {
            watched_cached = cached_after_external;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{25});
    }
    if (!watched_cached ||
        !bytes_equal(watched_cached->meta, byte_view("m2")) ||
        !bytes_equal_blocks(watched_cached->payload_blocks, byte_view("two"))) {
        OUTCOME_TRYV(unlink_path(path));
        return test_failure("file byte log watcher did not refresh cached read");
    }
    OUTCOME_TRY(auto fresh_opt, opened->get_latest("k", ReadOptions::Refresh));
    if (!fresh_opt) {
        OUTCOME_TRYV(unlink_path(path));
        return test_failure("file byte log refresh read missed external put");
    }
    const auto& fresh = *fresh_opt;
    if (fresh.counter != second_counter ||
        !bytes_equal(fresh.meta, byte_view("m2")) ||
        !bytes_equal_blocks(fresh.payload_blocks, byte_view("two"))) {
        OUTCOME_TRYV(unlink_path(path));
        return test_failure("file byte log refresh read missed external put");
    }

    std::cout << "file byte log cache/freshness OK\n";
    OUTCOME_TRYV(unlink_path(path));
    return outcome::success();
}

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
                auto committed = log->commit_payload(key, byteview{}, payload);
                if (!committed || committed.value().outcome != CommitOutcome::Success) {
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
            auto committed = writer->commit_payload("shared", byteview{}, payload);
            if (!committed || committed.value().outcome != CommitOutcome::Success) {
                fail("reader/writer stress commit failed");
                break;
            }
        }
        writer_done.store(true, std::memory_order_release);
    });

    std::vector<std::thread> readers;
    readers.reserve(kReaderCount);
    for (std::size_t reader_index = 0; reader_index < kReaderCount; ++reader_index) {
        readers.emplace_back([&, reader_index] {
            auto opened = LogFile::open(path);
            if (!opened) {
                fail("reader/writer stress reader open failed");
                return;
            }

            uint64_t last_counter = 0;
            while (!writer_done.load(std::memory_order_acquire) && !failed.load(std::memory_order_acquire)) {
                auto got = opened.value()->get_record_view("shared", LogReadMode::Refresh);
                if (!got) {
                    fail("reader/writer stress read failed");
                    return;
                }
                if (got.value().has_value()) {
                    const auto counter = got.value()->counter();
                    if (counter < last_counter) {
                        fail("reader/writer stress counter moved backwards");
                        return;
                    }
                    last_counter = counter;
                    if (got.value()->payload_blocks().empty()) {
                        fail("reader/writer stress read empty payload");
                        return;
                    }
                }
                const auto delay = std::chrono::milliseconds{1 + static_cast<int>(reader_index)};
                std::this_thread::sleep_for(delay);
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

outcome::status_result<void> borrowed_view_survives_append_refresh_test() {
    using namespace borinkdb::log::file;

    OUTCOME_TRY(auto path, make_temp_log_path());
    OUTCOME_TRY(auto log, LogFile::open(path));

    const auto meta = bytes("stable-meta");
    const std::vector<std::byte> stable_payload(5000, std::byte{0x5A});
    OUTCOME_TRY(auto stable_commit, log->commit_payload("stable", meta, stable_payload));
    if (stable_commit.outcome != CommitOutcome::Success) {
        OUTCOME_TRYV(unlink_path(path));
        return test_failure("stable-view setup commit failed");
    }

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

    std::vector<const std::byte*> original_block_addresses;
    original_block_addresses.reserve(original_blocks.size());
    for (const auto block : original_blocks) {
        original_block_addresses.push_back(block.data());
    }

    constexpr std::size_t kAppendCount = 160;
    for (std::size_t index = 0; index < kAppendCount; ++index) {
        const std::string key = "growth-" + std::to_string(index);
        const std::vector<std::byte> payload(1800 + (index % 7), static_cast<std::byte>(index & 0xFFu));
        OUTCOME_TRY(auto commit, log->commit_payload(key, byteview{}, payload));
        if (commit.outcome != CommitOutcome::Success) {
            OUTCOME_TRYV(unlink_path(path));
            return test_failure("stable-view growth commit failed");
        }
    }
    OUTCOME_TRYV(log->refresh());

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

outcome::status_result<void> borrowed_view_survives_other_handles_closing_test() {
    using namespace borinkdb::log::file;

    OUTCOME_TRY(auto path, make_temp_log_path());
    OUTCOME_TRY(auto owner, LogFile::open(path));
    OUTCOME_TRY(auto other, LogFile::open(path));

    const auto payload = bytes("owned-view");
    OUTCOME_TRY(auto commit, owner->commit_payload("k", byteview{}, payload));
    if (commit.outcome != CommitOutcome::Success) {
        OUTCOME_TRYV(unlink_path(path));
        return test_failure("other-handle close setup commit failed");
    }

    OUTCOME_TRY(auto view_opt, owner->get_record_view("k", LogReadMode::Cached));
    if (!view_opt) {
        OUTCOME_TRYV(unlink_path(path));
        return test_failure("other-handle close setup read failed");
    }
    auto view = std::move(*view_opt);
    other.reset();

    if (!bytes_equal_blocks(view.payload_blocks(), payload)) {
        OUTCOME_TRYV(unlink_path(path));
        return test_failure("borrowed view changed after independent handle closed");
    }

    std::cout << "borrowed view other-handle close OK\n";
    OUTCOME_TRYV(unlink_path(path));
    return outcome::success();
}

outcome::status_result<void> split_payload_limit_stress_test() {
    using namespace borinkdb::log::file;

    OUTCOME_TRY(auto path, make_temp_log_path());
    OUTCOME_TRY(auto log, LogFile::open(path));

    constexpr std::size_t kPayloadBytes = 800 * 9 + 123;
    std::vector<std::byte> payload;
    payload.reserve(kPayloadBytes);
    for (std::size_t index = 0; index < kPayloadBytes; ++index) {
        payload.push_back(static_cast<std::byte>((index * 31u) & 0xFFu));
    }

    OUTCOME_TRY(auto commit, log->commit_payload("split", byte_view("meta"), payload));
    if (commit.outcome != CommitOutcome::Success) {
        OUTCOME_TRYV(unlink_path(path));
        return test_failure("split payload stress commit failed");
    }

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

outcome::status_result<void> encode_size_limit_test() {
    using namespace borinkdb::log::file;

    const std::string too_large_key(0x10000u, 'k');
    auto key_result = encode_block({
        .kind = BlockKind::Group,
        .ts_counter = 1,
        .random_id = 1,
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
        .kind = BlockKind::Group,
        .ts_counter = 1,
        .random_id = 1,
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
        .kind = BlockKind::Group,
        .ts_counter = 1,
        .random_id = 1,
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

outcome::status_result<void> run_tests() {
    OUTCOME_TRYV(crc32c_known_answer_test());
    OUTCOME_TRYV(block_roundtrip_test());
    OUTCOME_TRYV(writer_reader_payload_test());
    OUTCOME_TRYV(wait_block_test());
    OUTCOME_TRYV(corrupt_skip_test());
    OUTCOME_TRYV(file_byte_log_cache_test());
    OUTCOME_TRYV(same_log_multithreaded_write_stress_test());
    OUTCOME_TRYV(reader_writer_multithreaded_stress_test());
    OUTCOME_TRYV(borrowed_view_survives_append_refresh_test());
    OUTCOME_TRYV(borrowed_view_survives_other_handles_closing_test());
    OUTCOME_TRYV(split_payload_limit_stress_test());
    OUTCOME_TRYV(encode_size_limit_test());
    return outcome::success();
}

}

int main() {
    auto result = run_tests();
    if (!result) {
        std::cerr << "test failed: " << result.error().message() << '\n';
        return 1;
    }
    return 0;
}
