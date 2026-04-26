#include <chrono>
#include <cstddef>
#include <cstring>
#include <iostream>
#include <outcome/config.hpp>
#include <span>
#include <string>
#include <string_view>
#include <thread>
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
    if (!got_opt || !bytes_equal(got_opt->payload_bytes, std::span<const std::byte>{payload.data(), payload.size()})) {
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
    if (!got_opt || !bytes_equal(got_opt->payload_bytes, byte_view("v"))) {
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
    if (!got_opt || !bytes_equal(got_opt->payload_bytes, byte_view("two"))) {
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
        !bytes_equal(cached.payload, byte_view("one"))) {
        OUTCOME_TRYV(unlink_path(path));
        return test_failure("file byte log cached read missed local put");
    }

    OUTCOME_TRY(auto other, FileLog::open(path));
    OUTCOME_TRY(auto second_counter, other->put("k", byte_view("m2"), byte_view("two")));

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
        !bytes_equal(watched_cached->payload, byte_view("two"))) {
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
        !bytes_equal(fresh.payload, byte_view("two"))) {
        OUTCOME_TRYV(unlink_path(path));
        return test_failure("file byte log refresh read missed external put");
    }

    std::cout << "file byte log cache/freshness OK\n";
    OUTCOME_TRYV(unlink_path(path));
    return outcome::success();
}

outcome::status_result<void> run_tests() {
    OUTCOME_TRYV(crc32c_known_answer_test());
    OUTCOME_TRYV(block_roundtrip_test());
    OUTCOME_TRYV(writer_reader_payload_test());
    OUTCOME_TRYV(wait_block_test());
    OUTCOME_TRYV(corrupt_skip_test());
    OUTCOME_TRYV(file_byte_log_cache_test());
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
