#include <chrono>
#include <cstddef>
#include <cstring>
#include <iostream>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <llfio/llfio.hpp>
#include "crc32c.hpp"
#include "log.hpp"
#include "log_api.hpp"

namespace {

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

std::string make_temp_log_path() {
    namespace llfio = LLFIO_V2_NAMESPACE;
    auto dir = llfio::path_discovery::storage_backed_temporary_files_directory().current_path().value();
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

bool unlink_path(std::string_view path) {
    namespace llfio = LLFIO_V2_NAMESPACE;
    auto h = llfio::file_handle::file(
        {},
        llfio::path_view(path.data(), path.size(), llfio::path_view::not_zero_terminated),
        llfio::file_handle::mode::write,
        llfio::file_handle::creation::open_existing);
    if (!h) {
        return false;
    }
    return h.value().unlink().has_value();
}

bool append_raw(std::string_view path, std::span<const std::byte> data) {
    namespace llfio = LLFIO_V2_NAMESPACE;
    auto h = llfio::file_handle::file(
        {},
        llfio::path_view(path.data(), path.size(), llfio::path_view::not_zero_terminated),
        llfio::file_handle::mode::append,
        llfio::file_handle::creation::if_needed);
    if (!h) {
        return false;
    }
    llfio::file_handle::const_buffer_type wb{data.data(), data.size()};
    llfio::file_handle::const_buffers_type wbs{&wb, 1};
    auto wr = h.value().write({wbs, 0});
    if (!wr) {
        return false;
    }
    std::size_t written = 0;
    for (const auto& b : wr.value()) {
        written += b.size();
    }
    return written == data.size();
}

bool crc32c_known_answer_test() {
    using namespace borinkdb;
    const uint32_t got = crc32c(byte_view("123456789"));
    if (got != 0xE3069283u) {
        std::cerr << "crc32c known-answer failed\n";
        return false;
    }
    std::cout << "crc32c OK\n";
    return true;
}

bool block_roundtrip_test() {
    using namespace borinkdb::log::file;

    const auto meta = bytes("opaque-meta");
    const auto payload = bytes("opaque-payload");
    auto encoded = encode_block({
        .kind = BlockKind::Standalone,
        .ts_counter = 7,
        .random_id = 0x1122334455667788ull,
        .group = std::nullopt,
        .key = "alpha",
        .meta_bytes = meta,
        .payload_bytes = payload,
    });
    if (!encoded) {
        std::cerr << "encode_block failed\n";
        return false;
    }

    auto decoded = decode_block(encoded.value());
    if (!decoded) {
        std::cerr << "decode_block failed\n";
        return false;
    }
    const auto& d = decoded.value();
    if (d.kind != BlockKind::Standalone ||
        d.ts_counter != 7 ||
        d.random_id != 0x1122334455667788ull ||
        d.total_size != encoded.value().size() ||
        d.key != "alpha" ||
        std::memcmp(d.meta_bytes.data(), meta.data(), meta.size()) != 0 ||
        std::memcmp(d.payload_bytes.data(), payload.data(), payload.size()) != 0) {
        std::cerr << "decoded block mismatch\n";
        return false;
    }

    auto corrupt = encoded.value();
    corrupt[12] = static_cast<std::byte>(static_cast<unsigned char>(corrupt[12]) ^ 0x55u);
    auto corrupt_decoded = decode_block(corrupt);
    if (corrupt_decoded ||
        !corrupt_decoded.error().equivalent(system_error2::make_status_code(system_error2::errc::illegal_byte_sequence))) {
        std::cerr << "decode_block did not reject CRC damage\n";
        return false;
    }

    std::cout << "block encode/decode OK\n";
    return true;
}

bool writer_reader_payload_test() {
    using namespace borinkdb::log::file;

    const std::string path = make_temp_log_path();
    auto writer = LogWriter::open(path);
    if (!writer) {
        std::cerr << "writer open failed\n";
        unlink_path(path);
        return false;
    }

    const auto meta = bytes("meta");
    const std::vector<std::byte> payload(2500, std::byte{0x42});
    auto commit = writer.value()->commit_payload("large-key", meta, payload);
    if (!commit || commit.value().outcome != CommitOutcome::Success) {
        std::cerr << "commit_payload failed\n";
        unlink_path(path);
        return false;
    }

    auto reader = LogReader::open(path);
    if (!reader) {
        std::cerr << "reader open failed\n";
        unlink_path(path);
        return false;
    }
    auto got = reader.value()->get_payload_view("large-key");
    if (!got || !got.value().has_value() ||
        !bytes_equal(*got.value(), std::span<const std::byte>{payload.data(), payload.size()})) {
        std::cerr << "reader payload mismatch\n";
        unlink_path(path);
        return false;
    }

    std::cout << "writer/reader payload OK\n";
    unlink_path(path);
    return true;
}

bool wait_block_test() {
    using namespace borinkdb::log::file;

    const std::string path = make_temp_log_path();
    auto writer = LogWriter::open(path);
    if (!writer) {
        std::cerr << "writer open failed\n";
        unlink_path(path);
        return false;
    }

    auto wait = writer.value()->commit_wait();
    auto put = writer.value()->commit_payload("k", std::span<const std::byte>{}, byte_view("v"));
    if (!wait || !put ||
        wait.value().outcome != CommitOutcome::Success ||
        put.value().outcome != CommitOutcome::Success) {
        std::cerr << "wait/write sequence failed\n";
        unlink_path(path);
        return false;
    }

    auto reader = LogReader::open(path);
    if (!reader || reader.value()->latest_counter() != 2) {
        std::cerr << "reader did not count wait block\n";
        unlink_path(path);
        return false;
    }
    auto got = reader.value()->get_payload_view("k");
    if (!got || !got.value().has_value() || !bytes_equal(*got.value(), byte_view("v"))) {
        std::cerr << "wait block affected value index\n";
        unlink_path(path);
        return false;
    }

    std::cout << "wait block OK\n";
    unlink_path(path);
    return true;
}

bool corrupt_skip_test() {
    using namespace borinkdb::log::file;

    const std::string path = make_temp_log_path();
    auto one = encode_block({BlockKind::Standalone, 1, 11, std::nullopt, "k", std::span<const std::byte>{}, byte_view("one")});
    auto bad = encode_block({BlockKind::Standalone, 2, 22, std::nullopt, "k", std::span<const std::byte>{}, byte_view("bad")});
    auto two = encode_block({BlockKind::Standalone, 3, 33, std::nullopt, "k", std::span<const std::byte>{}, byte_view("two")});
    if (!one || !bad || !two) {
        std::cerr << "corrupt skip encode failed\n";
        unlink_path(path);
        return false;
    }
    bad.value()[12] = static_cast<std::byte>(static_cast<unsigned char>(bad.value()[12]) ^ 0xAAu);

    if (!append_raw(path, one.value()) || !append_raw(path, bad.value()) || !append_raw(path, two.value())) {
        std::cerr << "corrupt skip append failed\n";
        unlink_path(path);
        return false;
    }

    auto reader = LogReader::open(path);
    if (!reader) {
        std::cerr << "corrupt skip reader open failed\n";
        unlink_path(path);
        return false;
    }
    auto got = reader.value()->get_payload_view("k");
    if (!got || !got.value().has_value() || !bytes_equal(*got.value(), byte_view("two"))) {
        std::cerr << "reader did not skip corrupt block\n";
        unlink_path(path);
        return false;
    }

    std::cout << "corrupt skip OK\n";
    unlink_path(path);
    return true;
}

bool file_byte_log_cache_test() {
    using namespace borinkdb::bytelog;

    const std::string path = make_temp_log_path();
    auto opened = FileLog::open(path);
    if (!opened) {
        std::cerr << "file byte log open failed\n";
        unlink_path(path);
        return false;
    }

    auto first = opened.log->put("k", byte_view("m1"), byte_view("one"));
    if (!first) {
        std::cerr << "file byte log put failed\n";
        unlink_path(path);
        return false;
    }

    auto cached = opened.log->get_latest("k", ReadOptions::UseCached);
    if (!cached || cached.counter != first.counter ||
        !bytes_equal(cached.meta, byte_view("m1")) ||
        !bytes_equal(cached.payload, byte_view("one"))) {
        std::cerr << "file byte log cached read missed local put\n";
        unlink_path(path);
        return false;
    }

    auto other = FileLog::open(path);
    if (!other) {
        std::cerr << "second file byte log open failed\n";
        unlink_path(path);
        return false;
    }
    auto second = other.log->put("k", byte_view("m2"), byte_view("two"));
    if (!second) {
        std::cerr << "second file byte log put failed\n";
        unlink_path(path);
        return false;
    }

    auto still_cached = opened.log->get_latest("k", ReadOptions::UseCached);
    if (!still_cached || still_cached.counter != first.counter ||
        !bytes_equal(still_cached.payload, byte_view("one"))) {
        std::cerr << "file byte log cached read refreshed unexpectedly\n";
        unlink_path(path);
        return false;
    }

    auto fresh = opened.log->get_latest("k", ReadOptions::Refresh);
    if (!fresh || fresh.counter != second.counter ||
        !bytes_equal(fresh.meta, byte_view("m2")) ||
        !bytes_equal(fresh.payload, byte_view("two"))) {
        std::cerr << "file byte log refresh read missed external put\n";
        unlink_path(path);
        return false;
    }

    std::cout << "file byte log cache/freshness OK\n";
    unlink_path(path);
    return true;
}

}

int main() {
    if (!crc32c_known_answer_test() ||
        !block_roundtrip_test() ||
        !writer_reader_payload_test() ||
        !wait_block_test() ||
        !corrupt_skip_test() ||
        !file_byte_log_cache_test()
    )  {
        return 1;
    }
    return 0;
}
