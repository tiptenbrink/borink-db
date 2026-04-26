#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <array>
#include <atomic>
#include <deque>
#include <vector>

#include <outcome/experimental/status_result.hpp>
#include <llfio/llfio.hpp>

namespace borinkdb::log::file {
enum class Error
{
  success = 0,
  not_found,
  bad_argument,
  invalid_block_kind,
  missing_group_info,
  unexpected_group_info,
  invalid_group_info,
  key_too_large,
  metadata_too_large,
  payload_too_large,
  block_too_large,
  truncated_block,
  invalid_magic,
  invalid_block_length,
  invalid_timestamp_length,
  malformed_block,
  checksum_mismatch,
  short_write,
  mapped_file_unavailable,
  invalid_file_offset,
  corrupt_group,
  commit_lost
};

}

namespace system_error2 {
using borinkdb::log::file::Error;

template <> struct quick_status_code_from_enum<Error> : quick_status_code_from_enum_defaults<Error>
{
  static constexpr const auto domain_name = "borinkdb filelog";
  static constexpr const auto domain_uuid = "{0047eab7-db4f-4564-8556-f2e604811888}";

  static const std::initializer_list<mapping> &value_mappings()
  {
    static const std::initializer_list<mapping> v = {
    {Error::not_found, "item not found", {errc::no_such_file_or_directory}},
    {Error::bad_argument, "invoked wrong", {errc::invalid_argument}},
    {Error::invalid_block_kind, "invalid block kind", {errc::invalid_argument}},
    {Error::missing_group_info, "group block missing group info", {errc::invalid_argument}},
    {Error::unexpected_group_info, "non-group block has group info", {errc::invalid_argument}},
    {Error::invalid_group_info, "invalid group info", {errc::invalid_argument}},
    {Error::key_too_large, "key too large", {errc::message_size}},
    {Error::metadata_too_large, "metadata too large", {errc::message_size}},
    {Error::payload_too_large, "payload too large", {errc::message_size}},
    {Error::block_too_large, "block too large", {errc::message_size}},
    {Error::truncated_block, "truncated block", {errc::message_size}},
    {Error::invalid_magic, "invalid block magic", {errc::illegal_byte_sequence}},
    {Error::invalid_block_length, "invalid block length", {errc::illegal_byte_sequence}},
    {Error::invalid_timestamp_length, "invalid timestamp length", {errc::illegal_byte_sequence}},
    {Error::malformed_block, "malformed block", {errc::illegal_byte_sequence}},
    {Error::checksum_mismatch, "block checksum mismatch", {errc::illegal_byte_sequence}},
    {Error::short_write, "short write", {errc::io_error}},
    {Error::mapped_file_unavailable, "mapped file unavailable", {errc::bad_address}},
    {Error::invalid_file_offset, "invalid file offset", {errc::result_out_of_range}},
    {Error::corrupt_group, "corrupt grouped record", {errc::illegal_byte_sequence}},
    {Error::commit_lost, "commit lost", {errc::operation_canceled}},
    };
    return v;
  }
};
}

namespace borinkdb::log::file {

namespace outcome = OUTCOME_V2_NAMESPACE::experimental;
namespace llfio = LLFIO_V2_NAMESPACE;
using byteview = std::span<const std::byte>;

inline constexpr std::array<std::byte, 4> kBlockMagic = {
    std::byte{'B'}, std::byte{'D'}, std::byte{'B'}, std::byte{'1'},
};
inline constexpr std::size_t kMaxBlockBytes = 1024;
inline constexpr std::size_t kReaderMapReservationBytes = 64ull * 1024 * 1024 * 1024;

enum class BlockKind : uint8_t {
    Wait       = 0,
    Dictionary = 1,
    Group      = 2,
};

struct GroupInfo {
    uint16_t index;
    uint16_t count;
};

struct EncodeRequest {
    BlockKind kind;
    uint64_t ts_counter;
    uint64_t random_id;
    std::optional<GroupInfo> group;

    std::string_view key;
    byteview meta_bytes;
    byteview payload_bytes;
};

struct DecodedBlock {
    BlockKind kind;
    uint64_t ts_counter;
    uint64_t random_id;
    std::optional<GroupInfo> group;
    std::string_view key;
    byteview meta_bytes;
    byteview payload_bytes;
    std::size_t total_size;
};

bool is_grouped(BlockKind k) noexcept;
outcome::status_result<std::vector<std::byte>> encode_block(const EncodeRequest &req);
outcome::status_result<DecodedBlock>           decode_block(byteview input) noexcept;

enum class CommitOutcome { Success, Lost };

struct CommitResult {
    CommitOutcome outcome;
    uint64_t      ts_counter;
    uint64_t      random_id;
};

struct LogRecordView {
    uint64_t                   counter;
    byteview meta_bytes;
    byteview payload_bytes;
};

enum class LogReadMode { Cached, Refresh };

class FileWatcher {
public:
    using Callback = std::function<void()>;

    static outcome::status_result<std::unique_ptr<FileWatcher>>
        start(std::string path, std::chrono::milliseconds poll_interval, Callback callback);

    FileWatcher(const FileWatcher&) = delete;
    FileWatcher& operator=(const FileWatcher&) = delete;
    FileWatcher(FileWatcher&&) = delete;
    FileWatcher& operator=(FileWatcher&&) = delete;
    ~FileWatcher();

private:
    struct Snapshot {
        uint64_t size = 0;
        std::chrono::system_clock::time_point modified;

        bool operator!=(const Snapshot& other) const noexcept {
            return size != other.size || modified != other.modified;
        }
    };

    FileWatcher(std::string path,
                llfio::file_handle handle,
                Snapshot initial,
                std::chrono::milliseconds poll_interval,
                Callback callback);

    static outcome::status_result<Snapshot> snapshot(llfio::file_handle& handle);
    void run();

    std::string path_;
    llfio::file_handle handle_;
    Snapshot last_;
    std::chrono::milliseconds poll_interval_;
    Callback callback_;
    std::atomic<bool> stop_{false};
    std::thread thread_;
};

// File-backed byte log. It keeps one mmap-backed read/index view and opens the
// append handle lazily only when a commit path first needs it.
class LogFile {
public:
    static outcome::status_result<std::unique_ptr<LogFile>> open(std::string_view path);

    LogFile(const LogFile &) = delete;
    LogFile &operator=(const LogFile &) = delete;
    LogFile(LogFile&&) = delete;
    LogFile& operator=(LogFile&&) = delete;
    ~LogFile();

    outcome::status_result<CommitResult> commit_wait();

    outcome::status_result<CommitResult> commit_payload(
        std::string_view key,
        byteview meta_bytes,
        byteview payload_bytes);

    // Returns a borrowed log record view.
    //
    // Mmap-backed spans are assumed stable across ordinary append-only growth:
    // LLFIO's update_map() should not invalidate previous block addresses while
    // the existing map/section remains valid, the file is not truncated or
    // relinked, and the file stays within kReaderMapReservationBytes. A map or
    // section can become invalid if the file is zero-length when mapped, is later
    // truncated to zero, is relinked/replaced, or LLFIO must recreate the mapping.
    // Windows read-only mapping growth needs dedicated testing before we treat
    // this as more than an implementation assumption there.
    //
    // Multi-block payload spans are backed by grouped_payload_scratch_ and are
    // invalidated by the next multi-block read on this LogFile.
    outcome::status_result<std::optional<LogRecordView>>
        get_record_view(std::string_view key, LogReadMode mode);

    outcome::status_result<void> refresh();

    [[nodiscard]] uint64_t latest_counter() const;
    [[nodiscard]] uint64_t scanned_through() const;

private:
    LogFile(std::string path, llfio::mapped_file_handle mapped_h, std::mt19937_64 rng);

    using BlockVisitor = std::function<void(uint64_t file_offset, const DecodedBlock &)>;
    using CorruptRangeVisitor = std::function<void()>;

    // Scans [start, end) for complete valid blocks, skipping corrupt bytes until
    // the next block magic. Returns the file offset fully consumed; a trailing
    // partial block is left unconsumed for a later refresh.
    outcome::status_result<uint64_t>      scan_range(byteview mapped, uint64_t start, uint64_t end,
                                                                     const BlockVisitor &visit,
                                                                     const CorruptRangeVisitor& on_corrupt = {});
    outcome::status_result<void>           ensure_append_handle();
    outcome::status_result<CommitOutcome>  resolve_outcome(uint64_t our_ts, uint64_t our_id,
                                                                          std::optional<uint16_t> group_count);
    outcome::status_result<void>           write_block(byteview bytes);
    void refresh_from_watcher() noexcept;

    struct IndexedValue { uint64_t ts_counter; uint64_t file_offset; };
    struct PendingGroup {
        bool active = false;
        uint64_t ts = 0;
        uint64_t id = 0;
        uint64_t first_offset = 0;
        uint64_t expected_offset = 0;
        uint16_t next_index = 0;
        uint16_t count = 0;
        std::string key;
    };

    struct KeyHash {
        using is_transparent = void;
        std::size_t operator()(std::string_view s) const noexcept;
    };
    struct KeyEqual {
        using is_transparent = void;
        bool operator()(std::string_view a, std::string_view b) const noexcept { return a == b; }
    };

    class KeyArena {
    public:
        std::string_view store(std::string_view key);
    private:
        static constexpr std::size_t kBlockBytes = 64 * 1024;
        struct Block { std::unique_ptr<char[]> data; std::size_t used = 0; std::size_t capacity = 0; };
        std::deque<Block> blocks_;
    };

    outcome::status_result<void>                    refresh_index();
    void index_block(uint64_t file_offset, const DecodedBlock& blk, PendingGroup& group);
    outcome::status_result<LogRecordView>           record_view_at(uint64_t file_offset);
    outcome::status_result<byteview> mapped_bytes();
    void maybe_index_value(uint64_t file_offset, uint64_t ts_counter,
                           std::string_view key);

    std::string path_;
    std::optional<llfio::file_handle> append_h_;
    llfio::mapped_file_handle mapped_h_;
    mutable std::mutex mutex_;
    std::mt19937_64    rng_;
    uint64_t  scanned_through_offset_ = 0;
    uint64_t  latest_ts_              = 0;
    KeyArena  key_arena_;
    std::unordered_map<std::string_view, IndexedValue, KeyHash, KeyEqual> latest_by_key_;
    std::vector<std::byte>       grouped_payload_scratch_;
    std::unique_ptr<FileWatcher> watcher_;
};

} // namespace borinkdb
