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
#include <unordered_set>
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
  invalid_group_info,
  key_too_large,
  metadata_too_large,
  payload_too_large,
  too_many_group_blocks,
  block_too_large,
  truncated_block,
  invalid_magic,
  invalid_block_length,
  invalid_id_length,
  malformed_block,
  checksum_mismatch,
  short_write,
  mapped_file_unavailable,
  invalid_file_offset,
  corrupt_group
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
    {Error::invalid_group_info, "invalid group info", {errc::invalid_argument}},
    {Error::key_too_large, "key too large", {errc::message_size}},
    {Error::metadata_too_large, "metadata too large", {errc::message_size}},
    {Error::payload_too_large, "payload too large", {errc::message_size}},
    {Error::too_many_group_blocks, "too many group blocks", {errc::message_size}},
    {Error::block_too_large, "block too large", {errc::message_size}},
    {Error::truncated_block, "truncated block", {errc::message_size}},
    {Error::invalid_magic, "invalid block magic", {errc::illegal_byte_sequence}},
    {Error::invalid_block_length, "invalid block length", {errc::illegal_byte_sequence}},
    {Error::invalid_id_length, "invalid logical block id length", {errc::illegal_byte_sequence}},
    {Error::malformed_block, "malformed block", {errc::illegal_byte_sequence}},
    {Error::checksum_mismatch, "block checksum mismatch", {errc::illegal_byte_sequence}},
    {Error::short_write, "short write", {errc::io_error}},
    {Error::mapped_file_unavailable, "mapped file unavailable", {errc::bad_address}},
    {Error::invalid_file_offset, "invalid file offset", {errc::result_out_of_range}},
    {Error::corrupt_group, "corrupt grouped record", {errc::illegal_byte_sequence}},
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
inline constexpr std::size_t kGroupPayloadChunkBytes = 800;
// Group index/count are u16 on disk, so one logical record can contain at most
// 0xFFFF group blocks, i.e. 65,535 blocks. With 800 payload bytes per block,
// the maximum grouped payload is 52,428,000 bytes, about 50.0 MiB.
inline constexpr std::size_t kMaxGroupBlocks = 0xFFFFu;
inline constexpr std::size_t kMaxGroupedPayloadBytes = kGroupPayloadChunkBytes * kMaxGroupBlocks;

struct GroupInfo {
    // Encoded as u16 fields on disk. This is the source of kMaxGroupBlocks.
    uint16_t index;
    uint16_t count;
};

struct BlockEntry {
    std::string_view key = {};
    byteview meta_bytes = {};
    byteview payload_bytes = {};
};

struct EncodeRequest {
    uint64_t id_hi;
    uint64_t id_lo;
    GroupInfo group;

    std::string_view key = {};
    byteview meta_bytes = {};
    byteview payload_bytes = {};
    uint64_t transaction_ref_hi = 0;
    uint64_t transaction_ref_lo = 0;
    std::span<const BlockEntry> entries = {};
};

struct DecodedEntry {
    std::string_view key;
    byteview meta_bytes;
    byteview payload_bytes;
};

struct DecodedBlock {
    uint64_t id_hi;
    uint64_t id_lo;
    uint64_t transaction_ref_hi;
    uint64_t transaction_ref_lo;
    GroupInfo group;
    std::string_view key;
    byteview meta_bytes;
    byteview payload_bytes;
    std::vector<DecodedEntry> entries;
    std::size_t total_size;
};

outcome::status_result<std::vector<std::byte>> encode_block(const EncodeRequest &req);
outcome::status_result<DecodedBlock>           decode_block(byteview input);

struct CommitResult {
    uint64_t id_hi;
    uint64_t id_lo;
};

struct CommitOptions {
    std::optional<CommitResult> id;
    CommitResult transaction_ref{};
};

struct TransactionEntry {
    std::string_view key;
    byteview meta_bytes;
    byteview payload_bytes;
};

// Most records fit in one block, and even split records usually need only a
// handful of block views. Keep those views inline so reading a record does not
// allocate just to describe the payload layout; spill only for unusually large
// payloads.
class PayloadBlockList {
public:
    void reserve(std::size_t capacity);
    void push_back(byteview block);

    [[nodiscard]] std::span<const byteview> view() const noexcept;

private:
    static constexpr std::size_t kInlineCapacity = 4;

    std::array<byteview, kInlineCapacity> inline_{};
    std::vector<byteview> heap_;
    std::size_t size_ = 0;
};

class LogRecordView {
public:
    LogRecordView(LogRecordView&&) noexcept = default;
    LogRecordView& operator=(LogRecordView&&) noexcept = default;
    LogRecordView(const LogRecordView&) = delete;
    LogRecordView& operator=(const LogRecordView&) = delete;
    ~LogRecordView() = default;

    [[nodiscard]] uint64_t id_hi() const noexcept { return id_hi_; }
    [[nodiscard]] uint64_t id_lo() const noexcept { return id_lo_; }
    [[nodiscard]] uint64_t transaction_ref_hi() const noexcept { return transaction_ref_hi_; }
    [[nodiscard]] uint64_t transaction_ref_lo() const noexcept { return transaction_ref_lo_; }
    [[nodiscard]] uint16_t group_index() const noexcept { return group_index_; }
    [[nodiscard]] byteview meta_bytes() const noexcept { return meta_bytes_; }
    [[nodiscard]] std::span<const byteview> payload_blocks() const noexcept {
        return payload_blocks_.view();
    }

private:
    friend class LogFile;

    LogRecordView(uint64_t id_hi,
                  uint64_t id_lo,
                  uint64_t transaction_ref_hi,
                  uint64_t transaction_ref_lo,
                  uint16_t group_index,
                  byteview meta_bytes,
                  PayloadBlockList payload_blocks);

    uint64_t id_hi_ = 0;
    uint64_t id_lo_ = 0;
    uint64_t transaction_ref_hi_ = 0;
    uint64_t transaction_ref_lo_ = 0;
    uint16_t group_index_ = 0;
    byteview meta_bytes_;
    PayloadBlockList payload_blocks_;
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

    outcome::status_result<CommitResult> commit_transaction(std::span<const TransactionEntry> entries);
    outcome::status_result<CommitResult> commit_transaction(std::span<const TransactionEntry> entries,
                                                            const CommitOptions& options);

    // Returns a borrowed log record view. Keep LogFile alive longer than any
    // view returned from it.
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
    // Payload is exposed as block spans instead of one contiguous buffer. Callers
    // that need contiguous bytes can copy while holding the returned view.
    outcome::status_result<std::optional<LogRecordView>>
        get_record_view(std::string_view key, LogReadMode mode);

    outcome::status_result<void> refresh();

    using RecordVisitor = std::function<void(std::string_view key, const LogRecordView& record)>;
    outcome::status_result<void> visit_records(const RecordVisitor& visit);

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
    outcome::status_result<void>           write_block(byteview bytes);
    void refresh_from_watcher() noexcept;

    struct LogicalBlockId {
        uint64_t hi;
        uint64_t lo;

        bool operator==(const LogicalBlockId& other) const noexcept {
            return hi == other.hi && lo == other.lo;
        }
    };
    struct LogicalBlockIdHash {
        std::size_t operator()(const LogicalBlockId& id) const noexcept {
            const auto hi_hash = std::hash<uint64_t>{}(id.hi);
            const auto lo_hash = std::hash<uint64_t>{}(id.lo);
            return hi_hash ^ (lo_hash + 0x9e3779b97f4a7c15ull + (hi_hash << 6U) + (hi_hash >> 2U));
        }
    };
    struct IndexedValue { LogicalBlockId id; uint64_t file_offset; uint16_t entry_index; };
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
    outcome::status_result<bool> group_is_complete(byteview mapped, uint64_t file_offset, const DecodedBlock& first_block);
    outcome::status_result<void> index_block(byteview mapped, uint64_t file_offset, const DecodedBlock& blk);
    outcome::status_result<LogRecordView>           record_view_at(uint64_t file_offset, uint16_t entry_index);
    outcome::status_result<byteview> mapped_bytes();
    outcome::status_result<byteview> current_mapped_bytes() const;
    void maybe_index_value(uint64_t file_offset, uint16_t entry_index, LogicalBlockId id,
                           std::string_view key);

    std::string path_;
    std::optional<llfio::file_handle> append_h_;
    llfio::mapped_file_handle mapped_h_;
    mutable std::mutex mutex_;
    std::mt19937_64    rng_;
    uint64_t  scanned_through_offset_ = 0;
    KeyArena  key_arena_;
    std::unordered_set<LogicalBlockId, LogicalBlockIdHash> indexed_logical_blocks_;
    std::unordered_map<std::string_view, IndexedValue, KeyHash, KeyEqual> latest_by_key_;
    std::unique_ptr<FileWatcher> watcher_;
};

} // namespace borinkdb
