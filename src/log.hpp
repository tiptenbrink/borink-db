#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <random>
#include <span>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <array>
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
inline constexpr std::size_t kReaderChunkBytes = 8 * 1024 * 1024;
inline constexpr std::size_t kReaderMapReservationBytes = 64ull * 1024 * 1024 * 1024;

enum class BlockKind : uint8_t {
    Standalone = 0,
    Wait       = 1,
    Dictionary = 2,
    GroupFirst = 3,
    GroupMid   = 4,
    GroupLast  = 5,
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

// File-backed byte log writer.
class LogWriter {
public:
    static outcome::status_result<std::unique_ptr<LogWriter>> open(std::string_view path);

    LogWriter(const LogWriter &) = delete;
    LogWriter &operator=(const LogWriter &) = delete;
    LogWriter(LogWriter &&)                 = default;
    LogWriter &operator=(LogWriter &&)      = default;
    ~LogWriter()                            = default;

    outcome::status_result<CommitResult> commit_standalone(
        std::string_view key,
        byteview meta_bytes,
        byteview payload_bytes);

    outcome::status_result<CommitResult> commit_wait();

    outcome::status_result<CommitResult> commit_payload(
        std::string_view key,
        byteview meta_bytes,
        byteview payload_bytes);

    [[nodiscard]] uint64_t latest_counter()  const noexcept { return latest_ts_; }
    [[nodiscard]] uint64_t scanned_through() const noexcept { return scanned_through_offset_; }

private:
    LogWriter(llfio::file_handle append_h, llfio::file_handle read_h, std::mt19937_64 rng);

    using BlockVisitor = std::function<void(uint64_t file_offset, const DecodedBlock &)>;

    // review: explain the contract of this function
    outcome::status_result<uint64_t>      scan_range(uint64_t start, uint64_t end,
                                                                     const BlockVisitor &visit);
    outcome::status_result<void>           refresh_tail();
    outcome::status_result<CommitOutcome>  resolve_outcome(uint64_t our_ts, uint64_t our_id,
                                                                          std::optional<uint16_t> group_count);
    outcome::status_result<CommitResult>   commit_blocks(std::vector<std::vector<std::byte>> blocks,
                                                                        uint64_t our_ts, uint64_t our_id,
                                                                        std::optional<uint16_t> group_count);

    llfio::file_handle append_h_;
    llfio::file_handle read_h_;
    std::mt19937_64    rng_;
    uint64_t           scanned_through_offset_ = 0;
    uint64_t           latest_ts_              = 0;
};

class LogReader {
public:
    static outcome::status_result<std::unique_ptr<LogReader>> open(std::string_view path);

    LogReader(const LogReader &) = delete;
    LogReader &operator=(const LogReader &) = delete;
    LogReader(LogReader &&)                 = default;
    LogReader &operator=(LogReader &&)      = default;
    ~LogReader()                            = default;

    // Returns a borrowed payload view — invalidated by the next call that may
    // refresh the mapping or reuse grouped scratch storage.
    outcome::status_result<std::optional<byteview>>
        get_payload_view(std::string_view key);

    outcome::status_result<std::optional<LogRecordView>>
        get_record_view(std::string_view key, LogReadMode mode);

    outcome::status_result<void> refresh();

    [[nodiscard]] uint64_t latest_counter()  const noexcept { return latest_ts_; }
    [[nodiscard]] uint64_t scanned_through() const noexcept { return scanned_through_offset_; }

private:
    explicit LogReader(llfio::mapped_file_handle mapped_h);

    struct IndexedValue { uint64_t ts_counter; uint64_t file_offset; };

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
    outcome::status_result<LogRecordView>           record_view_at(uint64_t file_offset);
    outcome::status_result<byteview> payload_view_at(uint64_t file_offset);
    outcome::status_result<byteview> mapped_bytes();
    void maybe_index_value(uint64_t file_offset, uint64_t ts_counter,
                           std::string_view key, byteview payload_bytes);

    llfio::mapped_file_handle mapped_h_;
    uint64_t  scanned_through_offset_ = 0;
    uint64_t  latest_ts_              = 0;
    KeyArena  key_arena_;
    std::unordered_map<std::string_view, IndexedValue, KeyHash, KeyEqual> latest_by_key_;
    std::unordered_set<uint64_t> committed_counters_;
    std::vector<std::byte>       grouped_payload_scratch_;
};

} // namespace borinkdb
