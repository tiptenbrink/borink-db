#include "log.hpp"
#include "crc32c.hpp"
#include "byte_io.hpp"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <outcome/experimental/status-code/include/status-code/system_code.hpp>
#include <outcome/try.hpp>

namespace borinkdb::log::file {

// namespace outcome = OUTCOME_V2_NAMESPACE::experimental;

constexpr std::size_t kMagicSize = kBlockMagic.size();
constexpr std::size_t kTotalLenSize = 2;
constexpr std::size_t kKindSize = 1;
constexpr std::size_t kFlagsSize = 1;
constexpr std::size_t kTsLenSize = 1;
constexpr std::size_t kRandomIdSize = 8;
constexpr std::size_t kGroupFieldsSize = 4;
constexpr std::size_t kKeyLenSize = 2;
constexpr std::size_t kMetaLenSize = 2;
constexpr std::size_t kPayloadLenSize = 2;
constexpr std::size_t kCrcSize = 4;
constexpr std::size_t kMaxTsLen = 8;
constexpr std::size_t kFixedHeaderPrefix =
    kMagicSize + kTotalLenSize + kKindSize + kFlagsSize + kTsLenSize;

bool kind_is_known(uint8_t raw) noexcept {
    return raw <= static_cast<uint8_t>(BlockKind::GroupLast);
}

std::size_t find_magic(std::span<const std::byte> bytes, std::size_t start) {
    if (bytes.size() < kBlockMagic.size() || start > bytes.size() - kBlockMagic.size()) {
        return bytes.size();
    }
    for (std::size_t i = start; i <= bytes.size() - kBlockMagic.size(); ++i) {
        if (std::memcmp(bytes.data() + i, kBlockMagic.data(), kBlockMagic.size()) == 0) {
            return i;
        }
    }
    return bytes.size();
}

bool is_grouped(BlockKind k) noexcept {
    return k == BlockKind::GroupFirst ||
           k == BlockKind::GroupMid ||
           k == BlockKind::GroupLast;
}

outcome::status_result<std::vector<std::byte>> encode_block(const EncodeRequest& req) {
    if (!kind_is_known(static_cast<uint8_t>(req.kind))) {
        return Error::invalid_block_kind;
    }
    const bool grouped = is_grouped(req.kind);
    if (grouped && !req.group.has_value()) {
        return Error::missing_group_info;
    }
    if (!grouped && req.group.has_value()) {
        return Error::unexpected_group_info;
    }
    if (req.key.size() > 0xFFFFu) {
        return Error::key_too_large;
    }
    if (req.meta_bytes.size() > 0xFFFFu) {
        return Error::metadata_too_large;
    }
    if (req.payload_bytes.size() > 0xFFFFu) {
        return Error::payload_too_large;
    }

    const std::size_t ts_len = detail::ts_byte_length(req.ts_counter);
    const std::size_t group_extra = grouped ? kGroupFieldsSize : 0;
    const std::size_t total =
        kFixedHeaderPrefix + ts_len + kRandomIdSize + group_extra +
        kKeyLenSize + req.key.size() +
        kMetaLenSize + req.meta_bytes.size() +
        kPayloadLenSize + req.payload_bytes.size() +
        kCrcSize;
    if (total >= kMaxBlockBytes) {
        return Error::block_too_large;
    }

    std::vector<std::byte> out(total);
    std::byte* p = out.data();

    detail::write_bytes(p, kBlockMagic.data(), kMagicSize);
    detail::write_u16_le(p, static_cast<uint16_t>(total));
    *p++ = static_cast<std::byte>(req.kind);
    *p++ = std::byte{0};
    *p++ = static_cast<std::byte>(ts_len);
    detail::write_ts_le(p, req.ts_counter, ts_len);
    detail::write_u64_le(p, req.random_id);

    if (grouped) {
        detail::write_u16_le(p, req.group->index);
        detail::write_u16_le(p, req.group->count);
    }

    detail::write_u16_le(p, static_cast<uint16_t>(req.key.size()));
    detail::write_bytes(p, req.key);

    detail::write_u16_le(p, static_cast<uint16_t>(req.meta_bytes.size()));
    detail::write_bytes(p, req.meta_bytes);

    detail::write_u16_le(p, static_cast<uint16_t>(req.payload_bytes.size()));
    detail::write_bytes(p, req.payload_bytes);

    detail::write_u32_le(p, crc32c({out.data(), total - kCrcSize}));
    return out;
}

outcome::status_result<DecodedBlock> decode_block(std::span<const std::byte> input) noexcept {
    const std::size_t min_size =
        kFixedHeaderPrefix + 1 + kRandomIdSize + kKeyLenSize + kMetaLenSize + kPayloadLenSize + kCrcSize;
    if (input.size() < min_size) {
        return Error::truncated_block;
    }
    if (std::memcmp(input.data(), kBlockMagic.data(), kMagicSize) != 0) {
        return Error::invalid_magic;
    }

    const std::byte* total_len_p = input.data() + kMagicSize;
    const uint16_t total_len = detail::read_u16_le(total_len_p);
    if (total_len < min_size || total_len > kMaxBlockBytes) {
        return Error::invalid_block_length;
    }
    if (total_len > input.size()) {
        return Error::truncated_block;
    }

    const uint8_t kind_raw = static_cast<uint8_t>(input[kMagicSize + kTotalLenSize]);
    if (!kind_is_known(kind_raw)) {
        return Error::invalid_block_kind;
    }
    const auto kind = static_cast<BlockKind>(kind_raw);

    const uint8_t ts_len = static_cast<uint8_t>(input[kMagicSize + kTotalLenSize + kKindSize + kFlagsSize]);
    if (ts_len < 1 || ts_len > kMaxTsLen) {
        return Error::invalid_timestamp_length;
    }

    const std::byte* p = input.data() + kFixedHeaderPrefix;
    const std::byte* const end_excl_crc = input.data() + (total_len - kCrcSize);
    const std::size_t group_extra = is_grouped(kind) ? kGroupFieldsSize : 0;
    const std::size_t fixed_body =
        ts_len + kRandomIdSize + group_extra + kKeyLenSize + kMetaLenSize + kPayloadLenSize;
    if (p + fixed_body > end_excl_crc) {
        return Error::malformed_block;
    }

    const uint64_t ts_counter = detail::read_ts_le(p, ts_len);
    const uint64_t random_id = detail::read_u64_le(p);

    std::optional<GroupInfo> group;
    if (is_grouped(kind)) {
        GroupInfo gi;
        gi.index = detail::read_u16_le(p);
        gi.count = detail::read_u16_le(p);
        group = gi;
    }

    const uint16_t key_len = detail::read_u16_le(p);
    if (p + key_len > end_excl_crc) {
        return Error::malformed_block;
    }
    std::string_view key{reinterpret_cast<const char*>(p), key_len};
    p += key_len;

    const uint16_t meta_len = detail::read_u16_le(p);
    if (p + meta_len > end_excl_crc) {
        return Error::malformed_block;
    }
    std::span<const std::byte> meta_bytes{p, meta_len};
    p += meta_len;

    const uint16_t payload_len = detail::read_u16_le(p);
    if (p + payload_len != end_excl_crc) {
        return Error::malformed_block;
    }
    std::span<const std::byte> payload_bytes{p, payload_len};

    const std::byte* expected_crc_p = input.data() + (total_len - kCrcSize);
    const uint32_t expected_crc = detail::read_u32_le(expected_crc_p);
    const uint32_t actual_crc = crc32c({input.data(), static_cast<std::size_t>(total_len - kCrcSize)});
    if (expected_crc != actual_crc) {
        return Error::checksum_mismatch;
    }

    return DecodedBlock{
        .kind = kind,
        .ts_counter = ts_counter,
        .random_id = random_id,
        .group = group,
        .key = key,
        .meta_bytes = meta_bytes,
        .payload_bytes = payload_bytes,
        .total_size = total_len,
    };
}

outcome::status_result<std::unique_ptr<LogWriter>> LogWriter::open(std::string_view path) {
    using mode = llfio::file_handle::mode;
    using creation = llfio::file_handle::creation;
    using caching = llfio::file_handle::caching;

    const llfio::path_view path_view{path.data(), path.size(), llfio::path_view::not_zero_terminated};
    OUTCOME_TRY(auto append_h, llfio::file_handle::file({}, path_view, mode::append, creation::if_needed, caching::all));
    OUTCOME_TRY(auto read_h, llfio::file_handle::file({}, path_view, mode::read, creation::open_existing, caching::all));

    std::random_device rd;
    std::seed_seq seed{rd(), rd(), rd(), rd(),
                       static_cast<unsigned>(std::chrono::steady_clock::now().time_since_epoch().count())};
    auto writer = std::unique_ptr<LogWriter>(
        new LogWriter(std::move(append_h), std::move(read_h), std::mt19937_64(seed)));
    OUTCOME_TRYV(writer->refresh_tail());
    return writer;
}

LogWriter::LogWriter(llfio::file_handle append_h, llfio::file_handle read_h, std::mt19937_64 rng)
    : append_h_(std::move(append_h)), read_h_(std::move(read_h)), rng_(std::move(rng)) {}

outcome::status_result<uint64_t> LogWriter::scan_range(uint64_t start_offset,
                                               uint64_t end_offset,
                                               const BlockVisitor& visit) {
    if (start_offset >= end_offset) {
        return start_offset;
    }

    std::vector<std::byte> chunk;
    chunk.reserve(kReaderChunkBytes);
    std::vector<std::byte> carry;
    std::vector<std::byte> combined;
    uint64_t cursor = start_offset;
    uint64_t consumed = start_offset;

    while (cursor < end_offset) {
        const uint64_t want = std::min<uint64_t>(kReaderChunkBytes, end_offset - cursor);
        chunk.resize(static_cast<std::size_t>(want));

        llfio::file_handle::buffer_type rb{chunk.data(), chunk.size()};
        llfio::file_handle::buffers_type rbs{&rb, 1};
        OUTCOME_TRY(auto rr, read_h_.read({rbs, cursor}));
        std::size_t got = 0;
        for (const auto& b : rr) {
            got += b.size();
        }
        if (got == 0) {
            break;
        }
        chunk.resize(got);
        cursor += got;

        std::span<const std::byte> view;
        if (carry.empty()) {
            view = {chunk.data(), chunk.size()};
        } else {
            combined.clear();
            combined.reserve(carry.size() + chunk.size());
            combined.insert(combined.end(), carry.begin(), carry.end());
            combined.insert(combined.end(), chunk.begin(), chunk.end());
            view = {combined.data(), combined.size()};
        }

        std::size_t pos = 0;
        while (pos < view.size()) {
            auto d = decode_block(view.subspan(pos));
            if (!d) {
                if (d.error().equivalent(system_error2::make_status_code(system_error2::errc::message_size))) {
                    break;
                }
                const std::size_t next = find_magic(view, pos + 1);
                if (next == view.size()) {
                    const std::size_t keep = std::min<std::size_t>(kBlockMagic.size() - 1, view.size() - pos);
                    pos = view.size() - keep;
                    break;
                }
                pos = next;
                continue;
            }
            visit(consumed + pos, d.value());
            pos += d.value().total_size;
        }
        consumed += pos;
        carry.assign(view.begin() + pos, view.end());
    }

    return consumed;
}

outcome::status_result<void> LogWriter::refresh_tail() {
    OUTCOME_TRY(auto end, read_h_.maximum_extent());
    if (end <= scanned_through_offset_) {
        return llfio::success();
    }

    OUTCOME_TRY(auto consumed, scan_range(scanned_through_offset_, end,
                                     [this](uint64_t, const DecodedBlock& blk) {
                                         latest_ts_ = std::max(latest_ts_, blk.ts_counter);
                                     }));
    scanned_through_offset_ = consumed;
    return llfio::success();
}

outcome::status_result<CommitOutcome> LogWriter::resolve_outcome(uint64_t our_ts,
                                                        uint64_t our_id,
                                                        std::optional<uint16_t> group_count) {
    OUTCOME_TRY(auto end, read_h_.maximum_extent());
    if (end <= scanned_through_offset_) {
        return CommitOutcome::Lost;
    }

    bool found_our_ts = false;
    CommitOutcome outcome = CommitOutcome::Lost;
    uint16_t matched_group_blocks = 0;
    uint64_t expected_next_offset = 0;

    OUTCOME_TRY(auto consumed, scan_range(scanned_through_offset_, end,
                               [&](uint64_t offset, const DecodedBlock& blk) {
                                   if (!found_our_ts && blk.ts_counter == our_ts) {
                                       found_our_ts = true;
                                       if (blk.random_id != our_id) {
                                           outcome = CommitOutcome::Lost;
                                       } else if (!group_count.has_value()) {
                                           outcome = CommitOutcome::Success;
                                       } else if (blk.kind == BlockKind::GroupFirst &&
                                                  blk.group &&
                                                  blk.group->index == 0 &&
                                                  blk.group->count == *group_count) {
                                           matched_group_blocks = 1;
                                           expected_next_offset = offset + blk.total_size;
                                           outcome = (*group_count == 1) ? CommitOutcome::Success : CommitOutcome::Lost;
                                       }
                                   } else if (found_our_ts && group_count &&
                                              outcome == CommitOutcome::Lost &&
                                              matched_group_blocks > 0 &&
                                              matched_group_blocks < *group_count) {
                                       if (offset == expected_next_offset &&
                                           blk.ts_counter == our_ts &&
                                           blk.random_id == our_id &&
                                           blk.group &&
                                           blk.group->index == matched_group_blocks &&
                                           blk.group->count == *group_count) {
                                           const bool is_last = matched_group_blocks + 1 == *group_count;
                                           const bool kind_ok = is_last
                                               ? blk.kind == BlockKind::GroupLast
                                               : blk.kind == BlockKind::GroupMid;
                                           if (kind_ok) {
                                               ++matched_group_blocks;
                                               expected_next_offset = offset + blk.total_size;
                                               if (matched_group_blocks == *group_count) {
                                                   outcome = CommitOutcome::Success;
                                               }
                                           } else {
                                               matched_group_blocks = 0;
                                           }
                                       } else {
                                           matched_group_blocks = 0;
                                       }
                                   }
                                   latest_ts_ = std::max(latest_ts_, blk.ts_counter);
                               }));
    scanned_through_offset_ = consumed;
    if (!found_our_ts) {
        outcome = CommitOutcome::Lost;
    }
    return outcome;
}

outcome::status_result<CommitResult> LogWriter::commit_blocks(std::vector<std::vector<std::byte>> blocks,
                                                     uint64_t our_ts,
                                                     uint64_t our_id,
                                                     std::optional<uint16_t> group_count) {
    for (const auto& bytes : blocks) {
        llfio::file_handle::const_buffer_type wb{bytes.data(), bytes.size()};
        llfio::file_handle::const_buffers_type wbs{&wb, 1};
        OUTCOME_TRY(auto wr, append_h_.write({wbs, 0}));

        std::size_t written = 0;
        for (const auto& b : wr) {
            written += b.size();
        }
        if (written != bytes.size()) {
            return Error::short_write;
        }
    }

    OUTCOME_TRY(auto commit_outcome, resolve_outcome(our_ts, our_id, group_count));
    return CommitResult{.outcome = commit_outcome, .ts_counter = our_ts, .random_id = our_id};
}

outcome::status_result<CommitResult> LogWriter::commit_standalone(
    std::string_view key,
    std::span<const std::byte> meta_bytes,
    std::span<const std::byte> payload_bytes) {
    OUTCOME_TRYV(refresh_tail());
    const uint64_t our_ts = latest_ts_ + 1;
    const uint64_t our_id = rng_();
    OUTCOME_TRY(auto encoded, encode_block({
        .kind = BlockKind::Standalone,
        .ts_counter = our_ts,
        .random_id = our_id,
        .group = std::nullopt,
        .key = key,
        .meta_bytes = meta_bytes,
        .payload_bytes = payload_bytes,
    }));
    std::vector<std::vector<std::byte>> blocks;
    blocks.push_back(std::move(encoded));
    return commit_blocks(std::move(blocks), our_ts, our_id, std::nullopt);
}

outcome::status_result<CommitResult> LogWriter::commit_wait() {
    OUTCOME_TRYV(refresh_tail());
    const uint64_t our_ts = latest_ts_ + 1;
    const uint64_t our_id = rng_();
    const std::span<const std::byte> empty;
    OUTCOME_TRY(auto encoded, encode_block({
        .kind = BlockKind::Wait,
        .ts_counter = our_ts,
        .random_id = our_id,
        .group = std::nullopt,
        .key = {},
        .meta_bytes = empty,
        .payload_bytes = empty,
    }));
    std::vector<std::vector<std::byte>> blocks;
    blocks.push_back(std::move(encoded));
    return commit_blocks(std::move(blocks), our_ts, our_id, std::nullopt);
}

outcome::status_result<CommitResult> LogWriter::commit_payload(
    std::string_view key,
    std::span<const std::byte> meta_bytes,
    std::span<const std::byte> payload_bytes) {
    auto single_probe = encode_block({
        .kind = BlockKind::Standalone,
        .ts_counter = latest_ts_ + 1,
        .random_id = 0,
        .group = std::nullopt,
        .key = key,
        .meta_bytes = meta_bytes,
        .payload_bytes = payload_bytes,
    });
    if (single_probe ||
        !single_probe.error().equivalent(system_error2::make_status_code(system_error2::errc::message_size))) {
        return commit_standalone(key, meta_bytes, payload_bytes);
    }
    OUTCOME_TRYV(refresh_tail());

    constexpr std::size_t kChunkPayloadBytes = 800;
    const std::size_t count_sz = (payload_bytes.size() + kChunkPayloadBytes - 1) / kChunkPayloadBytes;
    if (count_sz == 0 || count_sz > 0xFFFFu) {
        return Error::payload_too_large;
    }
    const auto count = static_cast<uint16_t>(count_sz);
    const uint64_t our_ts = latest_ts_ + 1;
    const uint64_t our_id = rng_();

    std::vector<std::vector<std::byte>> blocks;
    blocks.reserve(count);
    for (uint16_t i = 0; i < count; ++i) {
        const std::size_t begin = static_cast<std::size_t>(i) * kChunkPayloadBytes;
        const std::size_t n = std::min(kChunkPayloadBytes, payload_bytes.size() - begin);
        const auto kind = i == 0 ? BlockKind::GroupFirst
                         : (i + 1 == count ? BlockKind::GroupLast : BlockKind::GroupMid);
        const auto block_key = i == 0 ? key : std::string_view{};
        const auto meta = i == 0 ? meta_bytes : std::span<const std::byte>{};
        OUTCOME_TRY(auto encoded, encode_block({
            .kind = kind,
            .ts_counter = our_ts,
            .random_id = our_id,
            .group = GroupInfo{.index = i, .count = count},
            .key = block_key,
            .meta_bytes = meta,
            .payload_bytes = payload_bytes.subspan(begin, n),
        }));
        blocks.push_back(std::move(encoded));
    }
    return commit_blocks(std::move(blocks), our_ts, our_id, count);
}

outcome::status_result<std::unique_ptr<LogReader>> LogReader::open(std::string_view path) {
    const llfio::path_view path_view{path.data(), path.size(), llfio::path_view::not_zero_terminated};
    OUTCOME_TRY(auto mapped_h, llfio::mapped_file_handle::mapped_file(
        kReaderMapReservationBytes,
        {},
        path_view,
        llfio::mapped_file_handle::mode::read,
        llfio::mapped_file_handle::creation::open_existing,
        llfio::mapped_file_handle::caching::reads,
        llfio::mapped_file_handle::flag::disable_safety_barriers |
            llfio::mapped_file_handle::flag::maximum_prefetching,
        llfio::section_handle::flag::read));
    auto reader = std::unique_ptr<LogReader>(new LogReader(std::move(mapped_h)));
    OUTCOME_TRYV(reader->refresh_index());
    return reader;
}

LogReader::LogReader(llfio::mapped_file_handle mapped_h) : mapped_h_(std::move(mapped_h)) {}

std::size_t LogReader::KeyHash::operator()(std::string_view s) const noexcept {
    return std::hash<std::string_view>{}(s);
}

std::string_view LogReader::KeyArena::store(std::string_view key) {
    const std::size_t needed = key.size();
    if (blocks_.empty() || blocks_.back().capacity - blocks_.back().used < needed) {
        const std::size_t capacity = std::max(kBlockBytes, needed);
        blocks_.push_back(Block{
            .data = std::make_unique<char[]>(capacity),
            .used = 0,
            .capacity = capacity,
        });
    }

    auto& block = blocks_.back();
    char* dst = block.data.get() + block.used;
    if (needed != 0) {
        std::memcpy(dst, key.data(), needed);
    }
    block.used += needed;
    return {dst, needed};
}

void LogReader::maybe_index_value(uint64_t file_offset,
                                  uint64_t ts_counter,
                                  std::string_view key,
                                  std::span<const std::byte>) {
    if (key.empty()) {
        return;
    }
    auto it = latest_by_key_.find(key);
    if (it != latest_by_key_.end()) {
        if (ts_counter >= it->second.ts_counter) {
            it->second.ts_counter = ts_counter;
            it->second.file_offset = file_offset;
        }
        return;
    }

    const std::string_view stored_key = key_arena_.store(key);
    latest_by_key_.emplace(stored_key, IndexedValue{
        .ts_counter = ts_counter,
        .file_offset = file_offset,
    });
}

outcome::status_result<std::span<const std::byte>> LogReader::mapped_bytes() {
    OUTCOME_TRY(auto extent, mapped_h_.update_map());
    if (extent == 0) {
        return std::span<const std::byte>{};
    }
    const auto* p = reinterpret_cast<const std::byte*>(mapped_h_.address());
    if (p == nullptr) {
        return Error::mapped_file_unavailable;
    }
    return std::span<const std::byte>{p, static_cast<std::size_t>(extent)};
}

outcome::status_result<std::span<const std::byte>> LogReader::payload_view_at(uint64_t file_offset) {
    OUTCOME_TRY(auto mapped, mapped_bytes());
    if (file_offset >= mapped.size()) {
        return Error::invalid_file_offset;
    }

    const auto offset = static_cast<std::size_t>(file_offset);
    auto first = decode_block(mapped.subspan(offset));
    if (!first) {
        return std::move(first).as_failure();
    }

    const auto& first_block = first.value();
    if (first_block.kind == BlockKind::Standalone) {
        return first_block.payload_bytes;
    }
    if (first_block.kind != BlockKind::GroupFirst || !first_block.group || first_block.group->index != 0) {
        return Error::corrupt_group;
    }

    grouped_payload_scratch_.clear();
    grouped_payload_scratch_.reserve(static_cast<std::size_t>(first_block.group->count) * 800);
    grouped_payload_scratch_.insert(
        grouped_payload_scratch_.end(),
        first_block.payload_bytes.begin(),
        first_block.payload_bytes.end());

    uint64_t next_offset = file_offset + first_block.total_size;
    for (uint16_t next_index = 1; next_index < first_block.group->count; ++next_index) {
        if (next_offset >= mapped.size()) {
            return Error::invalid_file_offset;
        }
        auto decoded = decode_block(mapped.subspan(static_cast<std::size_t>(next_offset)));
        if (!decoded) {
            return std::move(decoded).as_failure();
        }
        const auto& block = decoded.value();
        const bool is_last = next_index + 1 == first_block.group->count;
        const bool kind_ok = is_last ? block.kind == BlockKind::GroupLast : block.kind == BlockKind::GroupMid;
        if (!kind_ok ||
            !block.group ||
            block.group->index != next_index ||
            block.group->count != first_block.group->count ||
            block.ts_counter != first_block.ts_counter ||
            block.random_id != first_block.random_id) {
            return Error::corrupt_group;
        }
        grouped_payload_scratch_.insert(
            grouped_payload_scratch_.end(),
            block.payload_bytes.begin(),
            block.payload_bytes.end());
        next_offset += block.total_size;
    }
    return std::span<const std::byte>{grouped_payload_scratch_.data(), grouped_payload_scratch_.size()};
}

outcome::status_result<void> LogReader::refresh_index() {
    OUTCOME_TRY(auto mapped, mapped_bytes());
    const uint64_t end_offset = mapped.size();
    if (end_offset <= scanned_through_offset_) {
        return llfio::success();
    }

    struct PendingGroup {
        bool active = false;
        uint64_t ts = 0;
        uint64_t id = 0;
        uint64_t first_offset = 0;
        uint64_t expected_offset = 0;
        uint16_t next_index = 0;
        uint16_t count = 0;
        std::string key;
    } group;

    uint64_t consumed = scanned_through_offset_;
    auto view = mapped.subspan(static_cast<std::size_t>(scanned_through_offset_));

    std::size_t pos = 0;
    while (pos < view.size()) {
        auto decoded = decode_block(view.subspan(pos));
        if (!decoded) {
            if (decoded.error().equivalent(system_error2::make_status_code(system_error2::errc::message_size))) {
                break;
            }
            group.active = false;
            const std::size_t next = find_magic(view, pos + 1);
            if (next == view.size()) {
                pos = view.size();
                break;
            }
            pos = next;
            continue;
        }

        const uint64_t offset = consumed + pos;
        const auto& blk = decoded.value();
        latest_ts_ = std::max(latest_ts_, blk.ts_counter);

        if (blk.kind == BlockKind::Standalone) {
            group.active = false;
            if (committed_counters_.insert(blk.ts_counter).second) {
                maybe_index_value(offset, blk.ts_counter, blk.key, blk.payload_bytes);
            }
        } else if (blk.kind == BlockKind::GroupFirst && blk.group && blk.group->index == 0) {
            if (committed_counters_.contains(blk.ts_counter)) {
                group.active = false;
                pos += blk.total_size;
                continue;
            }
            group.active = true;
            group.ts = blk.ts_counter;
            group.id = blk.random_id;
            group.first_offset = offset;
            group.expected_offset = offset + blk.total_size;
            group.next_index = 1;
            group.count = blk.group->count;
            group.key.assign(blk.key.data(), blk.key.size());
            if (group.count == 1) {
                if (committed_counters_.insert(group.ts).second) {
                    maybe_index_value(group.first_offset, group.ts, group.key, std::span<const std::byte>{});
                }
                group.active = false;
            }
        } else if (group.active && blk.group &&
                   offset == group.expected_offset &&
                   blk.ts_counter == group.ts &&
                   blk.random_id == group.id &&
                   blk.group->index == group.next_index &&
                   blk.group->count == group.count) {
            const bool is_last = group.next_index + 1 == group.count;
            const bool kind_ok = is_last ? blk.kind == BlockKind::GroupLast : blk.kind == BlockKind::GroupMid;
            if (kind_ok) {
                group.expected_offset = offset + blk.total_size;
                ++group.next_index;
                if (is_last) {
                    if (committed_counters_.insert(group.ts).second) {
                        maybe_index_value(group.first_offset, group.ts, group.key, std::span<const std::byte>{});
                    }
                    group.active = false;
                }
            } else {
                group.active = false;
            }
        } else if (blk.kind == BlockKind::Wait || blk.kind == BlockKind::Dictionary) {
            group.active = false;
            committed_counters_.insert(blk.ts_counter);
        } else {
            group.active = false;
        }

        pos += blk.total_size;
    }
    consumed += pos;

    scanned_through_offset_ = consumed;
    return llfio::success();
}

outcome::status_result<void> LogReader::refresh() {
    return refresh_index();
}

outcome::status_result<LogRecordView> LogReader::record_view_at(uint64_t file_offset) {
    OUTCOME_TRY(auto mapped, mapped_bytes());
    if (file_offset >= mapped.size()) {
        return Error::invalid_file_offset;
    }

    const auto offset = static_cast<std::size_t>(file_offset);
    auto first = decode_block(mapped.subspan(offset));
    if (!first) {
        return std::move(first).as_failure();
    }

    const auto& first_block = first.value();
    if (first_block.kind == BlockKind::Standalone) {
        return LogRecordView{
            .counter = first_block.ts_counter,
            .meta_bytes = first_block.meta_bytes,
            .payload_bytes = first_block.payload_bytes,
        };
    }
    if (first_block.kind != BlockKind::GroupFirst || !first_block.group || first_block.group->index != 0) {
        return Error::corrupt_group;
    }

    grouped_payload_scratch_.clear();
    grouped_payload_scratch_.reserve(static_cast<std::size_t>(first_block.group->count) * 800);
    grouped_payload_scratch_.insert(
        grouped_payload_scratch_.end(),
        first_block.payload_bytes.begin(),
        first_block.payload_bytes.end());

    uint64_t next_offset = file_offset + first_block.total_size;
    for (uint16_t next_index = 1; next_index < first_block.group->count; ++next_index) {
        if (next_offset >= mapped.size()) {
            return Error::invalid_file_offset;
        }
        auto decoded = decode_block(mapped.subspan(static_cast<std::size_t>(next_offset)));
        if (!decoded) {
            return std::move(decoded).as_failure();
        }
        const auto& block = decoded.value();
        const bool is_last = next_index + 1 == first_block.group->count;
        const bool kind_ok = is_last ? block.kind == BlockKind::GroupLast : block.kind == BlockKind::GroupMid;
        if (!kind_ok ||
            !block.group ||
            block.group->index != next_index ||
            block.group->count != first_block.group->count ||
            block.ts_counter != first_block.ts_counter ||
            block.random_id != first_block.random_id) {
            return Error::corrupt_group;
        }
        grouped_payload_scratch_.insert(
            grouped_payload_scratch_.end(),
            block.payload_bytes.begin(),
            block.payload_bytes.end());
        next_offset += block.total_size;
    }
    return LogRecordView{
        .counter = first_block.ts_counter,
        .meta_bytes = first_block.meta_bytes,
        .payload_bytes = {grouped_payload_scratch_.data(), grouped_payload_scratch_.size()},
    };
}

outcome::status_result<std::optional<LogRecordView>> LogReader::get_record_view(std::string_view key, LogReadMode mode) {
    if (mode == LogReadMode::Refresh) {
        OUTCOME_TRYV(refresh_index());
    }
    auto it = latest_by_key_.find(key);
    if (it == latest_by_key_.end()) {
        return std::optional<LogRecordView>{};
    }
    OUTCOME_TRY(auto record, record_view_at(it->second.file_offset));
    return std::optional<LogRecordView>{std::move(record)};
}

outcome::status_result<std::optional<std::span<const std::byte>>> LogReader::get_payload_view(std::string_view key) {
    OUTCOME_TRY(auto record, get_record_view(key, LogReadMode::Refresh));
    if (!record.has_value()) {
        return std::optional<std::span<const std::byte>>{};
    }
    return std::optional<std::span<const std::byte>>{record->payload_bytes};
}

}
