#include "log.hpp"
#include "crc32c.hpp"
#include "byte_io.hpp"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <outcome/try.hpp>

namespace borinkdb::log::file {

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

constexpr bool kind_is_known(uint8_t raw) noexcept {
    return raw >= static_cast<uint8_t>(BlockKind::Wait) && raw <= static_cast<uint8_t>(BlockKind::Group);
}

// Returns the first possible block boundary at or after start, or bytes.size()
// if none exists. Callers use this to resynchronize after corrupt bytes.
std::size_t find_magic(byteview bytes, std::size_t start) {
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
    return k == BlockKind::Group;
}

bool is_truncated_block_error(const outcome::status_result<DecodedBlock>& decoded) noexcept {
    return decoded.error().equivalent(system_error2::make_status_code(Error::truncated_block));
}

bool is_recoverable_block_error(const outcome::status_result<DecodedBlock>& decoded) noexcept {
    return decoded.error().equivalent(system_error2::make_status_code(Error::invalid_magic)) ||
           decoded.error().equivalent(system_error2::make_status_code(Error::invalid_block_length)) ||
           decoded.error().equivalent(system_error2::make_status_code(Error::invalid_block_kind)) ||
           decoded.error().equivalent(system_error2::make_status_code(Error::invalid_timestamp_length)) ||
           decoded.error().equivalent(system_error2::make_status_code(Error::malformed_block)) ||
           decoded.error().equivalent(system_error2::make_status_code(Error::checksum_mismatch));
}

struct CommitScanState {
    uint64_t our_ts;
    uint64_t our_id;
    std::optional<uint16_t> group_count;
    bool found_our_ts = false;
    CommitOutcome outcome = CommitOutcome::Lost;
    uint16_t matched_group_blocks = 0;
    uint64_t expected_next_offset = 0;

    void observe(uint64_t offset, const DecodedBlock& blk) {
        if (!found_our_ts) {
            observe_first_candidate(offset, blk);
            return;
        }
        observe_group_continuation(offset, blk);
    }

private:
    void observe_first_candidate(uint64_t offset, const DecodedBlock& blk) {
        if (blk.ts_counter != our_ts) {
            return;
        }

        found_our_ts = true;
        if (blk.random_id != our_id) {
            outcome = CommitOutcome::Lost;
            return;
        }
        if (!group_count.has_value()) {
            outcome = CommitOutcome::Success;
            return;
        }
        if (blk.kind == BlockKind::Group &&
            blk.group &&
            blk.group->index == 0 &&
            blk.group->count == *group_count) {
            matched_group_blocks = 1;
            expected_next_offset = offset + blk.total_size;
            outcome = (*group_count == 1) ? CommitOutcome::Success : CommitOutcome::Lost;
        }
    }

    void observe_group_continuation(uint64_t offset, const DecodedBlock& blk) {
        if (!group_count ||
            outcome != CommitOutcome::Lost ||
            matched_group_blocks == 0 ||
            matched_group_blocks >= *group_count) {
            return;
        }

        if (offset != expected_next_offset ||
            blk.ts_counter != our_ts ||
            blk.random_id != our_id ||
            blk.kind != BlockKind::Group ||
            !blk.group ||
            blk.group->index != matched_group_blocks ||
            blk.group->count != *group_count) {
            matched_group_blocks = 0;
            return;
        }

        ++matched_group_blocks;
        expected_next_offset = offset + blk.total_size;
        if (matched_group_blocks == *group_count) {
            outcome = CommitOutcome::Success;
        }
    }
};

outcome::status_result<byteview> mapped_bytes(llfio::mapped_file_handle& mapped_h) {
    OUTCOME_TRY(auto extent, mapped_h.update_map());
    if (extent == 0) {
        return byteview{};
    }
    const auto* p = reinterpret_cast<const std::byte*>(mapped_h.address());
    if (p == nullptr) {
        return Error::mapped_file_unavailable;
    }
    return byteview{p, static_cast<std::size_t>(extent)};
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
    if (grouped && (req.group->count == 0 || req.group->index >= req.group->count)) {
        return Error::invalid_group_info;
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

outcome::status_result<DecodedBlock> decode_block(byteview input) noexcept {
    constexpr std::size_t min_size =
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
        if (gi.count == 0 || gi.index >= gi.count) {
            return Error::malformed_block;
        }
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
    byteview meta_bytes{p, meta_len};
    p += meta_len;

    const uint16_t payload_len = detail::read_u16_le(p);
    if (p + payload_len != end_excl_crc) {
        return Error::malformed_block;
    }
    byteview payload_bytes{p, payload_len};

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

outcome::status_result<std::unique_ptr<LogFile>> LogFile::open(std::string_view path) {
    const llfio::path_view path_view{path.data(), path.size(), llfio::path_view::not_zero_terminated};
    OUTCOME_TRY(auto mapped_h, llfio::mapped_file_handle::mapped_file(
        kReaderMapReservationBytes,
        {},
        path_view,
        llfio::mapped_file_handle::mode::read,
        llfio::mapped_file_handle::creation::if_needed,
        llfio::mapped_file_handle::caching::reads,
        llfio::mapped_file_handle::flag::disable_safety_barriers |
            llfio::mapped_file_handle::flag::maximum_prefetching,
        llfio::section_handle::flag::read));

    std::random_device rd;
    std::seed_seq seed{rd(), rd(), rd(), rd(),
                       static_cast<unsigned>(std::chrono::steady_clock::now().time_since_epoch().count())};
    auto log = std::unique_ptr<LogFile>(new LogFile(std::string{path}, std::move(mapped_h), std::mt19937_64(seed)));
    return log;
}

LogFile::LogFile(std::string path, llfio::mapped_file_handle mapped_h, std::mt19937_64 rng)
    : path_(std::move(path)), mapped_h_(std::move(mapped_h)), rng_(std::move(rng)) {}

outcome::status_result<uint64_t> LogFile::scan_range(byteview mapped,
                                               uint64_t start_offset,
                                               uint64_t end_offset,
                                               const BlockVisitor& visit,
                                               const CorruptRangeVisitor& on_corrupt) {
    end_offset = std::min<uint64_t>(end_offset, mapped.size());
    if (start_offset >= end_offset) {
        return start_offset;
    }

    const auto range_start = static_cast<std::size_t>(start_offset);
    const auto range_size = static_cast<std::size_t>(end_offset - start_offset);
    const byteview view = mapped.subspan(range_start, range_size);

    std::size_t pos = 0;
    while (pos < view.size()) {
        auto decoded = decode_block(view.subspan(pos));
        if (!decoded) {
            if (is_truncated_block_error(decoded)) {
                break;
            }
            if (!is_recoverable_block_error(decoded)) {
                return std::move(decoded).as_failure();
            }
            if (on_corrupt) {
                on_corrupt();
            }
            const std::size_t next = find_magic(view, pos + 1);
            if (next == view.size()) {
                pos = view.size();
                break;
            }
            pos = next;
            continue;
        }
        visit(start_offset + pos, decoded.value());
        pos += decoded.value().total_size;
    }

    return start_offset + pos;
}

outcome::status_result<void> LogFile::ensure_append_handle() {
    if (append_h_.has_value()) {
        return outcome::success();
    }

    const llfio::path_view path_view{path_.data(), path_.size(), llfio::path_view::not_zero_terminated};
    OUTCOME_TRY(auto append_h, llfio::file_handle::file(
        {},
        path_view,
        llfio::file_handle::mode::append,
        llfio::file_handle::creation::if_needed,
        llfio::file_handle::caching::all));
    append_h_.emplace(std::move(append_h));
    return outcome::success();
}

outcome::status_result<CommitOutcome> LogFile::resolve_outcome(uint64_t our_ts,
                                                        uint64_t our_id,
                                                        std::optional<uint16_t> group_count) {
    OUTCOME_TRY(auto mapped, borinkdb::log::file::mapped_bytes(mapped_h_));
    const uint64_t end = mapped.size();
    if (end <= scanned_through_offset_) {
        return CommitOutcome::Lost;
    }

    CommitScanState state{
        .our_ts = our_ts,
        .our_id = our_id,
        .group_count = group_count,
    };
    auto scan_result = scan_range(mapped, scanned_through_offset_, end,
                                  [&](uint64_t offset, const DecodedBlock& blk) {
                                      state.observe(offset, blk);
                                      latest_ts_ = std::max(latest_ts_, blk.ts_counter);
                                  });
    if (!scan_result) {
        return std::move(scan_result).as_failure();
    }
    return state.found_our_ts ? state.outcome : CommitOutcome::Lost;
}

outcome::status_result<void> LogFile::write_block(byteview bytes) {
    OUTCOME_TRYV(ensure_append_handle());
    llfio::file_handle::const_buffer_type write_buffer{bytes.data(), bytes.size()};
    llfio::file_handle::const_buffers_type write_buffers{&write_buffer, 1};
    OUTCOME_TRY(auto write_result, append_h_->write({write_buffers, 0}));

    std::size_t written = 0;
    for (const auto& returned_buffer : write_result) {
        written += returned_buffer.size();
    }
    if (written != bytes.size()) {
        return Error::short_write;
    }
    return outcome::success();
}

outcome::status_result<CommitResult> LogFile::commit_wait() {
    OUTCOME_TRYV(refresh_index());
    const uint64_t our_ts = latest_ts_ + 1;
    const uint64_t our_id = rng_();
    const byteview empty;
    OUTCOME_TRY(auto encoded, encode_block({
        .kind = BlockKind::Wait,
        .ts_counter = our_ts,
        .random_id = our_id,
        .group = std::nullopt,
        .key = {},
        .meta_bytes = empty,
        .payload_bytes = empty,
    }));
    OUTCOME_TRYV(write_block(encoded));
    OUTCOME_TRY(auto commit_outcome, resolve_outcome(our_ts, our_id, std::nullopt));
    OUTCOME_TRYV(refresh_index());
    return CommitResult{.outcome = commit_outcome, .ts_counter = our_ts, .random_id = our_id};
}

outcome::status_result<CommitResult> LogFile::commit_payload(
    std::string_view key,
    byteview meta_bytes,
    byteview payload_bytes) {
    OUTCOME_TRYV(refresh_index());
    const uint64_t our_ts = latest_ts_ + 1;
    const uint64_t our_id = rng_();

    auto single_probe = encode_block({
        .kind = BlockKind::Group,
        .ts_counter = our_ts,
        .random_id = our_id,
        .group = GroupInfo{.index = 0, .count = 1},
        .key = key,
        .meta_bytes = meta_bytes,
        .payload_bytes = payload_bytes,
    });

    if (single_probe) {
        OUTCOME_TRYV(write_block(single_probe.value()));
        OUTCOME_TRY(auto commit_outcome, resolve_outcome(our_ts, our_id, 1));
        OUTCOME_TRYV(refresh_index());
        return CommitResult{.outcome = commit_outcome, .ts_counter = our_ts, .random_id = our_id};
    }

    const bool should_group =
        single_probe.error().equivalent(system_error2::make_status_code(Error::block_too_large)) ||
        single_probe.error().equivalent(system_error2::make_status_code(Error::payload_too_large));
    if (!should_group) {
        return std::move(single_probe).as_failure();
    }

    constexpr std::size_t kChunkPayloadBytes = 800;
    const std::size_t count_sz = (payload_bytes.size() + kChunkPayloadBytes - 1) / kChunkPayloadBytes;
    if (count_sz == 0 || count_sz > 0xFFFFu) {
        return Error::payload_too_large;
    }
    const auto count = static_cast<uint16_t>(count_sz);

    for (uint16_t i = 0; i < count; ++i) {
        const std::size_t begin = static_cast<std::size_t>(i) * kChunkPayloadBytes;
        const std::size_t n = std::min(kChunkPayloadBytes, payload_bytes.size() - begin);
        const auto block_key = i == 0 ? key : std::string_view{};
        const auto meta = i == 0 ? meta_bytes : byteview{};
        OUTCOME_TRY(auto encoded, encode_block({
            .kind = BlockKind::Group,
            .ts_counter = our_ts,
            .random_id = our_id,
            .group = GroupInfo{.index = i, .count = count},
            .key = block_key,
            .meta_bytes = meta,
            .payload_bytes = payload_bytes.subspan(begin, n),
        }));
        OUTCOME_TRYV(write_block(encoded));
    }
    OUTCOME_TRY(auto commit_outcome, resolve_outcome(our_ts, our_id, count));
    OUTCOME_TRYV(refresh_index());
    return CommitResult{.outcome = commit_outcome, .ts_counter = our_ts, .random_id = our_id};
}

std::size_t LogFile::KeyHash::operator()(std::string_view s) const noexcept {
    return std::hash<std::string_view>{}(s);
}

std::string_view LogFile::KeyArena::store(std::string_view key) {
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

void LogFile::maybe_index_value(uint64_t file_offset,
                                  uint64_t ts_counter,
                                  std::string_view key) {
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

outcome::status_result<byteview> LogFile::mapped_bytes() {
    return borinkdb::log::file::mapped_bytes(mapped_h_);
}

void LogFile::index_block(uint64_t file_offset, const DecodedBlock& blk, PendingGroup& group) {
    latest_ts_ = std::max(latest_ts_, blk.ts_counter);

    if (blk.kind == BlockKind::Group && blk.group && blk.group->index == 0) {
        group.active = true;
        group.ts = blk.ts_counter;
        group.id = blk.random_id;
        group.first_offset = file_offset;
        group.expected_offset = file_offset + blk.total_size;
        group.next_index = 1;
        group.count = blk.group->count;
        group.key.assign(blk.key.data(), blk.key.size());
        if (group.count == 1) {
            maybe_index_value(group.first_offset, group.ts, group.key);
            group.active = false;
        }
        return;
    }

    if (group.active &&
        blk.kind == BlockKind::Group &&
        blk.group &&
        file_offset == group.expected_offset &&
        blk.ts_counter == group.ts &&
        blk.random_id == group.id &&
        blk.group->index == group.next_index &&
        blk.group->count == group.count) {
        const bool is_last = group.next_index + 1 == group.count;
        group.expected_offset = file_offset + blk.total_size;
        ++group.next_index;
        if (is_last) {
            maybe_index_value(group.first_offset, group.ts, group.key);
            group.active = false;
        }
        return;
    }

    group.active = false;
}

outcome::status_result<void> LogFile::refresh_index() {
    OUTCOME_TRY(auto mapped, mapped_bytes());
    const uint64_t end_offset = mapped.size();
    if (end_offset <= scanned_through_offset_) {
        return outcome::success();
    }

    PendingGroup group;
    OUTCOME_TRY(auto consumed, scan_range(
        mapped,
        scanned_through_offset_,
        end_offset,
        [&](uint64_t offset, const DecodedBlock& blk) {
            index_block(offset, blk, group);
        },
        [&]() {
            group.active = false;
        }));
    scanned_through_offset_ = consumed;
    return outcome::success();
}

outcome::status_result<void> LogFile::refresh() {
    return refresh_index();
}

outcome::status_result<LogRecordView> LogFile::record_view_at(uint64_t file_offset) {
    OUTCOME_TRY(auto mapped, mapped_bytes());
    if (file_offset >= mapped.size()) {
        return Error::invalid_file_offset;
    }

    const auto offset = static_cast<std::size_t>(file_offset);
    OUTCOME_TRY(auto first_block, decode_block(mapped.subspan(offset)));
    if (first_block.kind != BlockKind::Group || !first_block.group || first_block.group->index != 0) {
        return Error::corrupt_group;
    }

    if (first_block.group->count == 1) {
        return LogRecordView{
            .counter = first_block.ts_counter,
            .meta_bytes = first_block.meta_bytes,
            .payload_bytes = first_block.payload_bytes,
        };
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
        OUTCOME_TRY(auto block, decode_block(mapped.subspan(static_cast<std::size_t>(next_offset))));
        if (block.kind != BlockKind::Group ||
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

outcome::status_result<std::optional<LogRecordView>> LogFile::get_record_view(std::string_view key, LogReadMode mode) {
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

}
