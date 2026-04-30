#include "log.hpp"
#include "crc32c.hpp"
#include "byte_io.hpp"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <outcome/try.hpp>
#include <thread>

namespace borinkdb::log::file {

constexpr std::size_t kMagicSize = kBlockMagic.size();
constexpr std::size_t kTotalLenSize = 2;
constexpr std::size_t kReservedSize = 1;
constexpr std::size_t kFlagsSize = 1;
constexpr std::size_t kIdHiLenSize = 1;
constexpr std::size_t kIdLoSize = 8;
constexpr std::size_t kGroupFieldsSize = 4;
constexpr std::size_t kEntryCountSize = 2;
constexpr std::size_t kKeyLenSize = 2;
constexpr std::size_t kMetaLenSize = 2;
constexpr std::size_t kPayloadLenSize = 2;
constexpr std::size_t kCrcSize = 4;
constexpr std::size_t kMaxIdHiLen = 8;
constexpr std::size_t kEntryHeaderSize = kKeyLenSize + kMetaLenSize + kPayloadLenSize;

constexpr std::size_t kFixedHeaderPrefix =
    kMagicSize + kTotalLenSize + kReservedSize + kFlagsSize + kIdHiLenSize;

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

bool is_truncated_block_error(const outcome::status_result<DecodedBlock>& decoded) noexcept {
    return decoded.error().equivalent(system_error2::make_status_code(Error::truncated_block));
}

bool is_recoverable_block_error(const outcome::status_result<DecodedBlock>& decoded) noexcept {
    return decoded.error().equivalent(system_error2::make_status_code(Error::invalid_magic)) ||
           decoded.error().equivalent(system_error2::make_status_code(Error::invalid_block_length)) ||
           decoded.error().equivalent(system_error2::make_status_code(Error::invalid_id_length)) ||
           decoded.error().equivalent(system_error2::make_status_code(Error::malformed_block)) ||
           decoded.error().equivalent(system_error2::make_status_code(Error::checksum_mismatch));
}

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

LogRecordView::LogRecordView(uint64_t id_hi,
                             uint64_t id_lo,
                             uint16_t group_index,
                             byteview meta_bytes,
                             PayloadBlockList payload_blocks)
    : id_hi_(id_hi),
      id_lo_(id_lo),
      group_index_(group_index),
      meta_bytes_(meta_bytes),
      payload_blocks_(std::move(payload_blocks)) {}

void PayloadBlockList::reserve(std::size_t capacity) {
    if (capacity <= kInlineCapacity || !heap_.empty()) {
        if (!heap_.empty()) {
            heap_.reserve(capacity);
        }
        return;
    }

    heap_.reserve(capacity);
    heap_.insert(heap_.end(), inline_.begin(), inline_.begin() + static_cast<std::ptrdiff_t>(size_));
}

void PayloadBlockList::push_back(byteview block) {
    if (heap_.empty() && size_ < kInlineCapacity) {
        inline_[size_] = block;
    } else {
        if (heap_.empty()) {
            reserve(kInlineCapacity + 1);
        }
        heap_.push_back(block);
    }
    ++size_;
}

std::span<const byteview> PayloadBlockList::view() const noexcept {
    if (!heap_.empty()) {
        return {heap_.data(), heap_.size()};
    }
    return {inline_.data(), size_};
}

outcome::status_result<std::vector<std::byte>> encode_block(const EncodeRequest& req) {
    if (req.group.count == 0 || req.group.index >= req.group.count) {
        return Error::invalid_group_info;
    }

    const BlockEntry single_entry{
        .key = req.key,
        .meta_bytes = req.meta_bytes,
        .payload_bytes = req.payload_bytes,
    };
    const auto entries = req.entries.empty()
                             ? std::span<const BlockEntry>{&single_entry, 1}
                             : req.entries;
    if (entries.size() > 0xFFFFu) {
        return Error::too_many_group_blocks;
    }

    const std::size_t id_hi_len = detail::varuint_byte_length(req.id_hi);
    const std::size_t total =
        kFixedHeaderPrefix + id_hi_len + kIdLoSize + kGroupFieldsSize + kEntryCountSize +
        kCrcSize;
    std::size_t entries_size = 0;
    for (const auto& entry : entries) {
        if (entry.key.size() > 0xFFFFu) {
            return Error::key_too_large;
        }
        if (entry.meta_bytes.size() > 0xFFFFu) {
            return Error::metadata_too_large;
        }
        if (entry.payload_bytes.size() > 0xFFFFu) {
            return Error::payload_too_large;
        }
        entries_size += kEntryHeaderSize + entry.key.size() + entry.meta_bytes.size() + entry.payload_bytes.size();
    }
    const std::size_t block_total = total + entries_size;
    if (block_total >= kMaxBlockBytes) {
        return Error::block_too_large;
    }

    std::vector<std::byte> out(block_total);
    std::byte* p = out.data();

    detail::write_bytes(p, kBlockMagic.data(), kMagicSize);
    detail::write_u16_le(p, static_cast<uint16_t>(block_total));
    *p++ = std::byte{0};
    *p++ = std::byte{0};
    *p++ = static_cast<std::byte>(id_hi_len);
    detail::write_varuint_le(p, req.id_hi, id_hi_len);
    detail::write_u64_le(p, req.id_lo);
    detail::write_u16_le(p, req.group.index);
    detail::write_u16_le(p, req.group.count);

    detail::write_u16_le(p, static_cast<uint16_t>(entries.size()));
    for (const auto& entry : entries) {
        detail::write_u16_le(p, static_cast<uint16_t>(entry.key.size()));
        detail::write_bytes(p, entry.key);

        detail::write_u16_le(p, static_cast<uint16_t>(entry.meta_bytes.size()));
        detail::write_bytes(p, entry.meta_bytes);

        detail::write_u16_le(p, static_cast<uint16_t>(entry.payload_bytes.size()));
        detail::write_bytes(p, entry.payload_bytes);
    }

    detail::write_u32_le(p, crc32c({out.data(), block_total - kCrcSize}));
    return out;
}

outcome::status_result<DecodedBlock> decode_block(byteview input) {
    constexpr std::size_t min_size =
        kFixedHeaderPrefix + 1 + kIdLoSize + kGroupFieldsSize + kEntryCountSize + kEntryHeaderSize + kCrcSize;
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

    const uint8_t id_hi_len = static_cast<uint8_t>(input[kMagicSize + kTotalLenSize + kReservedSize + kFlagsSize]);
    if (id_hi_len < 1 || id_hi_len > kMaxIdHiLen) {
        return Error::invalid_id_length;
    }

    const std::byte* p = input.data() + kFixedHeaderPrefix;
    const std::byte* const end_excl_crc = input.data() + (total_len - kCrcSize);
    const std::size_t fixed_body =
        id_hi_len + kIdLoSize + kGroupFieldsSize + kEntryCountSize;
    if (p + fixed_body > end_excl_crc) {
        return Error::malformed_block;
    }

    const uint64_t id_hi = detail::read_varuint_le(p, id_hi_len);
    const uint64_t id_lo = detail::read_u64_le(p);

    GroupInfo group;
    group.index = detail::read_u16_le(p);
    group.count = detail::read_u16_le(p);
    if (group.count == 0 || group.index >= group.count) {
        return Error::malformed_block;
    }

    const uint16_t entry_count = detail::read_u16_le(p);
    if (entry_count == 0) {
        return Error::malformed_block;
    }

    std::vector<DecodedEntry> entries;
    entries.reserve(entry_count);
    for (uint16_t i = 0; i < entry_count; ++i) {
        if (p + kEntryHeaderSize > end_excl_crc) {
            return Error::malformed_block;
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
        if (p + payload_len > end_excl_crc) {
            return Error::malformed_block;
        }
        byteview payload_bytes{p, payload_len};
        p += payload_len;

        entries.push_back(DecodedEntry{
            .key = key,
            .meta_bytes = meta_bytes,
            .payload_bytes = payload_bytes,
        });
    }
    if (p != end_excl_crc) {
        return Error::malformed_block;
    }

    const std::byte* expected_crc_p = input.data() + (total_len - kCrcSize);
    const uint32_t expected_crc = detail::read_u32_le(expected_crc_p);
    const uint32_t actual_crc = crc32c({input.data(), static_cast<std::size_t>(total_len - kCrcSize)});
    if (expected_crc != actual_crc) {
        return Error::checksum_mismatch;
    }

    return DecodedBlock{
        .id_hi = id_hi,
        .id_lo = id_lo,
        .group = group,
        .key = entries.front().key,
        .meta_bytes = entries.front().meta_bytes,
        .payload_bytes = entries.front().payload_bytes,
        .entries = std::move(entries),
        .total_size = total_len,
    };
}

outcome::status_result<std::unique_ptr<FileWatcher>>
FileWatcher::start(std::string path, std::chrono::milliseconds poll_interval, Callback callback) {
    const llfio::path_view path_view{path.data(), path.size(), llfio::path_view::not_zero_terminated};
    OUTCOME_TRY(auto handle, llfio::file_handle::file(
        {},
        path_view,
        llfio::file_handle::mode::read,
        llfio::file_handle::creation::open_existing,
        llfio::file_handle::caching::reads));
    OUTCOME_TRY(auto initial, snapshot(handle));
    return std::unique_ptr<FileWatcher>(
        new FileWatcher(std::move(path), std::move(handle), initial, poll_interval, std::move(callback)));
}

FileWatcher::FileWatcher(std::string path,
                         llfio::file_handle handle,
                         Snapshot initial,
                         std::chrono::milliseconds poll_interval,
                         Callback callback)
    : path_(std::move(path)),
      handle_(std::move(handle)),
      last_(initial),
      poll_interval_(poll_interval),
      callback_(std::move(callback)),
      thread_([this] { run(); }) {}

FileWatcher::~FileWatcher() {
    stop_.store(true, std::memory_order_release);
    if (thread_.joinable()) {
        thread_.join();
    }
}

outcome::status_result<FileWatcher::Snapshot> FileWatcher::snapshot(llfio::file_handle& handle) {
    llfio::stat_t stat(nullptr);
    OUTCOME_TRY(auto filled, stat.fill(handle, llfio::stat_t::want::size | llfio::stat_t::want::mtim));
    (void) filled;
    return Snapshot{
        .size = stat.st_size,
        .modified = stat.st_mtim,
    };
}

void FileWatcher::run() {
    while (!stop_.load(std::memory_order_acquire)) {
        std::this_thread::sleep_for(poll_interval_);
        if (stop_.load(std::memory_order_acquire)) {
            break;
        }

        auto current = snapshot(handle_);
        if (!current) {
            continue;
        }
        if (current.value() != last_) {
            last_ = current.value();
            callback_();
        }
    }
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
    OUTCOME_TRY(auto watcher, FileWatcher::start(log->path_, std::chrono::milliseconds{50}, [log_ptr = log.get()] {
        log_ptr->refresh_from_watcher();
    }));
    log->watcher_ = std::move(watcher);
    return log;
}

LogFile::LogFile(std::string path, llfio::mapped_file_handle mapped_h, std::mt19937_64 rng)
    : path_(std::move(path)), mapped_h_(std::move(mapped_h)), rng_(std::move(rng)) {}

LogFile::~LogFile() = default;

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

outcome::status_result<CommitResult> LogFile::commit_transaction(std::span<const TransactionEntry> entries) {
    std::lock_guard lock(mutex_);
    if (entries.empty()) {
        return Error::bad_argument;
    }

    OUTCOME_TRYV(refresh_index());
    const uint64_t our_id_hi = rng_();
    const uint64_t our_id_lo = rng_();

    std::vector<std::vector<BlockEntry>> blocks;
    std::vector<BlockEntry> current_block;
    auto flush_current = [&] {
        if (!current_block.empty()) {
            blocks.push_back(std::move(current_block));
            current_block = {};
        }
    };
    auto fits_current_block = [&](const BlockEntry& entry) -> outcome::status_result<bool> {
        auto candidate = current_block;
        candidate.push_back(entry);
        auto encoded = encode_block({
            .id_hi = our_id_hi,
            .id_lo = our_id_lo,
            .group = GroupInfo{.index = 0, .count = 1},
            .key = {},
            .meta_bytes = {},
            .payload_bytes = {},
            .entries = candidate,
        });
        if (encoded) {
            return true;
        }
        if (encoded.error().equivalent(system_error2::make_status_code(Error::block_too_large))) {
            return false;
        }
        return std::move(encoded).as_failure();
    };
    auto add_packable_entry = [&](BlockEntry entry) -> outcome::status_result<void> {
        OUTCOME_TRY(auto fits, fits_current_block(entry));
        if (!fits) {
            flush_current();
            OUTCOME_TRY(auto fits_empty, fits_current_block(entry));
            if (!fits_empty) {
                return Error::block_too_large;
            }
        }
        current_block.push_back(entry);
        return outcome::success();
    };

    for (const auto& entry : entries) {
        const std::size_t chunks =
            std::max<std::size_t>(1, (entry.payload_bytes.size() + kGroupPayloadChunkBytes - 1) / kGroupPayloadChunkBytes);
        if (chunks > kMaxGroupBlocks || blocks.size() > kMaxGroupBlocks - chunks) {
            return Error::too_many_group_blocks;
        }
        if (chunks == 1) {
            OUTCOME_TRYV(add_packable_entry(BlockEntry{
                .key = entry.key,
                .meta_bytes = entry.meta_bytes,
                .payload_bytes = entry.payload_bytes,
            }));
            continue;
        }

        flush_current();
        for (std::size_t chunk = 0; chunk < chunks; ++chunk) {
            const std::size_t begin = chunk * kGroupPayloadChunkBytes;
            const std::size_t n = entry.payload_bytes.empty()
                                      ? 0
                                      : std::min(kGroupPayloadChunkBytes, entry.payload_bytes.size() - begin);
            BlockEntry block_entry{
                .key = chunk == 0 ? entry.key : std::string_view{},
                .meta_bytes = chunk == 0 ? entry.meta_bytes : byteview{},
                .payload_bytes = entry.payload_bytes.subspan(begin, n),
            };
            OUTCOME_TRY(auto fits_single, fits_current_block(block_entry));
            if (!fits_single) {
                return Error::block_too_large;
            }
            blocks.push_back(std::vector<BlockEntry>{block_entry});
        }
    }
    flush_current();

    if (blocks.empty() || blocks.size() > kMaxGroupBlocks) {
        return Error::too_many_group_blocks;
    }
    const auto count = static_cast<uint16_t>(blocks.size());

    for (uint16_t index = 0; index < count; ++index) {
        OUTCOME_TRY(auto encoded, encode_block({
            .id_hi = our_id_hi,
            .id_lo = our_id_lo,
            .group = GroupInfo{.index = index, .count = count},
            .key = {},
            .meta_bytes = {},
            .payload_bytes = {},
            .entries = blocks[index],
        }));
        OUTCOME_TRYV(write_block(encoded));
    }

    OUTCOME_TRYV(refresh_index());
    return CommitResult{.id_hi = our_id_hi, .id_lo = our_id_lo};
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
                                uint16_t entry_index,
                                LogicalBlockId id,
                                std::string_view key) {
    if (key.empty()) {
        return;
    }
    auto it = latest_by_key_.find(key);
    if (it != latest_by_key_.end()) {
        it->second.id = id;
        it->second.file_offset = file_offset;
        it->second.entry_index = entry_index;
        return;
    }

    const std::string_view stored_key = key_arena_.store(key);
    latest_by_key_.emplace(stored_key, IndexedValue{
        .id = id,
        .file_offset = file_offset,
        .entry_index = entry_index,
    });
}

outcome::status_result<byteview> LogFile::mapped_bytes() {
    return borinkdb::log::file::mapped_bytes(mapped_h_);
}

outcome::status_result<byteview> LogFile::current_mapped_bytes() const {
    const auto extent = mapped_h_.map().length();
    if (extent == 0) {
        return byteview{};
    }
    const auto* p = reinterpret_cast<const std::byte*>(mapped_h_.address());
    if (p == nullptr) {
        return Error::mapped_file_unavailable;
    }
    return byteview{p, static_cast<std::size_t>(extent)};
}

outcome::status_result<bool> LogFile::group_is_complete(byteview mapped,
                                                        uint64_t file_offset,
                                                        const DecodedBlock& first_block) {
    if (first_block.group.index != 0) {
        return false;
    }
    if (first_block.group.count == 1) {
        return true;
    }

    std::vector<bool> seen(first_block.group.count, false);
    seen[0] = true;
    uint16_t seen_count = 1;
    OUTCOME_TRYV(scan_range(
        mapped,
        file_offset + first_block.total_size,
        mapped.size(),
        [&](uint64_t, const DecodedBlock& block) {
            if (seen_count == first_block.group.count) {
                return;
            }
            if (block.id_hi == first_block.id_hi &&
                block.id_lo == first_block.id_lo &&
                block.group.count == first_block.group.count &&
                !seen[block.group.index]) {
                seen[block.group.index] = true;
                ++seen_count;
            }
        }));
    return seen_count == first_block.group.count;
}

outcome::status_result<void> LogFile::index_block(byteview mapped, uint64_t file_offset, const DecodedBlock& blk) {
    if (blk.group.index != 0) {
        return outcome::success();
    }

    OUTCOME_TRY(auto complete, group_is_complete(mapped, file_offset, blk));
    if (!complete) {
        return outcome::success();
    }

    const LogicalBlockId id{.hi = blk.id_hi, .lo = blk.id_lo};
    auto [_, inserted] = indexed_logical_blocks_.insert(id);
    if (!inserted) {
        return outcome::success();
    }
    for (uint16_t i = 0; i < blk.entries.size(); ++i) {
        if (!blk.entries[i].key.empty()) {
            maybe_index_value(file_offset, i, id, blk.entries[i].key);
        }
    }
    std::vector<bool> seen(blk.group.count, false);
    seen[0] = true;
    uint16_t seen_count = 1;
    OUTCOME_TRYV(scan_range(
        mapped,
        file_offset + blk.total_size,
        mapped.size(),
        [&](uint64_t offset, const DecodedBlock& block) {
            if (seen_count == blk.group.count) {
                return;
            }
            if (block.id_hi != blk.id_hi ||
                block.id_lo != blk.id_lo ||
                block.group.count != blk.group.count ||
                seen[block.group.index]) {
                return;
            }

            seen[block.group.index] = true;
            ++seen_count;
            for (uint16_t i = 0; i < block.entries.size(); ++i) {
                if (!block.entries[i].key.empty()) {
                    maybe_index_value(offset, i, id, block.entries[i].key);
                }
            }
        }
    ));
    return outcome::success();
}

outcome::status_result<void> LogFile::refresh_index() {
    OUTCOME_TRY(auto mapped, mapped_bytes());
    const uint64_t end_offset = mapped.size();
    if (end_offset <= scanned_through_offset_) {
        return outcome::success();
    }

    outcome::status_result<void> index_result = outcome::success();
    OUTCOME_TRY(auto consumed, scan_range(
        mapped,
        scanned_through_offset_,
        end_offset,
        [&](uint64_t offset, const DecodedBlock& blk) {
            if (!index_result) {
                return;
            }
            index_result = index_block(mapped, offset, blk);
        }));
    if (!index_result) {
        return index_result;
    }
    scanned_through_offset_ = consumed;
    return outcome::success();
}

outcome::status_result<void> LogFile::refresh() {
    std::lock_guard lock(mutex_);
    return refresh_index();
}

outcome::status_result<void> LogFile::visit_records(const RecordVisitor& visit) {
    std::lock_guard lock(mutex_);
    OUTCOME_TRYV(refresh_index());
    OUTCOME_TRY(auto mapped, current_mapped_bytes());

    outcome::status_result<void> visit_result = outcome::success();
    OUTCOME_TRYV(scan_range(
        mapped,
        0,
        mapped.size(),
        [&](uint64_t offset, const DecodedBlock& blk) {
            if (!visit_result) {
                return;
            }

            for (uint16_t i = 0; i < blk.entries.size(); ++i) {
                const auto& entry = blk.entries[i];
                if (entry.key.empty()) {
                    continue;
                }

                auto record = record_view_at(offset, i);
                if (!record) {
                    visit_result = std::move(record).as_failure();
                    return;
                }
                visit(entry.key, record.value());
            }
        }));
    return visit_result;
}

outcome::status_result<LogRecordView> LogFile::record_view_at(uint64_t file_offset, uint16_t entry_index) {
    OUTCOME_TRY(auto mapped, current_mapped_bytes());
    if (file_offset >= mapped.size()) {
        return Error::invalid_file_offset;
    }

    const auto offset = static_cast<std::size_t>(file_offset);
    OUTCOME_TRY(auto first_block, decode_block(mapped.subspan(offset)));
    if (entry_index >= first_block.entries.size()) {
        return Error::corrupt_group;
    }
    const auto& first_entry = first_block.entries[entry_index];
    if (first_entry.key.empty()) {
        return Error::corrupt_group;
    }

    PayloadBlockList payload_blocks;
    payload_blocks.reserve(first_block.group.count);
    payload_blocks.push_back(first_entry.payload_bytes);

    uint16_t next_index = static_cast<uint16_t>(first_block.group.index + 1);

    OUTCOME_TRYV(scan_range(
        mapped,
        file_offset + first_block.total_size,
        mapped.size(),
        [&](uint64_t, const DecodedBlock& block) {
            if (next_index >= first_block.group.count) {
                return;
            }
            if (block.id_hi != first_block.id_hi ||
                block.id_lo != first_block.id_lo ||
                block.group.count != first_block.group.count) {
                return;
            }
            if (block.entries.size() != 1 || !block.entries.front().key.empty()) {
                return;
            }
            if (block.group.index == next_index) {
                payload_blocks.push_back(block.entries.front().payload_bytes);
                ++next_index;
            }
        }));

    return LogRecordView(first_block.id_hi, first_block.id_lo, first_block.group.index, first_entry.meta_bytes, std::move(payload_blocks));
}

outcome::status_result<std::optional<LogRecordView>> LogFile::get_record_view(std::string_view key, LogReadMode mode) {
    std::lock_guard lock(mutex_);
    if (mode == LogReadMode::Refresh) {
        OUTCOME_TRYV(refresh_index());
    }
    auto it = latest_by_key_.find(key);
    if (it == latest_by_key_.end()) {
        return std::optional<LogRecordView>{};
    }
    OUTCOME_TRY(auto record, record_view_at(it->second.file_offset, it->second.entry_index));
    return std::optional<LogRecordView>{std::move(record)};
}

uint64_t LogFile::scanned_through() const {
    std::lock_guard lock(mutex_);
    return scanned_through_offset_;
}

void LogFile::refresh_from_watcher() noexcept {
    std::unique_lock lock(mutex_, std::try_to_lock);
    if (!lock.owns_lock()) {
        return;
    }
    (void) refresh_index();
}

}
