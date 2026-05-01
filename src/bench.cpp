#include "bench.hpp"

#include "log.hpp"
#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <limits>
#include <map>
#include <optional>
#include <set>
#include <random>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <outcome/experimental/status-code/include/status-code/iostream_support.hpp>
#include <outcome/try.hpp>
#include <rfl/msgpack.hpp>

namespace borinkdb::coordinator {
namespace outcome = OUTCOME_V2_NAMESPACE::experimental;
outcome::status_result<void> run_epoch_once(std::filesystem::path root,
                                            std::string epoch,
                                            std::size_t batch_limit,
                                            bool quiet);
}

namespace borinkdb::bench {
namespace {

namespace filelog = borinkdb::log::file;
namespace outcome = OUTCOME_V2_NAMESPACE::experimental;
using Clock = std::chrono::steady_clock;
using byteview = std::span<const std::byte>;
constexpr std::size_t kRetryHistogramBuckets = 256;
constexpr std::string_view kProposalKeyPrefix = "__borinkdb/proposal/";
constexpr std::string_view kFailedKey = "__borinkdb/tx/failed";

struct Options {
    std::chrono::milliseconds duration{1000};
    uint64_t target_per_writer_per_second = 200;
    uint64_t retry_backoff_us = 0;
    uint64_t retry_jitter_us = 0;
    std::string_view scenario_set = "default";
};

struct Scenario {
    std::string_view name;
    std::size_t processes;
    std::size_t threads_per_process;
    uint64_t target_per_writer_per_second;
};

struct Stats {
    uint64_t logical_successes = 0;
    uint64_t logical_key_writes = 0;
    uint64_t attempts = 0;
    uint64_t retries = 0;
    uint64_t errors = 0;
    uint64_t verified = 0;
    uint64_t verification_candidates = 0;
    uint64_t extra_physical_records = 0;
    uint64_t missing = 0;
    uint64_t corrupt = 0;
    uint64_t incomplete_transactions = 0;
    uint64_t physical_blocks = 0;
    uint64_t logical_blocks = 0;
    uint64_t transaction_block_refs = 0;
    uint64_t max_transactions_per_block = 0;
    uint64_t max_keys_per_block = 0;
    uint64_t latency_ns_total = 0;
    uint64_t latency_ns_max = 0;
    uint64_t retries_per_success_max = 0;
    std::array<uint64_t, kRetryHistogramBuckets> retries_per_success_hist{};
    std::vector<uint64_t> completed_retries;
    std::vector<uint64_t> completed_latency_ns;
};

std::vector<std::byte> bytes(std::string_view s) {
    const auto view = std::as_bytes(std::span{s.data(), s.size()});
    return {view.begin(), view.end()};
}

void record_completed_transaction(Stats& stats, uint64_t retries_for_write, uint64_t latency_ns) {
    ++stats.logical_successes;
    stats.retries_per_success_max = std::max(stats.retries_per_success_max, retries_for_write);
    const auto bucket = static_cast<std::size_t>(
        std::min<uint64_t>(retries_for_write, kRetryHistogramBuckets - 1));
    ++stats.retries_per_success_hist[bucket];
    stats.latency_ns_total += latency_ns;
    stats.latency_ns_max = std::max(stats.latency_ns_max, latency_ns);
    stats.completed_retries.push_back(retries_for_write);
    stats.completed_latency_ns.push_back(latency_ns);
}

std::string doc_key(std::size_t key_index) {
    return "doc-key-" + std::to_string(key_index);
}

std::string doc_tx_id(std::size_t process_index, uint64_t sequence) {
    return std::to_string(process_index) + ":" + std::to_string(sequence);
}

std::size_t choose_doc_key(std::mt19937_64& rng) {
    std::uniform_int_distribution<int> hotness{0, 99};
    const int bucket = hotness(rng);
    if (bucket < 30) {
        std::uniform_int_distribution<std::size_t> dist{0, 999};
        return dist(rng);
    }
    if (bucket < 80) {
        std::uniform_int_distribution<std::size_t> dist{1000, 9999};
        return dist(rng);
    }
    std::uniform_int_distribution<std::size_t> dist{10000, 99999};
    return dist(rng);
}

std::size_t choose_doc_transaction_key_count(std::mt19937_64& rng) {
    std::uniform_int_distribution<int> one_key{0, 99};
    if (one_key(rng) < 80) {
        return 1;
    }

    // The non-single-key tail is intentionally simple and bounded. It gives the
    // benchmark multi-key transactions without making rare huge transactions
    // dominate every short run.
    std::uniform_int_distribution<std::size_t> dist{2, 20};
    return dist(rng);
}

struct BenchMsgpackWrite {
    std::string key;
    std::string op;
    std::vector<uint8_t> meta;
    std::vector<uint8_t> payload;
};

struct BenchMsgpackProposal {
    uint64_t last_observed_transaction = 0;
    std::vector<std::string> read_set;
    std::vector<BenchMsgpackWrite> writes;
};

std::vector<std::byte> bench_msgpack_bytes(const BenchMsgpackProposal& proposal) {
    const auto raw = rfl::msgpack::write(proposal);
    const auto view = std::as_bytes(std::span{raw.data(), raw.size()});
    return {view.begin(), view.end()};
}

bool bytes_equal(byteview a, byteview b) {
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

std::optional<uint64_t> parse_u64(std::string_view value);

struct DocMeta {
    std::string tx_id;
    std::size_t key_index = 0;
    std::size_t key_count = 0;
};

std::optional<DocMeta> parse_doc_meta(byteview bytes) {
    const std::string_view text{reinterpret_cast<const char*>(bytes.data()), bytes.size()};
    constexpr std::string_view prefix = "doc-tx:";
    if (text.rfind(prefix, 0) != 0) {
        return std::nullopt;
    }

    auto rest = text.substr(prefix.size());
    const auto first = rest.find(':');
    if (first == std::string_view::npos) {
        return std::nullopt;
    }
    const auto second = rest.find(':', first + 1);
    if (second == std::string_view::npos) {
        return std::nullopt;
    }
    const auto third = rest.find(':', second + 1);
    if (third == std::string_view::npos) {
        return std::nullopt;
    }

    const auto tx_id = std::string{rest.substr(0, second)};
    const auto key_index = parse_u64(rest.substr(second + 1, third - second - 1));
    const auto key_count = parse_u64(rest.substr(third + 1));
    if (!key_index || !key_count || *key_count == 0 || *key_index >= *key_count) {
        return std::nullopt;
    }
    return DocMeta{
        .tx_id = tx_id,
        .key_index = static_cast<std::size_t>(*key_index),
        .key_count = static_cast<std::size_t>(*key_count),
    };
}

outcome::status_result<Stats> verify_proposal_scenario(std::string_view canonical_path) {
    OUTCOME_TRY(auto reader, filelog::LogFile::open(canonical_path));

    struct SeenTransaction {
        std::size_t expected_key_count = 0;
        std::set<std::size_t> seen_key_indexes;
        bool corrupt = false;
    };
    struct SeenBlock {
        uint64_t key_records = 0;
        std::set<std::string> transaction_ids;
    };

    Stats verification;
    std::map<std::string, SeenTransaction> transactions;
    std::set<std::pair<uint64_t, uint64_t>> failed_transactions;
    std::map<std::tuple<uint64_t, uint64_t, uint16_t>, SeenBlock> blocks;
    std::set<std::pair<uint64_t, uint64_t>> logical_blocks;

    OUTCOME_TRYV(reader->visit_records([&](std::string_view key, const filelog::LogRecordView& record) {
        if (key == kFailedKey) {
            failed_transactions.insert({record.transaction_ref_hi(), record.transaction_ref_lo()});
            return;
        }

        constexpr std::string_view key_prefix = "doc-key-";
        if (key.rfind(key_prefix, 0) != 0) {
            return;
        }

        const auto meta = parse_doc_meta(record.meta_bytes());
        if (!meta) {
            ++verification.corrupt;
            return;
        }
        const auto expected_payload = bytes("doc-payload:" + meta->tx_id);
        if (!bytes_equal_blocks(record.payload_blocks(), expected_payload)) {
            ++verification.corrupt;
            return;
        }

        auto& tx = transactions[meta->tx_id];
        if (tx.expected_key_count == 0) {
            tx.expected_key_count = meta->key_count;
        } else if (tx.expected_key_count != meta->key_count) {
            tx.corrupt = true;
        }
        if (!tx.seen_key_indexes.insert(meta->key_index).second) {
            tx.corrupt = true;
        }

        auto& block = blocks[{record.id_hi(), record.id_lo(), record.group_index()}];
        ++block.key_records;
        block.transaction_ids.insert(meta->tx_id);
        logical_blocks.insert({record.id_hi(), record.id_lo()});
        ++verification.verified;
    }));

    verification.logical_successes = transactions.size();
    verification.logical_key_writes = verification.verified;
    verification.retries = failed_transactions.size();
    verification.attempts = verification.logical_successes + verification.retries;
    verification.retries_per_success_hist[0] = verification.logical_successes;
    verification.verification_candidates = verification.verified;
    for (const auto& [_, tx] : transactions) {
        if (tx.corrupt) {
            ++verification.corrupt;
        }
        if (tx.seen_key_indexes.size() != tx.expected_key_count) {
            ++verification.incomplete_transactions;
            if (tx.seen_key_indexes.size() < tx.expected_key_count) {
                verification.missing += tx.expected_key_count - tx.seen_key_indexes.size();
            } else {
                verification.extra_physical_records += tx.seen_key_indexes.size() - tx.expected_key_count;
            }
        }
    }
    verification.physical_blocks = blocks.size();
    verification.logical_blocks = logical_blocks.size();
    for (const auto& [_, block] : blocks) {
        verification.transaction_block_refs += block.transaction_ids.size();
        verification.max_transactions_per_block =
            std::max<uint64_t>(verification.max_transactions_per_block, block.transaction_ids.size());
        verification.max_keys_per_block =
            std::max<uint64_t>(verification.max_keys_per_block, block.key_records);
    }
    return verification;
}

struct CanonicalResult {
    bool success = false;
};

struct CanonicalSnapshot {
    uint64_t version = 0;
    std::unordered_map<std::string, CanonicalResult> proposal_results;
};

std::string proposal_tx_key(filelog::CommitResult id) {
    return std::to_string(id.id_hi) + ":" + std::to_string(id.id_lo);
}

outcome::status_result<CanonicalSnapshot> scan_canonical(std::string_view canonical_path) {
    CanonicalSnapshot snapshot;
    OUTCOME_TRY(auto reader, filelog::LogFile::open(canonical_path));
    OUTCOME_TRYV(reader->visit_records([&](std::string_view key, const filelog::LogRecordView& record) {
        snapshot.version = std::max(snapshot.version, record.id_hi());
        if (record.transaction_ref_hi() == 0 && record.transaction_ref_lo() == 0) {
            return;
        }

        const auto tx = proposal_tx_key(filelog::CommitResult{
            .id_hi = record.transaction_ref_hi(),
            .id_lo = record.transaction_ref_lo(),
        });
        snapshot.proposal_results[tx] = CanonicalResult{.success = key != kFailedKey};
    }));
    return snapshot;
}

struct PendingTransaction {
    std::size_t writer_index = 0;
    uint64_t logical_sequence = 0;
    Clock::time_point started{};
    std::vector<std::size_t> key_indexes;
    std::optional<filelog::CommitResult> in_flight;
    uint64_t retries = 0;
};

struct ProposalWriter {
    std::unique_ptr<filelog::LogFile> log;
    std::deque<PendingTransaction> pending;
    uint64_t next_logical_sequence = 0;
    uint64_t next_proposal_sequence = 1;
    std::mt19937_64 rng;
};

std::vector<std::size_t> choose_transaction_keys(std::mt19937_64& rng) {
    std::set<std::size_t> unique_keys;
    const auto desired_key_count = choose_doc_transaction_key_count(rng);
    while (unique_keys.size() < desired_key_count) {
        unique_keys.insert(choose_doc_key(rng));
    }
    return {unique_keys.begin(), unique_keys.end()};
}

filelog::CommitResult next_proposal_id(ProposalWriter& writer, std::size_t writer_index) {
    return filelog::CommitResult{
        .id_hi = (static_cast<uint64_t>(writer_index) << 32U) | writer.next_proposal_sequence++,
        .id_lo = 0xBDBDBDBD00000000ull | static_cast<uint64_t>(writer_index),
    };
}

BenchMsgpackProposal make_bench_proposal(const PendingTransaction& tx, uint64_t observed_version) {
    const auto tx_id = doc_tx_id(tx.writer_index, tx.logical_sequence);
    BenchMsgpackProposal proposal{
        .last_observed_transaction = observed_version,
        .read_set = {},
        .writes = {},
    };
    proposal.read_set.reserve(tx.key_indexes.size());
    proposal.writes.reserve(tx.key_indexes.size());
    for (const auto key_index : tx.key_indexes) {
        const auto key = doc_key(key_index);
        proposal.read_set.push_back(key);
        const auto meta_text = "doc-tx:" + tx_id + ":" +
                               std::to_string(proposal.writes.size()) + ":" +
                               std::to_string(tx.key_indexes.size());
        const auto payload_text = "doc-payload:" + tx_id;
        proposal.writes.push_back(BenchMsgpackWrite{
            .key = key,
            .op = "put_if",
            .meta = {meta_text.begin(), meta_text.end()},
            .payload = {payload_text.begin(), payload_text.end()},
        });
    }
    return proposal;
}

void enqueue_new_transactions(ProposalWriter& writer,
                              std::size_t writer_index,
                              std::size_t count,
                              Clock::time_point now) {
    for (std::size_t i = 0; i < count; ++i) {
        writer.pending.push_back(PendingTransaction{
            .writer_index = writer_index,
            .logical_sequence = writer.next_logical_sequence++,
            .started = now,
            .key_indexes = choose_transaction_keys(writer.rng),
            .in_flight = std::nullopt,
            .retries = 0,
        });
    }
}

outcome::status_result<void> append_ready_proposals(ProposalWriter& writer,
                                                    std::size_t writer_index,
                                                    uint64_t observed_version,
                                                    Stats& stats) {
    std::vector<filelog::TransactionEntry> records;
    std::vector<std::string> keys;
    std::vector<std::vector<std::byte>> payloads;
    records.reserve(writer.pending.size());
    keys.reserve(writer.pending.size());
    payloads.reserve(writer.pending.size());

    for (auto& tx : writer.pending) {
        if (tx.in_flight.has_value()) {
            continue;
        }
        tx.in_flight = next_proposal_id(writer, writer_index);
        payloads.push_back(bench_msgpack_bytes(make_bench_proposal(tx, observed_version)));
        keys.push_back(std::string{kProposalKeyPrefix} + proposal_tx_key(*tx.in_flight));
        records.push_back(filelog::TransactionEntry{
            .key = keys.back(),
            .meta_bytes = {},
            .payload_bytes = payloads.back(),
        });
        ++stats.attempts;
    }

    if (!records.empty()) {
        OUTCOME_TRY(auto ignored, writer.log->commit_transaction(records));
        (void) ignored;
    }
    return outcome::success();
}

void apply_proposal_results(ProposalWriter& writer,
                            const CanonicalSnapshot& snapshot,
                            Clock::time_point now,
                            Stats& stats) {
    for (auto it = writer.pending.begin(); it != writer.pending.end();) {
        if (!it->in_flight.has_value()) {
            ++it;
            continue;
        }

        const auto result = snapshot.proposal_results.find(proposal_tx_key(*it->in_flight));
        if (result == snapshot.proposal_results.end()) {
            ++it;
            continue;
        }

        if (result->second.success) {
            const auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(now - it->started);
            stats.logical_key_writes += it->key_indexes.size();
            record_completed_transaction(stats, it->retries, static_cast<uint64_t>(elapsed.count()));
            it = writer.pending.erase(it);
        } else {
            ++stats.retries;
            ++it->retries;
            it->in_flight = std::nullopt;
            ++it;
        }
    }
}

bool has_pending_transactions(const std::vector<ProposalWriter>& writers) {
    return std::any_of(writers.begin(), writers.end(), [](const ProposalWriter& writer) {
        return !writer.pending.empty();
    });
}

outcome::status_result<Stats> run_scenario(const Scenario& scenario, const Options& options) {
    const auto root = std::filesystem::temp_directory_path() /
                      ("borinkdb-proposal-bench-" + std::to_string(Clock::now().time_since_epoch().count()));
    const std::string epoch = "epoch-bench";
    const auto proposals_dir = root / epoch / "proposals";
    const auto canonical_path = root / epoch / "canonical.log";
    std::filesystem::create_directories(proposals_dir);

    Stats stats;
    std::vector<ProposalWriter> writers;
    writers.reserve(scenario.processes);
    for (std::size_t writer_index = 0; writer_index < scenario.processes; ++writer_index) {
        OUTCOME_TRY(auto log, filelog::LogFile::open((proposals_dir / ("writer-" + std::to_string(writer_index) + ".log")).string()));
        writers.push_back(ProposalWriter{
            .log = std::move(log),
            .pending = {},
            .next_logical_sequence = 0,
            .next_proposal_sequence = 1,
            .rng = std::mt19937_64{
                static_cast<uint64_t>(writer_index + 1) * 0x9E3779B97F4A7C15ull ^
                static_cast<uint64_t>(options.duration.count())},
        });
    }

    uint64_t observed_version = 0;
    const auto end_time = Clock::now() + options.duration;
    auto next_batch = Clock::now();
    const auto per_writer_batch = static_cast<std::size_t>(
        std::max<uint64_t>(1, scenario.target_per_writer_per_second));

    while (Clock::now() < end_time || has_pending_transactions(writers)) {
        const auto generating = Clock::now() < end_time;
        if (generating) {
            next_batch += std::chrono::seconds{1};
            const auto now = Clock::now();
            for (std::size_t writer_index = 0; writer_index < writers.size(); ++writer_index) {
                enqueue_new_transactions(writers[writer_index], writer_index, per_writer_batch, now);
            }
        }

        for (std::size_t writer_index = 0; writer_index < writers.size(); ++writer_index) {
            OUTCOME_TRYV(append_ready_proposals(writers[writer_index], writer_index, observed_version, stats));
        }
        OUTCOME_TRYV(borinkdb::coordinator::run_epoch_once(root, epoch, 1000000, true));
        OUTCOME_TRY(auto snapshot, scan_canonical(canonical_path.string()));
        observed_version = snapshot.version;
        const auto now = Clock::now();
        for (auto& writer : writers) {
            apply_proposal_results(writer, snapshot, now, stats);
        }

        if (generating) {
            std::this_thread::sleep_until(next_batch);
        }
    }

    OUTCOME_TRY(auto verification, verify_proposal_scenario(canonical_path.string()));
    if (verification.logical_successes != stats.logical_successes ||
        verification.logical_key_writes != stats.logical_key_writes ||
        verification.retries != stats.retries) {
        ++stats.corrupt;
    }
    stats.verified = verification.verified;
    stats.verification_candidates = verification.verification_candidates;
    stats.extra_physical_records = verification.extra_physical_records;
    stats.missing = verification.missing;
    stats.corrupt += verification.corrupt;
    stats.incomplete_transactions = verification.incomplete_transactions;
    stats.physical_blocks = verification.physical_blocks;
    stats.logical_blocks = verification.logical_blocks;
    stats.transaction_block_refs = verification.transaction_block_refs;
    stats.max_transactions_per_block = verification.max_transactions_per_block;
    stats.max_keys_per_block = verification.max_keys_per_block;

    std::filesystem::remove_all(root);
    return stats;
}

double as_double(uint64_t value) {
    return static_cast<double>(value);
}

uint64_t retry_percentile(const Stats& stats, uint64_t percentile) {
    if (!stats.completed_retries.empty()) {
        auto values = stats.completed_retries;
        std::sort(values.begin(), values.end());
        const auto index = static_cast<std::size_t>(
            std::min<uint64_t>(values.size() - 1, (values.size() * percentile + 99u) / 100u - 1u));
        return values[index];
    }
    if (stats.logical_successes == 0) {
        return 0;
    }
    const uint64_t rank = (stats.logical_successes * percentile + 99u) / 100u;
    uint64_t cumulative = 0;
    for (std::size_t i = 0; i < stats.retries_per_success_hist.size(); ++i) {
        cumulative += stats.retries_per_success_hist[i];
        if (cumulative >= rank) {
            return static_cast<uint64_t>(i);
        }
    }
    return kRetryHistogramBuckets - 1;
}

uint64_t latency_percentile_us(const Stats& stats, uint64_t percentile) {
    if (stats.completed_latency_ns.empty()) {
        return 0;
    }
    auto values = stats.completed_latency_ns;
    std::sort(values.begin(), values.end());
    const auto index = static_cast<std::size_t>(
        std::min<uint64_t>(values.size() - 1, (values.size() * percentile + 99u) / 100u - 1u));
    return values[index] / 1000u;
}

void print_result(const Scenario& scenario, const Options& options, const Stats& stats) {
    const auto writers = scenario.processes * scenario.threads_per_process;
    const auto duration_seconds = static_cast<double>(options.duration.count()) / 1000.0;
    const auto successes_per_second = as_double(stats.logical_successes) / duration_seconds;
    const auto key_writes_per_second = as_double(stats.logical_key_writes) / duration_seconds;
    const auto attempts_per_second = as_double(stats.attempts) / duration_seconds;
    const auto retry_or_conflict_percent =
        stats.attempts == 0 ? 0.0 : (as_double(stats.retries) * 100.0 / as_double(stats.attempts));
    const auto avg_retries =
        stats.logical_successes == 0 ? 0.0 : as_double(stats.retries) / as_double(stats.logical_successes);
    const auto p50_retries = retry_percentile(stats, 50);
    const auto p90_retries = retry_percentile(stats, 90);
    const auto p95_retries = retry_percentile(stats, 95);
    const auto p99_retries = retry_percentile(stats, 99);
    const auto avg_latency_us =
        stats.logical_successes == 0 ? 0.0 : as_double(stats.latency_ns_total) / as_double(stats.logical_successes) / 1000.0;
    const auto max_latency_us = as_double(stats.latency_ns_max) / 1000.0;
    const auto p50_latency_us = latency_percentile_us(stats, 50);
    const auto p90_latency_us = latency_percentile_us(stats, 90);
    const auto p99_latency_us = latency_percentile_us(stats, 99);
    const auto avg_keys_per_tx =
        stats.logical_successes == 0 ? 0.0 : as_double(stats.logical_key_writes) / as_double(stats.logical_successes);
    const auto avg_keys_per_block =
        stats.physical_blocks == 0 ? 0.0 : as_double(stats.verified) / as_double(stats.physical_blocks);
    const auto avg_tx_per_block =
        stats.physical_blocks == 0 ? 0.0 : as_double(stats.transaction_block_refs) / as_double(stats.physical_blocks);
    const auto avg_physical_blocks_per_logical =
        stats.logical_blocks == 0 ? 0.0 : as_double(stats.physical_blocks) / as_double(stats.logical_blocks);

    std::cout
        << std::left << std::setw(28) << scenario.name
        << " proc=" << scenario.processes
        << " threads/proc=" << scenario.threads_per_process
        << " writers=" << writers
        << " target/writer/s=" << scenario.target_per_writer_per_second
        << " successful_writes/s=" << std::fixed << std::setprecision(1) << successes_per_second
        << " key_writes/s=" << key_writes_per_second
        << " avg_keys/tx=" << avg_keys_per_tx
        << " attempts/s=" << attempts_per_second;
    std::cout
        << " conflicts=" << stats.retries
        << " conflict%=" << retry_or_conflict_percent
        << " avg_retries/success=" << avg_retries
        << " p50_retries=" << p50_retries
        << " p90_retries=" << p90_retries
        << " p95_retries=" << p95_retries
        << " p99_retries=" << p99_retries
        << " pmax_retries=" << stats.retries_per_success_max
        << " avg_us=" << avg_latency_us
        << " p50_us=" << p50_latency_us
        << " p90_us=" << p90_latency_us
        << " p99_us=" << p99_latency_us
        << " pmax_us=" << max_latency_us;
    std::cout
        << " physical_blocks=" << stats.physical_blocks
        << " logical_blocks=" << stats.logical_blocks
        << " avg_physical/logical=" << avg_physical_blocks_per_logical
        << " avg_keys/block=" << avg_keys_per_block
        << " avg_tx/block=" << avg_tx_per_block
        << " max_keys/block=" << stats.max_keys_per_block
        << " max_tx/block=" << stats.max_transactions_per_block
        << " verified=" << stats.verified << "/" << stats.verification_candidates
        << " extra_physical=" << stats.extra_physical_records
        << " missing=" << stats.missing
        << " corrupt=" << stats.corrupt
        << " incomplete_tx=" << stats.incomplete_transactions
        << " errors=" << stats.errors
        << '\n';
}

std::optional<uint64_t> parse_u64(std::string_view value) {
    uint64_t result = 0;
    if (value.empty()) {
        return std::nullopt;
    }
    for (const char ch : value) {
        if (ch < '0' || ch > '9') {
            return std::nullopt;
        }
        const uint64_t digit = static_cast<uint64_t>(ch - '0');
        if (result > (std::numeric_limits<uint64_t>::max() - digit) / 10u) {
            return std::nullopt;
        }
        result = result * 10u + digit;
    }
    return result;
}

Options parse_options(int argc, char** argv) {
    Options options;
    for (int i = 1; i < argc; ++i) {
        const std::string_view arg{argv[i]};
        constexpr std::string_view duration_prefix = "--bench-duration-ms=";
        constexpr std::string_view rate_prefix = "--bench-rate-per-writer=";
        constexpr std::string_view backoff_prefix = "--bench-retry-backoff-us=";
        constexpr std::string_view jitter_prefix = "--bench-retry-jitter-us=";
        constexpr std::string_view scenario_set_prefix = "--bench-scenario-set=";
        if (arg.rfind(duration_prefix, 0) == 0) {
            if (auto value = parse_u64(arg.substr(duration_prefix.size()))) {
                options.duration = std::chrono::milliseconds{static_cast<long long>(*value)};
            }
        } else if (arg.rfind(rate_prefix, 0) == 0) {
            if (auto value = parse_u64(arg.substr(rate_prefix.size()))) {
                options.target_per_writer_per_second = *value;
            }
        } else if (arg.rfind(backoff_prefix, 0) == 0) {
            if (auto value = parse_u64(arg.substr(backoff_prefix.size()))) {
                options.retry_backoff_us = *value;
            }
        } else if (arg.rfind(jitter_prefix, 0) == 0) {
            if (auto value = parse_u64(arg.substr(jitter_prefix.size()))) {
                options.retry_jitter_us = *value;
            }
        } else if (arg.rfind(scenario_set_prefix, 0) == 0) {
            options.scenario_set = arg.substr(scenario_set_prefix.size());
        }
    }
    return options;
}

std::vector<Scenario> scenarios_for(const Options& options) {
    if (options.scenario_set == "custom") {
        return {
            Scenario{"proposal custom", 8, 1, options.target_per_writer_per_second},
        };
    }

    return {
        Scenario{"proposal 1 proc 25 tx/s", 1, 1, 25},
        Scenario{"proposal 4 proc 25 tx/s", 4, 1, 25},
        Scenario{"proposal 8 proc 25 tx/s", 8, 1, 25},
        Scenario{"proposal 8 proc 50 tx/s", 8, 1, 50},
    };
}

} // namespace

int run(int argc, char** argv) {
    const auto options = parse_options(argc, argv);
    const auto scenarios = scenarios_for(options);

    std::cout << "borink-db file log benchmarks\n"
              << "duration_ms=" << options.duration.count()
              << " scenario_set=" << options.scenario_set
              << " default_rate_per_writer=" << options.target_per_writer_per_second
              << " retry_backoff_us=" << options.retry_backoff_us
              << " retry_jitter_us=" << options.retry_jitter_us
              << "\n"
              << "proposal benchmarks retry coordinator conflicts until each logical transaction commits.\n"
              << "verification scans the log once after every scenario and checks all benchmark records.\n";

    for (const auto& scenario : scenarios) {
        auto stats = run_scenario(scenario, options);
        if (!stats) {
            std::cerr << "benchmark failed for " << scenario.name << ": "
                      << stats.error().message() << '\n';
            return 1;
        }
        print_result(scenario, options, stats.value());
    }

    return 0;
}

}

namespace borinkdb::coordinator {
namespace {

namespace filelog = borinkdb::log::file;
namespace outcome = OUTCOME_V2_NAMESPACE::experimental;
using byteview = std::span<const std::byte>;

constexpr std::string_view kProposalKeyPrefix = "__borinkdb/proposal/";
constexpr std::string_view kFailedKey = "__borinkdb/tx/failed";
constexpr std::string_view kFailedPayload = "failed";

struct CoordOptions {
    std::filesystem::path root = "borinkdb-coordinator";
    std::string epoch;
    std::chrono::milliseconds interval{1000};
    std::size_t batch_limit = 1024;
    bool once = false;
    bool quiet = false;
};

enum class Operation {
    PutIf,
    Overwrite,
};

struct OwnedEntry {
    std::string key;
    std::vector<std::byte> meta;
    std::vector<std::byte> payload;
};

struct MsgpackWrite {
    std::string key;
    std::string op;
    std::vector<uint8_t> meta;
    std::vector<uint8_t> payload;
};

struct MsgpackProposal {
    uint64_t last_observed_transaction = 0;
    std::vector<std::string> read_set;
    std::vector<MsgpackWrite> writes;
};

struct ProposedWrite {
    std::string key;
    Operation operation = Operation::Overwrite;
    std::vector<std::byte> meta;
    std::vector<std::byte> payload;
};

struct ProposedTransaction {
    filelog::CommitResult id;
    uint64_t last_observed_transaction = 0;
    std::vector<std::string> read_set;
    std::vector<ProposedWrite> writes;
};

struct ProposalLog {
    std::filesystem::path path;
    std::unique_ptr<filelog::LogFile> log;
};

std::optional<uint64_t> parse_coord_u64(std::string_view value) {
    uint64_t result = 0;
    if (value.empty()) {
        return std::nullopt;
    }
    for (const char ch : value) {
        if (ch < '0' || ch > '9') {
            return std::nullopt;
        }
        const uint64_t digit = static_cast<uint64_t>(ch - '0');
        if (result > (UINT64_MAX - digit) / 10u) {
            return std::nullopt;
        }
        result = result * 10u + digit;
    }
    return result;
}

std::vector<std::byte> coord_bytes(std::string_view text) {
    const auto view = std::as_bytes(std::span{text.data(), text.size()});
    return {view.begin(), view.end()};
}

std::optional<filelog::CommitResult> parse_tx_key(std::string_view key) {
    const auto colon = key.find(':');
    if (colon == std::string_view::npos) {
        return std::nullopt;
    }
    const auto id_hi = parse_coord_u64(key.substr(0, colon));
    const auto id_lo = parse_coord_u64(key.substr(colon + 1));
    if (!id_hi || !id_lo) {
        return std::nullopt;
    }
    return filelog::CommitResult{
        .id_hi = *id_hi,
        .id_lo = *id_lo,
    };
}

std::optional<ProposedTransaction> parse_proposal_record(std::string_view key,
                                                         const filelog::LogRecordView& record) {
    if (key.rfind(kProposalKeyPrefix, 0) != 0) {
        return std::nullopt;
    }
    auto tx_id = parse_tx_key(key.substr(kProposalKeyPrefix.size()));
    if (!tx_id) {
        return std::nullopt;
    }
    std::vector<std::byte> raw;
    for (const auto block : record.payload_blocks()) {
        raw.insert(raw.end(), block.begin(), block.end());
    }
    auto decoded = rfl::msgpack::read<MsgpackProposal>(raw);
    if (!decoded) {
        return std::nullopt;
    }
    auto msg = decoded.value();
    ProposedTransaction proposal{
        .id = *tx_id,
        .last_observed_transaction = msg.last_observed_transaction,
        .read_set = std::move(msg.read_set),
        .writes = {},
    };
    proposal.writes.reserve(msg.writes.size());
    for (auto& write : msg.writes) {
        Operation op;
        if (write.op == "put_if") {
            op = Operation::PutIf;
        } else if (write.op == "overwrite") {
            op = Operation::Overwrite;
        } else {
            return std::nullopt;
        }
        proposal.writes.push_back(ProposedWrite{
            .key = std::move(write.key),
            .operation = op,
            .meta = {reinterpret_cast<const std::byte*>(write.meta.data()),
                     reinterpret_cast<const std::byte*>(write.meta.data() + write.meta.size())},
            .payload = {reinterpret_cast<const std::byte*>(write.payload.data()),
                        reinterpret_cast<const std::byte*>(write.payload.data() + write.payload.size())},
        });
    }
    return proposal;
}

OwnedEntry failed_entry() {
    return OwnedEntry{
        .key = std::string{kFailedKey},
        .meta = {},
        .payload = coord_bytes(kFailedPayload),
    };
}

std::vector<filelog::TransactionEntry> as_transaction_entries(std::vector<OwnedEntry>& entries) {
    std::vector<filelog::TransactionEntry> out;
    out.reserve(entries.size());
    for (auto& entry : entries) {
        out.push_back(filelog::TransactionEntry{
            .key = entry.key,
            .meta_bytes = entry.meta,
            .payload_bytes = entry.payload,
        });
    }
    return out;
}

std::string make_epoch_name() {
    const auto now = std::chrono::system_clock::now().time_since_epoch();
    const auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(now).count();
    std::random_device rd;
    std::mt19937_64 rng{(static_cast<uint64_t>(rd()) << 32U) ^ static_cast<uint64_t>(millis)};
    return "epoch-" + std::to_string(millis) + "-" + std::to_string(rng());
}

CoordOptions parse_coord_options(int argc, char** argv) {
    CoordOptions options;
    for (int i = 1; i < argc; ++i) {
        const std::string_view arg{argv[i]};
        constexpr std::string_view root_prefix = "--coord-root=";
        constexpr std::string_view epoch_prefix = "--coord-epoch=";
        constexpr std::string_view interval_prefix = "--coord-interval-ms=";
        constexpr std::string_view batch_prefix = "--coord-batch-limit=";
        if (arg.rfind(root_prefix, 0) == 0) {
            options.root = std::filesystem::path{std::string{arg.substr(root_prefix.size())}};
        } else if (arg.rfind(epoch_prefix, 0) == 0) {
            options.epoch = std::string{arg.substr(epoch_prefix.size())};
        } else if (arg.rfind(interval_prefix, 0) == 0) {
            if (const auto parsed = parse_coord_u64(arg.substr(interval_prefix.size()))) {
                options.interval = std::chrono::milliseconds{static_cast<long long>(*parsed)};
            }
        } else if (arg.rfind(batch_prefix, 0) == 0) {
            if (const auto parsed = parse_coord_u64(arg.substr(batch_prefix.size()))) {
                options.batch_limit = static_cast<std::size_t>(*parsed);
            }
        } else if (arg == "--coord-once") {
            options.once = true;
        }
    }
    if (options.epoch.empty()) {
        options.epoch = make_epoch_name();
    }
    return options;
}

std::string tx_key(filelog::CommitResult id) {
    return std::to_string(id.id_hi) + ":" + std::to_string(id.id_lo);
}

outcome::status_result<void> load_seen(filelog::LogFile& canonical,
                                       std::unordered_set<std::string>& seen,
                                       uint64_t& next_canonical_id) {
    OUTCOME_TRYV(canonical.visit_records([&](std::string_view, const filelog::LogRecordView& record) {
        next_canonical_id = std::max(next_canonical_id, record.id_hi() + 1);
        if (record.transaction_ref_hi() != 0 || record.transaction_ref_lo() != 0) {
            seen.insert(tx_key(filelog::CommitResult{
                .id_hi = record.transaction_ref_hi(),
                .id_lo = record.transaction_ref_lo(),
            }));
        }
    }));
    return outcome::success();
}

outcome::status_result<std::vector<ProposedTransaction>> load_proposals(std::vector<ProposalLog*>& logs,
                                                                        const std::unordered_set<std::string>& seen,
                                                                        std::size_t batch_limit) {
    std::map<std::string, ProposedTransaction> by_tx;
    for (auto* proposal_log : logs) {
        OUTCOME_TRYV(proposal_log->log->refresh());
        OUTCOME_TRYV(proposal_log->log->visit_records([&](std::string_view key, const filelog::LogRecordView& record) {
            if (by_tx.size() >= batch_limit) {
                return;
            }
            auto proposal = parse_proposal_record(key, record);
            if (!proposal || seen.contains(tx_key(proposal->id))) {
                return;
            }
            by_tx.emplace(tx_key(proposal->id), std::move(*proposal));
        }));
    }

    std::vector<ProposedTransaction> out;
    out.reserve(by_tx.size());
    for (auto& [_, tx] : by_tx) {
        out.push_back(std::move(tx));
    }
    return out;
}

bool current_matches(filelog::LogFile& canonical,
                     const std::unordered_set<std::string>& overlay,
                     std::string_view key,
                     uint64_t last_observed_transaction) {
    if (overlay.contains(std::string{key})) {
        return false;
    }
    auto current = canonical.get_record_view(key, filelog::LogReadMode::Cached);
    if (!current) {
        return false;
    }
    if (!current.value().has_value()) {
        return true;
    }
    return current.value()->id_hi() <= last_observed_transaction;
}

outcome::status_result<std::size_t> process_batch(filelog::LogFile& canonical,
                                                  const std::vector<ProposedTransaction>& batch,
                                                  std::unordered_set<std::string>& seen,
                                                  uint64_t& next_canonical_id) {
    OUTCOME_TRYV(canonical.refresh());

    std::unordered_set<std::string> overlay;
    std::size_t finalized = 0;

    for (const auto& tx : batch) {
        const auto tx_id = tx_key(tx.id);
        if (tx.writes.empty() || seen.contains(tx_id)) {
            continue;
        }

        bool ok = true;
        std::unordered_set<std::string> write_keys;
        for (const auto& key : tx.read_set) {
            if (!current_matches(canonical, overlay, key, tx.last_observed_transaction)) {
                ok = false;
                break;
            }
        }
        for (const auto& write : tx.writes) {
            if (!write_keys.insert(write.key).second) {
                ok = false;
                break;
            }
        }

        std::vector<OwnedEntry> canonical_entries;
        if (ok) {
            for (const auto& entry : tx.writes) {
                canonical_entries.push_back(OwnedEntry{
                    .key = entry.key,
                    .meta = entry.meta,
                    .payload = entry.payload,
                });
                overlay.insert(entry.key);
            }
        } else {
            canonical_entries.push_back(failed_entry());
        }
        auto transaction_entries = as_transaction_entries(canonical_entries);
        OUTCOME_TRY(auto commit, canonical.commit_transaction(
            transaction_entries,
            filelog::CommitOptions{
                .id = filelog::CommitResult{.id_hi = next_canonical_id++, .id_lo = 0},
                .transaction_ref = tx.id,
            }));
        (void) commit;
        seen.insert(tx_id);
        ++finalized;
    }
    return finalized;
}

outcome::status_result<void> run_loop(const CoordOptions& options) {
    const auto epoch_dir = options.root / options.epoch;
    const auto proposals_dir = epoch_dir / "proposals";
    const auto canonical_path = (epoch_dir / "canonical.log").string();

    std::filesystem::create_directories(proposals_dir);
    OUTCOME_TRY(auto canonical, filelog::LogFile::open(canonical_path));

    std::unordered_set<std::string> seen;
    uint64_t next_canonical_id = 1;
    OUTCOME_TRYV(load_seen(*canonical, seen, next_canonical_id));

    std::map<std::filesystem::path, ProposalLog> proposal_logs;
    std::mt19937_64 rng{std::random_device{}()};

    if (!options.quiet) {
        std::cout << "coordinator epoch=" << epoch_dir.string()
                  << " proposals=" << proposals_dir.string()
                  << " canonical=" << canonical_path << '\n';
    }

    for (;;) {
        for (const auto& entry : std::filesystem::directory_iterator(proposals_dir)) {
            if (!entry.is_regular_file() || entry.path().extension() != ".log") {
                continue;
            }
            if (proposal_logs.contains(entry.path())) {
                continue;
            }
            auto opened = filelog::LogFile::open(entry.path().string());
            if (!opened) {
                std::cerr << "failed to open proposal log " << entry.path().string()
                          << ": " << opened.error().message() << '\n';
                continue;
            }
            proposal_logs.emplace(entry.path(), ProposalLog{
                                                    .path = entry.path(),
                                                    .log = std::move(opened.value()),
                                                });
        }

        std::vector<ProposalLog*> logs;
        logs.reserve(proposal_logs.size());
        for (auto& [_, log] : proposal_logs) {
            logs.push_back(&log);
        }
        std::shuffle(logs.begin(), logs.end(), rng);

        OUTCOME_TRY(auto proposals, load_proposals(logs, seen, options.batch_limit));
        if (!proposals.empty()) {
            OUTCOME_TRY(auto finalized, process_batch(*canonical, proposals, seen, next_canonical_id));
            if (!options.quiet) {
                std::cout << "coordinator finalized=" << finalized
                          << " seen=" << seen.size()
                          << " proposal_logs=" << proposal_logs.size() << '\n';
            }
        }

        if (options.once) {
            return outcome::success();
        }
        std::this_thread::sleep_for(options.interval);
    }
}

std::vector<std::byte> msgpack_bytes(const MsgpackProposal& proposal) {
    const auto raw = rfl::msgpack::write(proposal);
    const auto view = std::as_bytes(std::span{raw.data(), raw.size()});
    return {view.begin(), view.end()};
}

outcome::status_result<void> write_proposal(std::filesystem::path path,
                                            filelog::CommitResult tx_id,
                                            const MsgpackProposal& proposal) {
    OUTCOME_TRY(auto log, filelog::LogFile::open(path.string()));
    auto payload = msgpack_bytes(proposal);
    const auto key = std::string{kProposalKeyPrefix} + tx_key(tx_id);
    const filelog::TransactionEntry entry{
        .key = key,
        .meta_bytes = {},
        .payload_bytes = payload,
    };
    OUTCOME_TRY(auto commit, log->commit_transaction(
                                 std::span<const filelog::TransactionEntry>{&entry, 1},
                                 filelog::CommitOptions{.id = tx_id, .transaction_ref = {}}));
    (void) commit;
    return outcome::success();
}

outcome::status_result<bool> canonical_has_tx(filelog::LogFile& canonical,
                                              filelog::CommitResult tx_id,
                                              std::string_view expected_key,
                                              std::string_view expected_payload) {
    bool found = false;
    OUTCOME_TRYV(canonical.visit_records([&](std::string_view key, const filelog::LogRecordView& record) {
        if (record.transaction_ref_hi() != tx_id.id_hi || record.transaction_ref_lo() != tx_id.id_lo) {
            return;
        }
        if (key != expected_key) {
            return;
        }
        const auto expected = coord_bytes(expected_payload);
        if (record.payload_blocks().size() == 1 &&
            record.payload_blocks().front().size() == expected.size() &&
            std::equal(record.payload_blocks().front().begin(), record.payload_blocks().front().end(), expected.begin())) {
            found = true;
        }
    }));
    return found;
}

outcome::status_result<void> proposer_committer_integration_test() {
    const auto root = std::filesystem::temp_directory_path() /
                      ("borinkdb-coordinator-test-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    const std::string epoch = "epoch-test";
    const auto epoch_dir = root / epoch;
    const auto proposals_dir = epoch_dir / "proposals";
    std::filesystem::create_directories(proposals_dir);

    const auto canonical_path = epoch_dir / "canonical.log";
    OUTCOME_TRY(auto canonical, filelog::LogFile::open(canonical_path.string()));
    {
        auto meta = coord_bytes("seed");
        auto one = coord_bytes("1");
        auto five = coord_bytes("5");
        std::array<filelog::TransactionEntry, 2> seed{
            filelog::TransactionEntry{.key = "A", .meta_bytes = meta, .payload_bytes = one},
            filelog::TransactionEntry{.key = "B", .meta_bytes = meta, .payload_bytes = five},
        };
        OUTCOME_TRY(auto seed_commit, canonical->commit_transaction(
                                         seed,
                                         filelog::CommitOptions{.id = filelog::CommitResult{.id_hi = 25, .id_lo = 0}, .transaction_ref = {}}));
        (void) seed_commit;
    }

    const auto success_tx = filelog::CommitResult{.id_hi = 0xA, .id_lo = 0x1};
    const auto failed_tx = filelog::CommitResult{.id_hi = 0xA, .id_lo = 0x2};
    OUTCOME_TRYV(write_proposal(
        proposals_dir / "writer-success.log",
        success_tx,
        MsgpackProposal{
            .last_observed_transaction = 25,
            .read_set = {"A", "B"},
            .writes = {
                MsgpackWrite{.key = "A", .op = "put_if", .meta = {}, .payload = {static_cast<uint8_t>('7')}},
                MsgpackWrite{.key = "C", .op = "overwrite", .meta = {}, .payload = {static_cast<uint8_t>('1'), static_cast<uint8_t>('0')}},
                MsgpackWrite{.key = "D", .op = "overwrite", .meta = {}, .payload = {static_cast<uint8_t>('1'), static_cast<uint8_t>('1')}},
            },
        }));

    OUTCOME_TRYV(run_loop(CoordOptions{.root = root, .epoch = epoch, .interval = std::chrono::milliseconds{1}, .batch_limit = 16, .once = true}));
    OUTCOME_TRYV(canonical->refresh());
    OUTCOME_TRY(auto success_seen, canonical_has_tx(*canonical, success_tx, "A", "7"));
    if (!success_seen) {
        std::filesystem::remove_all(root);
        return filelog::Error::bad_argument;
    }
    std::filesystem::remove(proposals_dir / "writer-success.log");

    OUTCOME_TRYV(write_proposal(
        proposals_dir / "writer-fail.log",
        failed_tx,
        MsgpackProposal{
            .last_observed_transaction = 25,
            .read_set = {"A"},
            .writes = {
                MsgpackWrite{.key = "A", .op = "put_if", .meta = {}, .payload = {static_cast<uint8_t>('9'), static_cast<uint8_t>('9')}},
            },
        }));

    OUTCOME_TRYV(run_loop(CoordOptions{.root = root, .epoch = epoch, .interval = std::chrono::milliseconds{1}, .batch_limit = 16, .once = true}));
    OUTCOME_TRYV(canonical->refresh());
    OUTCOME_TRY(auto failure_seen, canonical_has_tx(*canonical, failed_tx, kFailedKey, kFailedPayload));
    if (!failure_seen) {
        std::filesystem::remove_all(root);
        return filelog::Error::bad_argument;
    }
    std::filesystem::remove(proposals_dir / "writer-fail.log");

    if (std::filesystem::exists(proposals_dir / "writer-success.log") ||
        std::filesystem::exists(proposals_dir / "writer-fail.log")) {
        std::filesystem::remove_all(root);
        return filelog::Error::bad_argument;
    }

    std::filesystem::remove_all(root);
    return outcome::success();
}

} // namespace

outcome::status_result<void> run_epoch_once(std::filesystem::path root,
                                            std::string epoch,
                                            std::size_t batch_limit,
                                            bool quiet) {
    return run_loop(CoordOptions{
        .root = std::move(root),
        .epoch = std::move(epoch),
        .interval = std::chrono::milliseconds{1},
        .batch_limit = batch_limit,
        .once = true,
        .quiet = quiet,
    });
}

outcome::status_result<void> run_integration_test() {
    return proposer_committer_integration_test();
}

int run(int argc, char** argv) {
    const auto options = parse_coord_options(argc, argv);
    auto result = run_loop(options);
    if (!result) {
        std::cerr << "coordinator failed: " << result.error().message() << '\n';
        return 1;
    }
    return 0;
}

}
