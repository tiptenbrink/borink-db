#include "bench.hpp"

#include "log.hpp"
#include "log_api.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <limits>
#include <mutex>
#include <optional>
#include <random>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <llfio/llfio.hpp>
#include <outcome/experimental/status-code/include/status-code/iostream_support.hpp>
#include <outcome/try.hpp>

#if !defined(_WIN32)
#include <sys/wait.h>
#include <unistd.h>
#endif

namespace borinkdb::bench {
namespace {

namespace bytelog = borinkdb::bytelog;
namespace filelog = borinkdb::log::file;
namespace llfio = LLFIO_V2_NAMESPACE;
namespace outcome = OUTCOME_V2_NAMESPACE::experimental;
using Clock = std::chrono::steady_clock;
using byteview = std::span<const std::byte>;
constexpr std::size_t kRetryHistogramBuckets = 256;

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
    uint64_t attempts = 0;
    uint64_t retries = 0;
    uint64_t errors = 0;
    uint64_t verified = 0;
    uint64_t verification_candidates = 0;
    uint64_t extra_physical_records = 0;
    uint64_t missing = 0;
    uint64_t corrupt = 0;
    uint64_t latency_ns_total = 0;
    uint64_t latency_ns_max = 0;
    uint64_t retries_per_success_max = 0;
    std::array<uint64_t, kRetryHistogramBuckets> retries_per_success_hist{};
};

std::vector<std::byte> bytes(std::string_view s) {
    const auto view = std::as_bytes(std::span{s.data(), s.size()});
    return {view.begin(), view.end()};
}

void add_stats(Stats& dst, const Stats& src) {
    dst.logical_successes += src.logical_successes;
    dst.attempts += src.attempts;
    dst.retries += src.retries;
    dst.errors += src.errors;
    dst.verified += src.verified;
    dst.verification_candidates += src.verification_candidates;
    dst.extra_physical_records += src.extra_physical_records;
    dst.missing += src.missing;
    dst.corrupt += src.corrupt;
    dst.latency_ns_total += src.latency_ns_total;
    dst.latency_ns_max = std::max(dst.latency_ns_max, src.latency_ns_max);
    dst.retries_per_success_max = std::max(dst.retries_per_success_max, src.retries_per_success_max);
    for (std::size_t i = 0; i < dst.retries_per_success_hist.size(); ++i) {
        dst.retries_per_success_hist[i] += src.retries_per_success_hist[i];
    }
}

void record_retries_per_success(Stats& stats, uint64_t retries_for_write) {
    stats.retries_per_success_max = std::max(stats.retries_per_success_max, retries_for_write);
    const auto bucket = static_cast<std::size_t>(
        std::min<uint64_t>(retries_for_write, kRetryHistogramBuckets - 1));
    ++stats.retries_per_success_hist[bucket];
}

std::string bench_key(std::size_t process_index, std::size_t thread_index, uint64_t sequence) {
    return "bench-" + std::to_string(process_index) + "-" +
           std::to_string(thread_index) + "-" + std::to_string(sequence);
}

std::string bench_count_key(std::size_t process_index, std::size_t thread_index) {
    return "bench-count-" + std::to_string(process_index) + "-" + std::to_string(thread_index);
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

outcome::status_result<std::string> make_temp_log_path() {
    OUTCOME_TRY(auto dir, llfio::path_discovery::storage_backed_temporary_files_directory().current_path());
    std::string path = dir.string();
    if (!path.empty() && path.back() != '/') {
        path.push_back('/');
    }
    path += "borinkdb-bench-";
    path += std::to_string(static_cast<unsigned long long>(
        Clock::now().time_since_epoch().count()));
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

Stats run_thread_worker(std::string_view path,
                        std::size_t process_index,
                        std::size_t thread_index,
                        Clock::time_point end_time,
                        uint64_t target_per_second,
                        const Options& options) {
    Stats stats;
    auto opened = bytelog::FileLog::open(path);
    if (!opened) {
        stats.errors = 1;
        return stats;
    }

    const auto payload = bytes("benchmark-payload");
    const auto meta = bytes("benchmark-meta");
    const std::optional<std::chrono::nanoseconds> interval =
        target_per_second == 0
            ? std::nullopt
            : std::optional<std::chrono::nanoseconds>{
                  std::chrono::nanoseconds{1000000000ll / static_cast<long long>(target_per_second)}};
    auto next_start = Clock::now();
    uint64_t sequence = 0;
    std::mt19937_64 jitter_rng{
        static_cast<uint64_t>(process_index * 0x9E3779B97F4A7C15ull) ^
        static_cast<uint64_t>(thread_index + 1)};
    std::uniform_int_distribution<uint64_t> jitter_dist{0, options.retry_jitter_us};

    while (Clock::now() < end_time) {
        if (interval.has_value()) {
            next_start += *interval;
            std::this_thread::sleep_until(next_start);
            if (Clock::now() >= end_time) {
                break;
            }
        }

        const std::string key = bench_key(process_index, thread_index, sequence);

        const auto started = Clock::now();
        uint64_t retries_for_write = 0;
        for (;;) {
            ++stats.attempts;
            auto put = opened.value()->put(key, meta, payload);
            if (!put) {
                ++stats.errors;
                break;
            }
            if (put.value().outcome == bytelog::PutOutcome::Success) {
                const auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - started);
                const auto ns = static_cast<uint64_t>(elapsed.count());
                ++stats.logical_successes;
                ++sequence;
                record_retries_per_success(stats, retries_for_write);
                stats.latency_ns_total += ns;
                stats.latency_ns_max = std::max(stats.latency_ns_max, ns);
                break;
            }

            ++stats.retries;
            ++retries_for_write;
            const uint64_t delay_us = options.retry_backoff_us +
                                      (options.retry_jitter_us == 0 ? 0 : jitter_dist(jitter_rng));
            if (delay_us == 0) {
                std::this_thread::yield();
            } else {
                std::this_thread::sleep_for(std::chrono::microseconds{static_cast<long long>(delay_us)});
            }
            if (Clock::now() >= end_time) {
                break;
            }
        }
    }

    // Verification needs exact per-writer counts. Store that as a normal log
    // record after the timed region, but do not include it in throughput stats.
    const auto count_payload = bytes(std::to_string(sequence));
    const auto count_key = bench_count_key(process_index, thread_index);
    for (int attempt = 0; attempt < 100; ++attempt) {
        auto put = opened.value()->put(count_key, byteview{}, count_payload);
        if (!put) {
            ++stats.errors;
            break;
        }
        if (put.value().outcome == bytelog::PutOutcome::Success) {
            break;
        }
        std::this_thread::yield();
    }

    return stats;
}

Stats run_process_workers(std::string_view path,
                          std::size_t process_index,
                          std::size_t threads_per_process,
                          std::chrono::milliseconds duration,
                          uint64_t target_per_writer_per_second,
                          const Options& options) {
    const auto end_time = Clock::now() + duration;
    std::mutex mutex;
    Stats combined;
    std::vector<std::thread> threads;
    threads.reserve(threads_per_process);

    for (std::size_t thread_index = 0; thread_index < threads_per_process; ++thread_index) {
        threads.emplace_back([&, thread_index] {
            auto stats = run_thread_worker(
                path,
                process_index,
                thread_index,
                end_time,
                target_per_writer_per_second,
                options);
            std::lock_guard lock(mutex);
            add_stats(combined, stats);
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
    return combined;
}

std::optional<uint64_t> parse_u64(std::string_view value);

struct ParsedBenchKey {
    std::size_t process_index = 0;
    std::size_t thread_index = 0;
    uint64_t sequence = 0;
};

struct ParsedCountKey {
    std::size_t process_index = 0;
    std::size_t thread_index = 0;
};

struct WorkerVerification {
    bool marker_seen = false;
    uint64_t marker_count = 0;
    std::vector<uint64_t> sequences;
};

std::optional<ParsedBenchKey> parse_bench_key(std::string_view key) {
    constexpr std::string_view prefix = "bench-";
    if (key.rfind(prefix, 0) != 0 || key.rfind("bench-count-", 0) == 0) {
        return std::nullopt;
    }

    key.remove_prefix(prefix.size());
    const auto first_dash = key.find('-');
    if (first_dash == std::string_view::npos) {
        return std::nullopt;
    }
    const auto second_dash = key.find('-', first_dash + 1);
    if (second_dash == std::string_view::npos) {
        return std::nullopt;
    }

    const auto process = parse_u64(key.substr(0, first_dash));
    const auto thread = parse_u64(key.substr(first_dash + 1, second_dash - first_dash - 1));
    const auto sequence = parse_u64(key.substr(second_dash + 1));
    if (!process || !thread || !sequence) {
        return std::nullopt;
    }
    return ParsedBenchKey{
        .process_index = static_cast<std::size_t>(*process),
        .thread_index = static_cast<std::size_t>(*thread),
        .sequence = *sequence,
    };
}

std::optional<ParsedCountKey> parse_count_key(std::string_view key) {
    constexpr std::string_view prefix = "bench-count-";
    if (key.rfind(prefix, 0) != 0) {
        return std::nullopt;
    }

    key.remove_prefix(prefix.size());
    const auto dash = key.find('-');
    if (dash == std::string_view::npos) {
        return std::nullopt;
    }

    const auto process = parse_u64(key.substr(0, dash));
    const auto thread = parse_u64(key.substr(dash + 1));
    if (!process || !thread) {
        return std::nullopt;
    }
    return ParsedCountKey{
        .process_index = static_cast<std::size_t>(*process),
        .thread_index = static_cast<std::size_t>(*thread),
    };
}

outcome::status_result<Stats> verify_scenario(std::string_view path, const Scenario& scenario) {
    OUTCOME_TRY(auto reader, filelog::LogFile::open(path));

    const auto expected_payload = bytes("benchmark-payload");
    Stats verification;
    std::vector<WorkerVerification> workers(scenario.processes * scenario.threads_per_process);

    auto worker_index = [&](std::size_t process_index, std::size_t thread_index) -> std::optional<std::size_t> {
        if (process_index >= scenario.processes || thread_index >= scenario.threads_per_process) {
            return std::nullopt;
        }
        return process_index * scenario.threads_per_process + thread_index;
    };

    OUTCOME_TRYV(reader->visit_records([&](std::string_view key, const filelog::LogRecordView& record) {
        if (const auto count_key = parse_count_key(key)) {
            const auto index = worker_index(count_key->process_index, count_key->thread_index);
            if (!index.has_value() || record.payload_blocks().size() != 1) {
                ++verification.corrupt;
                return;
            }
            const auto count_bytes = record.payload_blocks().front();
            const std::string_view count_text{
                reinterpret_cast<const char*>(count_bytes.data()),
                count_bytes.size(),
            };
            const auto parsed_count = parse_u64(count_text);
            if (!parsed_count.has_value()) {
                ++verification.corrupt;
                return;
            }

            auto& worker = workers[*index];
            worker.marker_seen = true;
            worker.marker_count = *parsed_count;
            return;
        }

        const auto parsed = parse_bench_key(key);
        if (!parsed.has_value()) {
            return;
        }
        const auto index = worker_index(parsed->process_index, parsed->thread_index);
        if (!index.has_value()) {
            ++verification.corrupt;
            return;
        }

        auto& worker = workers[*index];
        worker.sequences.push_back(parsed->sequence);
        ++verification.verified;
        if (!bytes_equal_blocks(record.payload_blocks(), expected_payload)) {
            ++verification.corrupt;
        }
    }));

    for (const auto& worker : workers) {
        if (!worker.marker_seen) {
            ++verification.missing;
            verification.verification_candidates += worker.sequences.size();
            continue;
        }

        verification.verification_candidates += worker.marker_count;
        auto sequences = worker.sequences;
        std::sort(sequences.begin(), sequences.end());

        uint64_t unique_expected_sequences = 0;
        std::optional<uint64_t> previous;
        for (const uint64_t sequence : sequences) {
            if (previous.has_value() && *previous == sequence) {
                ++verification.extra_physical_records;
                continue;
            }
            previous = sequence;
            if (sequence < worker.marker_count) {
                ++unique_expected_sequences;
            } else {
                ++verification.extra_physical_records;
            }
        }
        if (unique_expected_sequences < worker.marker_count) {
            verification.missing += worker.marker_count - unique_expected_sequences;
        }
    }
    return verification;
}

#if !defined(_WIN32)
bool write_all(int fd, const void* data, std::size_t size) {
    const auto* p = static_cast<const char*>(data);
    std::size_t written = 0;
    while (written < size) {
        const ssize_t n = write(fd, p + written, size - written);
        if (n <= 0) {
            return false;
        }
        written += static_cast<std::size_t>(n);
    }
    return true;
}

bool read_all(int fd, void* data, std::size_t size) {
    auto* p = static_cast<char*>(data);
    std::size_t read_bytes = 0;
    while (read_bytes < size) {
        const ssize_t n = read(fd, p + read_bytes, size - read_bytes);
        if (n <= 0) {
            return false;
        }
        read_bytes += static_cast<std::size_t>(n);
    }
    return true;
}
#endif

outcome::status_result<Stats> run_scenario(const Scenario& scenario, const Options& options) {
    OUTCOME_TRY(auto path, make_temp_log_path());
    {
        OUTCOME_TRY(auto created, bytelog::FileLog::open(path));
    }

#if defined(_WIN32)
    if (scenario.processes != 1) {
        OUTCOME_TRYV(unlink_path(path));
        return filelog::Error::bad_argument;
    }
    auto stats = run_process_workers(
        path,
        0,
        scenario.threads_per_process,
        options.duration,
        scenario.target_per_writer_per_second,
        options);
#else
    Stats stats;
    std::vector<pid_t> children;
    std::vector<int> read_fds;
    children.reserve(scenario.processes);
    read_fds.reserve(scenario.processes);

    for (std::size_t process_index = 0; process_index < scenario.processes; ++process_index) {
        int fds[2] = {-1, -1};
        if (pipe(fds) != 0) {
            OUTCOME_TRYV(unlink_path(path));
            return filelog::Error::bad_argument;
        }

        const pid_t pid = fork();
        if (pid < 0) {
            close(fds[0]);
            close(fds[1]);
            OUTCOME_TRYV(unlink_path(path));
            return filelog::Error::bad_argument;
        }
        if (pid == 0) {
            close(fds[0]);
            const auto child_stats = run_process_workers(
                path,
                process_index,
                scenario.threads_per_process,
                options.duration,
                scenario.target_per_writer_per_second,
                options);
            const bool wrote = write_all(fds[1], &child_stats, sizeof(child_stats));
            close(fds[1]);
            std::_Exit(wrote ? 0 : 1);
        }

        close(fds[1]);
        children.push_back(pid);
        read_fds.push_back(fds[0]);
    }

    for (int fd : read_fds) {
        Stats child_stats;
        if (!read_all(fd, &child_stats, sizeof(child_stats))) {
            ++stats.errors;
        } else {
            add_stats(stats, child_stats);
        }
        close(fd);
    }

    for (const pid_t pid : children) {
        int status = 0;
        if (waitpid(pid, &status, 0) < 0 || !WIFEXITED(status) || WEXITSTATUS(status) != 0) {
            ++stats.errors;
        }
    }
#endif

    OUTCOME_TRY(auto verification, verify_scenario(path, scenario));
    stats.verified = verification.verified;
    stats.verification_candidates = verification.verification_candidates;
    stats.extra_physical_records = verification.extra_physical_records;
    stats.missing = verification.missing;
    stats.corrupt = verification.corrupt;

    OUTCOME_TRYV(unlink_path(path));
    return stats;
}

double as_double(uint64_t value) {
    return static_cast<double>(value);
}

uint64_t retry_percentile(const Stats& stats, uint64_t percentile) {
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

void print_result(const Scenario& scenario, const Options& options, const Stats& stats) {
    const auto writers = scenario.processes * scenario.threads_per_process;
    const auto duration_seconds = static_cast<double>(options.duration.count()) / 1000.0;
    const auto successes_per_second = as_double(stats.logical_successes) / duration_seconds;
    const auto attempts_per_second = as_double(stats.attempts) / duration_seconds;
    const auto retry_percent =
        stats.attempts == 0 ? 0.0 : (as_double(stats.retries) * 100.0 / as_double(stats.attempts));
    const auto avg_retries =
        stats.logical_successes == 0 ? 0.0 : as_double(stats.retries) / as_double(stats.logical_successes);
    const auto p95_retries = retry_percentile(stats, 95);
    const auto p99_retries = retry_percentile(stats, 99);
    const auto avg_latency_us =
        stats.logical_successes == 0 ? 0.0 : as_double(stats.latency_ns_total) / as_double(stats.logical_successes) / 1000.0;
    const auto max_latency_us = as_double(stats.latency_ns_max) / 1000.0;

    std::cout
        << std::left << std::setw(28) << scenario.name
        << " proc=" << scenario.processes
        << " threads/proc=" << scenario.threads_per_process
        << " writers=" << writers
        << " target/writer/s=" << scenario.target_per_writer_per_second
        << " successful_writes/s=" << std::fixed << std::setprecision(1) << successes_per_second
        << " attempts/s=" << attempts_per_second
        << " retries=" << stats.retries
        << " retry%=" << retry_percent
        << " avg_retries/success=" << avg_retries
        << " p95_retries/success=" << p95_retries
        << " p99_retries/success=" << p99_retries
        << " max_retries/success=" << stats.retries_per_success_max
        << " avg_us=" << avg_latency_us
        << " max_us=" << max_latency_us
        << " verified=" << stats.verified << "/" << stats.verification_candidates
        << " extra_physical=" << stats.extra_physical_records
        << " missing=" << stats.missing
        << " corrupt=" << stats.corrupt
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
    if (options.scenario_set == "5000") {
        return {
            Scenario{"1 proc 1 thread 5000/s", 1, 1, 5000},
            Scenario{"2 proc 1 thread 5000/s", 2, 1, 5000},
            Scenario{"4 proc 1 thread 5000/s", 4, 1, 5000},
            Scenario{"8 proc 1 thread 5000/s", 8, 1, 5000},
            Scenario{"4 proc 2 threads 5000/s", 4, 2, 5000},
            Scenario{"8 proc 2 threads 5000/s", 8, 2, 5000},
        };
    }

    return {
        Scenario{"single writer max", 1, 1, 0},
        Scenario{"single proc 4 threads max", 1, 4, 0},
        Scenario{"single proc 4 threads 100/s", 1, 4, 100},
        Scenario{"single proc 4 threads 500/s", 1, 4, 500},
        Scenario{"single proc 4 threads custom", 1, 4, options.target_per_writer_per_second},
        Scenario{"2 proc 1 thread max", 2, 1, 0},
        Scenario{"4 proc 1 thread 100/s", 4, 1, 100},
        Scenario{"4 proc 1 thread 500/s", 4, 1, 500},
        Scenario{"4 proc 2 threads 100/s", 4, 2, 100},
        Scenario{"4 proc 2 threads 500/s", 4, 2, 500},
        Scenario{"4 proc 2 threads custom", 4, 2, options.target_per_writer_per_second},
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
              << "target/writer/s=0 means unlimited; retries are normal PutOutcome::Retry results.\n"
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
