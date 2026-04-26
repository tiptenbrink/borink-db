#include "crc32c.hpp"

#include <array>
#include <cstring>

#if defined(__x86_64__) || defined(_M_X64)
#include <nmmintrin.h>
#endif

namespace borinkdb {

namespace {

constexpr std::array<uint32_t, 256> make_crc_table() {
    std::array<uint32_t, 256> t{};
    for (uint32_t i = 0; i < 256; ++i) {
        uint32_t c = i;
        for (int k = 0; k < 8; ++k) {
            c = (c >> 1) ^ (0x82F63B78u & -(c & 1u));
        }
        t[i] = c;
    }
    return t;
}

constexpr auto kCrcTable = make_crc_table();

uint32_t crc32c_software(std::span<const std::byte> data) noexcept {
    uint32_t crc = 0xFFFFFFFFu;
    for (std::byte b : data) {
        const uint32_t idx = (crc ^ static_cast<uint32_t>(static_cast<uint8_t>(b))) & 0xFFu;
        crc = kCrcTable[idx] ^ (crc >> 8);
    }
    return crc ^ 0xFFFFFFFFu;
}

#if defined(__x86_64__) || defined(_M_X64)
__attribute__((target("sse4.2")))
uint32_t crc32c_sse42(std::span<const std::byte> data) noexcept {
    uint64_t crc = 0xFFFFFFFFu;
    const auto* p = reinterpret_cast<const uint8_t*>(data.data());
    std::size_t n = data.size();

    while (n >= sizeof(uint64_t)) {
        uint64_t chunk = 0;
        std::memcpy(&chunk, p, sizeof(chunk));
        crc = _mm_crc32_u64(crc, chunk);
        p += sizeof(uint64_t);
        n -= sizeof(uint64_t);
    }
    while (n >= sizeof(uint32_t)) {
        uint32_t chunk = 0;
        std::memcpy(&chunk, p, sizeof(chunk));
        crc = _mm_crc32_u32(static_cast<uint32_t>(crc), chunk);
        p += sizeof(uint32_t);
        n -= sizeof(uint32_t);
    }
    while (n > 0) {
        crc = _mm_crc32_u8(static_cast<uint32_t>(crc), *p);
        ++p;
        --n;
    }
    return static_cast<uint32_t>(crc) ^ 0xFFFFFFFFu;
}

using CrcFn = uint32_t (*)(std::span<const std::byte>) noexcept;

CrcFn resolve_crc32c() noexcept {
#if defined(__GNUC__) || defined(__clang__)
    if (__builtin_cpu_supports("sse4.2")) {
        return crc32c_sse42;
    }
#endif
    return crc32c_software;
}
#endif

}

uint32_t crc32c(std::span<const std::byte> data) noexcept {
#if defined(__x86_64__) || defined(_M_X64)
    static const CrcFn fn = resolve_crc32c();
    return fn(data);
#else
    return crc32c_software(data);
#endif
}

}
