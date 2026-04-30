#include "byte_io.hpp"

#include <cstring>

namespace borinkdb::detail {

std::size_t varuint_byte_length(uint64_t v) noexcept {
    std::size_t n = 1;
    for (uint64_t x = v >> 8; x != 0; x >>= 8) {
        ++n;
    }
    return n;
}

void write_u16_le(std::byte*& dst, uint16_t v) noexcept {
    dst[0] = static_cast<std::byte>(v & 0xFFu);
    dst[1] = static_cast<std::byte>((v >> 8) & 0xFFu);
    dst += 2;
}

void write_u32_le(std::byte*& dst, uint32_t v) noexcept {
    for (int i = 0; i < 4; ++i) {
        dst[i] = static_cast<std::byte>((v >> (8 * i)) & 0xFFu);
    }
    dst += 4;
}

void write_u64_le(std::byte*& dst, uint64_t v) noexcept {
    for (int i = 0; i < 8; ++i) {
        dst[i] = static_cast<std::byte>((v >> (8 * i)) & 0xFFu);
    }
    dst += 8;
}

void write_varuint_le(std::byte*& dst, uint64_t v, std::size_t byte_count) noexcept {
    for (std::size_t i = 0; i < byte_count; ++i) {
        dst[i] = static_cast<std::byte>((v >> (8 * i)) & 0xFFu);
    }
    dst += byte_count;
}

void write_bytes(std::byte*& dst, const void* src, std::size_t size) noexcept {
    if (size != 0) {
        std::memcpy(dst, src, size);
        dst += size;
    }
}

void write_bytes(std::byte*& dst, std::span<const std::byte> bytes) noexcept {
    write_bytes(dst, bytes.data(), bytes.size());
}

void write_bytes(std::byte*& dst, std::string_view bytes) noexcept {
    write_bytes(dst, bytes.data(), bytes.size());
}

uint16_t read_u16_le(const std::byte*& src) noexcept {
    const auto lo = static_cast<uint16_t>(static_cast<uint8_t>(src[0]));
    const auto hi = static_cast<uint16_t>(static_cast<uint16_t>(static_cast<uint8_t>(src[1])) << 8);
    const auto v = static_cast<uint16_t>(lo | hi);
    src += 2;
    return v;
}

uint32_t read_u32_le(const std::byte*& src) noexcept {
    uint32_t v = 0;
    for (int i = 0; i < 4; ++i) {
        v |= static_cast<uint32_t>(static_cast<uint8_t>(src[i])) << (8 * i);
    }
    src += 4;
    return v;
}

uint64_t read_u64_le(const std::byte*& src) noexcept {
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) {
        v |= static_cast<uint64_t>(static_cast<uint8_t>(src[i])) << (8 * i);
    }
    src += 8;
    return v;
}

uint64_t read_varuint_le(const std::byte*& src, std::size_t byte_count) noexcept {
    uint64_t v = 0;
    for (std::size_t i = 0; i < byte_count; ++i) {
        v |= static_cast<uint64_t>(static_cast<uint8_t>(src[i])) << (8 * i);
    }
    src += byte_count;
    return v;
}

}
