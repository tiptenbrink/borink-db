#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>

namespace borinkdb::detail {

std::size_t ts_byte_length(uint64_t v) noexcept;

void write_u16_le(std::byte*& dst, uint16_t v) noexcept;
void write_u32_le(std::byte*& dst, uint32_t v) noexcept;
void write_u64_le(std::byte*& dst, uint64_t v) noexcept;
void write_ts_le(std::byte*& dst, uint64_t v, std::size_t byte_count) noexcept;
void write_bytes(std::byte*& dst, const void* src, std::size_t size) noexcept;
void write_bytes(std::byte*& dst, std::span<const std::byte> bytes) noexcept;
void write_bytes(std::byte*& dst, std::string_view bytes) noexcept;

uint16_t read_u16_le(const std::byte*& src) noexcept;
uint32_t read_u32_le(const std::byte*& src) noexcept;
uint64_t read_u64_le(const std::byte*& src) noexcept;
uint64_t read_ts_le(const std::byte*& src, std::size_t byte_count) noexcept;

}
