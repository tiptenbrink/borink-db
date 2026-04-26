#pragma once

#include <cstddef>
#include <cstdint>
#include <span>

namespace borinkdb {

uint32_t crc32c(std::span<const std::byte> data) noexcept;

}
