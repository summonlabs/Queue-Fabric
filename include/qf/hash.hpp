// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>

namespace qf {

namespace detail {

constexpr std::array<std::uint32_t, 256> make_crc32c_table() noexcept {
  std::array<std::uint32_t, 256> table{};
  // Built through a pointer into the array so the index arithmetic is explicit
  // and provably in range for both compilers and static analyzers.
  std::uint32_t* const storage = table.data();
  for (std::size_t index = 0; index < table.size(); ++index) {
    std::uint32_t crc = static_cast<std::uint32_t>(index);
    for (int bit = 0; bit < 8; ++bit) {
      crc = ((crc & 1u) != 0u) ? (0x82F63B78u ^ (crc >> 1)) : (crc >> 1);
    }
    storage[index] = crc;
  }
  return table;
}

inline constexpr std::array<std::uint32_t, 256> kCrc32cTable = make_crc32c_table();

}  // namespace detail

/// CRC-32C (Castagnoli). Used for journal records, frames and durable state
/// integrity. Software implementation; no hardware dependency.
inline std::uint32_t crc32c_extend(std::uint32_t seed, const void* data, std::size_t size) noexcept {
  const auto* bytes = static_cast<const std::uint8_t*>(data);
  std::uint32_t crc = ~seed;
  for (std::size_t i = 0; i < size; ++i) {
    const std::size_t index = static_cast<std::size_t>((crc ^ bytes[i]) & 0xFFu);
    crc = detail::kCrc32cTable[index] ^ (crc >> 8);
  }
  return ~crc;
}

inline std::uint32_t crc32c_extend(std::uint32_t seed, std::span<const std::byte> data) noexcept {
  return crc32c_extend(seed, data.data(), data.size());
}

inline std::uint32_t crc32c(const void* data, std::size_t size) noexcept { return crc32c_extend(0u, data, size); }

inline std::uint32_t crc32c(std::span<const std::byte> data) noexcept { return crc32c_extend(0u, data); }

inline std::uint32_t crc32c(std::string_view text) noexcept { return crc32c(text.data(), text.size()); }

/// FNV-1a 64-bit. Used for structural fingerprints of requests and payloads.
inline std::uint64_t fnv1a64(const void* data, std::size_t size) noexcept {
  const auto* bytes = static_cast<const std::uint8_t*>(data);
  std::uint64_t hash = 0xCBF29CE484222325ull;
  for (std::size_t i = 0; i < size; ++i) {
    hash ^= bytes[i];
    hash *= 0x100000001B3ull;
  }
  return hash;
}

inline std::uint64_t fnv1a64(std::span<const std::byte> data) noexcept { return fnv1a64(data.data(), data.size()); }
inline std::uint64_t fnv1a64(std::string_view text) noexcept { return fnv1a64(text.data(), text.size()); }

/// Order-sensitive mixing used to build composite fingerprints.
constexpr void hash_mix(std::uint64_t& seed, std::uint64_t value) noexcept {
  seed ^= value + 0x9E3779B97F4A7C15ull + (seed << 6) + (seed >> 2);
}

inline void hash_mix(std::uint64_t& seed, std::string_view text) noexcept { hash_mix(seed, fnv1a64(text)); }

constexpr void hash_mix(std::uint64_t& seed, bool value) noexcept {
  hash_mix(seed, static_cast<std::uint64_t>(value ? 1u : 0u));
}

}  // namespace qf
