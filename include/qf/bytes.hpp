// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "qf/limits.hpp"
#include "qf/status.hpp"

namespace qf {

using Bytes = std::vector<std::byte>;

inline std::span<const std::byte> as_bytes(std::string_view text) noexcept {
  return {reinterpret_cast<const std::byte*>(text.data()), text.size()};
}

inline std::span<const std::byte> as_bytes(const void* data, std::size_t size) noexcept {
  return {static_cast<const std::byte*>(data), size};
}

/// Bounded little-endian writer. Never grows past a configured ceiling; the
/// first overflowing write latches a failure that the caller must observe.
class ByteWriter {
 public:
  explicit ByteWriter(std::size_t limit = limits::kMaxJournalRecordBytes) : limit_(limit) { buffer_.reserve(64); }

  [[nodiscard]] bool failed() const noexcept { return failed_; }
  [[nodiscard]] std::size_t size() const noexcept { return buffer_.size(); }
  [[nodiscard]] Bytes take() noexcept { return std::move(buffer_); }

  void reset() noexcept {
    buffer_.clear();
    failed_ = false;
  }

  void put_u8(std::uint8_t value) noexcept {
    const std::uint8_t raw = value;
    put_raw(&raw, 1);
  }

  void put_bool(bool value) noexcept { put_u8(value ? std::uint8_t{1} : std::uint8_t{0}); }

  void put_u16(std::uint16_t value) noexcept {
    std::uint8_t raw[2];
    for (int i = 0; i < 2; ++i) {
      raw[i] = static_cast<std::uint8_t>((value >> (8 * i)) & 0xFFu);
    }
    put_raw(raw, 2);
  }

  void put_u32(std::uint32_t value) noexcept {
    std::uint8_t raw[4];
    for (int i = 0; i < 4; ++i) {
      raw[i] = static_cast<std::uint8_t>((value >> (8 * i)) & 0xFFu);
    }
    put_raw(raw, 4);
  }

  void put_u64(std::uint64_t value) noexcept {
    std::uint8_t raw[8];
    for (int i = 0; i < 8; ++i) {
      raw[i] = static_cast<std::uint8_t>((value >> (8 * i)) & 0xFFu);
    }
    put_raw(raw, 8);
  }

  void put_i64(std::int64_t value) noexcept { put_u64(static_cast<std::uint64_t>(value)); }

  void put_raw(const void* data, std::size_t size) noexcept {
    if (failed_) {
      return;
    }
    if (size > limit_ || buffer_.size() > limit_ - size) {
      failed_ = true;
      return;
    }
    const auto* first = static_cast<const std::byte*>(data);
    buffer_.insert(buffer_.end(), first, first + size);
  }

  void put_string(std::string_view text) noexcept {
    if (text.size() > limits::kMaxNameChars) {
      failed_ = true;
      return;
    }
    put_u16(static_cast<std::uint16_t>(text.size()));
    put_raw(text.data(), text.size());
  }

  void put_bytes(std::span<const std::byte> data) noexcept {
    put_u32(static_cast<std::uint32_t>(data.size()));
    put_raw(data.data(), data.size());
  }

 private:
  Bytes buffer_;
  std::size_t limit_;
  bool failed_{false};
};

/// Bounded little-endian reader. Every accessor is bounds checked; a read past
/// the end latches a failure and returns a zero value, so a malformed frame can
/// never produce undefined behaviour or partially applied input.
class ByteReader {
 public:
  explicit ByteReader(std::span<const std::byte> data) noexcept : data_(data) {}

  [[nodiscard]] bool failed() const noexcept { return failed_; }
  [[nodiscard]] bool ok() const noexcept { return !failed_; }
  [[nodiscard]] std::size_t remaining() const noexcept { return data_.size() - position_; }
  [[nodiscard]] std::size_t position() const noexcept { return position_; }

  [[nodiscard]] std::uint8_t get_u8() noexcept {
    if (!ensure(1)) {
      return 0;
    }
    const auto value = static_cast<std::uint8_t>(data_[position_]);
    position_ += 1;
    return value;
  }

  [[nodiscard]] bool get_bool() noexcept { return get_u8() != 0u; }

  [[nodiscard]] std::uint16_t get_u16() noexcept {
    if (!ensure(2)) {
      return 0;
    }
    std::uint16_t value = 0;
    for (int i = 0; i < 2; ++i) {
      const auto byte = static_cast<std::uint16_t>(data_[position_ + static_cast<std::size_t>(i)]);
      value = static_cast<std::uint16_t>(value | static_cast<std::uint16_t>(byte << (8 * i)));
    }
    position_ += 2;
    return value;
  }

  [[nodiscard]] std::uint32_t get_u32() noexcept {
    if (!ensure(4)) {
      return 0;
    }
    std::uint32_t value = 0;
    for (int i = 0; i < 4; ++i) {
      const auto byte = static_cast<std::uint32_t>(data_[position_ + static_cast<std::size_t>(i)]);
      value |= static_cast<std::uint32_t>(byte << (8 * i));
    }
    position_ += 4;
    return value;
  }

  [[nodiscard]] std::uint64_t get_u64() noexcept {
    if (!ensure(8)) {
      return 0;
    }
    std::uint64_t value = 0;
    for (int i = 0; i < 8; ++i) {
      const auto byte = static_cast<std::uint64_t>(data_[position_ + static_cast<std::size_t>(i)]);
      value |= static_cast<std::uint64_t>(byte << (8 * i));
    }
    position_ += 8;
    return value;
  }

  [[nodiscard]] std::int64_t get_i64() noexcept { return static_cast<std::int64_t>(get_u64()); }

  [[nodiscard]] std::span<const std::byte> get_raw(std::size_t size) noexcept {
    if (!ensure(size)) {
      return {};
    }
    const auto view = data_.subspan(position_, size);
    position_ += size;
    return view;
  }

  [[nodiscard]] std::string get_string(std::size_t max_chars = limits::kMaxNameChars) noexcept {
    const std::uint16_t length = get_u16();
    if (failed_ || length > max_chars) {
      failed_ = true;
      return {};
    }
    const auto raw = get_raw(length);
    if (failed_) {
      return {};
    }
    return std::string(reinterpret_cast<const char*>(raw.data()), raw.size());
  }

  [[nodiscard]] std::span<const std::byte> get_bytes(std::size_t max_bytes = limits::kMaxFramePayloadBytes) noexcept {
    const std::uint32_t length = get_u32();
    if (failed_ || length > max_bytes) {
      failed_ = true;
      return {};
    }
    return get_raw(length);
  }

 private:
  [[nodiscard]] bool ensure(std::size_t size) noexcept {
    if (failed_ || size > data_.size() - position_) {
      failed_ = true;
      return false;
    }
    return true;
  }

  std::span<const std::byte> data_;
  std::size_t position_{0};
  bool failed_{false};
};

}  // namespace qf
