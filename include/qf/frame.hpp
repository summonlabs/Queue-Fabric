// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <cstddef>
#include <cstdint>
#include <span>

#include "qf/bytes.hpp"
#include "qf/limits.hpp"
#include "qf/status.hpp"

namespace qf {

/// Wire frame.
///
/// Layout (little-endian): magic u32, protocol version u16, type u16, flags u16,
/// reserved u16, sequence u32, payload length u32, payload CRC-32C u32, payload.
/// The CRC covers the first 20 header bytes plus the payload, so a frame cannot
/// be reinterpreted after truncation, extension or bit damage.
struct Frame {
  std::uint16_t type{0};
  std::uint32_t sequence{0};
  Bytes payload{};
};

inline constexpr std::size_t kFrameHeaderBytes = 24;
inline constexpr std::uint32_t kFrameMagic = 0x31464651u;  // "QFF1"

/// Encodes one frame. Returns an empty buffer if the payload is out of bounds.
Bytes encode_frame(const Frame& frame, std::size_t max_payload = limits::kMaxFramePayloadBytes);

/// Incremental decoder. Bounded: the internal buffer never exceeds the maximum
/// frame size, and a declared payload length above the bound is rejected before
/// any memory is reserved for it.
class FrameDecoder {
 public:
  explicit FrameDecoder(std::size_t max_payload = limits::kMaxFramePayloadBytes) noexcept
      : max_payload_(max_payload) {}

  /// Appends received bytes. Fails when the buffer bound would be exceeded.
  Status push(std::span<const std::byte> data);

  /// Decodes the next complete frame.
  /// - ok(frame): a complete frame was consumed.
  /// - not_found: not enough bytes yet (call push again).
  /// - anything else: the stream is unusable and stays failed.
  [[nodiscard]] Result<Frame> next();

  [[nodiscard]] std::size_t buffered() const noexcept { return buffer_.size() - offset_; }
  [[nodiscard]] std::size_t frames_decoded() const noexcept { return frames_decoded_; }
  [[nodiscard]] std::size_t frames_rejected() const noexcept { return frames_rejected_; }
  /// Bytes removed from the buffer after successful decoding.
  [[nodiscard]] std::size_t bytes_consumed() const noexcept { return bytes_consumed_; }
  [[nodiscard]] bool failed() const noexcept { return failed_; }
  [[nodiscard]] Status failure() const noexcept { return failure_; }

  void reset() noexcept;

 private:
  void compact() noexcept;

  Bytes buffer_{};
  std::size_t offset_{0};
  std::size_t max_payload_{limits::kMaxFramePayloadBytes};
  std::size_t frames_decoded_{0};
  std::size_t frames_rejected_{0};
  std::size_t bytes_consumed_{0};
  bool failed_{false};
  Status failure_{};
};

}  // namespace qf
