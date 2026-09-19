// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
#include "qf/frame.hpp"

#include <algorithm>

#include "qf/hash.hpp"
#include "qf/version.hpp"

namespace qf {

namespace {

void put_u16_at(std::uint8_t* out, std::uint16_t value) noexcept {
  out[0] = static_cast<std::uint8_t>(value & 0xFFu);
  out[1] = static_cast<std::uint8_t>((value >> 8) & 0xFFu);
}

void put_u32_at(std::uint8_t* out, std::uint32_t value) noexcept {
  for (int i = 0; i < 4; ++i) {
    out[i] = static_cast<std::uint8_t>((value >> (8 * i)) & 0xFFu);
  }
}

std::uint16_t get_u16_at(const std::uint8_t* in) noexcept {
  return static_cast<std::uint16_t>(static_cast<std::uint16_t>(in[0]) | (static_cast<std::uint16_t>(in[1]) << 8));
}

std::uint32_t get_u32_at(const std::uint8_t* in) noexcept {
  std::uint32_t value = 0;
  for (int i = 0; i < 4; ++i) {
    value |= static_cast<std::uint32_t>(in[i]) << (8 * i);
  }
  return value;
}

}  // namespace

Bytes encode_frame(const Frame& frame, std::size_t max_payload) {
  if (frame.payload.size() > max_payload || frame.payload.size() > limits::kMaxFramePayloadBytes) {
    return {};
  }
  Bytes out(kFrameHeaderBytes + frame.payload.size());
  auto* raw = reinterpret_cast<std::uint8_t*>(out.data());
  put_u32_at(raw, kFrameMagic);
  put_u16_at(raw + 4, kProtocolVersion);
  put_u16_at(raw + 6, frame.type);
  put_u16_at(raw + 8, 0);
  put_u16_at(raw + 10, 0);
  put_u32_at(raw + 12, frame.sequence);
  put_u32_at(raw + 16, static_cast<std::uint32_t>(frame.payload.size()));
  std::uint32_t crc = crc32c(raw, 20);
  if (!frame.payload.empty()) {
    crc = crc32c_extend(crc, frame.payload.data(), frame.payload.size());
    std::copy(frame.payload.begin(), frame.payload.end(), out.begin() + static_cast<std::ptrdiff_t>(kFrameHeaderBytes));
  }
  put_u32_at(raw + 20, crc);
  return out;
}

Status FrameDecoder::push(std::span<const std::byte> data) {
  if (failed_) {
    return failure_;
  }
  if (data.empty()) {
    return Status{};
  }
  const std::size_t bound = max_payload_ + kFrameHeaderBytes;
  if (buffered() > bound || data.size() > bound - buffered()) {
    failed_ = true;
    failure_ = Status{Code::capacity_exceeded, "frame buffer bound exceeded"};
    ++frames_rejected_;
    return failure_;
  }
  buffer_.insert(buffer_.end(), data.begin(), data.end());
  return Status{};
}

Result<Frame> FrameDecoder::next() {
  if (failed_) {
    return failure_;
  }
  const std::size_t available = buffered();
  if (available < kFrameHeaderBytes) {
    return Status{Code::not_found, "frame header incomplete"};
  }
  const auto* raw = reinterpret_cast<const std::uint8_t*>(buffer_.data() + offset_);
  if (get_u32_at(raw) != kFrameMagic) {
    failed_ = true;
    failure_ = Status{Code::malformed_input, "frame magic mismatch"};
    ++frames_rejected_;
    return failure_;
  }
  const std::uint16_t version = get_u16_at(raw + 4);
  if (version != kProtocolVersion) {
    failed_ = true;
    failure_ = Status{Code::unsupported, "frame protocol version is not supported"};
    ++frames_rejected_;
    return failure_;
  }
  const std::uint32_t payload_len = get_u32_at(raw + 16);
  if (payload_len > max_payload_) {
    failed_ = true;
    failure_ = Status{Code::capacity_exceeded, "frame payload exceeds the configured bound"};
    ++frames_rejected_;
    return failure_;
  }
  const std::size_t total = kFrameHeaderBytes + payload_len;
  if (available < total) {
    return Status{Code::not_found, "frame payload incomplete"};
  }
  const std::uint32_t stored_crc = get_u32_at(raw + 20);
  const std::uint32_t computed = crc32c_extend(crc32c(raw, 20), raw + kFrameHeaderBytes, payload_len);
  if (stored_crc != computed) {
    failed_ = true;
    failure_ = Status{Code::integrity_failure, "frame checksum mismatch"};
    ++frames_rejected_;
    return failure_;
  }
  Frame frame{};
  frame.type = get_u16_at(raw + 6);
  frame.sequence = get_u32_at(raw + 12);
  frame.payload.assign(buffer_.begin() + static_cast<std::ptrdiff_t>(offset_ + kFrameHeaderBytes),
                       buffer_.begin() + static_cast<std::ptrdiff_t>(offset_ + total));
  offset_ += total;
  ++frames_decoded_;
  if (offset_ == buffer_.size()) {
    buffer_.clear();
    offset_ = 0;
  } else {
    compact();
  }
  return frame;
}

void FrameDecoder::compact() noexcept {
  if (offset_ == 0) {
    return;
  }
  if (offset_ >= buffer_.size()) {
    bytes_consumed_ += buffer_.size();
    buffer_.clear();
    offset_ = 0;
    return;
  }
  buffer_.erase(buffer_.begin(), buffer_.begin() + static_cast<std::ptrdiff_t>(offset_));
  bytes_consumed_ += offset_;
  offset_ = 0;
}

void FrameDecoder::reset() noexcept {
  buffer_.clear();
  offset_ = 0;
  failed_ = false;
  failure_ = Status{};
}

}  // namespace qf
