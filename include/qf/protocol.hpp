// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <cstdint>
#include <span>
#include <string>
#include <vector>

#include "qf/bytes.hpp"
#include "qf/fabric.hpp"
#include "qf/ids.hpp"
#include "qf/limits.hpp"
#include "qf/mutation.hpp"
#include "qf/occupancy.hpp"
#include "qf/status.hpp"

namespace qf {

/// Role of a connected peer.
enum class Role : std::uint8_t {
  worker = 1,   ///< Publishes occupancy and may submit mutations.
  monitor = 2,  ///< Read-only statistics client.
};

const char* to_string(Role value) noexcept;

enum class MessageType : std::uint16_t {
  none = 0,
  hello = 1,
  hello_ack = 2,
  heartbeat = 3,
  occupancy_batch = 4,
  mutation = 5,
  mutation_response = 6,
  epoch_advance = 7,
  fence_notice = 8,
  stats_request = 9,
  stats_response = 10,
  shutdown = 11,
  error_message = 12,
};

const char* to_string(MessageType value) noexcept;

/// First message on every connection.
struct Hello {
  Role role{Role::worker};
  NodeId node{};
  Incarnation incarnation{};
  BootId boot{};
  Epoch epoch{};
  std::string name{};
};

/// Admission decision for a connection.
struct HelloAck {
  bool accepted{false};
  Code code{Code::ok};
  std::string message{};
  Epoch coordinator_epoch{};
  Incarnation coordinator_incarnation{};
};

struct Heartbeat {
  Sequence sequence{};
  Epoch epoch{};
};

struct OccupancyBatch {
  std::vector<OccupancySample> samples{};
};

struct MutationResponse {
  AttemptId attempt{};
  QueueId queue{};
  Code code{Code::ok};
  std::string message{};
  Generation generation{};
  bool replayed{false};
};

struct FenceNotice {
  QueueId queue{};
  Generation generation{};
  std::uint64_t token{0};
  std::string reason{};
};

/// Coordinator statistics reported to monitors. Every field is a completed-work
/// counter; none of them describe physical network behaviour.
struct CoordinatorStats {
  Epoch epoch{};
  Incarnation coordinator_incarnation{};
  std::uint64_t workers{0};
  std::uint64_t queues{0};
  std::uint64_t occupancy_accepted{0};
  std::uint64_t occupancy_rejected{0};
  std::uint64_t occupancy_overflow{0};
  std::uint64_t mutations_applied{0};
  std::uint64_t mutations_replayed{0};
  std::uint64_t mutations_rejected{0};
  std::uint64_t stale_epoch_rejections{0};
  std::uint64_t stale_incarnation_rejections{0};
  std::uint64_t revalidations_required{0};
  std::uint64_t ambiguous_attempts{0};
  std::uint64_t frames_received{0};
  std::uint64_t frames_rejected{0};
  std::uint64_t connections_accepted{0};
  std::uint64_t connections_lost{0};
  std::uint64_t liveness_expirations{0};
  std::uint64_t recoveries{0};
  std::uint64_t durable_records{0};
};

struct ErrorMessage {
  Code code{Code::internal};
  std::string message{};
};

/// One protocol message. A single flat structure keeps the wire codec small,
/// total and easy to audit; only the fields relevant to the message type are
/// encoded.
struct Message {
  MessageType type{MessageType::none};
  std::uint32_t sequence{0};

  Hello hello{};
  HelloAck hello_ack{};
  Heartbeat heartbeat{};
  OccupancyBatch occupancy{};
  MutationRequest mutation{};
  MutationResponse mutation_response{};
  Epoch epoch{};
  FenceNotice fence{};
  CoordinatorStats stats{};
  ErrorMessage error{};
  std::string text{};
};

/// Encodes a message body for framing. Returns empty on any bound violation.
Bytes encode_message(const Message& message, std::size_t max_bytes = limits::kMaxFramePayloadBytes);

/// Decodes a message body. Every field is bounds checked; unknown types,
/// truncated payloads, oversized batches and trailing bytes are refused.
Result<Message> decode_message(std::span<const std::byte> data);

}  // namespace qf
