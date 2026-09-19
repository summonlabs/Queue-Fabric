// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
#include "qf/protocol.hpp"

#include "qf/serialize.hpp"

namespace qf {

const char* to_string(Role value) noexcept {
  switch (value) {
    case Role::worker: return "worker";
    case Role::monitor: return "monitor";
  }
  return "unknown";
}

const char* to_string(MessageType value) noexcept {
  switch (value) {
    case MessageType::none: return "none";
    case MessageType::hello: return "hello";
    case MessageType::hello_ack: return "hello_ack";
    case MessageType::heartbeat: return "heartbeat";
    case MessageType::occupancy_batch: return "occupancy_batch";
    case MessageType::mutation: return "mutation";
    case MessageType::mutation_response: return "mutation_response";
    case MessageType::epoch_advance: return "epoch_advance";
    case MessageType::fence_notice: return "fence_notice";
    case MessageType::stats_request: return "stats_request";
    case MessageType::stats_response: return "stats_response";
    case MessageType::shutdown: return "shutdown";
    case MessageType::error_message: return "error_message";
  }
  return "unknown";
}

namespace {

void put_attempt(ByteWriter& writer, const AttemptId& attempt) {
  writer.put_u64(attempt.publisher.value());
  writer.put_u64(attempt.counter);
}

AttemptId get_attempt(ByteReader& reader) {
  AttemptId attempt{};
  attempt.publisher = PublisherId::from_value(reader.get_u64());
  attempt.counter = reader.get_u64();
  return attempt;
}

void put_occupancy(ByteWriter& writer, const OccupancySample& sample) {
  writer.put_u64(sample.queue.value());
  writer.put_u64(sample.queue_generation.value());
  writer.put_u64(sample.bytes);
  writer.put_u64(sample.packets);
  writer.put_u64(sample.epoch.value());
  writer.put_u64(sample.sequence.value());
  writer.put_i64(sample.observed_at);
  encode_provenance(writer, sample.provenance);
}

OccupancySample get_occupancy(ByteReader& reader) {
  OccupancySample sample{};
  sample.queue = QueueId::from_value(reader.get_u64());
  sample.queue_generation = Generation::from_value(reader.get_u64());
  sample.bytes = reader.get_u64();
  sample.packets = reader.get_u64();
  sample.epoch = Epoch::from_value(reader.get_u64());
  sample.sequence = Sequence::from_value(reader.get_u64());
  sample.observed_at = reader.get_i64();
  sample.provenance = decode_provenance(reader);
  return sample;
}

}  // namespace

Bytes encode_message(const Message& message, std::size_t max_bytes) {
  ByteWriter writer(max_bytes);
  writer.put_u16(static_cast<std::uint16_t>(message.type));
  writer.put_u32(message.sequence);
  switch (message.type) {
    case MessageType::hello:
      writer.put_u8(static_cast<std::uint8_t>(message.hello.role));
      writer.put_u64(message.hello.node.value());
      writer.put_u64(message.hello.incarnation.value());
      writer.put_u64(message.hello.boot.hi);
      writer.put_u64(message.hello.boot.lo);
      writer.put_u64(message.hello.epoch.value());
      writer.put_string(message.hello.name);
      break;
    case MessageType::hello_ack:
      writer.put_bool(message.hello_ack.accepted);
      writer.put_u16(static_cast<std::uint16_t>(message.hello_ack.code));
      writer.put_string(message.hello_ack.message);
      writer.put_u64(message.hello_ack.coordinator_epoch.value());
      writer.put_u64(message.hello_ack.coordinator_incarnation.value());
      break;
    case MessageType::heartbeat:
      writer.put_u64(message.heartbeat.sequence.value());
      writer.put_u64(message.heartbeat.epoch.value());
      break;
    case MessageType::occupancy_batch: {
      const std::size_t count = message.occupancy.samples.size();
      if (count > limits::kMaxOccupancyBatch) {
        return {};
      }
      writer.put_u32(static_cast<std::uint32_t>(count));
      for (const auto& sample : message.occupancy.samples) {
        put_occupancy(writer, sample);
      }
      break;
    }
    case MessageType::mutation: {
      const Bytes encoded = encode_mutation(message.mutation);
      if (encoded.empty()) {
        return {};
      }
      writer.put_bytes(encoded);
      break;
    }
    case MessageType::mutation_response:
      put_attempt(writer, message.mutation_response.attempt);
      writer.put_u64(message.mutation_response.queue.value());
      writer.put_u16(static_cast<std::uint16_t>(message.mutation_response.code));
      writer.put_string(message.mutation_response.message);
      writer.put_u64(message.mutation_response.generation.value());
      writer.put_bool(message.mutation_response.replayed);
      break;
    case MessageType::epoch_advance:
      writer.put_u64(message.epoch.value());
      break;
    case MessageType::fence_notice:
      writer.put_u64(message.fence.queue.value());
      writer.put_u64(message.fence.generation.value());
      writer.put_u64(message.fence.token);
      writer.put_string(message.fence.reason);
      break;
    case MessageType::stats_request:
      break;
    case MessageType::stats_response:
      writer.put_u64(message.stats.epoch.value());
      writer.put_u64(message.stats.coordinator_incarnation.value());
      writer.put_u64(message.stats.workers);
      writer.put_u64(message.stats.queues);
      writer.put_u64(message.stats.occupancy_accepted);
      writer.put_u64(message.stats.occupancy_rejected);
      writer.put_u64(message.stats.occupancy_overflow);
      writer.put_u64(message.stats.mutations_applied);
      writer.put_u64(message.stats.mutations_replayed);
      writer.put_u64(message.stats.mutations_rejected);
      writer.put_u64(message.stats.stale_epoch_rejections);
      writer.put_u64(message.stats.stale_incarnation_rejections);
      writer.put_u64(message.stats.revalidations_required);
      writer.put_u64(message.stats.ambiguous_attempts);
      writer.put_u64(message.stats.frames_received);
      writer.put_u64(message.stats.frames_rejected);
      writer.put_u64(message.stats.connections_accepted);
      writer.put_u64(message.stats.connections_lost);
      writer.put_u64(message.stats.liveness_expirations);
      writer.put_u64(message.stats.recoveries);
      writer.put_u64(message.stats.durable_records);
      break;
    case MessageType::shutdown:
      writer.put_string(message.text);
      break;
    case MessageType::error_message:
      writer.put_u16(static_cast<std::uint16_t>(message.error.code));
      writer.put_string(message.error.message);
      break;
    case MessageType::none:
    default:
      return {};
  }
  if (writer.failed()) {
    return {};
  }
  return writer.take();
}

Result<Message> decode_message(std::span<const std::byte> data) {
  ByteReader reader(data);
  Message message{};
  message.type = static_cast<MessageType>(reader.get_u16());
  message.sequence = reader.get_u32();
  if (reader.failed()) {
    return Status{Code::malformed_input, "message header is truncated"};
  }
  switch (message.type) {
    case MessageType::hello:
      message.hello.role = static_cast<Role>(reader.get_u8());
      message.hello.node = NodeId::from_value(reader.get_u64());
      message.hello.incarnation = Incarnation::from_value(reader.get_u64());
      message.hello.boot.hi = reader.get_u64();
      message.hello.boot.lo = reader.get_u64();
      message.hello.epoch = Epoch::from_value(reader.get_u64());
      message.hello.name = reader.get_string();
      if (message.hello.role != Role::worker && message.hello.role != Role::monitor) {
        return Status{Code::malformed_input, "hello role is unknown"};
      }
      break;
    case MessageType::hello_ack:
      message.hello_ack.accepted = reader.get_bool();
      message.hello_ack.code = static_cast<Code>(reader.get_u16());
      message.hello_ack.message = reader.get_string(limits::kMaxMessageChars);
      message.hello_ack.coordinator_epoch = Epoch::from_value(reader.get_u64());
      message.hello_ack.coordinator_incarnation = Incarnation::from_value(reader.get_u64());
      break;
    case MessageType::heartbeat:
      message.heartbeat.sequence = Sequence::from_value(reader.get_u64());
      message.heartbeat.epoch = Epoch::from_value(reader.get_u64());
      break;
    case MessageType::occupancy_batch: {
      const std::uint32_t count = reader.get_u32();
      if (reader.failed() || count > limits::kMaxOccupancyBatch) {
        return Status{Code::malformed_input, "occupancy batch count is out of range"};
      }
      message.occupancy.samples.reserve(count);
      for (std::uint32_t i = 0; i < count; ++i) {
        message.occupancy.samples.push_back(get_occupancy(reader));
      }
      break;
    }
    case MessageType::mutation: {
      const auto blob = reader.get_bytes(limits::kMaxFramePayloadBytes);
      if (reader.failed()) {
        return Status{Code::malformed_input, "mutation payload is truncated"};
      }
      auto decoded = decode_mutation(blob);
      if (!decoded.ok()) {
        return decoded.status();
      }
      message.mutation = std::move(decoded.value());
      break;
    }
    case MessageType::mutation_response:
      message.mutation_response.attempt = get_attempt(reader);
      message.mutation_response.queue = QueueId::from_value(reader.get_u64());
      message.mutation_response.code = static_cast<Code>(reader.get_u16());
      message.mutation_response.message = reader.get_string(limits::kMaxMessageChars);
      message.mutation_response.generation = Generation::from_value(reader.get_u64());
      message.mutation_response.replayed = reader.get_bool();
      break;
    case MessageType::epoch_advance:
      message.epoch = Epoch::from_value(reader.get_u64());
      break;
    case MessageType::fence_notice:
      message.fence.queue = QueueId::from_value(reader.get_u64());
      message.fence.generation = Generation::from_value(reader.get_u64());
      message.fence.token = reader.get_u64();
      message.fence.reason = reader.get_string(limits::kMaxReasonChars);
      break;
    case MessageType::stats_request:
      break;
    case MessageType::stats_response:
      message.stats.epoch = Epoch::from_value(reader.get_u64());
      message.stats.coordinator_incarnation = Incarnation::from_value(reader.get_u64());
      message.stats.workers = reader.get_u64();
      message.stats.queues = reader.get_u64();
      message.stats.occupancy_accepted = reader.get_u64();
      message.stats.occupancy_rejected = reader.get_u64();
      message.stats.occupancy_overflow = reader.get_u64();
      message.stats.mutations_applied = reader.get_u64();
      message.stats.mutations_replayed = reader.get_u64();
      message.stats.mutations_rejected = reader.get_u64();
      message.stats.stale_epoch_rejections = reader.get_u64();
      message.stats.stale_incarnation_rejections = reader.get_u64();
      message.stats.revalidations_required = reader.get_u64();
      message.stats.ambiguous_attempts = reader.get_u64();
      message.stats.frames_received = reader.get_u64();
      message.stats.frames_rejected = reader.get_u64();
      message.stats.connections_accepted = reader.get_u64();
      message.stats.connections_lost = reader.get_u64();
      message.stats.liveness_expirations = reader.get_u64();
      message.stats.recoveries = reader.get_u64();
      message.stats.durable_records = reader.get_u64();
      break;
    case MessageType::shutdown:
      message.text = reader.get_string(limits::kMaxMessageChars);
      break;
    case MessageType::error_message:
      message.error.code = static_cast<Code>(reader.get_u16());
      message.error.message = reader.get_string(limits::kMaxMessageChars);
      break;
    case MessageType::none:
    default:
      return Status{Code::unsupported, "message type is unknown"};
  }
  if (reader.failed() || reader.remaining() != 0) {
    return Status{Code::malformed_input, "message payload is malformed"};
  }
  return message;
}

}  // namespace qf
