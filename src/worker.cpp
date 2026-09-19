// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
#include "qf/worker.hpp"

#include <utility>
#include <vector>

#include "qf/frame.hpp"

namespace qf {

struct WorkerClient::Impl {
  WorkerConfig config{};
  IClock* clock{nullptr};
  TcpTransport transport{};
  FrameDecoder decoder{limits::kMaxFramePayloadBytes};
  Sequence sequence{Sequence::from_value(1)};
  Epoch coordinator_epoch{};
  bool admitted{false};
  std::uint64_t frames_sent{0};
  std::uint64_t frames_received{0};
  std::uint64_t frames_rejected{0};
  std::uint64_t epoch_notices{0};
  Status last_error{};

  Status send(const Message& message);
  Result<Message> await(MessageType expected);
  Status handle_notice(const Message& message);
};

Status WorkerClient::Impl::send(const Message& message) {
  const Bytes payload = encode_message(message);
  if (payload.empty()) {
    return Status{Code::internal, "message could not be encoded"};
  }
  Frame frame{};
  frame.type = static_cast<std::uint16_t>(message.type);
  frame.sequence = message.sequence;
  frame.payload = payload;
  const Bytes encoded = encode_frame(frame);
  if (encoded.empty()) {
    return Status{Code::capacity_exceeded, "frame exceeds the configured bound"};
  }
  const Status sent = transport.send(encoded);
  if (sent.ok()) {
    ++frames_sent;
  } else {
    last_error = sent;
  }
  return sent;
}

Status WorkerClient::Impl::handle_notice(const Message& message) {
  switch (message.type) {
    case MessageType::epoch_advance:
      coordinator_epoch = message.epoch;
      ++epoch_notices;
      return Status{};
    case MessageType::fence_notice:
      return Status{};
    case MessageType::error_message:
      last_error = Status{message.error.code, message.error.message};
      return last_error;
    default:
      return Status{};
  }
}

Result<Message> WorkerClient::Impl::await(MessageType expected) {
  std::vector<std::byte> buffer(config.receive_buffer_bytes);
  for (;;) {
    if (!transport.open()) {
      return Status{Code::not_found, "connection closed before a response arrived"};
    }
    auto received = transport.receive(buffer);
    if (!received.ok()) {
      last_error = received.status();
      return received.status();
    }
    if (received.value() == 0) {
      transport.close();
      return Status{Code::not_found, "coordinator closed the connection"};
    }
    const Status pushed = decoder.push(std::span<const std::byte>(buffer.data(), received.value()));
    if (!pushed.ok()) {
      return pushed;
    }
    for (;;) {
      auto frame = decoder.next();
      if (!frame.ok()) {
        if (frame.status().code() == Code::not_found) {
          break;
        }
        ++frames_rejected;
        return frame.status();
      }
      ++frames_received;
      auto decoded = decode_message(frame.value().payload);
      if (!decoded.ok()) {
        ++frames_rejected;
        continue;
      }
      if (decoded.value().type == expected) {
        return decoded.value();
      }
      const Status notice = handle_notice(decoded.value());
      if (!notice.ok() && decoded.value().type == MessageType::error_message) {
        return notice;
      }
    }
  }
}

WorkerClient::WorkerClient(WorkerConfig config, IClock& clock) : impl_(std::make_unique<Impl>()) {
  impl_->config = std::move(config);
  impl_->clock = &clock;
}

WorkerClient::~WorkerClient() { close(); }

Status WorkerClient::connect() {
  auto transport = TcpTransport::connect(impl_->config.host, impl_->config.port);
  if (!transport.ok()) {
    return transport.status();
  }
  impl_->transport = std::move(transport.value());
  impl_->transport.set_no_delay();
  impl_->transport.set_keepalive();
  impl_->decoder.reset();
  return Status{};
}

Result<HelloAck> WorkerClient::handshake() {
  Message hello{};
  hello.type = MessageType::hello;
  hello.sequence = static_cast<std::uint32_t>(impl_->sequence.value());
  hello.hello.role = impl_->config.role;
  hello.hello.node = impl_->config.node;
  hello.hello.incarnation = impl_->config.incarnation;
  hello.hello.boot = impl_->config.boot.valid() ? impl_->config.boot : BootId{1, 1};
  hello.hello.epoch = impl_->config.epoch;
  hello.hello.name = impl_->config.name;
  const Status sent = impl_->send(hello);
  if (!sent.ok()) {
    return sent;
  }
  auto response = impl_->await(MessageType::hello_ack);
  if (!response.ok()) {
    return response.status();
  }
  impl_->coordinator_epoch = response.value().hello_ack.coordinator_epoch;
  impl_->admitted = response.value().hello_ack.accepted;
  return response.value().hello_ack;
}

bool WorkerClient::admitted() const { return impl_->admitted; }

Status WorkerClient::publish(const std::vector<OccupancySample>& samples) {
  if (!impl_->admitted) {
    return Status{Code::authority_denied, "connection is not admitted"};
  }
  if (samples.empty() || samples.size() > limits::kMaxOccupancyBatch) {
    return Status{Code::out_of_range, "occupancy batch size is out of range"};
  }
  Message message{};
  message.type = MessageType::occupancy_batch;
  message.sequence = static_cast<std::uint32_t>(impl_->sequence.value());
  message.occupancy.samples = samples;
  return impl_->send(message);
}

Status WorkerClient::send_heartbeat() {
  Message message{};
  message.type = MessageType::heartbeat;
  message.sequence = static_cast<std::uint32_t>(impl_->sequence.value());
  message.heartbeat.sequence = impl_->sequence;
  message.heartbeat.epoch = impl_->config.epoch;
  return impl_->send(message);
}

Result<MutationResponse> WorkerClient::send_mutation(const MutationRequest& request) {
  Message message{};
  message.type = MessageType::mutation;
  message.sequence = static_cast<std::uint32_t>(impl_->sequence.value());
  message.mutation = request;
  const Status sent = impl_->send(message);
  if (!sent.ok()) {
    return sent;
  }
  auto response = impl_->await(MessageType::mutation_response);
  if (!response.ok()) {
    return response.status();
  }
  return response.value().mutation_response;
}

Result<CoordinatorStats> WorkerClient::request_stats() {
  Message message{};
  message.type = MessageType::stats_request;
  message.sequence = static_cast<std::uint32_t>(impl_->sequence.value());
  const Status sent = impl_->send(message);
  if (!sent.ok()) {
    return sent;
  }
  auto response = impl_->await(MessageType::stats_response);
  if (!response.ok()) {
    return response.status();
  }
  return response.value().stats;
}

Status WorkerClient::send_shutdown(std::string_view reason) {
  Message message{};
  message.type = MessageType::shutdown;
  message.sequence = static_cast<std::uint32_t>(impl_->sequence.value());
  message.text.assign(reason.substr(0, limits::kMaxMessageChars));
  return impl_->send(message);
}

Status WorkerClient::poll() { return Status{}; }

void WorkerClient::close() noexcept {
  if (impl_ != nullptr) {
    impl_->transport.close();
    impl_->admitted = false;
  }
}

Epoch WorkerClient::coordinator_epoch() const { return impl_->coordinator_epoch; }

Sequence WorkerClient::next_sequence() noexcept {
  auto next = impl_->sequence.next();
  if (!next.ok()) {
    return impl_->sequence;
  }
  impl_->sequence = next.value();
  return impl_->sequence;
}

std::uint64_t WorkerClient::frames_sent() const { return impl_->frames_sent; }
std::uint64_t WorkerClient::frames_received() const { return impl_->frames_received; }
std::uint64_t WorkerClient::frames_rejected() const { return impl_->frames_rejected; }
std::uint64_t WorkerClient::epoch_notices() const { return impl_->epoch_notices; }

}  // namespace qf
