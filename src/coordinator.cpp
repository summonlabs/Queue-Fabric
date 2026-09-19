// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
#include "qf/coordinator.hpp"

#include <algorithm>
#include <memory>
#include <optional>
#include <unordered_map>
#include <utility>
#include <vector>

namespace qf {

namespace {

struct Connection {
  std::uint64_t id{0};
  TcpTransport transport{};
  FrameDecoder decoder{limits::kMaxFramePayloadBytes};
  WorkerSession session{};
  std::vector<QueueId> queues{};
  bool closing{false};
};

Status send_message(Connection& connection, const Message& message) {
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
  const Status sent = connection.transport.send(encoded);
  if (sent.ok()) {
    ++connection.session.frames_out;
  }
  return sent;
}

}  // namespace

struct CoordinatorServer::Impl {
  CoordinatorConfig config{};
  IClock* clock{nullptr};
  IEventSink* sink{nullptr};
  std::unique_ptr<FileJournal> journal{};
  std::optional<Fabric> fabric{};
  TcpListener listener{};
  std::vector<Connection> connections{};
  std::unordered_map<ResourceId, OwnerId> resource_principals{};
  CoordinatorStats stats{};
  std::uint64_t next_connection_id{1};
  std::uint32_t outbound_sequence{0};
  bool stopped{false};

  Status handle_message(Connection& connection, const Message& message);
  Status handle_hello(Connection& connection, const Message& message);
  Status handle_occupancy(Connection& connection, const Message& message);
  Status handle_mutation(Connection& connection, const Message& message);
  void handle_disconnect(Connection& connection, const char* reason);
  [[nodiscard]] OwnerId principal_for(QueueId queue);
  Status reply_error(Connection& connection, Code code, std::string_view text);
  [[nodiscard]] std::size_t active_worker_count() const noexcept;
  [[nodiscard]] CoordinatorStats snapshot_stats() const;
};

CoordinatorStats CoordinatorServer::Impl::snapshot_stats() const {
  CoordinatorStats snapshot = stats;
  const FabricStats fabric_stats = fabric->stats();
  snapshot.epoch = fabric->epoch();
  snapshot.workers = active_worker_count();
  snapshot.queues = fabric_stats.queues;
  snapshot.durable_records = fabric_stats.journal_records;
  snapshot.ambiguous_attempts = fabric->recovery_report().orphaned_begins;
  return snapshot;
}

OwnerId CoordinatorServer::Impl::principal_for(QueueId queue) {
  auto record = fabric->queue_record(queue);
  if (!record.ok()) {
    return OwnerId{};
  }
  const auto cached = resource_principals.find(record.value().def.resource);
  if (cached != resource_principals.end()) {
    return cached->second;
  }
  auto resource = fabric->resource_record(record.value().def.resource);
  if (!resource.ok()) {
    return OwnerId{};
  }
  resource_principals.emplace(resource.value().id, resource.value().owner);
  return resource.value().owner;
}

Status CoordinatorServer::Impl::reply_error(Connection& connection, Code code, std::string_view text) {
  Message reply{};
  reply.type = MessageType::error_message;
  reply.sequence = ++outbound_sequence;
  reply.error.code = code;
  reply.error.message.assign(text.substr(0, limits::kMaxMessageChars));
  return send_message(connection, reply);
}

std::size_t CoordinatorServer::Impl::active_worker_count() const noexcept {
  std::size_t count = 0;
  for (const auto& connection : connections) {
    if (connection.session.handshaken && connection.session.role == Role::worker) {
      ++count;
    }
  }
  return count;
}

Status CoordinatorServer::Impl::handle_hello(Connection& connection, const Message& message) {
  const Hello& hello = message.hello;
  Message reply{};
  reply.type = MessageType::hello_ack;
  reply.sequence = ++outbound_sequence;
  reply.hello_ack.coordinator_epoch = fabric->epoch();
  reply.hello_ack.coordinator_incarnation = stats.coordinator_incarnation;

  if (!hello.node.valid() || !hello.incarnation.valid() || !hello.boot.valid()) {
    reply.hello_ack.accepted = false;
    reply.hello_ack.code = Code::malformed_input;
    reply.hello_ack.message = "hello requires node, incarnation and boot identity";
    ++connection.session.frames_rejected;
    return send_message(connection, reply);
  }

  // A worker must state the coordinator epoch it believes it is talking to.
  // An absent epoch is UNKNOWN, and UNKNOWN is never admitted; a superseded
  // epoch means its occupancy and completions belong to an earlier life.
  if (hello.role == Role::worker) {
    if (!hello.epoch.valid()) {
      reply.hello_ack.accepted = false;
      reply.hello_ack.code = Code::stale_epoch;
      reply.hello_ack.message = "a coordinator epoch is required for admission";
      ++stats.stale_epoch_rejections;
      return send_message(connection, reply);
    }
    if (hello.epoch != fabric->epoch()) {
      reply.hello_ack.accepted = false;
      reply.hello_ack.code = Code::stale_epoch;
      reply.hello_ack.message = "presented epoch is not the current coordinator epoch";
      ++stats.stale_epoch_rejections;
      return send_message(connection, reply);
    }
  }

  // A node that reconnects with an older incarnation than the one already
  // admitted is a superseded process.
  for (const auto& existing : connections) {
    if (!existing.session.handshaken || existing.session.node != hello.node) {
      continue;
    }
    if (hello.incarnation < existing.session.incarnation) {
      reply.hello_ack.accepted = false;
      reply.hello_ack.code = Code::stale_incarnation;
      reply.hello_ack.message = "publisher incarnation is superseded by an admitted session";
      ++stats.stale_incarnation_rejections;
      return send_message(connection, reply);
    }
  }

  connection.session.handshaken = true;
  connection.session.role = hello.role;
  connection.session.node = hello.node;
  connection.session.incarnation = hello.incarnation;
  connection.session.boot = hello.boot;
  connection.session.epoch = fabric->epoch();
  connection.session.name = hello.name;
  connection.session.last_activity = clock->now_nanos();
  reply.hello_ack.accepted = true;
  reply.hello_ack.code = Code::ok;
  reply.hello_ack.message = "admitted";
  return send_message(connection, reply);
}

Status CoordinatorServer::Impl::handle_occupancy(Connection& connection, const Message& message) {
  const Epoch current = fabric->epoch();
  std::size_t accepted = 0;
  std::size_t rejected = 0;
  for (const auto& sample : message.occupancy.samples) {
    if (!sample.queue.valid() || (sample.epoch.valid() && sample.epoch != current)) {
      ++rejected;
      if (sample.epoch.valid() && sample.epoch != current) {
        ++stats.stale_epoch_rejections;
      }
      continue;
    }
    const OwnerId principal = principal_for(sample.queue);
    if (!principal.valid()) {
      ++rejected;
      continue;
    }
    MutationContext context{};
    context.principal = principal;
    context.provenance = sample.provenance;
    context.epoch = current;
    const auto outcome = fabric->ingest_occupancy(context, sample);
    if (outcome.ok()) {
      ++accepted;
      const auto found = std::find(connection.queues.begin(), connection.queues.end(), sample.queue);
      if (found == connection.queues.end() && connection.queues.size() < limits::kMaxSubscribeQueues) {
        connection.queues.push_back(sample.queue);
      }
    } else {
      ++rejected;
    }
  }
  stats.occupancy_accepted += accepted;
  stats.occupancy_rejected += rejected;
  if (rejected > 0) {
    ++stats.revalidations_required;
  }
  return Status{};
}

Status CoordinatorServer::Impl::handle_mutation(Connection& connection, const Message& message) {
  Message reply{};
  reply.type = MessageType::mutation_response;
  reply.sequence = ++outbound_sequence;
  reply.mutation_response.attempt = message.mutation.attempt;
  const auto outcome = fabric->apply(message.mutation);
  if (outcome.ok()) {
    reply.mutation_response.queue = outcome.value().queue;
    reply.mutation_response.code = Code::ok;
    reply.mutation_response.message = outcome.value().rule;
    reply.mutation_response.generation = outcome.value().generation;
    reply.mutation_response.replayed = outcome.value().replayed;
    ++stats.mutations_applied;
    if (outcome.value().replayed) {
      ++stats.mutations_replayed;
    }
    const auto found = std::find(connection.queues.begin(), connection.queues.end(), outcome.value().queue);
    if (outcome.value().queue.valid() && found == connection.queues.end() &&
        connection.queues.size() < limits::kMaxSubscribeQueues) {
      connection.queues.push_back(outcome.value().queue);
    }
  } else {
    reply.mutation_response.code = outcome.status().code();
    reply.mutation_response.message = outcome.status().message();
    ++stats.mutations_rejected;
  }
  return send_message(connection, reply);
}

Status CoordinatorServer::Impl::handle_message(Connection& connection, const Message& message) {
  connection.session.last_activity = clock->now_nanos();
  ++connection.session.messages_handled;

  if (!connection.session.handshaken && message.type != MessageType::hello) {
    ++connection.session.frames_rejected;
    return reply_error(connection, Code::authority_denied, "first message must be a hello");
  }
  switch (message.type) {
    case MessageType::hello:
      return handle_hello(connection, message);
    case MessageType::heartbeat:
      if (message.heartbeat.epoch.valid() && message.heartbeat.epoch != fabric->epoch()) {
        ++stats.stale_epoch_rejections;
        return reply_error(connection, Code::stale_epoch, "heartbeat epoch is superseded");
      }
      return Status{};
    case MessageType::occupancy_batch:
      return handle_occupancy(connection, message);
    case MessageType::mutation:
      return handle_mutation(connection, message);
    case MessageType::stats_request: {
      Message reply{};
      reply.type = MessageType::stats_response;
      reply.sequence = ++outbound_sequence;
      reply.stats = snapshot_stats();
      return send_message(connection, reply);
    }
    case MessageType::shutdown:
      stopped = true;
      return Status{};
    default:
      ++connection.session.frames_rejected;
      ++stats.frames_rejected;
      return reply_error(connection, Code::unsupported, "message type is not accepted from a peer");
  }
}

void CoordinatorServer::Impl::handle_disconnect(Connection& connection, const char* reason) {
  if (connection.closing) {
    return;
  }
  connection.closing = true;
  ++stats.connections_lost;
  if (!connection.queues.empty()) {
    const Epoch current = fabric->epoch();
    for (const auto& queue : connection.queues) {
      const OwnerId principal = principal_for(queue);
      if (!principal.valid()) {
        continue;
      }
      const Status invalidated = fabric->invalidate_evidence(queue, principal, current, reason);
      if (invalidated.ok()) {
        ++stats.revalidations_required;
      }
    }
  }
  connection.transport.close();
}

CoordinatorServer::CoordinatorServer(CoordinatorConfig config, IClock& clock, IEventSink* sink)
    : impl_(std::make_unique<Impl>()) {
  impl_->config = std::move(config);
  impl_->clock = &clock;
  impl_->sink = sink;
  FabricConfig fabric_config = impl_->config.fabric;
  if (!impl_->config.journal_path.empty()) {
    JournalConfig journal_config{};
    journal_config.path = impl_->config.journal_path;
    journal_config.sync_on_commit = impl_->config.sync_on_commit;
    impl_->journal = std::make_unique<FileJournal>(journal_config);
    fabric_config.durable = true;
  }
  impl_->fabric.emplace(fabric_config, clock, impl_->journal.get(), sink);
}

CoordinatorServer::~CoordinatorServer() { stop(); }

Status CoordinatorServer::start() {
  impl_->stopped = false;
  const Status recovered = impl_->fabric->recover();
  if (!recovered.ok()) {
    return recovered;
  }
  ++impl_->stats.recoveries;
  impl_->stats.coordinator_incarnation = Incarnation::from_value(impl_->fabric->epoch().value());
  auto listener = TcpListener::bind(impl_->config.host, impl_->config.port, impl_->config.max_connections);
  if (!listener.ok()) {
    return listener.status();
  }
  impl_->listener = std::move(listener.value());
  impl_->stats.epoch = impl_->fabric->epoch();
  return Status{};
}

std::uint16_t CoordinatorServer::port() const noexcept { return impl_->listener.port(); }

Status CoordinatorServer::run_once(std::uint32_t accept_timeout_ms) {
  if (impl_->stopped) {
    return Status{Code::shutting_down, "coordinator is stopped"};
  }

  // 1. Accept.
  if (impl_->listener.open() && impl_->connections.size() < impl_->config.max_connections) {
    auto accepted = impl_->listener.accept(accept_timeout_ms);
    if (accepted.ok()) {
      Connection connection{};
      connection.id = impl_->next_connection_id++;
      connection.transport = std::move(accepted.value());
      connection.session.id = connection.id;
      connection.session.peer = connection.transport.peer();
      connection.session.last_activity = impl_->clock->now_nanos();
      connection.transport.set_no_delay();
      connection.transport.set_keepalive();
      impl_->connections.push_back(std::move(connection));
      ++impl_->stats.connections_accepted;
    }
  }

  // 2. Service every connection.
  std::vector<std::byte> buffer(impl_->config.receive_buffer_bytes);
  for (auto& connection : impl_->connections) {
    if (!connection.transport.open()) {
      continue;
    }
    std::size_t frames_this_cycle = 0;
    while (frames_this_cycle < impl_->config.max_frames_per_cycle && connection.transport.readable(0)) {
      auto received = connection.transport.receive(buffer);
      if (!received.ok()) {
        impl_->handle_disconnect(connection, "transport-receive-failed");
        break;
      }
      if (received.value() == 0) {
        impl_->handle_disconnect(connection, "publisher-connection-closed");
        break;
      }
      const Status pushed = connection.decoder.push(std::span<const std::byte>(buffer.data(), received.value()));
      if (!pushed.ok()) {
        ++connection.session.frames_rejected;
        ++impl_->stats.frames_rejected;
        impl_->handle_disconnect(connection, "frame-buffer-bound-exceeded");
        break;
      }
      bool progressed = true;
      while (progressed && frames_this_cycle < impl_->config.max_frames_per_cycle) {
        progressed = false;
        auto frame = connection.decoder.next();
        if (frame.ok()) {
          progressed = true;
          ++frames_this_cycle;
          ++connection.session.frames_in;
          ++impl_->stats.frames_received;
          auto decoded = decode_message(frame.value().payload);
          if (!decoded.ok() || static_cast<std::uint16_t>(decoded.value().type) != frame.value().type) {
            ++connection.session.frames_rejected;
            ++impl_->stats.frames_rejected;
            impl_->reply_error(connection, Code::malformed_input, "frame body could not be decoded");
            continue;
          }
          const Status handled = impl_->handle_message(connection, decoded.value());
          if (!handled.ok()) {
            impl_->handle_disconnect(connection, "message-handling-failed");
          }
        } else if (frame.status().code() != Code::not_found) {
          ++connection.session.frames_rejected;
          ++impl_->stats.frames_rejected;
          impl_->handle_disconnect(connection, "frame-decoder-failed");
        }
      }
    }
  }

  // 3. Liveness: a handshaken peer that stopped producing frames beyond the
  //    deadline is gone, and its evidence is invalidated rather than trusted.
  const Nanos now = impl_->clock->now_nanos();
  for (auto& connection : impl_->connections) {
    if (!connection.transport.open() || !connection.session.handshaken) {
      continue;
    }
    if (impl_->config.heartbeat_deadline > 0 &&
        now - connection.session.last_activity > impl_->config.heartbeat_deadline) {
      ++impl_->stats.liveness_expirations;
      impl_->handle_disconnect(connection, "publisher-liveness-expired");
    }
  }

  // 4. Reap closed connections.
  impl_->connections.erase(std::remove_if(impl_->connections.begin(), impl_->connections.end(),
                                          [](const Connection& connection) { return !connection.transport.open(); }),
                           impl_->connections.end());
  return Status{};
}

Result<Epoch> CoordinatorServer::advance_epoch() {
  OwnerId principal{};
  for (const auto& session : sessions()) {
    if (session.handshaken && session.role == Role::monitor) {
      continue;
    }
  }
  // The coordinator acts as the administrative principal for epoch advancement.
  Provenance provenance{};
  provenance.publisher = PublisherId::from_value(1);
  provenance.node = NodeId::from_value(1);
  provenance.incarnation = Incarnation::from_value(impl_->stats.coordinator_incarnation.valid()
                                                       ? impl_->stats.coordinator_incarnation.value()
                                                       : 1);
  provenance.epoch = impl_->fabric->epoch();
  provenance.sequence = Sequence::from_value(impl_->outbound_sequence + 1);
  provenance.boot = impl_->fabric->boot();
  (void)principal;

  auto owner = impl_->resource_principals.empty() ? OwnerId{} : impl_->resource_principals.begin()->second;
  auto advanced = impl_->fabric->advance_epoch(owner, provenance);
  if (!advanced.ok()) {
    return advanced.status();
  }
  Message notice{};
  notice.type = MessageType::epoch_advance;
  notice.epoch = advanced.value();
  for (auto& connection : impl_->connections) {
    if (!connection.transport.open()) {
      continue;
    }
    notice.sequence = ++impl_->outbound_sequence;
    send_message(connection, notice);
  }
  impl_->stats.epoch = advanced.value();
  return advanced;
}

bool CoordinatorServer::stopped() const noexcept { return impl_->stopped; }

void CoordinatorServer::stop() noexcept {
  impl_->stopped = true;
  for (auto& connection : impl_->connections) {
    connection.transport.close();
  }
  impl_->connections.clear();
  impl_->listener.close();
}

Fabric& CoordinatorServer::fabric() noexcept { return *impl_->fabric; }
const Fabric& CoordinatorServer::fabric() const noexcept { return *impl_->fabric; }

std::vector<WorkerSession> CoordinatorServer::sessions() const {
  std::vector<WorkerSession> out;
  out.reserve(impl_->connections.size());
  for (const auto& connection : impl_->connections) {
    WorkerSession session = connection.session;
    session.subscribed_queues = connection.queues.size();
    out.push_back(std::move(session));
  }
  return out;
}

CoordinatorStats CoordinatorServer::stats() const { return impl_->snapshot_stats(); }

std::size_t CoordinatorServer::active_connections() const { return impl_->connections.size(); }

Status CoordinatorServer::send_to(std::uint64_t session_id, const Message& message) {
  for (auto& connection : impl_->connections) {
    if (connection.id == session_id) {
      return send_message(connection, message);
    }
  }
  return Status{Code::not_found, "session is not connected"};
}

}  // namespace qf
