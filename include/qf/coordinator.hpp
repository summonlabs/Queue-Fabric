// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <vector>

#include "qf/clock_forward.hpp"
#include "qf/fabric.hpp"
#include "qf/frame.hpp"
#include "qf/ids.hpp"
#include "qf/journal.hpp"
#include "qf/limits.hpp"
#include "qf/protocol.hpp"
#include "qf/status.hpp"
#include "qf/transport.hpp"

namespace qf {

/// Configuration for the coordinator service.
struct CoordinatorConfig {
  std::string host{"127.0.0.1"};
  std::uint16_t port{0};  ///< 0 selects an ephemeral port.
  FabricConfig fabric{};
  /// Durable store path. When empty the coordinator keeps no durable state and
  /// the fabric must be configured non-durable.
  std::string journal_path{};
  bool sync_on_commit{true};
  /// Liveness deadline for a handshaken connection that has stopped sending.
  Nanos heartbeat_deadline{5000000000ll};
  std::size_t max_connections{limits::kMaxConnections};
  std::size_t max_frames_per_cycle{512};
  std::size_t receive_buffer_bytes{64u * 1024u};
};

/// Observed state of one connected peer.
struct WorkerSession {
  std::uint64_t id{0};
  std::string peer{};
  Role role{Role::monitor};
  bool handshaken{false};
  NodeId node{};
  Incarnation incarnation{};
  BootId boot{};
  Epoch epoch{};
  std::string name{};
  Nanos last_activity{0};
  std::uint64_t frames_in{0};
  std::uint64_t frames_out{0};
  std::uint64_t frames_rejected{0};
  std::uint64_t messages_handled{0};
  std::size_t subscribed_queues{0};
};

/// Coordinator over a real framed TCP transport.
///
/// The server is single threaded by design: one loop accepts connections,
/// decodes frames, admits work and enforces liveness, so there is no lock
/// ordering between transport state and fabric state and no callback is ever
/// invoked while fabric state is locked.
class CoordinatorServer {
 public:
  CoordinatorServer(CoordinatorConfig config, IClock& clock, IEventSink* sink = nullptr);
  ~CoordinatorServer();

  CoordinatorServer(const CoordinatorServer&) = delete;
  CoordinatorServer& operator=(const CoordinatorServer&) = delete;

  /// Binds and listens. Does not accept connections until run_once is called.
  Status start();

  [[nodiscard]] std::uint16_t port() const noexcept;

  /// One service cycle: accept, decode, admit, enforce liveness, reap.
  Status run_once(std::uint32_t accept_timeout_ms);

  /// Advances the coordinator epoch and notifies connected peers.
  Result<Epoch> advance_epoch();

  /// Stops accepting new work and closes every connection.
  void stop() noexcept;

  /// True once a peer asked the coordinator to shut down.
  [[nodiscard]] bool stopped() const noexcept;

  [[nodiscard]] Fabric& fabric() noexcept;
  [[nodiscard]] const Fabric& fabric() const noexcept;

  [[nodiscard]] std::vector<WorkerSession> sessions() const;
  [[nodiscard]] CoordinatorStats stats() const;
  [[nodiscard]] std::size_t active_connections() const;

  /// Sends a message to one session. Returns not_found when the peer is gone.
  Status send_to(std::uint64_t session_id, const Message& message);

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace qf
