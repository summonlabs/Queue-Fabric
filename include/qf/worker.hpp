// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "qf/clock_forward.hpp"
#include "qf/ids.hpp"
#include "qf/mutation.hpp"
#include "qf/occupancy.hpp"
#include "qf/protocol.hpp"
#include "qf/status.hpp"
#include "qf/transport.hpp"

namespace qf {

/// Configuration for a publishing worker.
struct WorkerConfig {
  std::string host{"127.0.0.1"};
  std::uint16_t port{0};
  NodeId node{};
  Incarnation incarnation{};
  Epoch epoch{};
  BootId boot{};
  std::string name{};
  Role role{Role::worker};
  std::size_t receive_buffer_bytes{64u * 1024u};
};

/// Worker-side protocol client over a real framed TCP transport.
///
/// The client performs one request/response exchange at a time, handles
/// interleaved coordinator notices (epoch advance, fence notices) and never
/// reports a mutation as applied without a response from the coordinator.
class WorkerClient {
 public:
  WorkerClient(WorkerConfig config, IClock& clock);
  ~WorkerClient();

  WorkerClient(const WorkerClient&) = delete;
  WorkerClient& operator=(const WorkerClient&) = delete;

  Status connect();
  Result<HelloAck> handshake();
  [[nodiscard]] bool admitted() const;

  Status publish(const std::vector<OccupancySample>& samples);
  Status send_heartbeat();
  Result<MutationResponse> send_mutation(const MutationRequest& request);
  Result<CoordinatorStats> request_stats();
  Status send_shutdown(std::string_view reason);

  /// Handles any pending inbound messages. Returns immediately when idle.
  Status poll();

  void close() noexcept;

  [[nodiscard]] Epoch coordinator_epoch() const;
  [[nodiscard]] Sequence next_sequence() noexcept;
  [[nodiscard]] std::uint64_t frames_sent() const;
  [[nodiscard]] std::uint64_t frames_received() const;
  [[nodiscard]] std::uint64_t frames_rejected() const;
  [[nodiscard]] std::uint64_t epoch_notices() const;

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace qf
