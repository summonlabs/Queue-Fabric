// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
#include "qf/mutation.hpp"

#include "qf/hash.hpp"

namespace qf {

const char* to_string(MutationKind value) noexcept {
  switch (value) {
    case MutationKind::none: return "none";
    case MutationKind::declare_queue: return "declare_queue";
    case MutationKind::validate_queue: return "validate_queue";
    case MutationKind::activate_queue: return "activate_queue";
    case MutationKind::rebind_class: return "rebind_class";
    case MutationKind::set_thresholds: return "set_thresholds";
    case MutationKind::transfer_ownership: return "transfer_ownership";
    case MutationKind::ingest_occupancy: return "ingest_occupancy";
    case MutationKind::request_drain: return "request_drain";
    case MutationKind::cancel_drain: return "cancel_drain";
    case MutationKind::submit_drain_evidence: return "submit_drain_evidence";
    case MutationKind::quiesce_queue: return "quiesce_queue";
    case MutationKind::fence_queue: return "fence_queue";
    case MutationKind::unfence_queue: return "unfence_queue";
    case MutationKind::fail_queue: return "fail_queue";
    case MutationKind::retire_queue: return "retire_queue";
    case MutationKind::acknowledge_backend: return "acknowledge_backend";
    case MutationKind::register_backend: return "register_backend";
    case MutationKind::no_op: return "no_op";
  }
  return "unknown";
}

Capability required_capability(MutationKind kind) noexcept {
  switch (kind) {
    case MutationKind::declare_queue: return Capability::declare_queue;
    case MutationKind::validate_queue: return Capability::validate_queue;
    case MutationKind::activate_queue: return Capability::activate_queue;
    case MutationKind::rebind_class: return Capability::rebind_class;
    case MutationKind::set_thresholds: return Capability::set_thresholds;
    case MutationKind::transfer_ownership: return Capability::transfer_ownership;
    case MutationKind::ingest_occupancy: return Capability::ingest_occupancy;
    case MutationKind::request_drain: return Capability::request_drain;
    case MutationKind::cancel_drain: return Capability::request_drain;
    case MutationKind::submit_drain_evidence: return Capability::submit_evidence;
    case MutationKind::quiesce_queue: return Capability::quiesce_queue;
    case MutationKind::fence_queue: return Capability::fence_queue;
    case MutationKind::unfence_queue: return Capability::unfence_queue;
    case MutationKind::fail_queue: return Capability::fail_queue;
    case MutationKind::retire_queue: return Capability::retire_queue;
    case MutationKind::acknowledge_backend: return Capability::acknowledge_backend;
    case MutationKind::register_backend: return Capability::declare_topology;
    case MutationKind::no_op:
    case MutationKind::none:
      return Capability::none;
  }
  return Capability::none;
}

bool is_definitional(MutationKind kind) noexcept {
  switch (kind) {
    case MutationKind::declare_queue:
    case MutationKind::rebind_class:
    case MutationKind::set_thresholds:
    case MutationKind::transfer_ownership:
      return true;
    default:
      return false;
  }
}

bool is_dynamic(MutationKind kind) noexcept {
  switch (kind) {
    case MutationKind::ingest_occupancy:
    case MutationKind::submit_drain_evidence:
    case MutationKind::acknowledge_backend:
      return true;
    default:
      return false;
  }
}

bool is_durable(MutationKind kind) noexcept { return !is_dynamic(kind) && kind != MutationKind::no_op && kind != MutationKind::none; }

bool payload_matches(MutationKind kind, const MutationPayload& payload) noexcept {
  switch (kind) {
    case MutationKind::declare_queue: return std::holds_alternative<DeclareQueuePayload>(payload);
    case MutationKind::rebind_class: return std::holds_alternative<RebindClassPayload>(payload);
    case MutationKind::set_thresholds: return std::holds_alternative<ThresholdsPayload>(payload);
    case MutationKind::transfer_ownership: return std::holds_alternative<OwnershipPayload>(payload);
    case MutationKind::ingest_occupancy: return std::holds_alternative<OccupancyPayload>(payload);
    case MutationKind::submit_drain_evidence: return std::holds_alternative<DrainEvidencePayload>(payload);
    case MutationKind::fence_queue:
    case MutationKind::unfence_queue:
    case MutationKind::fail_queue: return std::holds_alternative<FencePayload>(payload);
    case MutationKind::acknowledge_backend: return std::holds_alternative<BackendAckPayload>(payload);
    case MutationKind::register_backend: return std::holds_alternative<BackendRegistrationPayload>(payload);
    case MutationKind::validate_queue:
    case MutationKind::activate_queue:
    case MutationKind::request_drain:
    case MutationKind::cancel_drain:
    case MutationKind::quiesce_queue:
    case MutationKind::retire_queue:
    case MutationKind::no_op: return std::holds_alternative<std::monostate>(payload);
    case MutationKind::none: return false;
  }
  return false;
}

Status MutationRequest::validate() const {
  if (!attempt.valid()) {
    return Status{Code::invalid_argument, "attempt identity is required"};
  }
  if (kind == MutationKind::none) {
    return Status{Code::invalid_argument, "mutation kind is required"};
  }
  if (!provenance.valid()) {
    return Status{Code::malformed_input, "complete provenance is required"};
  }
  if (!principal.valid()) {
    return Status{Code::invalid_argument, "principal identity is required"};
  }
  if (!payload_matches(kind, payload)) {
    return Status{Code::invalid_argument, "payload does not match mutation kind"};
  }
  if (kind == MutationKind::declare_queue) {
    const auto& declare = std::get<DeclareQueuePayload>(payload);
    if (!declare.resource.valid()) {
      return Status{Code::invalid_argument, "declare requires a resource identity"};
    }
    if (queue.valid()) {
      return Status{Code::invalid_argument, "declare must not name an existing queue identity"};
    }
    if (declare.name.empty() || declare.name.size() > limits::kMaxNameChars) {
      return Status{Code::out_of_range, "queue name length is out of range"};
    }
    if (!declare.cls.valid()) {
      return Status{Code::invalid_argument, "declare requires an exact class binding"};
    }
    if (!declare.pool.valid()) {
      return Status{Code::invalid_argument, "declare requires a pool reference"};
    }
    if (!declare.thresholds.consistent()) {
      return Status{Code::invalid_argument, "threshold ladder is inconsistent"};
    }
  } else if (!queue.valid()) {
    return Status{Code::invalid_argument, "mutation requires a queue identity"};
  }

  if (kind == MutationKind::ingest_occupancy) {
    const auto& occupancy = std::get<OccupancyPayload>(payload);
    if (!occupancy.sample.structurally_valid()) {
      return Status{Code::malformed_input, "occupancy sample is structurally invalid"};
    }
  }
  if (kind == MutationKind::set_thresholds) {
    const auto& thresholds = std::get<ThresholdsPayload>(payload);
    if (!thresholds.thresholds.consistent()) {
      return Status{Code::invalid_argument, "threshold ladder is inconsistent"};
    }
  }
  if (kind == MutationKind::rebind_class) {
    const auto& rebind = std::get<RebindClassPayload>(payload);
    if (!rebind.cls.valid()) {
      return Status{Code::invalid_argument, "rebind requires an exact class binding"};
    }
  }
  if (kind == MutationKind::transfer_ownership) {
    const auto& ownership = std::get<OwnershipPayload>(payload);
    if (!ownership.owner.valid()) {
      return Status{Code::invalid_argument, "transfer requires a target owner"};
    }
  }
  return Status{};
}

std::uint64_t MutationRequest::fingerprint() const noexcept {
  std::uint64_t seed = 0x243F6A8885A308D3ull;
  hash_mix(seed, static_cast<std::uint64_t>(kind));
  hash_mix(seed, queue.value());
  hash_mix(seed, expected_generation.value());
  hash_mix(seed, principal.value());
  if (const auto* declare = std::get_if<DeclareQueuePayload>(&payload)) {
    hash_mix(seed, declare->resource.value());
    hash_mix(seed, std::string_view{declare->name});
    hash_mix(seed, static_cast<std::uint64_t>(declare->depth));
    hash_mix(seed, declare->cls.id.value());
    hash_mix(seed, declare->cls.generation.value());
    hash_mix(seed, declare->pool.id.value());
    hash_mix(seed, declare->pool.generation.value());
    hash_mix(seed, declare->pool.capacity_bytes);
    hash_mix(seed, declare->thresholds.low_watermark_bytes);
    hash_mix(seed, declare->thresholds.high_watermark_bytes);
    hash_mix(seed, declare->thresholds.max_occupancy_bytes);
    hash_mix(seed, declare->thresholds.max_packets);
    hash_mix(seed, static_cast<std::uint64_t>(declare->thresholds.occupancy_stale_after));
  } else if (const auto* rebind = std::get_if<RebindClassPayload>(&payload)) {
    hash_mix(seed, rebind->cls.id.value());
    hash_mix(seed, rebind->cls.generation.value());
  } else if (const auto* thresholds = std::get_if<ThresholdsPayload>(&payload)) {
    hash_mix(seed, thresholds->thresholds.low_watermark_bytes);
    hash_mix(seed, thresholds->thresholds.high_watermark_bytes);
    hash_mix(seed, thresholds->thresholds.max_occupancy_bytes);
    hash_mix(seed, thresholds->thresholds.max_packets);
    hash_mix(seed, static_cast<std::uint64_t>(thresholds->thresholds.occupancy_stale_after));
  } else if (const auto* ownership = std::get_if<OwnershipPayload>(&payload)) {
    hash_mix(seed, ownership->owner.value());
  } else if (const auto* occupancy = std::get_if<OccupancyPayload>(&payload)) {
    hash_mix(seed, occupancy->sample.bytes);
    hash_mix(seed, occupancy->sample.packets);
    hash_mix(seed, occupancy->sample.sequence.value());
    hash_mix(seed, occupancy->sample.provenance.fingerprint());
    hash_mix(seed, static_cast<std::uint64_t>(occupancy->sample.observed_at));
  } else if (const auto* evidence = std::get_if<DrainEvidencePayload>(&payload)) {
    hash_mix(seed, static_cast<std::uint64_t>(evidence->evidence.records));
    hash_mix(seed, evidence->evidence.backend.reported_drained);
    hash_mix(seed, evidence->evidence.backend.request_accepted);
    hash_mix(seed, evidence->evidence.occupancy_zero_observed);
    hash_mix(seed, evidence->evidence.zero_sequence.value());
  } else if (const auto* fence = std::get_if<FencePayload>(&payload)) {
    hash_mix(seed, fence->fence);
    hash_mix(seed, std::string_view{fence->reason});
  } else if (const auto* ack = std::get_if<BackendAckPayload>(&payload)) {
    hash_mix(seed, ack->applied.sequence.value());
    hash_mix(seed, ack->applied.backend.value());
    hash_mix(seed, ack->applied.backend_incarnation.value());
  } else if (const auto* backend = std::get_if<BackendRegistrationPayload>(&payload)) {
    hash_mix(seed, backend->backend.value());
    hash_mix(seed, backend->resource.value());
    hash_mix(seed, backend->incarnation.value());
  }
  return seed;
}

}  // namespace qf
