// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <cstdint>
#include <string>
#include <variant>

#include "qf/authority.hpp"
#include "qf/backend.hpp"
#include "qf/class_binding.hpp"
#include "qf/drain.hpp"
#include "qf/ids.hpp"
#include "qf/lifecycle.hpp"
#include "qf/limits.hpp"
#include "qf/occupancy.hpp"
#include "qf/provenance.hpp"
#include "qf/status.hpp"
#include "qf/threshold.hpp"

namespace qf {

/// Every state-changing operation is expressed as one of these mutations. The
/// kind determines the required capability, whether it is definitional (it
/// advances the queue generation) and whether it is durable authority or
/// dynamic evidence that must be revalidated.
enum class MutationKind : std::uint16_t {
  none = 0,
  declare_queue = 1,
  validate_queue = 2,
  activate_queue = 3,
  rebind_class = 4,
  set_thresholds = 5,
  transfer_ownership = 6,
  ingest_occupancy = 7,
  request_drain = 8,
  cancel_drain = 9,
  submit_drain_evidence = 10,
  quiesce_queue = 11,
  fence_queue = 12,
  unfence_queue = 13,
  fail_queue = 14,
  retire_queue = 15,
  acknowledge_backend = 16,
  register_backend = 17,
  no_op = 18,
};

const char* to_string(MutationKind value) noexcept;

/// Capability required to perform a mutation kind.
[[nodiscard]] Capability required_capability(MutationKind kind) noexcept;

/// Definitional mutations advance the queue definition generation. Nothing that
/// does not change the definition may invalidate generation-bound evidence.
[[nodiscard]] bool is_definitional(MutationKind kind) noexcept;

/// Dynamic mutations carry evidence that is authoritative only while fresh and
/// is never restored as live authority after a restart.
[[nodiscard]] bool is_dynamic(MutationKind kind) noexcept;

/// Durable mutations are journalled and must be durable before they are
/// acknowledged. Dynamic mutations are not part of the durable authority set.
[[nodiscard]] bool is_durable(MutationKind kind) noexcept;

struct DeclareQueuePayload {
  ResourceId resource{};
  std::string name{};
  std::uint32_t depth{0};
  ClassBinding cls{};
  PoolRef pool{};
  Thresholds thresholds{};
};

struct RebindClassPayload {
  ClassBinding cls{};
};

struct ThresholdsPayload {
  Thresholds thresholds{};
};

struct OwnershipPayload {
  OwnerId owner{};
};

struct OccupancyPayload {
  OccupancySample sample{};
};

struct DrainEvidencePayload {
  DrainEvidence evidence{};
};

struct FencePayload {
  bool fence{true};
  std::string reason{};
};

struct BackendAckPayload {
  BackendApplied applied{};
};

struct BackendRegistrationPayload {
  BackendId backend{};
  ResourceId resource{};
  Incarnation incarnation{};
};

using MutationPayload = std::variant<std::monostate,
                                     DeclareQueuePayload,
                                     RebindClassPayload,
                                     ThresholdsPayload,
                                     OwnershipPayload,
                                     OccupancyPayload,
                                     DrainEvidencePayload,
                                     FencePayload,
                                     BackendAckPayload,
                                     BackendRegistrationPayload>;

/// A complete mutation request: identity of the attempt, the target, the
/// authority context it claims, and the payload.
struct MutationRequest {
  AttemptId attempt{};
  MutationKind kind{MutationKind::none};
  QueueId queue{};
  Generation expected_generation{};  ///< 0 means "expect no queue yet" (declare).
  Epoch expected_epoch{};
  OwnerId principal{};
  Provenance provenance{};
  MutationPayload payload{};

  /// Structural validation only: no authority, lifecycle or generation checks.
  [[nodiscard]] Status validate() const;

  /// Effect fingerprint. Excludes attempt identity, provenance and claimed
  /// epoch so that a retry of the same effect is recognisable as a duplicate.
  [[nodiscard]] std::uint64_t fingerprint() const noexcept;
};

/// True when the payload alternative matches the mutation kind.
[[nodiscard]] bool payload_matches(MutationKind kind, const MutationPayload& payload) noexcept;

/// Outcome of a mutation attempt, including idempotent replay.
struct MutationOutcome {
  Status status{};
  QueueId queue{};
  Generation generation{};
  AttemptId attempt{};
  Generation definition_generation{};
  Lifecycle lifecycle{Lifecycle::declared};
  Applicability applicability{Applicability::none};
  bool replayed{false};
  bool durable{false};
  const char* rule{"unset"};

  [[nodiscard]] bool ok() const noexcept { return status.ok(); }
};

/// Bounded memo of recently applied attempts for one queue. This is what makes
/// duplicate mutation delivery idempotent rather than double-applied.
struct AttemptMemo {
  AttemptId attempt{};
  std::uint64_t fingerprint{0};
  MutationKind kind{MutationKind::none};
  Code outcome{Code::ok};
  Generation generation{};
  bool durable{false};
};

}  // namespace qf
