// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <cstdint>
#include <string>

#include "qf/backend.hpp"
#include "qf/ids.hpp"
#include "qf/limits.hpp"
#include "qf/occupancy.hpp"
#include "qf/provenance.hpp"
#include "qf/time.hpp"

namespace qf {

/// Drain progress. A requested drain is never equivalent to a drained queue.
enum class DrainPhase : std::uint8_t {
  idle = 0,             ///< No drain in progress.
  requested = 1,        ///< Drain requested; no evidence yet.
  evidence_pending = 2, ///< Partial or unverified evidence received.
  completed = 3,        ///< Completion proven for the current attempt.
};

const char* to_string(DrainPhase value) noexcept;

/// Observation reported by a backend incarnation about drain progress.
struct BackendObservation {
  bool request_accepted{false};
  bool reported_drained{false};
  BackendId backend{};
  Incarnation backend_incarnation{};
  Epoch epoch{};
  Sequence sequence{};
  Nanos observed_at{0};
};

/// Evidence collected for a drain attempt. Completion requires all of it.
struct DrainEvidence {
  std::uint32_t records{0};
  BackendObservation backend{};
  bool occupancy_zero_observed{false};
  Nanos zero_observed_at{0};
  Sequence zero_sequence{};
  bool revalidated{true};  ///< False after restart: evidence must be recollected.
};

/// Durable drain state for a queue.
struct DrainState {
  DrainPhase phase{DrainPhase::idle};
  AttemptId attempt{};
  Nanos requested_at{0};
  Provenance requested_by{};
  DrainEvidence evidence{};

  [[nodiscard]] bool in_progress() const noexcept {
    return phase == DrainPhase::requested || phase == DrainPhase::evidence_pending;
  }
};

/// Outcome of evaluating whether a drain may be declared complete.
struct DrainAssessment {
  bool completable{false};
  Code code{Code::drain_incomplete};
  const char* rule{"unset"};
};

/// Evaluates drain completion against occupancy evidence, backend observations
/// and the live backend incarnation. Missing, stale or mismatched evidence
/// always leaves the queue incomplete.
[[nodiscard]] DrainAssessment assess_drain(const DrainState& drain,
                                           const OccupancyState& occupancy,
                                           const BackendIncarnation* live_backend,
                                           Epoch current_epoch,
                                           const OccupancyPolicy& policy,
                                           Nanos now) noexcept;

}  // namespace qf
