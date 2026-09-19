// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
#include "qf/drain.hpp"

namespace qf {

const char* to_string(DrainPhase value) noexcept {
  switch (value) {
    case DrainPhase::idle: return "idle";
    case DrainPhase::requested: return "requested";
    case DrainPhase::evidence_pending: return "evidence_pending";
    case DrainPhase::completed: return "completed";
  }
  return "unknown";
}

DrainAssessment assess_drain(const DrainState& drain,
                             const OccupancyState& occupancy,
                             const BackendIncarnation* live_backend,
                             Epoch current_epoch,
                             const OccupancyPolicy& policy,
                             Nanos now) noexcept {
  if (drain.phase == DrainPhase::idle) {
    return DrainAssessment{false, Code::drain_incomplete, "no-drain-requested"};
  }
  if (drain.phase == DrainPhase::completed) {
    return DrainAssessment{true, Code::ok, "drain-already-proven-complete"};
  }
  if (!drain.evidence.revalidated) {
    return DrainAssessment{false, Code::stale_occupancy, "drain-evidence-requires-revalidation-after-restart"};
  }
  if (drain.evidence.records == 0) {
    return DrainAssessment{false, Code::drain_incomplete, "no-drain-evidence-recorded"};
  }
  const BackendObservation& backend = drain.evidence.backend;
  if (!backend.request_accepted) {
    return DrainAssessment{false, Code::drain_incomplete, "backend-did-not-accept-drain-request"};
  }
  if (!backend.reported_drained) {
    return DrainAssessment{false, Code::drain_incomplete, "backend-has-not-reported-drained"};
  }
  if (backend.epoch != current_epoch) {
    return DrainAssessment{false, Code::stale_epoch, "drain-evidence-epoch-superseded"};
  }
  if (live_backend == nullptr) {
    return DrainAssessment{false, Code::stale_incarnation, "drain-evidence-names-unknown-backend"};
  }
  if (!live_backend->alive) {
    return DrainAssessment{false, Code::stale_incarnation, "drain-evidence-backend-not-live"};
  }
  if (backend.backend != live_backend->backend || backend.backend_incarnation != live_backend->incarnation) {
    return DrainAssessment{false, Code::stale_incarnation, "drain-evidence-backend-incarnation-superseded"};
  }
  if (!drain.evidence.occupancy_zero_observed) {
    return DrainAssessment{false, Code::drain_incomplete, "no-zero-occupancy-observation"};
  }
  if (!occupancy.present) {
    return DrainAssessment{false, Code::stale_occupancy, "occupancy-evidence-absent"};
  }
  if (occupancy.freshness != Freshness::fresh) {
    return DrainAssessment{false, Code::stale_occupancy, "occupancy-evidence-not-fresh"};
  }
  if (occupancy.bytes != 0 || occupancy.packets != 0) {
    return DrainAssessment{false, Code::drain_incomplete, "occupancy-is-not-zero"};
  }
  if (occupancy.observed_at < drain.requested_at) {
    return DrainAssessment{false, Code::drain_incomplete, "zero-occupancy-observation-precedes-drain-request"};
  }
  if (policy.max_age > 0 && occupancy_age(now, occupancy.observed_at) > policy.max_age) {
    return DrainAssessment{false, Code::stale_occupancy, "zero-occupancy-observation-is-stale"};
  }
  return DrainAssessment{true, Code::ok, "drain-evidence-complete-and-verifiable"};
}

}  // namespace qf
