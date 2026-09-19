// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
#include "qf/authority.hpp"

namespace qf {

const char* to_string(Capability value) noexcept {
  switch (value) {
    case Capability::none: return "none";
    case Capability::declare_topology: return "declare_topology";
    case Capability::declare_queue: return "declare_queue";
    case Capability::validate_queue: return "validate_queue";
    case Capability::activate_queue: return "activate_queue";
    case Capability::rebind_class: return "rebind_class";
    case Capability::set_thresholds: return "set_thresholds";
    case Capability::ingest_occupancy: return "ingest_occupancy";
    case Capability::request_drain: return "request_drain";
    case Capability::submit_evidence: return "submit_evidence";
    case Capability::quiesce_queue: return "quiesce_queue";
    case Capability::retire_queue: return "retire_queue";
    case Capability::fence_queue: return "fence_queue";
    case Capability::unfence_queue: return "unfence_queue";
    case Capability::fail_queue: return "fail_queue";
    case Capability::transfer_ownership: return "transfer_ownership";
    case Capability::acknowledge_backend: return "acknowledge_backend";
    case Capability::admin: return "admin";
    case Capability::all: return "all";
  }
  return "unknown";
}

AuthorityDecision authorize(const AuthorityVector& authority,
                            OwnerId principal,
                            Capability required,
                            Epoch presented_epoch,
                            Applicability applicability,
                            Lifecycle lifecycle) noexcept {
  AuthorityDecision decision{};
  decision.required = required;

  // Stage 1: the presented epoch must be the epoch the authority vector belongs
  // to. Authority from a superseded epoch is not authority.
  if (!presented_epoch.valid() || presented_epoch != authority.epoch) {
    decision.allowed = false;
    decision.code = Code::stale_epoch;
    decision.rule = "authority-epoch-mismatch";
    return decision;
  }

  // Stage 2: the principal must hold the required capability.
  const std::uint32_t mask = authority.capabilities_of(principal);
  if (!has_capability(mask, required)) {
    decision.allowed = false;
    decision.code = Code::authority_denied;
    decision.rule = "capability-not-granted";
    return decision;
  }

  // Stage 3: a fenced queue refuses every mutation except an unfence or an
  // explicit administrative override.
  if (has_flag(applicability, Applicability::fenced) && required != Capability::unfence_queue &&
      required != Capability::admin) {
    decision.allowed = false;
    decision.code = Code::fence_violation;
    decision.rule = "fenced-queue-refuses-fresh-mutation";
    return decision;
  }

  // Stage 4: retired and failed queues are closed to everything but retirement
  // bookkeeping.
  if ((lifecycle == Lifecycle::retired || lifecycle == Lifecycle::failed) && required != Capability::retire_queue &&
      required != Capability::admin) {
    decision.allowed = false;
    decision.code = Code::lifecycle_violation;
    decision.rule = lifecycle == Lifecycle::retired ? "retired-is-terminal" : "failed-queue-accepts-only-cleanup";
    return decision;
  }

  decision.allowed = true;
  decision.code = Code::ok;
  decision.rule = "authorized";
  return decision;
}

}  // namespace qf
