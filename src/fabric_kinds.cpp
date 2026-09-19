// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
#include <algorithm>

#include "fabric_impl.hpp"

namespace qf {

namespace {

std::string bounded(std::string_view text, std::size_t limit) {
  return std::string(text.substr(0, std::min(text.size(), limit)));
}

}  // namespace

Status Fabric::Impl::apply_kind_locked(QueueRecord& target,
                                       const MutationRequest& request,
                                       Nanos now,
                                       MutationOutcome& outcome,
                                       const char*& rule,
                                       std::vector<Event>& events) {
  (void)outcome;  // the outcome is filled by the caller from the queue record
  const auto push_event = [&](EventKind kind, Code code, std::string_view detail) {
    Event event{};
    event.kind = kind;
    event.at = now;
    event.queue = target.def.id;
    event.resource = target.def.resource;
    event.mutation = request.kind;
    event.attempt = request.attempt;
    event.code = code;
    event.generation = target.def.generation;
    event.lifecycle = target.lifecycle;
    event.detail.assign(detail);
    events.push_back(std::move(event));
  };

  const auto require_transition = [&](Lifecycle to) -> Status {
    const TransitionDecision decision = check_transition(target.lifecycle, to);
    if (!decision.allowed) {
      rule = decision.rule;
      return Status{decision.code, decision.rule};
    }
    rule = decision.rule;
    return Status{};
  };

  const auto check_class_binding = [&](const ClassBinding& binding) -> Status {
    const ResourceRecord* resource = find_resource(target.def.resource);
    if (resource == nullptr) {
      return Status{Code::internal, "queue references an unknown resource"};
    }
    const SchedulingClassDef* cls = resource->find_class(binding.id);
    if (cls == nullptr) {
      return Status{Code::not_found, "scheduling class is not declared"};
    }
    if (cls->generation != binding.generation) {
      return Status{Code::stale_generation, "class generation is not the current class generation"};
    }
    return Status{};
  };

  const auto invalidate_for_definition =
      [&](std::string_view detail) {
        target.applicability = target.applicability | Applicability::stale;
        invalidate_occupancy(target.occupancy, Freshness::stale_generation);
        target.applied.known = false;
        push_event(EventKind::occupancy_rejected, Code::stale_generation, detail);
      };

  switch (request.kind) {
    case MutationKind::validate_queue: {
      if (target.lifecycle == Lifecycle::validated) {
        rule = "no-op-idempotent";
        return Status{};
      }
      const Status transition = require_transition(Lifecycle::validated);
      if (!transition.ok()) {
        return transition;
      }
      const Status binding = check_class_binding(target.def.cls);
      if (!binding.ok()) {
        rule = "class-binding-not-exact-generation";
        return binding;
      }
      target.lifecycle = Lifecycle::validated;
      push_event(EventKind::lifecycle_changed, Code::ok, "queue validated");
      return Status{};
    }

    case MutationKind::activate_queue: {
      if (target.lifecycle == Lifecycle::active) {
        rule = "no-op-idempotent";
        return Status{};
      }
      const Status transition = require_transition(Lifecycle::active);
      if (!transition.ok()) {
        return transition;
      }
      const Status binding = check_class_binding(target.def.cls);
      if (!binding.ok()) {
        rule = "class-binding-not-exact-generation";
        return binding;
      }
      if (!target.def.thresholds.consistent()) {
        rule = "threshold-ladder-inconsistent";
        return Status{Code::invalid_argument, rule};
      }
      target.lifecycle = Lifecycle::active;
      push_event(EventKind::lifecycle_changed, Code::ok, "queue activated");
      return Status{};
    }

    case MutationKind::rebind_class: {
      const auto& payload = std::get<RebindClassPayload>(request.payload);
      const Status binding = check_class_binding(payload.cls);
      if (!binding.ok()) {
        rule = "class-binding-not-exact-generation";
        return binding;
      }
      if (target.def.cls == payload.cls) {
        rule = "binding-unchanged";
        return Status{};
      }
      auto next = target.def.generation.next();
      if (!next.ok()) {
        rule = "generation-exhausted";
        return next.status();
      }
      target.def.cls = payload.cls;
      target.def.generation = next.value();
      invalidate_for_definition("class rebinding advanced the queue generation");
      rule = "class-rebound-generation-advanced";
      return Status{};
    }

    case MutationKind::set_thresholds: {
      const auto& payload = std::get<ThresholdsPayload>(request.payload);
      if (!payload.thresholds.consistent()) {
        rule = "threshold-ladder-inconsistent";
        return Status{Code::invalid_argument, rule};
      }
      if (payload.thresholds == target.def.thresholds) {
        rule = "thresholds-unchanged";
        return Status{};
      }
      auto next = target.def.generation.next();
      if (!next.ok()) {
        rule = "generation-exhausted";
        return next.status();
      }
      target.def.thresholds = payload.thresholds;
      target.def.generation = next.value();
      invalidate_for_definition("threshold update advanced the queue generation");
      rule = "thresholds-updated-generation-advanced";
      return Status{};
    }

    case MutationKind::transfer_ownership: {
      const auto& payload = std::get<OwnershipPayload>(request.payload);
      if (payload.owner == target.def.owner) {
        rule = "owner-unchanged";
        return Status{};
      }
      auto next = target.def.generation.next();
      if (!next.ok()) {
        rule = "generation-exhausted";
        return next.status();
      }
      target.def.owner = payload.owner;
      target.def.generation = next.value();
      invalidate_for_definition("ownership transfer advanced the queue generation");
      rule = "ownership-transferred-generation-advanced";
      return Status{};
    }

    case MutationKind::ingest_occupancy: {
      const auto& sample = std::get<OccupancyPayload>(request.payload).sample;
      if (sample.queue != target.def.id) {
        rule = "occupancy-queue-mismatch";
        return Status{Code::invalid_argument, rule};
      }
      if (sample.queue_generation != target.def.generation) {
        rule = "occupancy-bound-to-other-generation";
        return Status{Code::stale_generation, rule};
      }
      if (request.expected_epoch != epoch || sample.epoch != epoch) {
        rule = "occupancy-from-superseded-epoch";
        return Status{Code::stale_epoch, rule};
      }
      const OccupancyPolicy policy = occupancy_policy_for(target);
      if (sample.observed_at > now && policy.max_future_skew > 0 && sample.observed_at - now > policy.max_future_skew) {
        rule = "observation-time-ahead-of-clock";
        return Status{Code::stale_occupancy, rule};
      }
      // Sequences must increase within one publisher life. A newer incarnation
      // or a new boot identifier legitimately restarts the sequence space.
      const bool same_publisher_life = target.occupancy.present &&
                                       target.occupancy.provenance.publisher == sample.provenance.publisher &&
                                       target.occupancy.provenance.incarnation == sample.provenance.incarnation &&
                                       target.occupancy.provenance.boot == sample.provenance.boot;
      if (same_publisher_life && sample.sequence <= target.occupancy.sequence) {
        rule = "occupancy-regression-for-publisher";
        return Status{Code::stale_occupancy, rule};
      }
      const Thresholds effective = config.policy.resolve(target.def.thresholds, target.def.cls.id);
      target.occupancy.present = true;
      target.occupancy.bytes = sample.bytes;
      target.occupancy.packets = sample.packets;
      target.occupancy.observed_at = sample.observed_at;
      target.occupancy.sequence = sample.sequence;
      target.occupancy.epoch = sample.epoch;
      target.occupancy.provenance = sample.provenance;
      target.occupancy.queue_generation = sample.queue_generation;
      target.occupancy.freshness = Freshness::fresh;
      target.occupancy.age = occupancy_age(now, sample.observed_at);
      target.applicability = clear_flag(target.applicability, Applicability::stale);
      ++stats.occupancy_accepted;

      if (target.drain.in_progress()) {
        if (sample.bytes == 0 && sample.packets == 0) {
          target.drain.evidence.occupancy_zero_observed = true;
          target.drain.evidence.zero_observed_at = sample.observed_at;
          target.drain.evidence.zero_sequence = sample.sequence;
        } else if (target.drain.evidence.occupancy_zero_observed) {
          target.drain.evidence.occupancy_zero_observed = false;
        }
      }

      const bool bytes_over = effective.max_occupancy_bytes != 0 && sample.bytes > effective.max_occupancy_bytes;
      const bool packets_over = effective.max_packets != 0 && sample.packets > effective.max_packets;
      rule = "occupancy-observed";
      if (bytes_over || packets_over) {
        ++target.overflow_events;
        ++stats.occupancy_overflow;
        rule = bytes_over ? "observed-occupancy-above-configured-maximum" : "observed-packets-above-configured-maximum";
        push_event(EventKind::occupancy_overflow, Code::occupancy_overflow, rule);
      } else {
        push_event(EventKind::occupancy_ingested, Code::ok, "occupancy observed");
      }
      return Status{};
    }

    case MutationKind::request_drain: {
      if (target.lifecycle == Lifecycle::draining) {
        rule = "drain-already-requested";
        return Status{};
      }
      const Status transition = require_transition(Lifecycle::draining);
      if (!transition.ok()) {
        return transition;
      }
      target.lifecycle = Lifecycle::draining;
      target.drain = DrainState{};
      target.drain.phase = DrainPhase::requested;
      target.drain.attempt = request.attempt;
      target.drain.requested_at = now;
      target.drain.requested_by = request.provenance;
      rule = "drain-requested-not-drained";
      push_event(EventKind::drain_progress, Code::drain_incomplete, "drain requested");
      return Status{};
    }

    case MutationKind::cancel_drain: {
      if (target.lifecycle != Lifecycle::draining) {
        rule = "no-drain-in-progress";
        return Status{Code::drain_incomplete, rule};
      }
      const Status transition = require_transition(Lifecycle::active);
      if (!transition.ok()) {
        return transition;
      }
      target.lifecycle = Lifecycle::active;
      target.drain = DrainState{};
      rule = "drain-cancelled-by-revalidation";
      push_event(EventKind::drain_progress, Code::cancelled, rule);
      return Status{};
    }

    case MutationKind::submit_drain_evidence: {
      if (!target.drain.in_progress()) {
        rule = "no-drain-in-progress";
        return Status{Code::drain_incomplete, rule};
      }
      const auto& evidence = std::get<DrainEvidencePayload>(request.payload).evidence;
      if (evidence.records == 0) {
        rule = "drain-evidence-empty";
        return Status{Code::invalid_argument, rule};
      }
      if (evidence.records > config.policy.max_drain_evidence_records) {
        rule = "drain-evidence-count-out-of-range";
        return Status{Code::out_of_range, rule};
      }
      if (evidence.backend.backend.valid()) {
        const BackendIncarnation* live = find_live_backend(target.def.resource, evidence.backend.backend);
        if (live == nullptr) {
          rule = "drain-evidence-backend-not-live";
          return Status{Code::stale_incarnation, rule};
        }
        if (live->incarnation != evidence.backend.backend_incarnation) {
          rule = "drain-evidence-incarnation-superseded";
          return Status{Code::stale_incarnation, rule};
        }
      }
      if (evidence.backend.epoch.valid() && evidence.backend.epoch != epoch) {
        rule = "drain-evidence-epoch-superseded";
        return Status{Code::stale_epoch, rule};
      }
      target.drain.evidence = evidence;
      target.drain.evidence.revalidated = true;
      target.drain.phase = DrainPhase::evidence_pending;
      rule = "drain-evidence-recorded";
      push_event(EventKind::drain_progress, Code::drain_incomplete, rule);
      return Status{};
    }

    case MutationKind::quiesce_queue: {
      if (target.lifecycle == Lifecycle::quiesced) {
        rule = "no-op-idempotent";
        return Status{};
      }
      const Status transition = require_transition(Lifecycle::quiesced);
      if (!transition.ok()) {
        return transition;
      }
      const BackendIncarnation* live = find_live_backend(target.def.resource, target.drain.evidence.backend.backend);
      const DrainAssessment assessment =
          assess_drain(target.drain, target.occupancy, live, epoch, occupancy_policy_for(target), now);
      if (!assessment.completable) {
        rule = assessment.rule;
        return Status{assessment.code, assessment.rule};
      }
      if (config.policy.require_backend_ack_for_drain) {
        if (live == nullptr) {
          rule = "backend-acknowledgement-required-before-quiesce";
          return Status{Code::stale_incarnation, rule};
        }
        const BackendVerification verification =
            verify_backend_applied(target.applied, *live, epoch, target.def.generation, target.def.cls);
        if (!verification.verified) {
          rule = "backend-applied-state-not-verifiable";
          return Status{Code::backend_mismatch, rule};
        }
      }
      target.lifecycle = Lifecycle::quiesced;
      target.drain.phase = DrainPhase::completed;
      rule = "drain-evidence-complete";
      push_event(EventKind::lifecycle_changed, Code::ok, "queue quiesced with drain evidence");
      return Status{};
    }

    case MutationKind::fence_queue: {
      const auto& payload = std::get<FencePayload>(request.payload);
      if (target.fence.fenced) {
        rule = "already-fenced";
        return Status{};
      }
      target.fence.fenced = true;
      target.fence.token = ++fence_tokens;
      target.fence.epoch = epoch;
      target.fence.fenced_by = request.principal;
      target.fence.reason = bounded(payload.reason, limits::kMaxReasonChars);
      target.fence.fenced_at = now;
      target.applicability = target.applicability | Applicability::fenced;
      rule = "queue-fenced";
      push_event(EventKind::fence_changed, Code::ok, target.fence.reason);
      return Status{};
    }

    case MutationKind::unfence_queue: {
      if (!target.fence.fenced) {
        rule = "already-unfenced";
        return Status{};
      }
      target.fence.fenced = false;
      target.fence.reason.clear();
      target.applicability = clear_flag(target.applicability, Applicability::fenced);
      rule = "queue-unfenced";
      push_event(EventKind::fence_changed, Code::ok, rule);
      return Status{};
    }

    case MutationKind::fail_queue: {
      if (target.lifecycle == Lifecycle::failed) {
        rule = "already-failed";
        return Status{};
      }
      const Status transition = require_transition(Lifecycle::failed);
      if (!transition.ok()) {
        return transition;
      }
      target.lifecycle = Lifecycle::failed;
      target.failure_reason = bounded(std::get<FencePayload>(request.payload).reason, limits::kMaxReasonChars);
      rule = "queue-failed";
      push_event(EventKind::lifecycle_changed, Code::ok, target.failure_reason);
      return Status{};
    }

    case MutationKind::retire_queue: {
      if (target.lifecycle == Lifecycle::retired) {
        rule = "already-retired";
        return Status{};
      }
      if (target.lifecycle == Lifecycle::active) {
        rule = "active-must-drain-before-retire";
        return Status{Code::lifecycle_violation, rule};
      }
      const Status transition = require_transition(Lifecycle::retired);
      if (!transition.ok()) {
        return transition;
      }
      target.lifecycle = Lifecycle::retired;
      target.occupancy = OccupancyState{};
      target.drain = DrainState{};
      target.applicability = Applicability::none;
      rule = "queue-retired";
      push_event(EventKind::queue_retired, Code::ok, rule);
      return Status{};
    }

    case MutationKind::acknowledge_backend: {
      const auto& applied = std::get<BackendAckPayload>(request.payload).applied;
      if (!applied.known || !applied.backend.valid()) {
        rule = "acknowledgement-without-backend-identity";
        return Status{Code::invalid_argument, rule};
      }
      const BackendIncarnation* live = find_live_backend(target.def.resource, applied.backend);
      if (live == nullptr) {
        rule = "backend-incarnation-not-live";
        return Status{Code::stale_incarnation, rule};
      }
      const BackendVerification verification =
          verify_backend_applied(applied, *live, epoch, target.def.generation, target.def.cls);
      if (!verification.verified) {
        rule = verification.rule;
        return Status{verification.code, rule};
      }
      target.applied = applied;
      target.applicability = clear_flag(target.applicability, Applicability::stale);
      rule = "backend-acknowledgement-verified";
      push_event(EventKind::mutation_applied, Code::ok, rule);
      return Status{};
    }

    case MutationKind::no_op: {
      rule = "no-op";
      return Status{};
    }

    case MutationKind::register_backend:
    case MutationKind::none:
    default: {
      rule = "mutation-kind-not-applicable";
      return Status{Code::unsupported, rule};
    }
  }
}

Status Fabric::Impl::apply_replay_locked(const MutationRequest& request, MutationOutcome& outcome) {
  std::vector<Event> events;
  replaying = true;
  const Status status = apply_locked(request, outcome, events);
  replaying = false;
  return status;
}

Status Fabric::Impl::journal_commit_locked(const MutationRequest& request) {
  if (journal == nullptr) {
    return Status{Code::not_durable, "no journal configured"};
  }
  const Bytes encoded = encode_mutation(request);
  if (encoded.empty()) {
    return Status{Code::internal, "mutation could not be encoded"};
  }
  const Status begin = journal->append(JournalRecordType::begin_attempt, encoded);
  if (!begin.ok()) {
    return begin;
  }
  const Status commit = journal->append(JournalRecordType::commit_mutation, encoded);
  if (!commit.ok()) {
    return commit;
  }
  const Status sync = journal->sync();
  if (!sync.ok()) {
    return Status{Code::not_durable, "durable commit did not reach stable storage"};
  }
  stats.journal_records += 2;
  ++stats.journal_syncs;
  return Status{};
}

}  // namespace qf
