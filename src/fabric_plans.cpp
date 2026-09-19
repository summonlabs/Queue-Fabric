// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
#include <algorithm>

#include "fabric_impl.hpp"

namespace qf {

namespace {

/// Structurally valid provenance for durable markers that carry no publisher
/// identity of their own (abort/no-op completions).
Provenance marker_provenance(const BackendPlan& plan) {
  Provenance provenance{};
  provenance.publisher = PublisherId::from_value(plan.principal.valid() ? plan.principal.value() : 1);
  provenance.node = NodeId::from_value(plan.resource.value());
  provenance.incarnation = Incarnation::from_value(1);
  provenance.epoch = plan.epoch;
  provenance.sequence = Sequence::from_value(plan.attempt.counter);
  provenance.boot = BootId{1, 1};
  return provenance;
}

MutationRequest plan_marker(const BackendPlan& plan, const Provenance& provenance, MutationKind kind) {
  MutationRequest request{};
  request.kind = kind;
  request.attempt = plan.attempt;
  request.queue = plan.queue;
  request.expected_generation = plan.expected_generation;
  request.expected_epoch = plan.epoch;
  request.principal = plan.principal;
  request.provenance = provenance;
  if (kind == MutationKind::no_op) {
    request.payload = std::monostate{};
  } else {
    BackendAckPayload payload{};
    payload.applied = BackendApplied{};
    request.payload = std::move(payload);
  }
  return request;
}

}  // namespace

Result<BackendPlan> Fabric::plan_backend_apply(QueueId queue_id,
                                               Generation expected,
                                               OwnerId principal,
                                               const Provenance& provenance) {
  std::vector<Event> events;
  Result<BackendPlan> result = Status{Code::internal, "unset"};
  {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    if (impl_->shutting_down) {
      return Status{Code::shutting_down, "fabric is shutting down"};
    }
    if (!impl_->healthy) {
      return Status{Code::not_durable, "fabric is not healthy"};
    }
    if (!provenance.valid()) {
      return Status{Code::malformed_input, "complete provenance is required"};
    }
    QueueRecord* queue = impl_->find_queue(queue_id);
    if (queue == nullptr) {
      return Status{Code::not_found, "queue is not declared"};
    }
    ResourceRecord* resource = impl_->find_resource(queue->def.resource);
    if (resource == nullptr) {
      return Status{Code::internal, "queue references an unknown resource"};
    }
    const AuthorityDecision decision = authorize(resource->authority, principal, Capability::acknowledge_backend,
                                                 impl_->epoch, queue->applicability, queue->lifecycle);
    if (!decision.allowed) {
      return Status{decision.code, decision.rule};
    }
    if (!expected.valid() || expected != queue->def.generation) {
      return Status{Code::stale_generation, "expected-generation-mismatch"};
    }
    const SchedulingClassDef* cls = resource->find_class(queue->def.cls.id);
    if (cls == nullptr || cls->generation != queue->def.cls.generation) {
      return Status{Code::stale_generation, "class-binding-not-exact-generation"};
    }
    if (impl_->plans.size() >= impl_->config.limits.max_pending_attempts) {
      return Status{Code::capacity_exceeded, "backend reservations are exhausted"};
    }
    const ProvenanceClass replay_class = impl_->provenance.classify(provenance);
    if (replay_class != ProvenanceClass::accepted) {
      return Status{replay_class == ProvenanceClass::duplicate ? Code::replay_detected : Code::stale_incarnation,
                    to_string(replay_class)};
    }

    BackendPlan plan{};
    plan.attempt = AttemptId{provenance.publisher, provenance.sequence.value()};
    plan.queue = queue_id;
    plan.resource = queue->def.resource;
    plan.expected_generation = expected;
    plan.cls = queue->def.cls;
    plan.thresholds = impl_->config.policy.resolve(queue->def.thresholds, queue->def.cls.id);
    plan.principal = principal;
    plan.epoch = impl_->epoch;
    plan.planned_at = impl_->monotonic_now();

    if (impl_->plans.find(plan.attempt) != impl_->plans.end()) {
      return Status{Code::conflict, "attempt identity is already reserved"};
    }

    const Status observed = impl_->provenance.observe(provenance);
    if (!observed.ok()) {
      return observed;
    }

    // Phase 1 durability: the reservation is recorded before any external
    // effect can happen, so a crash leaves a detectable unfinished attempt.
    if (impl_->journal != nullptr) {
      const MutationRequest marker = plan_marker(plan, provenance, MutationKind::acknowledge_backend);
      const Bytes encoded = encode_mutation(marker);
      if (encoded.empty()) {
        return Status{Code::internal, "reservation could not be encoded"};
      }
      const Status append = impl_->journal->append(JournalRecordType::begin_attempt, encoded);
      if (!append.ok()) {
        return append;
      }
      const Status sync = impl_->journal->sync();
      if (!sync.ok()) {
        impl_->healthy = false;
        return Status{Code::not_durable, "reservation did not reach stable storage"};
      }
      ++impl_->stats.journal_records;
      ++impl_->stats.journal_syncs;
    } else if (impl_->config.durable) {
      return Status{Code::not_durable, "durable fabric requires a journal"};
    }

    impl_->plans.emplace(plan.attempt, plan);
    impl_->pending.push_back(
        PendingAttempt{plan.attempt, queue_id, MutationKind::acknowledge_backend, plan.planned_at});
    if (impl_->pending.size() > impl_->config.limits.max_pending_attempts) {
      impl_->pending.erase(impl_->pending.begin());
    }

    Event event{};
    event.kind = EventKind::mutation_applied;
    event.at = plan.planned_at;
    event.queue = queue_id;
    event.resource = plan.resource;
    event.mutation = MutationKind::acknowledge_backend;
    event.attempt = plan.attempt;
    event.code = Code::ok;
    event.generation = expected;
    event.detail = "backend apply reserved";
    events.push_back(std::move(event));
    result = plan;
  }
  emit_events(impl_->sink, events, impl_->stats.events_emitted);
  return result;
}

Result<MutationOutcome> Fabric::commit_backend_apply(const BackendPlan& plan,
                                                     const BackendApplied& applied,
                                                     const Provenance& provenance) {
  std::vector<Event> events;
  MutationOutcome outcome{};
  Status status;
  {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    if (impl_->shutting_down) {
      return Status{Code::shutting_down, "fabric is shutting down"};
    }
    const auto it = impl_->plans.find(plan.attempt);
    if (it == impl_->plans.end()) {
      return Status{Code::not_found, "no such backend reservation"};
    }
    QueueRecord* queue = impl_->find_queue(plan.queue);
    if (queue == nullptr) {
      return Status{Code::not_found, "queue is not declared"};
    }
    if (queue->def.generation != plan.expected_generation) {
      return Status{Code::stale_generation, "queue definition changed while the apply was planned"};
    }
    if (!provenance.valid()) {
      return Status{Code::malformed_input, "complete provenance is required"};
    }

    MutationRequest request{};
    request.kind = MutationKind::acknowledge_backend;
    request.queue = plan.queue;
    request.expected_generation = plan.expected_generation;
    request.expected_epoch = impl_->epoch;
    request.principal = plan.principal;
    request.provenance = provenance;
    request.attempt = plan.attempt;
    BackendAckPayload payload{};
    payload.applied = applied;
    request.payload = std::move(payload);

    // The commit marker is durable before the acknowledgement becomes
    // authoritative in memory.
    if (impl_->journal != nullptr) {
      const Bytes encoded = encode_mutation(request);
      if (encoded.empty()) {
        return Status{Code::internal, "commit marker could not be encoded"};
      }
      const Status append = impl_->journal->append(JournalRecordType::commit_mutation, encoded);
      if (!append.ok()) {
        return append;
      }
      const Status sync = impl_->journal->sync();
      if (!sync.ok()) {
        impl_->healthy = false;
        return Status{Code::not_durable, "commit marker did not reach stable storage"};
      }
      ++impl_->stats.journal_records;
      ++impl_->stats.journal_syncs;
    } else if (impl_->config.durable) {
      return Status{Code::not_durable, "durable fabric requires a journal"};
    }

    status = impl_->apply_existing_locked(*queue, request, impl_->monotonic_now(), outcome, events);
    if (status.ok()) {
      impl_->plans.erase(it);
      for (auto pending = impl_->pending.begin(); pending != impl_->pending.end(); ++pending) {
        if (pending->attempt == plan.attempt) {
          impl_->pending.erase(pending);
          break;
        }
      }
    }
  }
  emit_events(impl_->sink, events, impl_->stats.events_emitted);
  if (!status.ok()) {
    return status;
  }
  return outcome;
}

Status Fabric::abort_backend_apply(const BackendPlan& plan, std::string_view reason) {
  std::vector<Event> events;
  {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    const auto it = impl_->plans.find(plan.attempt);
    if (it == impl_->plans.end()) {
      return Status{Code::not_found, "no such backend reservation"};
    }
    if (impl_->journal != nullptr) {
      const MutationRequest marker = plan_marker(plan, marker_provenance(plan), MutationKind::no_op);
      const Bytes encoded = encode_mutation(marker);
      if (encoded.empty()) {
        return Status{Code::internal, "abort marker could not be encoded"};
      }
      const Status append = impl_->journal->append(JournalRecordType::commit_mutation, encoded);
      if (!append.ok()) {
        return append;
      }
      const Status sync = impl_->journal->sync();
      if (!sync.ok()) {
        impl_->healthy = false;
        return Status{Code::not_durable, "abort marker did not reach stable storage"};
      }
      ++impl_->stats.journal_records;
      ++impl_->stats.journal_syncs;
    }
    impl_->plans.erase(it);
    for (auto pending = impl_->pending.begin(); pending != impl_->pending.end(); ++pending) {
      if (pending->attempt == plan.attempt) {
        impl_->pending.erase(pending);
        break;
      }
    }
    Event event{};
    event.kind = EventKind::mutation_rejected;
    event.at = impl_->monotonic_now();
    event.queue = plan.queue;
    event.resource = plan.resource;
    event.mutation = MutationKind::acknowledge_backend;
    event.attempt = plan.attempt;
    event.code = Code::cancelled;
    event.detail = std::string("backend apply aborted: ").append(reason.substr(0, limits::kMaxReasonChars));
    events.push_back(std::move(event));
  }
  emit_events(impl_->sink, events, impl_->stats.events_emitted);
  return Status{};
}

}  // namespace qf
