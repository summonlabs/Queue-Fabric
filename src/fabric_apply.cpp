// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
#include <algorithm>

#include "fabric_impl.hpp"

namespace qf {

OccupancyPolicy Fabric::Impl::occupancy_policy_for(const QueueRecord& queue) const noexcept {
  OccupancyPolicy policy = config.occupancy;
  const Thresholds effective = config.policy.resolve(queue.def.thresholds, queue.def.cls.id);
  const Nanos stale_after = config.policy.stale_after_for(effective, queue.def.cls.id);
  if (stale_after > 0) {
    policy.max_age = stale_after;
  }
  return policy;
}

const BackendIncarnation* Fabric::Impl::find_live_backend(ResourceId resource, BackendId backend) const noexcept {
  const ResourceRecord* record = find_resource(resource);
  if (record == nullptr || !backend.valid()) {
    return nullptr;
  }
  for (const auto& entry : record->backends) {
    if (entry.backend == backend && entry.alive) {
      return &entry;
    }
  }
  return nullptr;
}

void Fabric::Impl::record_rejection(QueueRecord* queue, const MutationRequest& request, const Status& status, const char* rule) {
  ++stats.mutations_rejected;
  if (queue != nullptr) {
    ++queue->rejected_count;
    queue->last_attempt = request.attempt;
    queue->last_status = status;
    queue->last_rule = rule;
    queue->remember(AttemptMemo{request.attempt, request.fingerprint(), request.kind, status.code(), queue->def.generation, false},
                    config.limits.max_attempt_memo);
  }
}

Result<MutationOutcome> Fabric::apply(const MutationRequest& request) {
  std::vector<Event> events;
  MutationOutcome outcome{};
  Status status;
  {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    status = impl_->apply_locked(request, outcome, events);
  }
  emit_events(impl_->sink, events, impl_->stats.events_emitted);
  if (!status.ok()) {
    return status;
  }
  return outcome;
}

Status Fabric::Impl::apply_locked(const MutationRequest& request, MutationOutcome& outcome, std::vector<Event>& events) {
  const Nanos now = monotonic_now();
  if (shutting_down) {
    return Status{Code::shutting_down, "fabric is shutting down"};
  }

  const Status structural = request.validate();
  if (!structural.ok()) {
    record_rejection(nullptr, request, structural, "structural-validation-failed");
    return structural;
  }

  if (request.kind == MutationKind::declare_queue) {
    return apply_declare_locked(request, now, outcome, events);
  }

  QueueRecord* queue = find_queue(request.queue);
  if (queue == nullptr) {
    const Status status{Code::not_found, "queue is not declared"};
    record_rejection(nullptr, request, status, "queue-not-found");
    return status;
  }
  return apply_existing_locked(*queue, request, now, outcome, events);
}

Status Fabric::Impl::apply_declare_locked(const MutationRequest& request,
                                          Nanos now,
                                          MutationOutcome& outcome,
                                          std::vector<Event>& events) {
  const auto& payload = std::get<DeclareQueuePayload>(request.payload);
  ResourceRecord* resource = find_resource(payload.resource);
  if (resource == nullptr) {
    const Status status{Code::not_found, "resource is not declared"};
    record_rejection(nullptr, request, status, "resource-not-found");
    return status;
  }

  // During replay the journaled request carries the epoch it was committed in;
  // the durable authority vector has already advanced to the current epoch.
  const Epoch presented_epoch = replaying ? resource->authority.epoch : request.expected_epoch;
  const AuthorityDecision decision = authorize(resource->authority, request.principal, Capability::declare_queue,
                                               presented_epoch, resource->applicability, Lifecycle::active);
  if (!decision.allowed) {
    const Status status{decision.code, decision.rule};
    record_rejection(nullptr, request, status, decision.rule);
    return status;
  }

  const auto name_it = queue_names.find(name_key(payload.resource, payload.name));
  if (name_it != queue_names.end()) {
    const QueueRecord* existing = find_queue(name_it->second);
    if (existing != nullptr && existing->def.declared_by.publisher == request.provenance.publisher &&
        existing->def.declared_by.sequence == request.provenance.sequence) {
      // Durable idempotency: the very same declaration was already committed.
      outcome.status = Status{};
      outcome.queue = existing->def.id;
      outcome.generation = existing->def.generation;
      outcome.definition_generation = existing->def.generation;
      outcome.attempt = request.attempt;
      outcome.lifecycle = existing->lifecycle;
      outcome.applicability = existing->applicability;
      outcome.replayed = true;
      outcome.durable = journal != nullptr;
      outcome.rule = "duplicate-declaration-identity";
      ++stats.mutations_replayed;
      Event event{};
      event.kind = EventKind::mutation_replayed;
      event.at = now;
      event.queue = existing->def.id;
      event.resource = payload.resource;
      event.mutation = request.kind;
      event.attempt = request.attempt;
      event.code = Code::replay_detected;
      event.detail = outcome.rule;
      events.push_back(std::move(event));
      return Status{};
    }
    const Status status{Code::already_exists, "queue name is already declared in this resource"};
    record_rejection(nullptr, request, status, "queue-name-already-declared");
    return status;
  }

  std::size_t resource_queues = 0;
  for (const auto& entry : queues) {
    if (entry.second.def.resource == payload.resource) {
      ++resource_queues;
    }
  }
  if (resource_queues >= config.limits.max_queues_per_resource) {
    const Status status{Code::capacity_exceeded, "queue capacity for this resource is exhausted"};
    record_rejection(nullptr, request, status, "resource-queue-capacity-exhausted");
    return status;
  }
  if (queues.size() >= config.limits.max_queues_total) {
    const Status status{Code::capacity_exceeded, "fabric queue capacity is exhausted"};
    record_rejection(nullptr, request, status, "fabric-queue-capacity-exhausted");
    return status;
  }

  const SchedulingClassDef* cls = resource->find_class(payload.cls.id);
  if (cls == nullptr) {
    const Status status{Code::not_found, "scheduling class is not declared"};
    record_rejection(nullptr, request, status, "class-not-declared");
    return status;
  }
  if (cls->generation != payload.cls.generation) {
    const Status status{Code::stale_generation, "class binding does not name the current class generation"};
    record_rejection(nullptr, request, status, "class-generation-mismatch");
    return status;
  }
  const BufferPoolDef* pool = resource->find_pool(payload.pool.id);
  if (pool == nullptr) {
    const Status status{Code::not_found, "buffer pool is not declared"};
    record_rejection(nullptr, request, status, "pool-not-declared");
    return status;
  }
  if (pool->generation != payload.pool.generation) {
    const Status status{Code::stale_generation, "pool reference does not name the current pool generation"};
    record_rejection(nullptr, request, status, "pool-generation-mismatch");
    return status;
  }
  if (!payload.thresholds.consistent()) {
    const Status status{Code::invalid_argument, "threshold ladder is inconsistent"};
    record_rejection(nullptr, request, status, "threshold-ladder-inconsistent");
    return status;
  }

  if (!replaying) {
    const ProvenanceClass provenance_class = provenance.classify(request.provenance);
    if (provenance_class != ProvenanceClass::accepted) {
      const Status status{provenance_class == ProvenanceClass::duplicate ? Code::replay_detected : Code::stale_incarnation,
                          to_string(provenance_class)};
      record_rejection(nullptr, request, status, "provenance-rejected");
      return status;
    }
  }

  QueueRecord record{};
  record.def.id = QueueId::from_value(next_queue_id);
  record.def.resource = payload.resource;
  record.def.name = payload.name;
  record.def.depth = payload.depth;
  record.def.cls = payload.cls;
  record.def.pool = payload.pool;
  record.def.thresholds = payload.thresholds;
  record.def.owner = request.principal;
  record.def.generation = Generation::from_value(1);
  record.def.declared_by = request.provenance;
  record.def.declared_at = now;

  if (journal != nullptr) {
    const Status journal_status = journal_commit_locked(request);
    if (!journal_status.ok()) {
      record_rejection(nullptr, request, journal_status, "journal-commit-failed");
      return journal_status;
    }
  } else if (config.durable) {
    const Status status{Code::not_durable, "durable fabric requires a journal"};
    record_rejection(nullptr, request, status, "journal-required");
    return status;
  }

  if (!replaying) {
    const Status observed = provenance.observe(request.provenance);
    if (!observed.ok()) {
      record_rejection(nullptr, request, observed, "provenance-rejected");
      return observed;
    }
  }

  const QueueId queue_id = record.def.id;
  ++next_queue_id;
  ++resource->queue_count;
  record.remember(AttemptMemo{request.attempt, request.fingerprint(), request.kind, Code::ok, record.def.generation,
                              journal != nullptr},
                  config.limits.max_attempt_memo);
  ++record.mutation_count;
  record.last_attempt = request.attempt;
  record.last_rule = "declared";
  queue_names.emplace(name_key(payload.resource, payload.name), queue_id);
  queues.emplace(queue_id, std::move(record));

  ++stats.mutations_applied;
  outcome.status = Status{};
  outcome.queue = queue_id;
  outcome.generation = Generation::from_value(1);
  outcome.definition_generation = Generation::from_value(1);
  outcome.attempt = request.attempt;
  outcome.lifecycle = Lifecycle::declared;
  outcome.applicability = Applicability::none;
  outcome.durable = journal != nullptr;
  outcome.rule = "declared";

  Event event{};
  event.kind = EventKind::queue_declared;
  event.at = now;
  event.queue = queue_id;
  event.resource = payload.resource;
  event.mutation = request.kind;
  event.attempt = request.attempt;
  event.generation = outcome.generation;
  event.lifecycle = Lifecycle::declared;
  event.detail = "queue declared";
  events.push_back(std::move(event));
  return Status{};
}

Status Fabric::Impl::apply_existing_locked(QueueRecord& queue,
                                           const MutationRequest& request,
                                           Nanos now,
                                           MutationOutcome& outcome,
                                           std::vector<Event>& events) {
  // 1. Idempotency: an attempt already recorded returns its original outcome.
  if (const AttemptMemo* memo = queue.find_attempt(request.attempt)) {
    if (memo->fingerprint != request.fingerprint()) {
      const Status status{Code::conflict, "attempt identity reused with different content"};
      record_rejection(&queue, request, status, "attempt-identity-conflict");
      return status;
    }
    ++queue.replayed_count;
    ++stats.mutations_replayed;
    if (memo->outcome != Code::ok) {
      const Status status{memo->outcome, "idempotent replay of a rejected attempt"};
      queue.last_attempt = request.attempt;
      queue.last_status = status;
      queue.last_rule = "idempotent-replay-of-rejection";
      return status;
    }
    outcome.status = Status{};
    outcome.queue = queue.def.id;
    outcome.generation = queue.def.generation;
    outcome.definition_generation = queue.def.generation;
    outcome.attempt = request.attempt;
    outcome.lifecycle = queue.lifecycle;
    outcome.applicability = queue.applicability;
    outcome.replayed = true;
    outcome.durable = memo->durable;
    outcome.rule = "idempotent-replay";
    Event event{};
    event.kind = EventKind::mutation_replayed;
    event.at = now;
    event.queue = queue.def.id;
    event.resource = queue.def.resource;
    event.mutation = request.kind;
    event.attempt = request.attempt;
    event.code = Code::replay_detected;
    event.generation = queue.def.generation;
    event.lifecycle = queue.lifecycle;
    event.detail = outcome.rule;
    events.push_back(std::move(event));
    return Status{};
  }

  // 2. Authority, epoch, fence and lifecycle terminality.
  ResourceRecord* resource = find_resource(queue.def.resource);
  if (resource == nullptr) {
    const Status status{Code::internal, "queue references an unknown resource"};
    record_rejection(&queue, request, status, "resource-index-corrupt");
    return status;
  }
  const Capability required = required_capability(request.kind);
  const Epoch presented_epoch = replaying ? resource->authority.epoch : request.expected_epoch;
  const AuthorityDecision decision = authorize(resource->authority, request.principal, required, presented_epoch,
                                               queue.applicability, queue.lifecycle);
  if (!decision.allowed) {
    const Status status{decision.code, decision.rule};
    record_rejection(&queue, request, status, decision.rule);
    return status;
  }

  // 3. Provenance: replay, stale incarnation, foreign boot and lagging epoch are
  //    refused before any state is touched.
  if (!replaying) {
    const ProvenanceClass provenance_class = provenance.classify(request.provenance);
    if (provenance_class != ProvenanceClass::accepted) {
      Code code = Code::stale_incarnation;
      const char* rule = "provenance-rejected";
      switch (provenance_class) {
        case ProvenanceClass::duplicate: code = Code::replay_detected; rule = "duplicate-sequence-without-memo"; break;
        case ProvenanceClass::stale_incarnation: code = Code::stale_incarnation; rule = "publisher-incarnation-superseded"; break;
        case ProvenanceClass::stale_boot: code = Code::stale_boot; rule = "publisher-boot-mismatch"; break;
        case ProvenanceClass::stale_epoch: code = Code::stale_epoch; rule = "publisher-epoch-behind"; break;
        case ProvenanceClass::capacity_exceeded: code = Code::capacity_exceeded; rule = "publisher-table-full"; break;
        case ProvenanceClass::invalid: code = Code::malformed_input; rule = "provenance-incomplete"; break;
        case ProvenanceClass::accepted: break;
      }
      const Status status{code, rule};
      record_rejection(&queue, request, status, rule);
      return status;
    }
    const Status observed = provenance.observe(request.provenance);
    if (!observed.ok()) {
      record_rejection(&queue, request, observed, "provenance-rejected");
      return observed;
    }
  }

  // 4. Exact-generation binding: a mutation that names a different generation
  //    than the one that is current now is stale work.
  if (!request.expected_generation.valid()) {
    const Status status{Code::invalid_argument, "expected queue generation is required"};
    record_rejection(&queue, request, status, "expected-generation-missing");
    return status;
  }
  if (request.expected_generation != queue.def.generation) {
    const Status status{Code::stale_generation, "expected generation is not the current queue generation"};
    record_rejection(&queue, request, status, "expected-generation-mismatch");
    return status;
  }
  if (!replaying && request.expected_epoch != epoch) {
    const Status status{Code::stale_epoch, "expected epoch is not the current coordinator epoch"};
    record_rejection(&queue, request, status, "expected-epoch-mismatch");
    return status;
  }

  // 5. Kind-specific work on a copy for durable mutations, in place for dynamic
  //    evidence (which is not part of the durable authority set).
  const bool durable_kind = is_durable(request.kind);
  QueueRecord working{};
  QueueRecord* target = &queue;
  if (durable_kind) {
    working = queue;
    target = &working;
  }

  const Lifecycle before_lifecycle = queue.lifecycle;
  const Generation before_generation = queue.def.generation;
  const char* rule = "applied";
  const Status applied = apply_kind_locked(*target, request, now, outcome, rule, events);
  if (!applied.ok()) {
    record_rejection(&queue, request, applied, rule);
    return applied;
  }

  if (durable_kind) {
    if (journal != nullptr) {
      const Status journal_status = journal_commit_locked(request);
      if (!journal_status.ok()) {
        record_rejection(&queue, request, journal_status, "journal-commit-failed");
        return journal_status;
      }
    } else if (config.durable) {
      const Status status{Code::not_durable, "durable fabric requires a journal"};
      record_rejection(&queue, request, status, "journal-required");
      return status;
    }
    queue = std::move(working);
  }

  QueueRecord& published = queue;
  ++published.mutation_count;
  published.last_attempt = request.attempt;
  published.last_status = Status{};
  published.last_rule = rule;
  published.remember(AttemptMemo{request.attempt, request.fingerprint(), request.kind, Code::ok, published.def.generation,
                                 durable_kind && journal != nullptr},
                     config.limits.max_attempt_memo);
  ++stats.mutations_applied;
  if (before_lifecycle != published.lifecycle) {
    ++stats.lifecycle_transitions;
  }
  if (before_generation != published.def.generation) {
    ++published.staleness_events;
    ++stats.staleness_events;
  }

  outcome.status = Status{};
  outcome.queue = published.def.id;
  outcome.generation = published.def.generation;
  outcome.definition_generation = published.def.generation;
  outcome.attempt = request.attempt;
  outcome.lifecycle = published.lifecycle;
  outcome.applicability = published.applicability;
  outcome.replayed = false;
  outcome.durable = durable_kind && journal != nullptr;
  outcome.rule = rule;

  if (!replaying) {
    Event event{};
    event.kind = EventKind::mutation_applied;
    event.at = now;
    event.queue = published.def.id;
    event.resource = published.def.resource;
    event.mutation = request.kind;
    event.attempt = request.attempt;
    event.code = Code::ok;
    event.generation = published.def.generation;
    event.lifecycle = published.lifecycle;
    event.detail = rule;
    events.push_back(std::move(event));
  }
  return Status{};
}

}  // namespace qf
