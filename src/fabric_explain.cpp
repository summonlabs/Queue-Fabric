// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
#include "fabric_impl.hpp"

namespace qf {

Result<Explanation> Fabric::explain(QueueId queue_id, OwnerId principal) const {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  const QueueRecord* queue = impl_->find_queue(queue_id);
  if (queue == nullptr) {
    return Status{Code::not_found, "queue is not declared"};
  }
  const ResourceRecord* resource = impl_->find_resource(queue->def.resource);
  const Nanos now = impl_->monotonic_now();
  const OccupancyPolicy policy = impl_->occupancy_policy_for(*queue);

  Explanation explanation{};
  explanation.queue = queue->def.id;
  explanation.resource = queue->def.resource;
  explanation.name = queue->def.name;
  explanation.lifecycle = queue->lifecycle;
  explanation.applicability = queue->applicability;
  explanation.generation = queue->def.generation;
  explanation.cls = queue->def.cls;
  explanation.pool = queue->def.pool;
  explanation.owner = queue->def.owner;

  if (resource != nullptr) {
    explanation.authority_epoch = resource->authority.epoch;
    const OwnerId who = principal.valid() ? principal : queue->def.owner;
    explanation.principal_capabilities = resource->authority.capabilities_of(who);
  }

  explanation.occupancy = queue->occupancy;
  explanation.occupancy.age = occupancy_age(now, queue->occupancy.observed_at);
  explanation.occupancy.freshness = classify_freshness(queue->occupancy, now, policy, queue->def.generation);

  explanation.thresholds = impl_->config.policy.resolve(queue->def.thresholds, queue->def.cls.id);
  explanation.threshold_eval = evaluate_thresholds(explanation.thresholds, explanation.occupancy.bytes,
                                                   explanation.occupancy.packets,
                                                   explanation.occupancy.freshness == Freshness::fresh);

  explanation.fence = queue->fence;
  explanation.drain = queue->drain;
  explanation.applied = queue->applied;

  const BackendIncarnation* live_evidence = impl_->find_live_backend(queue->def.resource, queue->drain.evidence.backend.backend);
  explanation.drain_assessment = assess_drain(queue->drain, explanation.occupancy, live_evidence, impl_->epoch, policy, now);

  const BackendId applied_backend =
      queue->applied.backend.valid() ? queue->applied.backend : queue->drain.evidence.backend.backend;
  const BackendIncarnation* live_applied = impl_->find_live_backend(queue->def.resource, applied_backend);
  if (live_applied != nullptr) {
    explanation.backend_verification =
        verify_backend_applied(queue->applied, *live_applied, impl_->epoch, queue->def.generation, queue->def.cls);
  } else {
    explanation.backend_verification =
        BackendVerification{false, Code::stale_incarnation, "backend-incarnation-not-live"};
  }

  for (const auto& pending : impl_->pending) {
    if (pending.queue == queue_id) {
      explanation.pending_attempt = pending.attempt;
      explanation.pending_kind = to_string(pending.kind);
      break;
    }
  }

  explanation.last_attempt = queue->last_attempt;
  explanation.last_rule = queue->last_rule;
  explanation.last_status = queue->last_status;
  explanation.mutation_count = queue->mutation_count;
  explanation.replayed_count = queue->replayed_count;
  explanation.rejected_count = queue->rejected_count;
  explanation.overflow_events = queue->overflow_events;
  explanation.staleness_events = queue->staleness_events;
  explanation.failure_reason = queue->failure_reason;

  if (queue->occupancy.present && explanation.occupancy.freshness != Freshness::fresh) {
    explanation.stale_reason = to_string(explanation.occupancy.freshness);
  } else if (has_flag(queue->applicability, Applicability::stale)) {
    explanation.stale_reason = "evidence-requires-revalidation";
  }

  if (queue->fence.fenced) {
    explanation.fence_reason = queue->fence.reason.empty() ? "fenced" : queue->fence.reason;
  } else if (!queue->failure_reason.empty()) {
    explanation.fence_reason.clear();
  }
  return explanation;
}

}  // namespace qf
