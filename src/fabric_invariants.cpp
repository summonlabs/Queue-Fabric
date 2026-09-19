// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
#include <algorithm>

#include "fabric_impl.hpp"

namespace qf {

std::size_t Fabric::validate_invariants(std::vector<std::string>& violations) const {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  const std::size_t before = violations.size();

  if (impl_->queues.size() > impl_->config.limits.max_queues_total) {
    violations.push_back("INV-CAPACITY: queue count exceeds the configured maximum");
  }
  if (impl_->resources.size() > impl_->config.limits.max_resources) {
    violations.push_back("INV-CAPACITY: resource count exceeds the configured maximum");
  }
  if (impl_->pending.size() > impl_->config.limits.max_pending_attempts) {
    violations.push_back("INV-CAPACITY: pending attempts exceed the configured maximum");
  }

  std::unordered_map<ResourceId, std::size_t> per_resource;
  std::unordered_map<std::string, std::size_t> names;

  for (const auto& entry : impl_->queues) {
    const QueueRecord& queue = entry.second;
    const std::string tag = "queue " + queue.def.id.to_string();

    if (queue.def.id != entry.first) {
      violations.push_back("INV-IDENTITY: queue record key does not match the queue identity: " + tag);
    }
    if (queue.def.name.empty() || queue.def.name.size() > limits::kMaxNameChars) {
      violations.push_back("INV-IDENTITY: queue name is empty or over the configured bound: " + tag);
    }
    if (!queue.def.generation.valid()) {
      violations.push_back("INV-IDENTITY: queue generation is not valid: " + tag);
    }
    if (!queue.def.cls.valid()) {
      violations.push_back("INV-BINDING: class binding is not valid: " + tag);
    }

    const ResourceRecord* resource = impl_->find_resource(queue.def.resource);
    if (resource == nullptr) {
      violations.push_back("INV-TOPOLOGY: queue references an unknown resource: " + tag);
      continue;
    }
    ++per_resource[queue.def.resource];

    const SchedulingClassDef* cls = resource->find_class(queue.def.cls.id);
    if (cls == nullptr) {
      violations.push_back("INV-BINDING: queue references an unknown scheduling class: " + tag);
    } else if (cls->generation != queue.def.cls.generation) {
      violations.push_back("INV-BINDING: class binding generation is not current: " + tag);
    }
    const BufferPoolDef* pool = resource->find_pool(queue.def.pool.id);
    if (pool == nullptr) {
      violations.push_back("INV-BINDING: queue references an unknown buffer pool: " + tag);
    } else if (pool->generation != queue.def.pool.generation) {
      violations.push_back("INV-BINDING: pool reference generation is not current: " + tag);
    }
    if (!queue.def.thresholds.consistent()) {
      violations.push_back("INV-THRESHOLD: threshold ladder is inconsistent: " + tag);
    }

    const std::string key = Impl::name_key(queue.def.resource, queue.def.name);
    if (++names[key] > 1) {
      violations.push_back("INV-IDENTITY: queue name is not unique within its resource: " + tag);
    }
    const auto index = impl_->queue_names.find(key);
    if (index == impl_->queue_names.end() || index->second != queue.def.id) {
      violations.push_back("INV-IDENTITY: name index does not resolve to this queue: " + tag);
    }

    if (queue.memo.size() > impl_->config.limits.max_attempt_memo) {
      violations.push_back("INV-BOUND: attempt memo exceeds the configured bound: " + tag);
    }
    if (queue.def.id.valid() && queue.def.generation.valid() && queue.def.generation.value() == 0) {
      violations.push_back("INV-GENERATION: queue generation is zero: " + tag);
    }

    // Evidence invariants. An observation that is fresh and above the maximum
    // currently in force must have been reported as an overflow when it was
    // ingested. Stored evidence from an earlier generation may exceed a lowered
    // maximum without an overflow event, precisely because it is stale and
    // cannot authorize anything.
    if (queue.occupancy.present && queue.occupancy.freshness == Freshness::fresh &&
        queue.occupancy.queue_generation == queue.def.generation && queue.def.thresholds.max_occupancy_bytes != 0 &&
        queue.occupancy.bytes > queue.def.thresholds.max_occupancy_bytes && queue.overflow_events == 0) {
      violations.push_back("INV-OVERFLOW: occupancy above the configured maximum was not reported: " + tag);
    }
    if (queue.lifecycle == Lifecycle::quiesced && (queue.occupancy.bytes != 0 || queue.occupancy.packets != 0)) {
      violations.push_back("INV-DRAIN: quiesced queue holds non-zero occupancy: " + tag);
    }
    if (queue.lifecycle == Lifecycle::quiesced && queue.drain.phase != DrainPhase::completed) {
      violations.push_back("INV-DRAIN: quiesced queue has no completed drain phase: " + tag);
    }
    if (queue.lifecycle == Lifecycle::retired && queue.occupancy.present) {
      violations.push_back("INV-RETIRE: retired queue still carries occupancy state: " + tag);
    }
    if (queue.drain.phase == DrainPhase::completed && queue.lifecycle != Lifecycle::quiesced) {
      violations.push_back("INV-DRAIN: completed drain without quiescence: " + tag);
    }
    if (queue.lifecycle == Lifecycle::active && queue.drain.phase == DrainPhase::completed) {
      violations.push_back("INV-DRAIN: active queue reports a completed drain: " + tag);
    }
    if (queue.fence.fenced != has_flag(queue.applicability, Applicability::fenced)) {
      violations.push_back("INV-FENCE: fence state and applicability flag disagree: " + tag);
    }
    if (is_terminal(queue.lifecycle) && queue.accepts_fresh_mutation()) {
      violations.push_back("INV-LIFECYCLE: retired queue accepts fresh mutation: " + tag);
    }
    if (queue.applied.known && queue.applied.queue_generation != queue.def.generation) {
      // Stale acknowledgements are permitted to exist; they must however not be
      // reported as verified. Verification is checked by explain().
      if (queue.applied.queue_generation > queue.def.generation) {
        violations.push_back("INV-BACKEND: acknowledgement names a future queue generation: " + tag);
      }
    }
    if (queue.occupancy.queue_generation.valid() && queue.occupancy.present &&
        queue.occupancy.queue_generation != queue.def.generation && queue.occupancy.freshness == Freshness::fresh) {
      violations.push_back("INV-STALE: occupancy from another generation is marked fresh: " + tag);
    }
  }

  for (const auto& entry : impl_->resources) {
    const ResourceRecord& resource = entry.second;
    if (entry.first != resource.id) {
      violations.push_back("INV-IDENTITY: resource key does not match the resource identity");
    }
    if (resource.authority.grants.size() > impl_->config.limits.max_grants_per_resource) {
      violations.push_back("INV-BOUND: capability grants exceed the configured bound");
    }
    if (resource.classes.size() > impl_->config.limits.max_classes_per_resource) {
      violations.push_back("INV-BOUND: class count exceeds the configured bound");
    }
    if (resource.pools.size() > impl_->config.limits.max_pools_per_resource) {
      violations.push_back("INV-BOUND: pool count exceeds the configured bound");
    }
    const std::size_t observed = per_resource.count(resource.id) != 0 ? per_resource.at(resource.id) : 0;
    if (resource.queue_count != observed) {
      violations.push_back("INV-TOPOLOGY: resource queue count does not match live queues");
    }
  }

  for (const auto& plan : impl_->plans) {
    const QueueRecord* queue = impl_->find_queue(plan.second.queue);
    if (queue == nullptr) {
      violations.push_back("INV-PLAN: a backend reservation names an unknown queue");
      continue;
    }
    if (queue->def.generation != plan.second.expected_generation) {
      violations.push_back("INV-PLAN: a backend reservation is bound to a superseded generation");
    }
  }

  return violations.size() - before;
}

}  // namespace qf
