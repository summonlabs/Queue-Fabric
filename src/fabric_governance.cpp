// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
#include "fabric_impl.hpp"

namespace qf {

Result<Epoch> Fabric::advance_epoch(OwnerId principal, const Provenance& provenance) {
  std::vector<Event> events;
  Result<Epoch> result = Status{Code::internal, "unset"};
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
    bool authorized = impl_->resources.empty();
    for (const auto& entry : impl_->resources) {
      if (has_capability(entry.second.authority.capabilities_of(principal), Capability::admin)) {
        authorized = true;
        break;
      }
    }
    if (!authorized) {
      return Status{Code::authority_denied, "epoch-advance-requires-administrative-authority"};
    }
    auto next = impl_->epoch.next();
    if (!next.ok()) {
      return next.status();
    }
    // The new epoch is durable before it becomes authoritative in memory.
    if (impl_->journal != nullptr) {
      EpochState state{};
      state.boot = impl_->boot;
      state.epoch = next.value();
      state.coordinator_incarnation = impl_->coordinator_incarnation;
      const Bytes encoded = encode_epoch_state(state);
      if (encoded.empty()) {
        return Status{Code::internal, "epoch state could not be encoded"};
      }
      const Status append = impl_->journal->append(JournalRecordType::epoch_state, encoded);
      if (!append.ok()) {
        return append;
      }
      const Status sync = impl_->journal->sync();
      if (!sync.ok()) {
        impl_->healthy = false;
        return Status{Code::not_durable, "epoch advance did not reach stable storage"};
      }
      ++impl_->stats.journal_records;
      ++impl_->stats.journal_syncs;
    } else if (impl_->config.durable) {
      return Status{Code::not_durable, "durable fabric requires a journal"};
    }
    impl_->epoch = next.value();
    for (auto& entry : impl_->resources) {
      entry.second.epoch = impl_->epoch;
      entry.second.authority.epoch = impl_->epoch;
    }
    const Nanos now = impl_->monotonic_now();
    for (auto& entry : impl_->queues) {
      QueueRecord& queue = entry.second;
      if (queue.occupancy.present) {
        invalidate_occupancy(queue.occupancy, Freshness::stale_epoch);
        queue.applicability = queue.applicability | Applicability::stale;
        ++queue.staleness_events;
        ++impl_->stats.staleness_events;
      }
      if (queue.applied.known) {
        queue.applicability = queue.applicability | Applicability::stale;
      }
    }
    Event event{};
    event.kind = EventKind::topology_changed;
    event.at = now;
    event.code = Code::ok;
    event.epoch = impl_->epoch;
    event.detail = "coordinator epoch advanced";
    events.push_back(std::move(event));
    result = impl_->epoch;
  }
  emit_events(impl_->sink, events, impl_->stats.events_emitted);
  return result;
}

Status Fabric::invalidate_evidence(QueueId queue_id, OwnerId principal, Epoch expected_epoch, std::string_view reason) {
  std::vector<Event> events;
  {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    QueueRecord* queue = impl_->find_queue(queue_id);
    if (queue == nullptr) {
      return Status{Code::not_found, "queue is not declared"};
    }
    ResourceRecord* resource = impl_->find_resource(queue->def.resource);
    if (resource == nullptr) {
      return Status{Code::internal, "queue references an unknown resource"};
    }
    const AuthorityDecision decision = authorize(resource->authority, principal, Capability::admin, expected_epoch,
                                                 queue->applicability, queue->lifecycle);
    if (!decision.allowed) {
      return Status{decision.code, decision.rule};
    }
    if (queue->occupancy.present) {
      invalidate_occupancy(queue->occupancy, Freshness::stale_backend);
    }
    queue->applicability = queue->applicability | Applicability::stale;
    ++queue->staleness_events;
    ++impl_->stats.staleness_events;
    Event event{};
    event.kind = EventKind::occupancy_rejected;
    event.at = impl_->monotonic_now();
    event.queue = queue_id;
    event.resource = queue->def.resource;
    event.code = Code::stale_occupancy;
    event.detail.assign(reason.substr(0, limits::kMaxReasonChars));
    events.push_back(std::move(event));
  }
  emit_events(impl_->sink, events, impl_->stats.events_emitted);
  return Status{};
}

}  // namespace qf
