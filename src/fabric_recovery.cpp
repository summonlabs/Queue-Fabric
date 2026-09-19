// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
#include <algorithm>

#include "fabric_impl.hpp"

namespace qf {

Status Fabric::recover() {
  std::vector<Event> events;
  Status status;
  {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    status = impl_->recover_locked(events);
  }
  emit_events(impl_->sink, events, impl_->stats.events_emitted);
  return status;
}

Status Fabric::Impl::recover_locked(std::vector<Event>& events) {
  recovery = RecoveryReport{};
  boot = BootId::generate(rng);
  recovery.current_boot = boot;
  epoch = Epoch::from_value(1);
  coordinator_incarnation = Incarnation::from_value(1);

  if (journal == nullptr) {
    recovery.status = Status{};
    recovery.note("no journal configured: fabric starts empty and is not durable");
    healthy = true;
    return Status{};
  }

  const Status open_status = journal->open();
  recovery = journal->recovery();
  recovery.current_boot = boot;
  if (!open_status.ok()) {
    healthy = false;
    recovery.status = open_status;
    recovery.note("durable state could not be loaded; fabric refuses to serve");
    return open_status;
  }

  Epoch previous_epoch = recovery.previous_epoch;
  if (recovery.previous_boot_known) {
    recovery.previous_boot = recovery.previous_boot;
  }

  struct RestoredResource {
    ResourceRecord record{};
    bool seen{false};
  };

  std::unordered_map<AttemptId, MutationRequest> outstanding;
  std::vector<MutationRequest> replay_order;

  EpochState epoch_state{};
  bool epoch_state_seen = false;

  for (const auto& record : journal->records()) {
    switch (record.type) {
      case JournalRecordType::epoch_state: {
        auto decoded = decode_epoch_state(record.payload);
        if (!decoded.ok()) {
          ++recovery.records_corrupt;
          recovery.note("epoch state record could not be decoded");
          break;
        }
        epoch_state = decoded.value();
        epoch_state_seen = true;
        if (epoch_state.epoch > previous_epoch) {
          previous_epoch = epoch_state.epoch;
        }
        break;
      }
      case JournalRecordType::resource_state: {
        auto decoded = decode_resource_state(record.payload);
        if (!decoded.ok()) {
          ++recovery.records_corrupt;
          recovery.note("resource state record could not be decoded");
          break;
        }
        ResourceRecord restored = std::move(decoded.value());
        resources[restored.id] = std::move(restored);
        break;
      }
      case JournalRecordType::queue_state: {
        auto decoded = decode_queue_state(record.payload);
        if (!decoded.ok()) {
          ++recovery.records_corrupt;
          recovery.note("queue state record could not be decoded");
          break;
        }
        QueueRecord restored = std::move(decoded.value());
        ++recovery.queues_restored;
        if (restored.occupancy.present) {
          invalidate_occupancy(restored.occupancy, Freshness::stale_restart);
          restored.applicability = restored.applicability | Applicability::stale;
          ++restored.staleness_events;
          ++recovery.queues_revalidated;
        }
        if (restored.drain.phase == DrainPhase::evidence_pending || restored.drain.phase == DrainPhase::requested ||
            restored.drain.phase == DrainPhase::completed) {
          restored.drain.evidence.revalidated = false;
          if (restored.drain.phase == DrainPhase::completed) {
            restored.drain.phase = DrainPhase::evidence_pending;
          }
          restored.applicability = restored.applicability | Applicability::stale;
        }
        if (restored.applied.known) {
          restored.applicability = restored.applicability | Applicability::stale;
        }
        queues[restored.def.id] = std::move(restored);
        break;
      }
      case JournalRecordType::begin_attempt: {
        auto decoded = decode_mutation(record.payload);
        if (!decoded.ok()) {
          ++recovery.records_corrupt;
          recovery.note("begin attempt record could not be decoded");
          break;
        }
        outstanding[decoded.value().attempt] = std::move(decoded.value());
        break;
      }
      case JournalRecordType::commit_mutation: {
        auto decoded = decode_mutation(record.payload);
        if (!decoded.ok()) {
          ++recovery.records_corrupt;
          recovery.note("commit record could not be decoded");
          break;
        }
        outstanding.erase(decoded.value().attempt);
        replay_order.push_back(std::move(decoded.value()));
        break;
      }
      case JournalRecordType::noop:
        break;
    }
  }

  // Backend liveness is never restored: an incarnation is minted per live
  // process and a restart invalidates every previous incarnation.
  for (auto& entry : resources) {
    for (auto& backend : entry.second.backends) {
      if (backend.alive) {
        backend.alive = false;
        recovery.note("backend incarnation not restored; re-registration required");
      }
    }
  }

  // Restore durable authority under the new epoch.
  auto next_epoch = previous_epoch.valid() ? previous_epoch.next() : Result<Epoch>{Epoch::from_value(1)};
  if (!next_epoch.ok()) {
    healthy = false;
    recovery.status = next_epoch.status();
    return next_epoch.status();
  }
  epoch = next_epoch.value();
  coordinator_incarnation = Incarnation::from_value(epoch_state_seen ? epoch_state.coordinator_incarnation.value() + 1 : 1);
  for (auto& entry : resources) {
    entry.second.epoch = epoch;
    entry.second.authority.epoch = epoch;
  }
  recovery.previous_epoch = previous_epoch;
  recovery.current_epoch = epoch;
  rebuild_indexes_locked();

  // Replay committed mutations in file order. The durable snapshot already
  // reflects everything before the first commit record.
  for (const auto& request : replay_order) {
    MutationOutcome outcome{};
    const Status status = apply_replay_locked(request, outcome);
    if (!status.ok()) {
      ++recovery.mutations_rejected;
      recovery.note(std::string{"replayed mutation rejected: "} + status.to_string());
    } else {
      ++recovery.mutations_replayed;
    }
  }

  // Every queue restored from durable state requires revalidation of its
  // dynamic evidence: occupancy freshness, backend acknowledgements and drain
  // evidence are facts about a previous process life, never live authority.
  for (auto& entry : queues) {
    QueueRecord& queue = entry.second;
    if (queue.occupancy.present) {
      invalidate_occupancy(queue.occupancy, Freshness::stale_restart);
    }
    queue.applicability = queue.applicability | Applicability::stale;
    ++queue.staleness_events;
    ++recovery.queues_revalidated;
  }

  // Attempts with a durable begin and no durable commit are unfinished and
  // their outcome is ambiguous: the queue is marked as requiring revalidation.
  for (const auto& entry : outstanding) {
    ++recovery.orphaned_begins;
    QueueRecord* queue = find_queue(entry.second.queue);
    if (queue != nullptr) {
      queue->applicability = queue->applicability | Applicability::stale;
      invalidate_occupancy(queue->occupancy, Freshness::stale_restart);
      ++queue->staleness_events;
    }
    recovery.note("unfinished attempt requires revalidation");
  }

  rebuild_indexes_locked();
  // Report the queues that exist after snapshot restore plus commit replay.
  recovery.queues_restored = queues.size();

  const Status header = journal->write_header(boot, epoch);
  if (!header.ok()) {
    healthy = false;
    recovery.status = header;
    return header;
  }
  if (recovery.records_truncated > 0) {
    const Status truncated = journal->truncate_to_valid_prefix();
    if (!truncated.ok()) {
      recovery.note("torn tail could not be truncated; appends continue after the valid prefix");
    }
  }
  const Status epoch_record = append_epoch_state_locked();
  if (!epoch_record.ok()) {
    healthy = false;
    recovery.status = epoch_record;
    return epoch_record;
  }

  recovery.status = Status{};
  healthy = true;
  Event event{};
  event.kind = EventKind::recovery;
  event.at = monotonic_now();
  event.code = Code::ok;
  event.epoch = epoch;
  event.detail = "durable state restored and epoch advanced";
  events.push_back(std::move(event));
  return Status{};
}

Status Fabric::Impl::append_epoch_state_locked() {
  if (journal == nullptr) {
    return Status{};
  }
  EpochState state{};
  state.boot = boot;
  state.epoch = epoch;
  state.coordinator_incarnation = coordinator_incarnation;
  const Bytes encoded = encode_epoch_state(state);
  if (encoded.empty()) {
    return Status{Code::internal, "epoch state could not be encoded"};
  }
  const Status append = journal->append(JournalRecordType::epoch_state, encoded);
  if (!append.ok()) {
    return append;
  }
  const Status sync = journal->sync();
  if (!sync.ok()) {
    return Status{Code::not_durable, "epoch state did not reach stable storage"};
  }
  ++stats.journal_records;
  ++stats.journal_syncs;
  return Status{};
}

void Fabric::Impl::rebuild_indexes_locked() {
  queue_names.clear();
  std::uint64_t max_queue = 0;
  std::uint64_t max_resource = 0;
  std::uint64_t max_class = 0;
  std::uint64_t max_pool = 0;
  std::uint64_t max_backend = 0;
  std::uint64_t max_fence = 0;

  for (auto& entry : resources) {
    ResourceRecord& resource = entry.second;
    max_resource = std::max(max_resource, resource.id.value());
    std::size_t queues_in_resource = 0;
    for (const auto& cls : resource.classes) {
      max_class = std::max(max_class, cls.id.value());
    }
    for (const auto& pool : resource.pools) {
      max_pool = std::max(max_pool, pool.id.value());
    }
    for (const auto& backend : resource.backends) {
      max_backend = std::max(max_backend, backend.backend.value());
    }
    for (const auto& queue_entry : queues) {
      if (queue_entry.second.def.resource == resource.id) {
        ++queues_in_resource;
      }
    }
    resource.queue_count = queues_in_resource;
  }
  for (auto& entry : queues) {
    QueueRecord& queue = entry.second;
    max_queue = std::max(max_queue, queue.def.id.value());
    max_fence = std::max(max_fence, queue.fence.token);
    queue_names[name_key(queue.def.resource, queue.def.name)] = queue.def.id;
  }
  next_resource_id = max_resource + 1;
  next_queue_id = max_queue + 1;
  next_class_id = max_class + 1;
  next_pool_id = max_pool + 1;
  next_backend_id = max_backend + 1;
  fence_tokens = max_fence;
}

void Fabric::Impl::refresh_queue_locked(QueueRecord& queue, Nanos now) {
  const OccupancyPolicy policy = occupancy_policy_for(queue);
  const Freshness freshness = classify_freshness(queue.occupancy, now, policy, queue.def.generation);
  queue.occupancy.age = occupancy_age(now, queue.occupancy.observed_at);
  if (freshness != queue.occupancy.freshness) {
    queue.occupancy.freshness = freshness;
    if (freshness != Freshness::fresh && freshness != Freshness::unknown) {
      ++queue.staleness_events;
      ++stats.staleness_events;
    }
  }
}

Status Fabric::refresh_freshness() {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  if (impl_->shutting_down) {
    return Status{Code::shutting_down, "fabric is shutting down"};
  }
  const Nanos now = impl_->monotonic_now();
  for (auto& entry : impl_->queues) {
    impl_->refresh_queue_locked(entry.second, now);
  }
  return Status{};
}

void Fabric::Impl::build_snapshot_locked(std::vector<JournalRecord>& records) const {
  records.clear();
  EpochState state{};
  state.boot = boot;
  state.epoch = epoch;
  state.coordinator_incarnation = coordinator_incarnation;
  JournalRecord epoch_record{};
  epoch_record.type = JournalRecordType::epoch_state;
  epoch_record.payload = encode_epoch_state(state);
  records.push_back(std::move(epoch_record));

  std::vector<ResourceId> resource_ids;
  resource_ids.reserve(resources.size());
  for (const auto& entry : resources) {
    resource_ids.push_back(entry.first);
  }
  std::sort(resource_ids.begin(), resource_ids.end());
  for (const auto& id : resource_ids) {
    const ResourceRecord& resource = resources.at(id);
    JournalRecord record{};
    record.type = JournalRecordType::resource_state;
    record.payload = encode_resource_state(resource);
    records.push_back(std::move(record));
  }

  std::vector<QueueId> queue_ids;
  queue_ids.reserve(queues.size());
  for (const auto& entry : queues) {
    queue_ids.push_back(entry.first);
  }
  std::sort(queue_ids.begin(), queue_ids.end());
  for (const auto& id : queue_ids) {
    const QueueRecord& queue = queues.at(id);
    JournalRecord record{};
    record.type = JournalRecordType::queue_state;
    record.payload = encode_queue_state(queue);
    records.push_back(std::move(record));
  }
}

Status Fabric::compact() {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  if (impl_->journal == nullptr) {
    return Status{Code::unsupported, "no journal configured"};
  }
  if (impl_->shutting_down) {
    return Status{Code::shutting_down, "fabric is shutting down"};
  }
  if (!impl_->plans.empty()) {
    return Status{Code::busy, "compaction refused while backend attempts are reserved"};
  }
  std::vector<JournalRecord> records;
  impl_->build_snapshot_locked(records);
  const Status status = impl_->journal->rewrite(records);
  if (!status.ok()) {
    return status;
  }
  impl_->stats.journal_records += records.size();
  return Status{};
}

}  // namespace qf
