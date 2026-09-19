// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
#include "fabric_impl.hpp"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <utility>

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#else
#include <unistd.h>
#endif

namespace qf {

const char* to_string(EventKind value) noexcept {
  switch (value) {
    case EventKind::mutation_applied: return "mutation_applied";
    case EventKind::mutation_replayed: return "mutation_replayed";
    case EventKind::mutation_rejected: return "mutation_rejected";
    case EventKind::lifecycle_changed: return "lifecycle_changed";
    case EventKind::occupancy_ingested: return "occupancy_ingested";
    case EventKind::occupancy_rejected: return "occupancy_rejected";
    case EventKind::occupancy_overflow: return "occupancy_overflow";
    case EventKind::drain_progress: return "drain_progress";
    case EventKind::fence_changed: return "fence_changed";
    case EventKind::queue_declared: return "queue_declared";
    case EventKind::queue_retired: return "queue_retired";
    case EventKind::topology_changed: return "topology_changed";
    case EventKind::backend_registered: return "backend_registered";
    case EventKind::backend_dead: return "backend_dead";
    case EventKind::recovery: return "recovery";
    case EventKind::shutdown: return "shutdown";
  }
  return "unknown";
}

IEventSink::~IEventSink() = default;

const SchedulingClassDef* ResourceRecord::find_class(ClassId cls) const noexcept {
  for (const auto& entry : classes) {
    if (entry.id == cls) {
      return &entry;
    }
  }
  return nullptr;
}

const BufferPoolDef* ResourceRecord::find_pool(PoolId pool) const noexcept {
  for (const auto& entry : pools) {
    if (entry.id == pool) {
      return &entry;
    }
  }
  return nullptr;
}

const BackendIncarnation* ResourceRecord::find_backend(BackendId backend) const noexcept {
  for (const auto& entry : backends) {
    if (entry.backend == backend) {
      return &entry;
    }
  }
  return nullptr;
}

// ---- Impl helpers -----------------------------------------------------------

ResourceRecord* Fabric::Impl::find_resource(ResourceId id) noexcept {
  const auto it = resources.find(id);
  return it == resources.end() ? nullptr : &it->second;
}

const ResourceRecord* Fabric::Impl::find_resource(ResourceId id) const noexcept {
  const auto it = resources.find(id);
  return it == resources.end() ? nullptr : &it->second;
}

QueueRecord* Fabric::Impl::find_queue(QueueId id) noexcept {
  const auto it = queues.find(id);
  return it == queues.end() ? nullptr : &it->second;
}

const QueueRecord* Fabric::Impl::find_queue(QueueId id) const noexcept {
  const auto it = queues.find(id);
  return it == queues.end() ? nullptr : &it->second;
}

std::string Fabric::Impl::name_key(ResourceId resource, std::string_view name) {
  std::string key = resource.to_string();
  key += '/';
  key.append(name);
  return key;
}

std::size_t Fabric::Impl::queue_count_locked() const noexcept { return queues.size(); }

Nanos Fabric::Impl::monotonic_now() noexcept {
  const Nanos now = clock->now_nanos();
  if (now > clock_watermark) {
    clock_watermark = now;
  }
  return clock_watermark;
}

// ---- Fabric construction ----------------------------------------------------

namespace {

/// Per-process boot seed. Clock, process id and instance address make an
/// accidental collision between two live or successive processes infeasible.
std::uint64_t derive_boot_seed(const void* instance) noexcept {
  const auto ticks = static_cast<std::uint64_t>(
      std::chrono::high_resolution_clock::now().time_since_epoch().count());
  const auto address = static_cast<std::uint64_t>(reinterpret_cast<std::uintptr_t>(instance));
#ifdef _WIN32
  const auto process = static_cast<std::uint64_t>(::GetCurrentProcessId());
#else
  const auto process = static_cast<std::uint64_t>(::getpid());
#endif
  std::uint64_t seed = ticks;
  hash_mix(seed, process);
  hash_mix(seed, address);
  hash_mix(seed, static_cast<std::uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count()));
  return seed;
}

}  // namespace

Fabric::Fabric(FabricConfig config, IClock& clock, IJournal* journal, IEventSink* sink) : impl_(new Impl()) {
  impl_->config = std::move(config);
  impl_->clock = &clock;
  impl_->journal = journal;
  impl_->sink = sink;
  impl_->provenance = ProvenanceTracker{limits::kMaxProvenanceNodes};
  const std::uint64_t seed = impl_->config.boot_seed != 0 ? impl_->config.boot_seed : derive_boot_seed(impl_);
  impl_->rng = Rng{seed};
}

Fabric::~Fabric() {
  delete impl_;
}

/// Event emission happens strictly outside the fabric lock.
void emit_events(IEventSink* sink, std::vector<Event>& events, std::uint64_t& counter) {
  if (sink == nullptr) {
    counter += events.size();
    events.clear();
    return;
  }
  for (const auto& event : events) {
    sink->on_event(event);
  }
  counter += events.size();
  events.clear();
}

namespace {

MutationRequest make_request(MutationKind kind, const MutationContext& ctx, QueueId queue, Generation expected) {
  MutationRequest request{};
  request.kind = kind;
  request.queue = queue;
  request.expected_generation = expected;
  request.expected_epoch = ctx.epoch;
  request.principal = ctx.principal;
  request.provenance = ctx.provenance;
  request.attempt = AttemptId{ctx.provenance.publisher, ctx.provenance.sequence.value()};
  return request;
}

}  // namespace

// ---- topology ---------------------------------------------------------------

Result<ResourceId> Fabric::declare_resource(std::string_view name, OwnerId owner, const Provenance& provenance) {
  std::vector<Event> events;
  Result<ResourceId> result = Status{Code::internal, "unset"};
  {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    if (impl_->shutting_down) {
      return Status{Code::shutting_down, "fabric is shutting down"};
    }
    if (!impl_->healthy) {
      return Status{Code::not_durable, "fabric is not healthy"};
    }
    if (!owner.valid()) {
      return Status{Code::invalid_argument, "owner identity is required"};
    }
    if (!provenance.valid()) {
      return Status{Code::malformed_input, "complete provenance is required"};
    }
    if (name.empty() || name.size() > limits::kMaxNameChars) {
      return Status{Code::out_of_range, "resource name length is out of range"};
    }
    if (impl_->resources.size() >= impl_->config.limits.max_resources) {
      return Status{Code::capacity_exceeded, "resource capacity is exhausted"};
    }
    for (const auto& entry : impl_->resources) {
      if (entry.second.name == name) {
        return Status{Code::already_exists, "resource name is already declared"};
      }
    }

    ResourceRecord record{};
    record.id = ResourceId::from_value(impl_->next_resource_id);
    record.name.assign(name);
    record.owner = owner;
    record.generation = Generation::from_value(1);
    record.epoch = impl_->epoch;
    record.authority.resource = record.id;
    record.authority.owner = owner;
    record.authority.epoch = impl_->epoch;
    record.authority.grants.push_back(AuthorityGrant{owner, static_cast<std::uint32_t>(Capability::all)});
    record.declared_by = provenance;
    record.declared_at = impl_->monotonic_now();

    if (impl_->journal != nullptr) {
      const Bytes encoded = encode_resource_state(record);
      if (encoded.empty()) {
        return Status{Code::internal, "resource state could not be encoded"};
      }
      Status append = impl_->journal->append(JournalRecordType::resource_state, encoded);
      if (!append.ok()) {
        return append;
      }
      Status sync = impl_->journal->sync();
      if (!sync.ok()) {
        impl_->healthy = false;
        return Status{Code::not_durable, "resource state is not durable"};
      }
      ++impl_->stats.journal_records;
      ++impl_->stats.journal_syncs;
    } else if (impl_->config.durable) {
      return Status{Code::not_durable, "durable fabric requires a journal"};
    }

    ++impl_->next_resource_id;
    const ResourceId id = record.id;
    impl_->resources.emplace(id, std::move(record));

    Event event{};
    event.kind = EventKind::topology_changed;
    event.at = impl_->monotonic_now();
    event.resource = id;
    event.code = Code::ok;
    event.detail = "resource declared";
    events.push_back(std::move(event));
    result = id;
  }
  emit_events(impl_->sink, events, impl_->stats.events_emitted);
  return result;
}

Status Fabric::grant_capabilities(ResourceId resource,
                                  OwnerId grantee,
                                  std::uint32_t capabilities,
                                  OwnerId principal,
                                  Epoch expected_epoch,
                                  const Provenance& provenance) {
  std::vector<Event> events;
  Status status;
  {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    if (impl_->shutting_down) {
      return Status{Code::shutting_down, "fabric is shutting down"};
    }
    if (!provenance.valid()) {
      return Status{Code::malformed_input, "complete provenance is required"};
    }
    ResourceRecord* record = impl_->find_resource(resource);
    if (record == nullptr) {
      return Status{Code::not_found, "resource is not declared"};
    }
    const AuthorityDecision decision =
        authorize(record->authority, principal, Capability::admin, expected_epoch, record->applicability, Lifecycle::active);
    if (!decision.allowed) {
      return Status{decision.code, decision.rule};
    }
    if (!grantee.valid()) {
      return Status{Code::invalid_argument, "grantee identity is required"};
    }
    ResourceRecord updated = *record;
    bool replaced = false;
    for (auto& grant : updated.authority.grants) {
      if (grant.owner == grantee) {
        grant.capabilities = capabilities;
        replaced = true;
        break;
      }
    }
    if (!replaced) {
      if (updated.authority.grants.size() >= impl_->config.limits.max_grants_per_resource) {
        return Status{Code::capacity_exceeded, "capability grant capacity is exhausted"};
      }
      updated.authority.grants.push_back(AuthorityGrant{grantee, capabilities});
    }
    if (impl_->journal != nullptr) {
      const Bytes encoded = encode_resource_state(updated);
      Status append = impl_->journal->append(JournalRecordType::resource_state, encoded);
      if (!append.ok()) {
        return append;
      }
      Status sync = impl_->journal->sync();
      if (!sync.ok()) {
        impl_->healthy = false;
        return Status{Code::not_durable, "capability grant is not durable"};
      }
      ++impl_->stats.journal_records;
      ++impl_->stats.journal_syncs;
    } else if (impl_->config.durable) {
      return Status{Code::not_durable, "durable fabric requires a journal"};
    }
    *record = std::move(updated);
    Event event{};
    event.kind = EventKind::topology_changed;
    event.at = impl_->monotonic_now();
    event.resource = resource;
    event.detail = "capabilities granted";
    events.push_back(std::move(event));
  }
  emit_events(impl_->sink, events, impl_->stats.events_emitted);
  return status;
}

Result<ClassId> Fabric::declare_class(ResourceId resource,
                                      std::string_view name,
                                      std::uint32_t priority,
                                      const QosRef& qos,
                                      OwnerId principal,
                                      Epoch expected_epoch,
                                      const Provenance& provenance) {
  std::vector<Event> events;
  Result<ClassId> result = Status{Code::internal, "unset"};
  {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    if (impl_->shutting_down) {
      return Status{Code::shutting_down, "fabric is shutting down"};
    }
    if (!provenance.valid()) {
      return Status{Code::malformed_input, "complete provenance is required"};
    }
    if (name.empty() || name.size() > limits::kMaxNameChars) {
      return Status{Code::out_of_range, "class name length is out of range"};
    }
    ResourceRecord* record = impl_->find_resource(resource);
    if (record == nullptr) {
      return Status{Code::not_found, "resource is not declared"};
    }
    const AuthorityDecision decision = authorize(record->authority, principal, Capability::declare_topology, expected_epoch,
                                                 record->applicability, Lifecycle::active);
    if (!decision.allowed) {
      return Status{decision.code, decision.rule};
    }

    ResourceRecord updated = *record;
    SchedulingClassDef* existing = nullptr;
    for (auto& entry : updated.classes) {
      if (entry.name == name) {
        existing = &entry;
        break;
      }
    }
    ClassId class_id{};
    if (existing != nullptr) {
      auto next = existing->generation.next();
      if (!next.ok()) {
        return next.status();
      }
      existing->generation = next.value();
      existing->priority = priority;
      existing->qos = qos;
      class_id = existing->id;
    } else {
      if (updated.classes.size() >= impl_->config.limits.max_classes_per_resource) {
        return Status{Code::capacity_exceeded, "class capacity is exhausted"};
      }
      SchedulingClassDef entry{};
      entry.id = ClassId::from_value(impl_->next_class_id);
      entry.resource = resource;
      entry.name.assign(name);
      entry.priority = priority;
      entry.qos = qos;
      entry.generation = Generation::from_value(1);
      entry.declared_by = provenance;
      class_id = entry.id;
      ++impl_->next_class_id;
      updated.classes.push_back(std::move(entry));
    }
    auto resource_generation = updated.generation.next();
    if (!resource_generation.ok()) {
      return resource_generation.status();
    }
    updated.generation = resource_generation.value();

    if (impl_->journal != nullptr) {
      const Bytes encoded = encode_resource_state(updated);
      Status append = impl_->journal->append(JournalRecordType::resource_state, encoded);
      if (!append.ok()) {
        return append;
      }
      Status sync = impl_->journal->sync();
      if (!sync.ok()) {
        impl_->healthy = false;
        return Status{Code::not_durable, "class state is not durable"};
      }
      ++impl_->stats.journal_records;
      ++impl_->stats.journal_syncs;
    } else if (impl_->config.durable) {
      return Status{Code::not_durable, "durable fabric requires a journal"};
    }

    // Publish only after the durable write succeeded: every queue bound to a
    // superseded class generation loses exact binding, so its evidence must be
    // re-established.
    *record = std::move(updated);
    std::size_t invalidated = 0;
    for (auto& entry : impl_->queues) {
      QueueRecord& queue = entry.second;
      if (queue.def.resource != resource || queue.def.cls.id != class_id) {
        continue;
      }
      const SchedulingClassDef* current = record->find_class(class_id);
      if (current != nullptr && queue.def.cls.generation != current->generation) {
        queue.applicability = queue.applicability | Applicability::stale;
        invalidate_occupancy(queue.occupancy, Freshness::stale_generation);
        ++queue.staleness_events;
        ++invalidated;
      }
    }

    Event event{};
    event.kind = EventKind::topology_changed;
    event.at = impl_->monotonic_now();
    event.resource = resource;
    event.detail = invalidated > 0 ? "class declared; bound queues require rebinding" : "class declared";
    events.push_back(std::move(event));
    result = class_id;
  }
  emit_events(impl_->sink, events, impl_->stats.events_emitted);
  return result;
}

Result<PoolId> Fabric::declare_pool(ResourceId resource,
                                    std::string_view name,
                                    std::uint64_t capacity_bytes,
                                    OwnerId principal,
                                    Epoch expected_epoch,
                                    const Provenance& provenance) {
  std::vector<Event> events;
  Result<PoolId> result = Status{Code::internal, "unset"};
  {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    if (impl_->shutting_down) {
      return Status{Code::shutting_down, "fabric is shutting down"};
    }
    if (!provenance.valid()) {
      return Status{Code::malformed_input, "complete provenance is required"};
    }
    if (name.empty() || name.size() > limits::kMaxNameChars) {
      return Status{Code::out_of_range, "pool name length is out of range"};
    }
    ResourceRecord* record = impl_->find_resource(resource);
    if (record == nullptr) {
      return Status{Code::not_found, "resource is not declared"};
    }
    const AuthorityDecision decision = authorize(record->authority, principal, Capability::declare_topology, expected_epoch,
                                                 record->applicability, Lifecycle::active);
    if (!decision.allowed) {
      return Status{decision.code, decision.rule};
    }
    ResourceRecord updated = *record;
    BufferPoolDef* existing = nullptr;
    for (auto& entry : updated.pools) {
      if (entry.name == name) {
        existing = &entry;
        break;
      }
    }
    PoolId pool_id{};
    if (existing != nullptr) {
      auto next = existing->generation.next();
      if (!next.ok()) {
        return next.status();
      }
      existing->generation = next.value();
      existing->capacity_bytes = capacity_bytes;
      pool_id = existing->id;
    } else {
      if (updated.pools.size() >= impl_->config.limits.max_pools_per_resource) {
        return Status{Code::capacity_exceeded, "pool capacity is exhausted"};
      }
      BufferPoolDef entry{};
      entry.id = PoolId::from_value(impl_->next_pool_id);
      entry.resource = resource;
      entry.name.assign(name);
      entry.capacity_bytes = capacity_bytes;
      entry.generation = Generation::from_value(1);
      entry.declared_by = provenance;
      pool_id = entry.id;
      ++impl_->next_pool_id;
      updated.pools.push_back(std::move(entry));
    }
    if (impl_->journal != nullptr) {
      const Bytes encoded = encode_resource_state(updated);
      Status append = impl_->journal->append(JournalRecordType::resource_state, encoded);
      if (!append.ok()) {
        return append;
      }
      Status sync = impl_->journal->sync();
      if (!sync.ok()) {
        impl_->healthy = false;
        return Status{Code::not_durable, "pool state is not durable"};
      }
      ++impl_->stats.journal_records;
      ++impl_->stats.journal_syncs;
    } else if (impl_->config.durable) {
      return Status{Code::not_durable, "durable fabric requires a journal"};
    }
    *record = std::move(updated);
    Event event{};
    event.kind = EventKind::topology_changed;
    event.at = impl_->monotonic_now();
    event.resource = resource;
    event.detail = "pool declared";
    events.push_back(std::move(event));
    result = pool_id;
  }
  emit_events(impl_->sink, events, impl_->stats.events_emitted);
  return result;
}

Result<BackendId> Fabric::register_backend(ResourceId resource,
                                        Incarnation incarnation,
                                        OwnerId principal,
                                        Epoch expected_epoch,
                                        const Provenance& provenance) {
  std::vector<Event> events;
  Result<BackendId> result = Status{Code::internal, "unset"};
  {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    if (impl_->shutting_down) {
      return Status{Code::shutting_down, "fabric is shutting down"};
    }
    if (!provenance.valid()) {
      return Status{Code::malformed_input, "complete provenance is required"};
    }
    if (!incarnation.valid()) {
      return Status{Code::invalid_argument, "backend incarnation is required"};
    }
    ResourceRecord* record = impl_->find_resource(resource);
    if (record == nullptr) {
      return Status{Code::not_found, "resource is not declared"};
    }
    const AuthorityDecision decision = authorize(record->authority, principal, Capability::declare_topology, expected_epoch,
                                                 record->applicability, Lifecycle::active);
    if (!decision.allowed) {
      return Status{decision.code, decision.rule};
    }
    const Nanos now = impl_->monotonic_now();
    BackendId backend_id{};
    // A registration always mints a new backend incarnation: an incarnation is
    // minted per live backend process, never reused across restarts.
    if (record->backends.size() >= impl_->config.limits.max_resources) {
      return Status{Code::capacity_exceeded, "backend capacity is exhausted"};
    }
    BackendIncarnation entry{};
    entry.backend = BackendId::from_value(impl_->next_backend_id);
    entry.resource = resource;
    entry.incarnation = incarnation;
    entry.epoch = impl_->epoch;
    entry.registered_at = now;
    entry.alive = true;
    backend_id = entry.backend;
    ++impl_->next_backend_id;
    record->backends.push_back(entry);

    if (impl_->journal != nullptr) {
      const Bytes encoded = encode_resource_state(*record);
      Status append = impl_->journal->append(JournalRecordType::resource_state, encoded);
      if (!append.ok()) {
        return append;
      }
      Status sync = impl_->journal->sync();
      if (!sync.ok()) {
        impl_->healthy = false;
        return Status{Code::not_durable, "backend registration is not durable"};
      }
      ++impl_->stats.journal_records;
      ++impl_->stats.journal_syncs;
    } else if (impl_->config.durable) {
      return Status{Code::not_durable, "durable fabric requires a journal"};
    }

    Event event{};
    event.kind = EventKind::backend_registered;
    event.at = now;
    event.resource = resource;
    event.detail = "backend incarnation registered";
    events.push_back(std::move(event));
    result = backend_id;
  }
  emit_events(impl_->sink, events, impl_->stats.events_emitted);
  return result;
}

Status Fabric::mark_backend_dead(BackendId backend, Incarnation incarnation, OwnerId principal, Epoch expected_epoch) {
  std::vector<Event> events;
  {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    if (impl_->shutting_down) {
      return Status{Code::shutting_down, "fabric is shutting down"};
    }
    bool found = false;
    for (auto& resource_entry : impl_->resources) {
      ResourceRecord& record = resource_entry.second;
      for (auto& entry : record.backends) {
        if (entry.backend != backend) {
          continue;
        }
        found = true;
        const AuthorityDecision decision = authorize(record.authority, principal, Capability::admin, expected_epoch,
                                                     record.applicability, Lifecycle::active);
        if (!decision.allowed) {
          return Status{decision.code, decision.rule};
        }
        if (entry.incarnation != incarnation) {
          return Status{Code::stale_incarnation, "backend incarnation does not match the live registration"};
        }
        ResourceRecord updated = record;
        for (auto& candidate : updated.backends) {
          if (candidate.backend == backend) {
            candidate.alive = false;
            break;
          }
        }
        if (impl_->journal != nullptr) {
          const Bytes encoded = encode_resource_state(updated);
          Status append = impl_->journal->append(JournalRecordType::resource_state, encoded);
          if (!append.ok()) {
            return append;
          }
          Status sync = impl_->journal->sync();
          if (!sync.ok()) {
            impl_->healthy = false;
            return Status{Code::not_durable, "backend death is not durable"};
          }
          ++impl_->stats.journal_records;
          ++impl_->stats.journal_syncs;
        }
        record = std::move(updated);
        Event event{};
        event.kind = EventKind::backend_dead;
        event.at = impl_->monotonic_now();
        event.resource = record.id;
        event.detail = "backend incarnation marked dead";
        events.push_back(std::move(event));
        break;
      }
      if (found) {
        break;
      }
    }
    if (!found) {
      return Status{Code::not_found, "backend is not registered"};
    }
  }
  emit_events(impl_->sink, events, impl_->stats.events_emitted);
  return Status{};
}

Status Fabric::shutdown() {
  std::vector<Event> events;
  {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    if (impl_->shutting_down) {
      return Status{};
    }
    impl_->shutting_down = true;
    impl_->pending.clear();
    if (impl_->journal != nullptr) {
      impl_->journal->sync();
    }
    Event event{};
    event.kind = EventKind::shutdown;
    event.at = impl_->monotonic_now();
    event.detail = "fabric stopped accepting work";
    events.push_back(std::move(event));
  }
  emit_events(impl_->sink, events, impl_->stats.events_emitted);
  return Status{};
}

bool Fabric::shutting_down() const {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  return impl_->shutting_down;
}

// ---- queries ----------------------------------------------------------------

Result<QueueRecord> Fabric::queue_record(QueueId queue) const {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  const QueueRecord* record = impl_->find_queue(queue);
  if (record == nullptr) {
    return Status{Code::not_found, "queue is not declared"};
  }
  return *record;
}

Result<ResourceRecord> Fabric::resource_record(ResourceId resource) const {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  const ResourceRecord* record = impl_->find_resource(resource);
  if (record == nullptr) {
    return Status{Code::not_found, "resource is not declared"};
  }
  return *record;
}

std::vector<QueueRecord> Fabric::queues_of(ResourceId resource) const {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  std::vector<QueueRecord> out;
  for (const auto& entry : impl_->queues) {
    if (entry.second.def.resource == resource) {
      out.push_back(entry.second);
    }
  }
  std::sort(out.begin(), out.end(), [](const QueueRecord& a, const QueueRecord& b) { return a.def.id < b.def.id; });
  return out;
}

std::vector<QueueRecord> Fabric::all_queues() const {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  std::vector<QueueRecord> out;
  out.reserve(impl_->queues.size());
  for (const auto& entry : impl_->queues) {
    out.push_back(entry.second);
  }
  std::sort(out.begin(), out.end(), [](const QueueRecord& a, const QueueRecord& b) { return a.def.id < b.def.id; });
  return out;
}

Result<QueueId> Fabric::find_queue(ResourceId resource, std::string_view name) const {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  const auto it = impl_->queue_names.find(Impl::name_key(resource, name));
  if (it == impl_->queue_names.end()) {
    return Status{Code::not_found, "queue name is not declared in this resource"};
  }
  return it->second;
}

Result<ResourceId> Fabric::find_resource(std::string_view name) const {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  for (const auto& entry : impl_->resources) {
    if (entry.second.name == name) {
      return entry.second.id;
    }
  }
  return Status{Code::not_found, "resource name is not declared"};
}

std::vector<ResourceRecord> Fabric::all_resources() const {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  std::vector<ResourceRecord> out;
  out.reserve(impl_->resources.size());
  for (const auto& entry : impl_->resources) {
    out.push_back(entry.second);
  }
  std::sort(out.begin(), out.end(), [](const ResourceRecord& a, const ResourceRecord& b) { return a.id < b.id; });
  return out;
}

std::vector<PendingAttempt> Fabric::pending_attempts() const {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  return impl_->pending;
}

std::vector<BackendPlan> Fabric::pending_plans() const {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  std::vector<BackendPlan> plans;
  for (const auto& entry : impl_->plans) {
    plans.push_back(entry.second);
  }
  std::sort(plans.begin(), plans.end(), [](const BackendPlan& a, const BackendPlan& b) { return a.attempt < b.attempt; });
  return plans;
}

FabricStats Fabric::stats() const {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  FabricStats out = impl_->stats;
  out.resources = impl_->resources.size();
  out.queues = impl_->queues.size();
  out.pending_attempts = impl_->pending.size();
  out.publishers = impl_->provenance.publisher_count();
  out.epoch = impl_->epoch;
  out.boot = impl_->boot;
  std::size_t classes = 0;
  std::size_t pools = 0;
  for (const auto& entry : impl_->resources) {
    classes += entry.second.classes.size();
    pools += entry.second.pools.size();
  }
  out.classes = classes;
  out.pools = pools;
  return out;
}

Epoch Fabric::epoch() const {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  return impl_->epoch;
}

BootId Fabric::boot() const {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  return impl_->boot;
}

RecoveryReport Fabric::recovery_report() const {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  return impl_->recovery;
}

FabricPolicy Fabric::policy() const {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  return impl_->config.policy;
}

// ---- mutation helpers -------------------------------------------------------

Result<MutationOutcome> Fabric::declare_queue(const MutationContext& ctx,
                                              ResourceId resource,
                                              std::string name,
                                              std::uint32_t depth,
                                              ClassBinding cls,
                                              PoolRef pool,
                                              Thresholds thresholds) {
  MutationRequest request = make_request(MutationKind::declare_queue, ctx, QueueId{}, Generation{});
  DeclareQueuePayload payload{};
  payload.resource = resource;
  payload.name = std::move(name);
  payload.depth = depth;
  payload.cls = cls;
  payload.pool = pool;
  payload.thresholds = thresholds;
  request.payload = std::move(payload);
  return apply(request);
}

Result<MutationOutcome> Fabric::validate_queue(const MutationContext& ctx, QueueId queue, Generation expected) {
  return apply(make_request(MutationKind::validate_queue, ctx, queue, expected));
}

Result<MutationOutcome> Fabric::activate_queue(const MutationContext& ctx, QueueId queue, Generation expected) {
  return apply(make_request(MutationKind::activate_queue, ctx, queue, expected));
}

Result<MutationOutcome> Fabric::rebind_class(const MutationContext& ctx, QueueId queue, Generation expected, ClassBinding cls) {
  MutationRequest request = make_request(MutationKind::rebind_class, ctx, queue, expected);
  RebindClassPayload payload{};
  payload.cls = cls;
  request.payload = payload;
  return apply(request);
}

Result<MutationOutcome> Fabric::set_thresholds(const MutationContext& ctx, QueueId queue, Generation expected, Thresholds thresholds) {
  MutationRequest request = make_request(MutationKind::set_thresholds, ctx, queue, expected);
  ThresholdsPayload payload{};
  payload.thresholds = thresholds;
  request.payload = payload;
  return apply(request);
}

Result<MutationOutcome> Fabric::transfer_ownership(const MutationContext& ctx, QueueId queue, Generation expected, OwnerId owner) {
  MutationRequest request = make_request(MutationKind::transfer_ownership, ctx, queue, expected);
  OwnershipPayload payload{};
  payload.owner = owner;
  request.payload = payload;
  return apply(request);
}

Result<MutationOutcome> Fabric::ingest_occupancy(const MutationContext& ctx, const OccupancySample& sample) {
  MutationRequest request = make_request(MutationKind::ingest_occupancy, ctx, sample.queue, sample.queue_generation);
  // The observation itself carries the publisher identity and sequence that
  // make it unique, so it also defines the attempt identity.
  request.provenance = sample.provenance;
  request.attempt = AttemptId{sample.provenance.publisher, sample.provenance.sequence.value()};
  OccupancyPayload payload{};
  payload.sample = sample;
  request.payload = payload;
  return apply(request);
}

Result<MutationOutcome> Fabric::request_drain(const MutationContext& ctx, QueueId queue, Generation expected) {
  return apply(make_request(MutationKind::request_drain, ctx, queue, expected));
}

Result<MutationOutcome> Fabric::cancel_drain(const MutationContext& ctx, QueueId queue, Generation expected) {
  return apply(make_request(MutationKind::cancel_drain, ctx, queue, expected));
}

Result<MutationOutcome> Fabric::submit_drain_evidence(const MutationContext& ctx,
                                                      QueueId queue,
                                                      Generation expected,
                                                      const DrainEvidence& evidence) {
  MutationRequest request = make_request(MutationKind::submit_drain_evidence, ctx, queue, expected);
  DrainEvidencePayload payload{};
  payload.evidence = evidence;
  request.payload = payload;
  return apply(request);
}

Result<MutationOutcome> Fabric::quiesce_queue(const MutationContext& ctx, QueueId queue, Generation expected) {
  return apply(make_request(MutationKind::quiesce_queue, ctx, queue, expected));
}

Result<MutationOutcome> Fabric::fence_queue(const MutationContext& ctx, QueueId queue, Generation expected, std::string_view reason) {
  MutationRequest request = make_request(MutationKind::fence_queue, ctx, queue, expected);
  FencePayload payload{};
  payload.fence = true;
  payload.reason.assign(reason.substr(0, limits::kMaxReasonChars));
  request.payload = std::move(payload);
  return apply(request);
}

Result<MutationOutcome> Fabric::unfence_queue(const MutationContext& ctx, QueueId queue, Generation expected) {
  MutationRequest request = make_request(MutationKind::unfence_queue, ctx, queue, expected);
  FencePayload payload{};
  payload.fence = false;
  request.payload = payload;
  return apply(request);
}

Result<MutationOutcome> Fabric::fail_queue(const MutationContext& ctx, QueueId queue, Generation expected, std::string_view reason) {
  MutationRequest request = make_request(MutationKind::fail_queue, ctx, queue, expected);
  FencePayload payload{};
  payload.fence = false;
  payload.reason.assign(reason.substr(0, limits::kMaxReasonChars));
  request.payload = std::move(payload);
  return apply(request);
}

Result<MutationOutcome> Fabric::retire_queue(const MutationContext& ctx, QueueId queue, Generation expected) {
  return apply(make_request(MutationKind::retire_queue, ctx, queue, expected));
}

Result<MutationOutcome> Fabric::acknowledge_backend(const MutationContext& ctx,
                                                    QueueId queue,
                                                    Generation expected,
                                                    const BackendApplied& applied) {
  MutationRequest request = make_request(MutationKind::acknowledge_backend, ctx, queue, expected);
  BackendAckPayload payload{};
  payload.applied = applied;
  request.payload = payload;
  return apply(request);
}

}  // namespace qf
