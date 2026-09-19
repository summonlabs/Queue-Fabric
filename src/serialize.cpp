// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
#include "qf/serialize.hpp"

namespace qf {

namespace {

void put_provenance(ByteWriter& writer, const Provenance& provenance) {
  writer.put_u64(provenance.publisher.value());
  writer.put_u64(provenance.node.value());
  writer.put_u64(provenance.incarnation.value());
  writer.put_u64(provenance.epoch.value());
  writer.put_u64(provenance.sequence.value());
  writer.put_u64(provenance.boot.hi);
  writer.put_u64(provenance.boot.lo);
}

Provenance get_provenance(ByteReader& reader) {
  Provenance provenance{};
  provenance.publisher = PublisherId::from_value(reader.get_u64());
  provenance.node = NodeId::from_value(reader.get_u64());
  provenance.incarnation = Incarnation::from_value(reader.get_u64());
  provenance.epoch = Epoch::from_value(reader.get_u64());
  provenance.sequence = Sequence::from_value(reader.get_u64());
  provenance.boot.hi = reader.get_u64();
  provenance.boot.lo = reader.get_u64();
  return provenance;
}

void put_attempt(ByteWriter& writer, const AttemptId& attempt) {
  writer.put_u64(attempt.publisher.value());
  writer.put_u64(attempt.counter);
}

AttemptId get_attempt(ByteReader& reader) {
  AttemptId attempt{};
  attempt.publisher = PublisherId::from_value(reader.get_u64());
  attempt.counter = reader.get_u64();
  return attempt;
}

void put_thresholds(ByteWriter& writer, const Thresholds& thresholds) {
  writer.put_u64(thresholds.low_watermark_bytes);
  writer.put_u64(thresholds.high_watermark_bytes);
  writer.put_u64(thresholds.max_occupancy_bytes);
  writer.put_u64(thresholds.max_packets);
  writer.put_i64(thresholds.occupancy_stale_after);
}

Thresholds get_thresholds(ByteReader& reader) {
  Thresholds thresholds{};
  thresholds.low_watermark_bytes = reader.get_u64();
  thresholds.high_watermark_bytes = reader.get_u64();
  thresholds.max_occupancy_bytes = reader.get_u64();
  thresholds.max_packets = reader.get_u64();
  thresholds.occupancy_stale_after = reader.get_i64();
  return thresholds;
}

void put_class_binding(ByteWriter& writer, const ClassBinding& binding) {
  writer.put_u64(binding.id.value());
  writer.put_u64(binding.generation.value());
}

ClassBinding get_class_binding(ByteReader& reader) {
  ClassBinding binding{};
  binding.id = ClassId::from_value(reader.get_u64());
  binding.generation = Generation::from_value(reader.get_u64());
  return binding;
}

void put_pool_ref(ByteWriter& writer, const PoolRef& pool) {
  writer.put_u64(pool.id.value());
  writer.put_u64(pool.generation.value());
  writer.put_u64(pool.capacity_bytes);
}

PoolRef get_pool_ref(ByteReader& reader) {
  PoolRef pool{};
  pool.id = PoolId::from_value(reader.get_u64());
  pool.generation = Generation::from_value(reader.get_u64());
  pool.capacity_bytes = reader.get_u64();
  return pool;
}

void put_occupancy_state(ByteWriter& writer, const OccupancyState& state) {
  writer.put_bool(state.present);
  writer.put_u64(state.bytes);
  writer.put_u64(state.packets);
  writer.put_i64(state.observed_at);
  writer.put_u64(state.sequence.value());
  writer.put_u64(state.epoch.value());
  put_provenance(writer, state.provenance);
  writer.put_u64(state.queue_generation.value());
  writer.put_u8(static_cast<std::uint8_t>(state.freshness));
  writer.put_i64(state.age);
}

OccupancyState get_occupancy_state(ByteReader& reader) {
  OccupancyState state{};
  state.present = reader.get_bool();
  state.bytes = reader.get_u64();
  state.packets = reader.get_u64();
  state.observed_at = reader.get_i64();
  state.sequence = Sequence::from_value(reader.get_u64());
  state.epoch = Epoch::from_value(reader.get_u64());
  state.provenance = get_provenance(reader);
  state.queue_generation = Generation::from_value(reader.get_u64());
  state.freshness = static_cast<Freshness>(reader.get_u8());
  state.age = reader.get_i64();
  return state;
}

void put_fence_state(ByteWriter& writer, const FenceState& fence) {
  writer.put_bool(fence.fenced);
  writer.put_u64(fence.token);
  writer.put_u64(fence.epoch.value());
  writer.put_u64(fence.fenced_by.value());
  writer.put_string(fence.reason);
  writer.put_i64(fence.fenced_at);
}

FenceState get_fence_state(ByteReader& reader) {
  FenceState fence{};
  fence.fenced = reader.get_bool();
  fence.token = reader.get_u64();
  fence.epoch = Epoch::from_value(reader.get_u64());
  fence.fenced_by = OwnerId::from_value(reader.get_u64());
  fence.reason = reader.get_string(limits::kMaxReasonChars);
  fence.fenced_at = reader.get_i64();
  return fence;
}

void put_drain_state(ByteWriter& writer, const DrainState& drain) {
  writer.put_u8(static_cast<std::uint8_t>(drain.phase));
  put_attempt(writer, drain.attempt);
  writer.put_i64(drain.requested_at);
  put_provenance(writer, drain.requested_by);
  writer.put_u32(drain.evidence.records);
  writer.put_bool(drain.evidence.backend.request_accepted);
  writer.put_bool(drain.evidence.backend.reported_drained);
  writer.put_u64(drain.evidence.backend.backend.value());
  writer.put_u64(drain.evidence.backend.backend_incarnation.value());
  writer.put_u64(drain.evidence.backend.epoch.value());
  writer.put_u64(drain.evidence.backend.sequence.value());
  writer.put_i64(drain.evidence.backend.observed_at);
  writer.put_bool(drain.evidence.occupancy_zero_observed);
  writer.put_i64(drain.evidence.zero_observed_at);
  writer.put_u64(drain.evidence.zero_sequence.value());
  writer.put_bool(drain.evidence.revalidated);
}

DrainState get_drain_state(ByteReader& reader) {
  DrainState drain{};
  drain.phase = static_cast<DrainPhase>(reader.get_u8());
  drain.attempt = get_attempt(reader);
  drain.requested_at = reader.get_i64();
  drain.requested_by = get_provenance(reader);
  drain.evidence.records = reader.get_u32();
  if (reader.failed() || drain.evidence.records > limits::kMaxDrainEvidenceRecords) {
    return drain;
  }
  drain.evidence.backend.request_accepted = reader.get_bool();
  drain.evidence.backend.reported_drained = reader.get_bool();
  drain.evidence.backend.backend = BackendId::from_value(reader.get_u64());
  drain.evidence.backend.backend_incarnation = Incarnation::from_value(reader.get_u64());
  drain.evidence.backend.epoch = Epoch::from_value(reader.get_u64());
  drain.evidence.backend.sequence = Sequence::from_value(reader.get_u64());
  drain.evidence.backend.observed_at = reader.get_i64();
  drain.evidence.occupancy_zero_observed = reader.get_bool();
  drain.evidence.zero_observed_at = reader.get_i64();
  drain.evidence.zero_sequence = Sequence::from_value(reader.get_u64());
  drain.evidence.revalidated = reader.get_bool();
  return drain;
}

void put_backend_applied(ByteWriter& writer, const BackendApplied& applied) {
  writer.put_bool(applied.known);
  writer.put_u64(applied.backend.value());
  writer.put_u64(applied.backend_incarnation.value());
  writer.put_u64(applied.epoch.value());
  writer.put_u64(applied.sequence.value());
  writer.put_i64(applied.applied_at);
  put_class_binding(writer, applied.applied_class);
  writer.put_u64(applied.queue_generation.value());
  writer.put_u64(applied.applied_high_watermark_bytes);
  writer.put_u64(applied.applied_max_occupancy_bytes);
}

BackendApplied get_backend_applied(ByteReader& reader) {
  BackendApplied applied{};
  applied.known = reader.get_bool();
  applied.backend = BackendId::from_value(reader.get_u64());
  applied.backend_incarnation = Incarnation::from_value(reader.get_u64());
  applied.epoch = Epoch::from_value(reader.get_u64());
  applied.sequence = Sequence::from_value(reader.get_u64());
  applied.applied_at = reader.get_i64();
  applied.applied_class = get_class_binding(reader);
  applied.queue_generation = Generation::from_value(reader.get_u64());
  applied.applied_high_watermark_bytes = reader.get_u64();
  applied.applied_max_occupancy_bytes = reader.get_u64();
  return applied;
}

}  // namespace

void encode_provenance(ByteWriter& writer, const Provenance& provenance) { put_provenance(writer, provenance); }
Provenance decode_provenance(ByteReader& reader) { return get_provenance(reader); }

Bytes encode_epoch_state(const EpochState& state) {
  ByteWriter writer(256);
  writer.put_u64(state.boot.hi);
  writer.put_u64(state.boot.lo);
  writer.put_u64(state.epoch.value());
  writer.put_u64(state.coordinator_incarnation.value());
  return writer.take();
}

Result<EpochState> decode_epoch_state(std::span<const std::byte> data) {
  ByteReader reader(data);
  EpochState state{};
  state.boot.hi = reader.get_u64();
  state.boot.lo = reader.get_u64();
  state.epoch = Epoch::from_value(reader.get_u64());
  state.coordinator_incarnation = Incarnation::from_value(reader.get_u64());
  if (reader.failed() || reader.remaining() != 0) {
    return Status{Code::malformed_input, "epoch state record is malformed"};
  }
  return state;
}

Bytes encode_resource_state(const ResourceRecord& resource) {
  ByteWriter writer;
  writer.put_u64(resource.id.value());
  writer.put_string(resource.name);
  writer.put_u64(resource.owner.value());
  writer.put_u64(resource.generation.value());
  writer.put_u64(resource.epoch.value());

  writer.put_u64(resource.authority.owner.value());
  writer.put_u64(resource.authority.epoch.value());
  writer.put_u32(static_cast<std::uint32_t>(resource.authority.grants.size()));
  for (const auto& grant : resource.authority.grants) {
    writer.put_u64(grant.owner.value());
    writer.put_u32(grant.capabilities);
  }

  writer.put_u32(static_cast<std::uint32_t>(resource.classes.size()));
  for (const auto& entry : resource.classes) {
    writer.put_u64(entry.id.value());
    writer.put_u64(entry.resource.value());
    writer.put_string(entry.name);
    writer.put_u32(entry.priority);
    writer.put_u32(entry.qos.priority_code);
    writer.put_u32(entry.qos.traffic_class);
    writer.put_string(entry.qos.group);
    writer.put_u64(entry.generation.value());
    put_provenance(writer, entry.declared_by);
  }

  writer.put_u32(static_cast<std::uint32_t>(resource.pools.size()));
  for (const auto& pool : resource.pools) {
    writer.put_u64(pool.id.value());
    writer.put_u64(pool.resource.value());
    writer.put_string(pool.name);
    writer.put_u64(pool.capacity_bytes);
    writer.put_u64(pool.generation.value());
    put_provenance(writer, pool.declared_by);
  }

  writer.put_u32(static_cast<std::uint32_t>(resource.backends.size()));
  for (const auto& backend : resource.backends) {
    writer.put_u64(backend.backend.value());
    writer.put_u64(backend.resource.value());
    writer.put_u64(backend.incarnation.value());
    writer.put_u64(backend.epoch.value());
    writer.put_i64(backend.registered_at);
    writer.put_bool(backend.alive);
  }

  writer.put_u8(static_cast<std::uint8_t>(resource.applicability));
  put_provenance(writer, resource.declared_by);
  writer.put_i64(resource.declared_at);
  writer.put_u64(static_cast<std::uint64_t>(resource.queue_count));
  return writer.failed() ? Bytes{} : writer.take();
}

Result<ResourceRecord> decode_resource_state(std::span<const std::byte> data) {
  ByteReader reader(data);
  ResourceRecord resource{};
  resource.id = ResourceId::from_value(reader.get_u64());
  resource.name = reader.get_string();
  resource.owner = OwnerId::from_value(reader.get_u64());
  resource.generation = Generation::from_value(reader.get_u64());
  resource.epoch = Epoch::from_value(reader.get_u64());

  resource.authority.owner = OwnerId::from_value(reader.get_u64());
  resource.authority.epoch = Epoch::from_value(reader.get_u64());
  const std::uint32_t grant_count = reader.get_u32();
  if (reader.failed() || grant_count > limits::kMaxGrantsPerResource) {
    return Status{Code::malformed_input, "resource grants are out of range"};
  }
  resource.authority.grants.reserve(grant_count);
  for (std::uint32_t i = 0; i < grant_count; ++i) {
    AuthorityGrant grant{};
    grant.owner = OwnerId::from_value(reader.get_u64());
    grant.capabilities = reader.get_u32();
    resource.authority.grants.push_back(grant);
  }

  const std::uint32_t class_count = reader.get_u32();
  if (reader.failed() || class_count > limits::kMaxClassesPerResource) {
    return Status{Code::malformed_input, "resource classes are out of range"};
  }
  resource.classes.reserve(class_count);
  for (std::uint32_t i = 0; i < class_count; ++i) {
    SchedulingClassDef entry{};
    entry.id = ClassId::from_value(reader.get_u64());
    entry.resource = ResourceId::from_value(reader.get_u64());
    entry.name = reader.get_string();
    entry.priority = reader.get_u32();
    entry.qos.priority_code = reader.get_u32();
    entry.qos.traffic_class = reader.get_u32();
    entry.qos.group = reader.get_string();
    entry.generation = Generation::from_value(reader.get_u64());
    entry.declared_by = get_provenance(reader);
    resource.classes.push_back(std::move(entry));
  }

  const std::uint32_t pool_count = reader.get_u32();
  if (reader.failed() || pool_count > limits::kMaxPoolsPerResource) {
    return Status{Code::malformed_input, "resource pools are out of range"};
  }
  resource.pools.reserve(pool_count);
  for (std::uint32_t i = 0; i < pool_count; ++i) {
    BufferPoolDef pool{};
    pool.id = PoolId::from_value(reader.get_u64());
    pool.resource = ResourceId::from_value(reader.get_u64());
    pool.name = reader.get_string();
    pool.capacity_bytes = reader.get_u64();
    pool.generation = Generation::from_value(reader.get_u64());
    pool.declared_by = get_provenance(reader);
    resource.pools.push_back(std::move(pool));
  }

  const std::uint32_t backend_count = reader.get_u32();
  if (reader.failed() || backend_count > limits::kMaxResources) {
    return Status{Code::malformed_input, "resource backends are out of range"};
  }
  resource.backends.reserve(backend_count);
  for (std::uint32_t i = 0; i < backend_count; ++i) {
    BackendIncarnation backend{};
    backend.backend = BackendId::from_value(reader.get_u64());
    backend.resource = ResourceId::from_value(reader.get_u64());
    backend.incarnation = Incarnation::from_value(reader.get_u64());
    backend.epoch = Epoch::from_value(reader.get_u64());
    backend.registered_at = reader.get_i64();
    backend.alive = reader.get_bool();
    resource.backends.push_back(backend);
  }

  resource.applicability = static_cast<Applicability>(reader.get_u8());
  resource.declared_by = get_provenance(reader);
  resource.declared_at = reader.get_i64();
  resource.queue_count = static_cast<std::size_t>(reader.get_u64());
  if (reader.failed() || reader.remaining() != 0) {
    return Status{Code::malformed_input, "resource state record is malformed"};
  }
  return resource;
}

Bytes encode_queue_state(const QueueRecord& queue) {
  ByteWriter writer;
  writer.put_u64(queue.def.id.value());
  writer.put_u64(queue.def.resource.value());
  writer.put_string(queue.def.name);
  writer.put_u32(queue.def.depth);
  put_class_binding(writer, queue.def.cls);
  put_pool_ref(writer, queue.def.pool);
  put_thresholds(writer, queue.def.thresholds);
  writer.put_u64(queue.def.owner.value());
  writer.put_u64(queue.def.generation.value());
  put_provenance(writer, queue.def.declared_by);
  writer.put_i64(queue.def.declared_at);

  writer.put_u8(static_cast<std::uint8_t>(queue.lifecycle));
  writer.put_u8(static_cast<std::uint8_t>(queue.applicability));
  writer.put_string(queue.failure_reason);
  put_occupancy_state(writer, queue.occupancy);
  put_fence_state(writer, queue.fence);
  put_drain_state(writer, queue.drain);
  put_backend_applied(writer, queue.applied);

  writer.put_u64(queue.mutation_count);
  writer.put_u64(queue.replayed_count);
  writer.put_u64(queue.rejected_count);
  writer.put_u64(queue.overflow_events);
  writer.put_u64(queue.staleness_events);
  put_attempt(writer, queue.last_attempt);

  writer.put_u32(static_cast<std::uint32_t>(queue.memo.size()));
  for (const auto& entry : queue.memo) {
    put_attempt(writer, entry.attempt);
    writer.put_u64(entry.fingerprint);
    writer.put_u16(static_cast<std::uint16_t>(entry.kind));
    writer.put_u16(static_cast<std::uint16_t>(entry.outcome));
    writer.put_u64(entry.generation.value());
    writer.put_bool(entry.durable);
  }
  return writer.failed() ? Bytes{} : writer.take();
}

Result<QueueRecord> decode_queue_state(std::span<const std::byte> data) {
  ByteReader reader(data);
  QueueRecord queue{};
  queue.def.id = QueueId::from_value(reader.get_u64());
  queue.def.resource = ResourceId::from_value(reader.get_u64());
  queue.def.name = reader.get_string();
  queue.def.depth = reader.get_u32();
  queue.def.cls = get_class_binding(reader);
  queue.def.pool = get_pool_ref(reader);
  queue.def.thresholds = get_thresholds(reader);
  queue.def.owner = OwnerId::from_value(reader.get_u64());
  queue.def.generation = Generation::from_value(reader.get_u64());
  queue.def.declared_by = get_provenance(reader);
  queue.def.declared_at = reader.get_i64();

  queue.lifecycle = static_cast<Lifecycle>(reader.get_u8());
  queue.applicability = static_cast<Applicability>(reader.get_u8());
  queue.failure_reason = reader.get_string(limits::kMaxReasonChars);
  queue.occupancy = get_occupancy_state(reader);
  queue.fence = get_fence_state(reader);
  queue.drain = get_drain_state(reader);
  queue.applied = get_backend_applied(reader);

  queue.mutation_count = reader.get_u64();
  queue.replayed_count = reader.get_u64();
  queue.rejected_count = reader.get_u64();
  queue.overflow_events = reader.get_u64();
  queue.staleness_events = reader.get_u64();
  queue.last_attempt = get_attempt(reader);

  const std::uint32_t memo_count = reader.get_u32();
  if (reader.failed() || memo_count > limits::kMaxAttemptMemoPerQueue) {
    return Status{Code::malformed_input, "queue memo is out of range"};
  }
  queue.memo.reserve(memo_count);
  for (std::uint32_t i = 0; i < memo_count; ++i) {
    AttemptMemo entry{};
    entry.attempt = get_attempt(reader);
    entry.fingerprint = reader.get_u64();
    entry.kind = static_cast<MutationKind>(reader.get_u16());
    entry.outcome = static_cast<Code>(reader.get_u16());
    entry.generation = Generation::from_value(reader.get_u64());
    entry.durable = reader.get_bool();
    queue.memo.push_back(entry);
  }
  if (reader.failed() || reader.remaining() != 0) {
    return Status{Code::malformed_input, "queue state record is malformed"};
  }
  return queue;
}

namespace {

enum class PayloadTag : std::uint8_t {
  monostate = 0,
  declare_queue = 1,
  rebind_class = 2,
  thresholds = 3,
  ownership = 4,
  occupancy = 5,
  drain_evidence = 6,
  fence = 7,
  backend_ack = 8,
  backend_registration = 9,
};

PayloadTag tag_of(const MutationPayload& payload) noexcept {
  switch (payload.index()) {
    case 1: return PayloadTag::declare_queue;
    case 2: return PayloadTag::rebind_class;
    case 3: return PayloadTag::thresholds;
    case 4: return PayloadTag::ownership;
    case 5: return PayloadTag::occupancy;
    case 6: return PayloadTag::drain_evidence;
    case 7: return PayloadTag::fence;
    case 8: return PayloadTag::backend_ack;
    case 9: return PayloadTag::backend_registration;
    default: return PayloadTag::monostate;
  }
}

void put_occupancy_sample(ByteWriter& writer, const OccupancySample& sample) {
  writer.put_u64(sample.queue.value());
  writer.put_u64(sample.queue_generation.value());
  writer.put_u64(sample.bytes);
  writer.put_u64(sample.packets);
  writer.put_u64(sample.epoch.value());
  writer.put_u64(sample.sequence.value());
  writer.put_i64(sample.observed_at);
  put_provenance(writer, sample.provenance);
}

OccupancySample get_occupancy_sample(ByteReader& reader) {
  OccupancySample sample{};
  sample.queue = QueueId::from_value(reader.get_u64());
  sample.queue_generation = Generation::from_value(reader.get_u64());
  sample.bytes = reader.get_u64();
  sample.packets = reader.get_u64();
  sample.epoch = Epoch::from_value(reader.get_u64());
  sample.sequence = Sequence::from_value(reader.get_u64());
  sample.observed_at = reader.get_i64();
  sample.provenance = get_provenance(reader);
  return sample;
}

void put_drain_evidence(ByteWriter& writer, const DrainEvidence& evidence) {
  writer.put_u32(evidence.records);
  writer.put_bool(evidence.backend.request_accepted);
  writer.put_bool(evidence.backend.reported_drained);
  writer.put_u64(evidence.backend.backend.value());
  writer.put_u64(evidence.backend.backend_incarnation.value());
  writer.put_u64(evidence.backend.epoch.value());
  writer.put_u64(evidence.backend.sequence.value());
  writer.put_i64(evidence.backend.observed_at);
  writer.put_bool(evidence.occupancy_zero_observed);
  writer.put_i64(evidence.zero_observed_at);
  writer.put_u64(evidence.zero_sequence.value());
  writer.put_bool(evidence.revalidated);
}

Result<DrainEvidence> get_drain_evidence(ByteReader& reader) {
  DrainEvidence evidence{};
  evidence.records = reader.get_u32();
  if (reader.failed() || evidence.records > limits::kMaxDrainEvidenceRecords) {
    return Status{Code::malformed_input, "drain evidence record count is out of range"};
  }
  evidence.backend.request_accepted = reader.get_bool();
  evidence.backend.reported_drained = reader.get_bool();
  evidence.backend.backend = BackendId::from_value(reader.get_u64());
  evidence.backend.backend_incarnation = Incarnation::from_value(reader.get_u64());
  evidence.backend.epoch = Epoch::from_value(reader.get_u64());
  evidence.backend.sequence = Sequence::from_value(reader.get_u64());
  evidence.backend.observed_at = reader.get_i64();
  evidence.occupancy_zero_observed = reader.get_bool();
  evidence.zero_observed_at = reader.get_i64();
  evidence.zero_sequence = Sequence::from_value(reader.get_u64());
  evidence.revalidated = reader.get_bool();
  if (reader.failed()) {
    return Status{Code::malformed_input, "drain evidence is malformed"};
  }
  return evidence;
}

}  // namespace

Bytes encode_mutation(const MutationRequest& request) {
  ByteWriter writer;
  put_attempt(writer, request.attempt);
  writer.put_u16(static_cast<std::uint16_t>(request.kind));
  writer.put_u64(request.queue.value());
  writer.put_u64(request.expected_generation.value());
  writer.put_u64(request.expected_epoch.value());
  writer.put_u64(request.principal.value());
  put_provenance(writer, request.provenance);
  writer.put_u8(static_cast<std::uint8_t>(tag_of(request.payload)));
  switch (tag_of(request.payload)) {
    case PayloadTag::declare_queue: {
      const auto& declare = std::get<DeclareQueuePayload>(request.payload);
      writer.put_u64(declare.resource.value());
      writer.put_string(declare.name);
      writer.put_u32(declare.depth);
      put_class_binding(writer, declare.cls);
      put_pool_ref(writer, declare.pool);
      put_thresholds(writer, declare.thresholds);
      break;
    }
    case PayloadTag::rebind_class:
      put_class_binding(writer, std::get<RebindClassPayload>(request.payload).cls);
      break;
    case PayloadTag::thresholds:
      put_thresholds(writer, std::get<ThresholdsPayload>(request.payload).thresholds);
      break;
    case PayloadTag::ownership:
      writer.put_u64(std::get<OwnershipPayload>(request.payload).owner.value());
      break;
    case PayloadTag::occupancy:
      put_occupancy_sample(writer, std::get<OccupancyPayload>(request.payload).sample);
      break;
    case PayloadTag::drain_evidence:
      put_drain_evidence(writer, std::get<DrainEvidencePayload>(request.payload).evidence);
      break;
    case PayloadTag::fence: {
      const auto& fence = std::get<FencePayload>(request.payload);
      writer.put_bool(fence.fence);
      writer.put_string(fence.reason);
      break;
    }
    case PayloadTag::backend_ack:
      put_backend_applied(writer, std::get<BackendAckPayload>(request.payload).applied);
      break;
    case PayloadTag::backend_registration: {
      const auto& backend = std::get<BackendRegistrationPayload>(request.payload);
      writer.put_u64(backend.backend.value());
      writer.put_u64(backend.resource.value());
      writer.put_u64(backend.incarnation.value());
      break;
    }
    case PayloadTag::monostate:
      break;
  }
  return writer.failed() ? Bytes{} : writer.take();
}

Result<MutationRequest> decode_mutation(std::span<const std::byte> data) {
  ByteReader reader(data);
  MutationRequest request{};
  request.attempt = get_attempt(reader);
  request.kind = static_cast<MutationKind>(reader.get_u16());
  request.queue = QueueId::from_value(reader.get_u64());
  request.expected_generation = Generation::from_value(reader.get_u64());
  request.expected_epoch = Epoch::from_value(reader.get_u64());
  request.principal = OwnerId::from_value(reader.get_u64());
  request.provenance = get_provenance(reader);
  const auto tag = static_cast<PayloadTag>(reader.get_u8());
  if (reader.failed()) {
    return Status{Code::malformed_input, "mutation record is truncated"};
  }
  switch (tag) {
    case PayloadTag::monostate:
      request.payload = std::monostate{};
      break;
    case PayloadTag::declare_queue: {
      DeclareQueuePayload declare{};
      declare.resource = ResourceId::from_value(reader.get_u64());
      declare.name = reader.get_string();
      declare.depth = reader.get_u32();
      declare.cls = get_class_binding(reader);
      declare.pool = get_pool_ref(reader);
      declare.thresholds = get_thresholds(reader);
      request.payload = std::move(declare);
      break;
    }
    case PayloadTag::rebind_class: {
      RebindClassPayload rebind{};
      rebind.cls = get_class_binding(reader);
      request.payload = rebind;
      break;
    }
    case PayloadTag::thresholds: {
      ThresholdsPayload thresholds{};
      thresholds.thresholds = get_thresholds(reader);
      request.payload = thresholds;
      break;
    }
    case PayloadTag::ownership: {
      OwnershipPayload ownership{};
      ownership.owner = OwnerId::from_value(reader.get_u64());
      request.payload = ownership;
      break;
    }
    case PayloadTag::occupancy: {
      OccupancyPayload occupancy{};
      occupancy.sample = get_occupancy_sample(reader);
      request.payload = occupancy;
      break;
    }
    case PayloadTag::drain_evidence: {
      auto evidence = get_drain_evidence(reader);
      if (!evidence.ok()) {
        return evidence.status();
      }
      DrainEvidencePayload payload{};
      payload.evidence = evidence.value();
      request.payload = payload;
      break;
    }
    case PayloadTag::fence: {
      FencePayload fence{};
      fence.fence = reader.get_bool();
      fence.reason = reader.get_string(limits::kMaxReasonChars);
      request.payload = fence;
      break;
    }
    case PayloadTag::backend_ack: {
      BackendAckPayload ack{};
      ack.applied = get_backend_applied(reader);
      request.payload = ack;
      break;
    }
    case PayloadTag::backend_registration: {
      BackendRegistrationPayload backend{};
      backend.backend = BackendId::from_value(reader.get_u64());
      backend.resource = ResourceId::from_value(reader.get_u64());
      backend.incarnation = Incarnation::from_value(reader.get_u64());
      request.payload = backend;
      break;
    }
    default:
      return Status{Code::malformed_input, "mutation payload tag is unknown"};
  }
  if (reader.failed() || reader.remaining() != 0) {
    return Status{Code::malformed_input, "mutation record is malformed"};
  }
  return request;
}

}  // namespace qf
