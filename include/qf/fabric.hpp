// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "qf/authority.hpp"
#include "qf/backend.hpp"
#include "qf/class_binding.hpp"
#include "qf/drain.hpp"
#include "qf/explain.hpp"
#include "qf/ids.hpp"
#include "qf/journal.hpp"
#include "qf/limits.hpp"
#include "qf/lifecycle.hpp"
#include "qf/mutation.hpp"
#include "qf/occupancy.hpp"
#include "qf/policy.hpp"
#include "qf/provenance.hpp"
#include "qf/queue.hpp"
#include "qf/status.hpp"
#include "qf/threshold.hpp"
#include "qf/time.hpp"

namespace qf {

/// Hard runtime bounds enforced by the fabric itself.
struct FabricLimits {
  std::size_t max_resources{limits::kMaxResources};
  std::size_t max_queues_per_resource{limits::kMaxQueuesPerResource};
  std::size_t max_classes_per_resource{limits::kMaxClassesPerResource};
  std::size_t max_pools_per_resource{limits::kMaxPoolsPerResource};
  std::size_t max_grants_per_resource{limits::kMaxGrantsPerResource};
  std::size_t max_attempt_memo{limits::kMaxAttemptMemoPerQueue};
  std::size_t max_pending_attempts{limits::kMaxPendingAttempts};
  std::size_t max_queues_total{limits::kMaxQueuesTotal};
};

/// Fabric configuration.
struct FabricConfig {
  FabricLimits limits{};
  FabricPolicy policy{};
  OccupancyPolicy occupancy{};
  bool durable{false};  ///< True requires a journal for durable mutations.
  /// Seed for boot-identity generation. Zero derives a per-process seed from
  /// the system clock, the process id and the instance address, so two
  /// processes can never claim the same boot identifier. Tests may set an
  /// explicit seed for reproducibility.
  std::uint64_t boot_seed{0};
};

enum class EventKind : std::uint8_t {
  mutation_applied = 0,
  mutation_replayed = 1,
  mutation_rejected = 2,
  lifecycle_changed = 3,
  occupancy_ingested = 4,
  occupancy_rejected = 5,
  occupancy_overflow = 6,
  drain_progress = 7,
  fence_changed = 8,
  queue_declared = 9,
  queue_retired = 10,
  topology_changed = 11,
  backend_registered = 12,
  backend_dead = 13,
  recovery = 14,
  shutdown = 15,
};

const char* to_string(EventKind value) noexcept;

/// Immutable notification. Events are always emitted after the authoritative
/// state change is committed and after all internal locks have been released;
/// no callback ever runs while the fabric is locked.
struct Event {
  EventKind kind{EventKind::mutation_applied};
  Nanos at{0};
  QueueId queue{};
  ResourceId resource{};
  MutationKind mutation{MutationKind::none};
  AttemptId attempt{};
  Code code{Code::ok};
  Generation generation{};
  Epoch epoch{};
  Lifecycle lifecycle{Lifecycle::declared};
  std::string detail{};
};

/// Event consumer. Implementations must not call back into the same Fabric
/// instance from on_event; the fabric never holds its lock while emitting.
class IEventSink {
 public:
  IEventSink() = default;
  IEventSink(const IEventSink&) = delete;
  IEventSink& operator=(const IEventSink&) = delete;
  virtual ~IEventSink();
  virtual void on_event(const Event& event) = 0;
};

/// Durable topology record for one resource.
struct ResourceRecord {
  ResourceId id{};
  std::string name{};
  OwnerId owner{};
  Generation generation{};
  Epoch epoch{};
  AuthorityVector authority{};
  std::vector<SchedulingClassDef> classes{};
  std::vector<BufferPoolDef> pools{};
  std::vector<BackendIncarnation> backends{};
  Applicability applicability{Applicability::none};
  Provenance declared_by{};
  Nanos declared_at{0};
  std::size_t queue_count{0};

  [[nodiscard]] const SchedulingClassDef* find_class(ClassId cls) const noexcept;
  [[nodiscard]] const BufferPoolDef* find_pool(PoolId pool) const noexcept;
  [[nodiscard]] const BackendIncarnation* find_backend(BackendId backend) const noexcept;
};

/// An attempt that has been accepted for processing and not yet committed.
struct PendingAttempt {
  AttemptId attempt{};
  QueueId queue{};
  MutationKind kind{MutationKind::none};
  Nanos started_at{0};
};

/// Aggregate counters. Used for explanation, accounting and tests.
struct FabricStats {
  std::size_t resources{0};
  std::size_t queues{0};
  std::size_t classes{0};
  std::size_t pools{0};
  std::size_t publishers{0};
  std::size_t pending_attempts{0};
  std::uint64_t mutations_applied{0};
  std::uint64_t mutations_replayed{0};
  std::uint64_t mutations_rejected{0};
  std::uint64_t occupancy_accepted{0};
  std::uint64_t occupancy_rejected{0};
  std::uint64_t occupancy_overflow{0};
  std::uint64_t staleness_events{0};
  std::uint64_t lifecycle_transitions{0};
  std::uint64_t events_emitted{0};
  std::uint64_t journal_records{0};
  std::uint64_t journal_syncs{0};
  Epoch epoch{};
  BootId boot{};
};

/// Caller-supplied authority context for a mutation.
struct MutationContext {
  OwnerId principal{};
  Provenance provenance{};
  Epoch epoch{};
};

/// Reservation for a two-phase backend application.
///
/// Phase 1 validates authority, lifecycle, generation and class binding, writes
/// a durable begin record and reserves the attempt. Phase 2 (external) asks the
/// backend to apply. Phase 3 verifies the acknowledgement against the live
/// backend incarnation and the exact generations and commits, or aborts. A
/// crash between phase 1 and phase 3 leaves an unfinished attempt that recovery
/// reports as ambiguous instead of guessing.
struct BackendPlan {
  AttemptId attempt{};
  QueueId queue{};
  ResourceId resource{};
  Generation expected_generation{};
  ClassBinding cls{};
  Thresholds thresholds{};
  OwnerId principal{};
  Epoch epoch{};
  Nanos planned_at{0};
};

/// The queue fabric: authoritative queue lifecycle, class binding, occupancy
/// and mutation governance for a set of resources.
///
/// Thread safety: every public method is safe to call concurrently. The fabric
/// uses a single internal mutex, never calls user code, backends or sinks while
/// holding it, and never re-enters its own lock. Explanations and query results
/// are returned by value.
class Fabric {
 public:
  Fabric(FabricConfig config, IClock& clock, IJournal* journal = nullptr, IEventSink* sink = nullptr);
  ~Fabric();

  Fabric(const Fabric&) = delete;
  Fabric& operator=(const Fabric&) = delete;

  /// Opens durable state, restores it and advances the coordinator epoch.
  /// Without a journal this simply initialises an empty, non-durable fabric.
  Status recover();

  /// Stops accepting work. In-flight mutations that already crossed their
  /// durable commit boundary remain committed; nothing later is accepted.
  Status shutdown();

  [[nodiscard]] bool shutting_down() const;

  // ---- topology (administrative, durable) ---------------------------------

  Result<ResourceId> declare_resource(std::string_view name, OwnerId owner, const Provenance& provenance);

  Status grant_capabilities(ResourceId resource,
                            OwnerId grantee,
                            std::uint32_t capabilities,
                            OwnerId principal,
                            Epoch expected_epoch,
                            const Provenance& provenance);

  Result<ClassId> declare_class(ResourceId resource,
                                std::string_view name,
                                std::uint32_t priority,
                                const QosRef& qos,
                                OwnerId principal,
                                Epoch expected_epoch,
                                const Provenance& provenance);

  Result<PoolId> declare_pool(ResourceId resource,
                              std::string_view name,
                              std::uint64_t capacity_bytes,
                              OwnerId principal,
                              Epoch expected_epoch,
                              const Provenance& provenance);

  Result<BackendId> register_backend(ResourceId resource,
                                     Incarnation incarnation,
                                     OwnerId principal,
                                     Epoch expected_epoch,
                                     const Provenance& provenance);

  Status mark_backend_dead(BackendId backend, Incarnation incarnation, OwnerId principal, Epoch expected_epoch);

  /// Advances the coordinator epoch. Durable and monotonic. Every epoch-bound
  /// acknowledgement and observation from the previous epoch stops being
  /// verifiable, so queues require revalidation under the new epoch.
  Result<Epoch> advance_epoch(OwnerId principal, const Provenance& provenance);

  /// Marks a queue's dynamic evidence as requiring revalidation. Used when a
  /// publisher or backend incarnation is lost.
  Status invalidate_evidence(QueueId queue, OwnerId principal, Epoch expected_epoch, std::string_view reason);

  // ---- queue governance path ----------------------------------------------

  /// Applies one mutation. Durable mutations are journalled and synced before
  /// they are acknowledged; duplicate attempts return the original outcome.
  Result<MutationOutcome> apply(const MutationRequest& request);

  Result<MutationOutcome> declare_queue(const MutationContext& ctx,
                                        ResourceId resource,
                                        std::string name,
                                        std::uint32_t depth,
                                        ClassBinding cls,
                                        PoolRef pool,
                                        Thresholds thresholds);
  Result<MutationOutcome> validate_queue(const MutationContext& ctx, QueueId queue, Generation expected);
  Result<MutationOutcome> activate_queue(const MutationContext& ctx, QueueId queue, Generation expected);
  Result<MutationOutcome> rebind_class(const MutationContext& ctx, QueueId queue, Generation expected, ClassBinding cls);
  Result<MutationOutcome> set_thresholds(const MutationContext& ctx, QueueId queue, Generation expected, Thresholds thresholds);
  Result<MutationOutcome> transfer_ownership(const MutationContext& ctx, QueueId queue, Generation expected, OwnerId owner);
  Result<MutationOutcome> ingest_occupancy(const MutationContext& ctx, const OccupancySample& sample);
  Result<MutationOutcome> request_drain(const MutationContext& ctx, QueueId queue, Generation expected);
  Result<MutationOutcome> cancel_drain(const MutationContext& ctx, QueueId queue, Generation expected);
  Result<MutationOutcome> submit_drain_evidence(const MutationContext& ctx,
                                                QueueId queue,
                                                Generation expected,
                                                const DrainEvidence& evidence);
  Result<MutationOutcome> quiesce_queue(const MutationContext& ctx, QueueId queue, Generation expected);
  Result<MutationOutcome> fence_queue(const MutationContext& ctx, QueueId queue, Generation expected, std::string_view reason);
  Result<MutationOutcome> unfence_queue(const MutationContext& ctx, QueueId queue, Generation expected);
  Result<MutationOutcome> fail_queue(const MutationContext& ctx, QueueId queue, Generation expected, std::string_view reason);
  Result<MutationOutcome> retire_queue(const MutationContext& ctx, QueueId queue, Generation expected);
  Result<MutationOutcome> acknowledge_backend(const MutationContext& ctx,
                                              QueueId queue,
                                              Generation expected,
                                              const BackendApplied& applied);

  // ---- two-phase backend application --------------------------------------

  /// Phase 1: validate, bind authority, plan and durably reserve an apply.
  Result<BackendPlan> plan_backend_apply(QueueId queue,
                                         Generation expected,
                                         OwnerId principal,
                                         const Provenance& provenance);

  /// Phase 3a: verify the acknowledgement and commit the planned apply.
  Result<MutationOutcome> commit_backend_apply(const BackendPlan& plan,
                                               const BackendApplied& applied,
                                               const Provenance& provenance);

  /// Phase 3b: release the reservation without committing anything.
  Status abort_backend_apply(const BackendPlan& plan, std::string_view reason);

  [[nodiscard]] std::vector<BackendPlan> pending_plans() const;

  // ---- queries -------------------------------------------------------------

  [[nodiscard]] Result<Explanation> explain(QueueId queue, OwnerId principal = OwnerId{}) const;
  [[nodiscard]] Result<QueueRecord> queue_record(QueueId queue) const;
  [[nodiscard]] Result<ResourceRecord> resource_record(ResourceId resource) const;
  [[nodiscard]] std::vector<QueueRecord> queues_of(ResourceId resource) const;
  [[nodiscard]] std::vector<QueueRecord> all_queues() const;
  [[nodiscard]] Result<QueueId> find_queue(ResourceId resource, std::string_view name) const;
  [[nodiscard]] Result<ResourceId> find_resource(std::string_view name) const;
  [[nodiscard]] std::vector<ResourceRecord> all_resources() const;
  [[nodiscard]] std::vector<PendingAttempt> pending_attempts() const;
  [[nodiscard]] FabricStats stats() const;
  [[nodiscard]] Epoch epoch() const;
  [[nodiscard]] BootId boot() const;
  [[nodiscard]] RecoveryReport recovery_report() const;
  [[nodiscard]] FabricPolicy policy() const;

  // ---- maintenance ---------------------------------------------------------

  /// Re-classifies stored occupancy against the current clock and generation,
  /// marking evidence stale when it is no longer authoritative.
  Status refresh_freshness();

  /// Rewrites durable storage with a compacted snapshot of authoritative state.
  Status compact();

  /// Self-audit: verifies the fabric's declared invariants against live state.
  /// Returns the number of violations found; details are appended to the vector.
  std::size_t validate_invariants(std::vector<std::string>& violations) const;

 private:
  struct Impl;
  Impl* impl_;
};

}  // namespace qf
