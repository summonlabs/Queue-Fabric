// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
//
// Internal engine state. Not installed and not part of the public surface.
#pragma once

#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "qf/fabric.hpp"
#include "qf/rng.hpp"
#include "qf/serialize.hpp"

namespace qf {

struct Fabric::Impl {
  FabricConfig config{};
  IClock* clock{nullptr};
  IJournal* journal{nullptr};
  IEventSink* sink{nullptr};

  mutable std::mutex mutex{};
  bool healthy{true};
  bool shutting_down{false};
  bool replaying{false};

  Epoch epoch{};
  BootId boot{};
  Incarnation coordinator_incarnation{};
  Rng rng{0x5EEDFAB1C0DE1234ull};
  ProvenanceTracker provenance{};
  RecoveryReport recovery{};

  std::unordered_map<ResourceId, ResourceRecord> resources{};
  std::unordered_map<QueueId, QueueRecord> queues{};
  std::unordered_map<std::string, QueueId> queue_names{};

  std::vector<PendingAttempt> pending{};
  std::unordered_map<AttemptId, BackendPlan> plans{};
  std::uint64_t next_resource_id{1};
  std::uint64_t next_queue_id{1};
  std::uint64_t next_class_id{1};
  std::uint64_t next_pool_id{1};
  std::uint64_t next_backend_id{1};
  std::uint64_t fence_tokens{0};

  FabricStats stats{};

  [[nodiscard]] ResourceRecord* find_resource(ResourceId id) noexcept;
  [[nodiscard]] const ResourceRecord* find_resource(ResourceId id) const noexcept;
  [[nodiscard]] QueueRecord* find_queue(QueueId id) noexcept;
  [[nodiscard]] const QueueRecord* find_queue(QueueId id) const noexcept;

  [[nodiscard]] static std::string name_key(ResourceId resource, std::string_view name);

  /// Applies one mutation. Caller holds the lock.
  Status apply_locked(const MutationRequest& request, MutationOutcome& outcome, std::vector<Event>& events);

  /// Declares a queue. Caller holds the lock.
  Status apply_declare_locked(const MutationRequest& request, Nanos now, MutationOutcome& outcome, std::vector<Event>& events);

  /// Applies a mutation against an existing queue. Caller holds the lock.
  Status apply_existing_locked(QueueRecord& queue,
                               const MutationRequest& request,
                               Nanos now,
                               MutationOutcome& outcome,
                               std::vector<Event>& events);

  /// Applies one mutation during replay or restore. Caller holds the lock.
  Status apply_replay_locked(const MutationRequest& request, MutationOutcome& outcome);

  /// Kind-specific validation and application on an already-authorized target.
  Status apply_kind_locked(QueueRecord& target,
                           const MutationRequest& request,
                           Nanos now,
                           MutationOutcome& outcome,
                           const char*& rule,
                           std::vector<Event>& events);

  /// Writes a durable mutation to the journal (begin + commit) and syncs.
  Status journal_commit_locked(const MutationRequest& request);

  /// Records a rejected attempt in the queue memo and counters.
  void record_rejection(QueueRecord* queue, const MutationRequest& request, const Status& status, const char* rule);

  /// Resolves the effective occupancy policy for one queue.
  [[nodiscard]] OccupancyPolicy occupancy_policy_for(const QueueRecord& queue) const noexcept;

  /// Finds the live backend incarnation named by a drain evidence record.
  [[nodiscard]] const BackendIncarnation* find_live_backend(ResourceId resource, BackendId backend) const noexcept;

  /// Restores durable state, advances the epoch and revalidates dynamic evidence.
  Status recover_locked(std::vector<Event>& events);

  /// Builds the compacted record set for the current authoritative state.
  void build_snapshot_locked(std::vector<JournalRecord>& records) const;

  /// Records the durable epoch/boot/incarnation marker.
  Status append_epoch_state_locked();

  /// Rebuilds derived counters and indexes from authoritative records.
  void rebuild_indexes_locked();

  /// Monotonic time watermark. A clock that jumps backwards must never
  /// resurrect authority that has already expired, so every freshness decision
  /// uses the highest time this fabric has ever observed.
  Nanos clock_watermark{0};
  [[nodiscard]] Nanos monotonic_now() noexcept;

  /// Recomputes the freshness classification of one queue against the clock.
  void refresh_queue_locked(QueueRecord& queue, Nanos now);

  [[nodiscard]] std::size_t queue_count_locked() const noexcept;
};

/// Emits collected events to the sink. Must be called without holding the lock.
void emit_events(IEventSink* sink, std::vector<Event>& events, std::uint64_t& counter);

}  // namespace qf
