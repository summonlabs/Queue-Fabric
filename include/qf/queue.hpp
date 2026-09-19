// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "qf/backend.hpp"
#include "qf/class_binding.hpp"
#include "qf/drain.hpp"
#include "qf/ids.hpp"
#include "qf/lifecycle.hpp"
#include "qf/limits.hpp"
#include "qf/mutation.hpp"
#include "qf/occupancy.hpp"
#include "qf/provenance.hpp"
#include "qf/threshold.hpp"
#include "qf/time.hpp"

namespace qf {

/// Authoritative queue definition. Identity, class binding and thresholds are
/// definitional; occupancy, drain progress and backend state are not.
struct QueueDefinition {
  QueueId id{};
  ResourceId resource{};
  std::string name{};
  std::uint32_t depth{0};
  ClassBinding cls{};
  PoolRef pool{};
  Thresholds thresholds{};
  OwnerId owner{};
  Generation generation{};
  Provenance declared_by{};
  Nanos declared_at{0};

  [[nodiscard]] bool valid() const noexcept {
    return id.valid() && resource.valid() && generation.valid() && !name.empty() && name.size() <= limits::kMaxNameChars &&
           cls.valid() && pool.valid() && thresholds.consistent();
  }
};

/// Fence state for a queue. While fenced, no fresh mutation is accepted except
/// an authorized unfence (or an administrative override).
struct FenceState {
  bool fenced{false};
  std::uint64_t token{0};
  Epoch epoch{};
  OwnerId fenced_by{};
  std::string reason{};
  Nanos fenced_at{0};
};

/// Complete authoritative record for one queue.
struct QueueRecord {
  QueueDefinition def{};
  Lifecycle lifecycle{Lifecycle::declared};
  Applicability applicability{Applicability::none};
  OccupancyState occupancy{};
  FenceState fence{};
  DrainState drain{};
  BackendApplied applied{};

  std::uint64_t mutation_count{0};
  std::uint64_t replayed_count{0};
  std::uint64_t rejected_count{0};
  std::uint64_t overflow_events{0};
  std::uint64_t staleness_events{0};

  AttemptId last_attempt{};
  Status last_status{};
  const char* last_rule{"unset"};
  std::string failure_reason{};

  std::vector<AttemptMemo> memo{};

  /// Looks up a previously applied attempt (idempotency).
  [[nodiscard]] const AttemptMemo* find_attempt(const AttemptId& attempt) const noexcept;

  /// Records a completed attempt in the bounded memo, evicting the oldest entry
  /// when full. Never grows without bound.
  void remember(const AttemptMemo& entry, std::size_t capacity);

  [[nodiscard]] bool accepts_fresh_mutation() const noexcept {
    return accepts_mutation(lifecycle, applicability);
  }

  [[nodiscard]] bool stale() const noexcept { return has_flag(applicability, Applicability::stale); }
  [[nodiscard]] bool fenced() const noexcept { return has_flag(applicability, Applicability::fenced); }
};

}  // namespace qf
