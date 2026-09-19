// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "qf/authority.hpp"
#include "qf/backend.hpp"
#include "qf/class_binding.hpp"
#include "qf/drain.hpp"
#include "qf/ids.hpp"
#include "qf/lifecycle.hpp"
#include "qf/limits.hpp"
#include "qf/occupancy.hpp"
#include "qf/queue.hpp"
#include "qf/threshold.hpp"

namespace qf {

/// Complete, bounded explanation of one queue's authoritative state and of the
/// rules that produced it. This is the operator-facing surface: identity,
/// lifecycle, class binding, occupancy freshness, thresholds, ownership,
/// authority vector, pending mutation, and stale/fence reasons.
struct Explanation {
  QueueId queue{};
  ResourceId resource{};
  std::string name{};
  Lifecycle lifecycle{Lifecycle::declared};
  Applicability applicability{Applicability::none};
  Generation generation{};

  ClassBinding cls{};
  PoolRef pool{};
  OwnerId owner{};
  Epoch authority_epoch{};
  std::uint32_t principal_capabilities{0};

  OccupancyState occupancy{};
  Thresholds thresholds{};
  ThresholdEvaluation threshold_eval{};

  FenceState fence{};
  DrainState drain{};
  DrainAssessment drain_assessment{};
  BackendApplied applied{};
  BackendVerification backend_verification{};

  AttemptId pending_attempt{};
  const char* pending_kind{"none"};
  AttemptId last_attempt{};
  const char* last_rule{"unset"};
  Status last_status{};

  std::uint64_t mutation_count{0};
  std::uint64_t replayed_count{0};
  std::uint64_t rejected_count{0};
  std::uint64_t overflow_events{0};
  std::uint64_t staleness_events{0};

  std::string stale_reason{};
  std::string fence_reason{};
  std::string failure_reason{};

  /// Bounded, stable human-readable rendering (never exceeds kMaxExplainBytes).
  [[nodiscard]] std::string render() const;

  /// Machine-readable single-line summary.
  [[nodiscard]] std::string to_json_like() const;
};

}  // namespace qf
