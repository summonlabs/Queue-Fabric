// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <cstdint>
#include <vector>

#include "qf/class_binding.hpp"
#include "qf/ids.hpp"
#include "qf/limits.hpp"
#include "qf/threshold.hpp"
#include "qf/time.hpp"

namespace qf {

/// Per-scheduling-class policy overrides.
struct ClassPolicy {
  ClassId cls{};
  Thresholds thresholds{};
  Nanos occupancy_stale_after{0};

  friend bool operator==(const ClassPolicy& a, const ClassPolicy& b) noexcept {
    return a.cls == b.cls && a.thresholds == b.thresholds && a.occupancy_stale_after == b.occupancy_stale_after;
  }
};

/// Fabric policy: defaults and per-class overrides applied when a queue does not
/// carry an explicit value. Policy resolution never invents evidence; it only
/// supplies configured limits.
struct FabricPolicy {
  PolicyId id{};
  Generation generation{};
  Thresholds default_thresholds{};
  Nanos occupancy_stale_after{1000000000ll};
  std::vector<ClassPolicy> classes{};
  std::size_t max_drain_evidence_records{limits::kMaxDrainEvidenceRecords};
  bool require_backend_ack_for_drain{true};

  [[nodiscard]] const ClassPolicy* find_class(ClassId cls) const noexcept;

  /// Effective thresholds for a queue. A zero field in the queue definition is
  /// unresolved and falls back to the class policy and then to the defaults.
  [[nodiscard]] Thresholds resolve(const Thresholds& queue_thresholds, ClassId cls) const noexcept;

  [[nodiscard]] Nanos stale_after_for(const Thresholds& effective, ClassId cls) const noexcept;
};

}  // namespace qf
