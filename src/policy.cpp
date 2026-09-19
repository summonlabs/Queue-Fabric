// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
#include "qf/policy.hpp"

namespace qf {

const ClassPolicy* FabricPolicy::find_class(ClassId cls) const noexcept {
  for (const auto& entry : classes) {
    if (entry.cls == cls) {
      return &entry;
    }
  }
  return nullptr;
}

Thresholds FabricPolicy::resolve(const Thresholds& queue_thresholds, ClassId cls) const noexcept {
  const ClassPolicy* class_policy = find_class(cls);
  Thresholds resolved{};
  resolved.low_watermark_bytes = queue_thresholds.low_watermark_bytes != 0
                                     ? queue_thresholds.low_watermark_bytes
                                     : (class_policy != nullptr ? class_policy->thresholds.low_watermark_bytes
                                                                : default_thresholds.low_watermark_bytes);
  resolved.high_watermark_bytes = queue_thresholds.high_watermark_bytes != 0
                                      ? queue_thresholds.high_watermark_bytes
                                      : (class_policy != nullptr ? class_policy->thresholds.high_watermark_bytes
                                                                 : default_thresholds.high_watermark_bytes);
  resolved.max_occupancy_bytes = queue_thresholds.max_occupancy_bytes != 0
                                     ? queue_thresholds.max_occupancy_bytes
                                     : (class_policy != nullptr ? class_policy->thresholds.max_occupancy_bytes
                                                                : default_thresholds.max_occupancy_bytes);
  resolved.max_packets = queue_thresholds.max_packets != 0
                             ? queue_thresholds.max_packets
                             : (class_policy != nullptr ? class_policy->thresholds.max_packets
                                                        : default_thresholds.max_packets);
  resolved.occupancy_stale_after = queue_thresholds.occupancy_stale_after != 0
                                       ? queue_thresholds.occupancy_stale_after
                                       : (class_policy != nullptr && class_policy->occupancy_stale_after != 0
                                              ? class_policy->occupancy_stale_after
                                              : occupancy_stale_after);
  return resolved;
}

Nanos FabricPolicy::stale_after_for(const Thresholds& effective, ClassId cls) const noexcept {
  if (effective.occupancy_stale_after != 0) {
    return effective.occupancy_stale_after;
  }
  const ClassPolicy* class_policy = find_class(cls);
  if (class_policy != nullptr && class_policy->occupancy_stale_after != 0) {
    return class_policy->occupancy_stale_after;
  }
  return occupancy_stale_after;
}

}  // namespace qf
