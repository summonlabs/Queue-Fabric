// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <cstdint>
#include <string>

#include "qf/class_binding.hpp"
#include "qf/ids.hpp"
#include "qf/limits.hpp"
#include "qf/time.hpp"

namespace qf {

/// State that a backend claims to have applied for a queue.
///
/// A backend acknowledgement is not applied effect. This record only ever
/// states what a specific backend incarnation reported; verification requires
/// matching the backend incarnation, epoch, class binding and queue generation
/// that were current when the acknowledgement was produced.
struct BackendApplied {
  bool known{false};
  BackendId backend{};
  Incarnation backend_incarnation{};
  Epoch epoch{};
  Sequence sequence{};
  Nanos applied_at{0};
  ClassBinding applied_class{};
  Generation queue_generation{};
  std::uint64_t applied_high_watermark_bytes{0};
  std::uint64_t applied_max_occupancy_bytes{0};

  friend bool operator==(const BackendApplied& a, const BackendApplied& b) noexcept {
    return a.known == b.known && a.backend == b.backend && a.backend_incarnation == b.backend_incarnation &&
           a.epoch == b.epoch && a.sequence == b.sequence && a.applied_at == b.applied_at &&
           a.applied_class == b.applied_class && a.queue_generation == b.queue_generation &&
           a.applied_high_watermark_bytes == b.applied_high_watermark_bytes &&
           a.applied_max_occupancy_bytes == b.applied_max_occupancy_bytes;
  }
};

/// Registry entry for a known backend incarnation.
struct BackendIncarnation {
  BackendId backend{};
  ResourceId resource{};
  Incarnation incarnation{};
  Epoch epoch{};
  Nanos registered_at{0};
  bool alive{true};
};

/// Explanation of whether a backend acknowledgement is currently verifiable.
struct BackendVerification {
  bool verified{false};
  Code code{Code::stale_incarnation};
  const char* rule{"unset"};
};

/// A backend acknowledgement is verified only when it names the live backend
/// incarnation, the current coordinator epoch, the current queue generation and
/// the current class binding generation.
[[nodiscard]] BackendVerification verify_backend_applied(const BackendApplied& applied,
                                                         const BackendIncarnation& live,
                                                         Epoch current_epoch,
                                                         Generation current_queue_generation,
                                                         ClassBinding current_class) noexcept;

}  // namespace qf
