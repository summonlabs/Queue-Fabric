// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <cstdint>

#include "qf/ids.hpp"
#include "qf/limits.hpp"
#include "qf/provenance.hpp"
#include "qf/status.hpp"
#include "qf/time.hpp"

namespace qf {

/// Why an occupancy observation is (not) usable as current authority.
enum class Freshness : std::uint8_t {
  unknown = 0,          ///< No observation has ever been accepted.
  fresh = 1,            ///< Within the freshness window and generation-exact.
  stale_age = 2,        ///< Older than the configured freshness window.
  stale_restart = 3,    ///< Produced before a restart; requires revalidation.
  stale_generation = 4, ///< Bound to a queue generation that is no longer current.
  stale_epoch = 5,      ///< Bound to a superseded coordinator epoch.
  future_timestamp = 6, ///< Observation time is ahead of the local clock.
  stale_backend = 7,    ///< Backend incarnation that produced it is gone.
};

const char* to_string(Freshness value) noexcept;

/// Freshness policy for occupancy evidence.
struct OccupancyPolicy {
  Nanos max_age{1000000000ll};                          ///< 1 s default window.
  Nanos max_future_skew{limits::kDefaultMaxFutureSkewNanos};
  bool require_generation_match{true};
};

/// One occupancy observation as published by a worker or backend.
struct OccupancySample {
  QueueId queue{};
  Generation queue_generation{};
  std::uint64_t bytes{0};
  std::uint64_t packets{0};
  Epoch epoch{};
  Sequence sequence{};
  Nanos observed_at{0};
  Provenance provenance{};

  [[nodiscard]] bool structurally_valid() const noexcept {
    return queue.valid() && queue_generation.valid() && sequence.valid() && epoch.valid() && provenance.valid();
  }
};

/// Authoritative occupancy state held for a queue. Present only when an
/// observation has been accepted; freshness is re-evaluated on every read.
struct OccupancyState {
  bool present{false};
  std::uint64_t bytes{0};
  std::uint64_t packets{0};
  Nanos observed_at{0};
  Sequence sequence{};
  Epoch epoch{};
  Provenance provenance{};
  Generation queue_generation{};
  Freshness freshness{Freshness::unknown};
  Nanos age{0};

  friend bool operator==(const OccupancyState& a, const OccupancyState& b) noexcept {
    return a.present == b.present && a.bytes == b.bytes && a.packets == b.packets && a.observed_at == b.observed_at &&
           a.sequence == b.sequence && a.epoch == b.epoch && a.queue_generation == b.queue_generation &&
           a.freshness == b.freshness && a.age == b.age;
  }
};

/// Age of an observation, saturated at zero if the clock went backwards.
[[nodiscard]] Nanos occupancy_age(Nanos now, Nanos observed_at) noexcept;

/// Classifies freshness without mutating state. The current generation is the
/// queue generation that is authoritative right now; a sample bound to any
/// other generation can never authorize a decision.
[[nodiscard]] Freshness classify_freshness(const OccupancyState& state,
                                           Nanos now,
                                           const OccupancyPolicy& policy,
                                           Generation current_generation) noexcept;

/// True only when the state is present and classified fresh.
[[nodiscard]] bool occupancy_authoritative(const OccupancyState& state) noexcept;

/// Marks restored state as requiring revalidation (restart, worker loss,
/// backend incarnation change). Dynamic evidence never survives a restart as
/// live authority.
void invalidate_occupancy(OccupancyState& state, Freshness reason) noexcept;

}  // namespace qf
