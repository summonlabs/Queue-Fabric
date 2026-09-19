// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
#include "qf/occupancy.hpp"

namespace qf {

const char* to_string(Freshness value) noexcept {
  switch (value) {
    case Freshness::unknown: return "unknown";
    case Freshness::fresh: return "fresh";
    case Freshness::stale_age: return "stale_age";
    case Freshness::stale_restart: return "stale_restart";
    case Freshness::stale_generation: return "stale_generation";
    case Freshness::stale_epoch: return "stale_epoch";
    case Freshness::future_timestamp: return "future_timestamp";
    case Freshness::stale_backend: return "stale_backend";
  }
  return "unknown";
}

Nanos occupancy_age(Nanos now, Nanos observed_at) noexcept {
  if (observed_at >= now) {
    return 0;
  }
  return now - observed_at;
}

namespace {

// Invalidations that record why evidence stopped being authoritative are
// sticky: only new evidence clears them.
bool is_sticky(Freshness reason) noexcept {
  return reason == Freshness::stale_restart || reason == Freshness::stale_epoch ||
         reason == Freshness::stale_backend;
}

}  // namespace

Freshness classify_freshness(const OccupancyState& state,
                             Nanos now,
                             const OccupancyPolicy& policy,
                             Generation current_generation) noexcept {
  if (!state.present) {
    return Freshness::unknown;
  }
  if (is_sticky(state.freshness)) {
    return state.freshness;
  }
  if (policy.require_generation_match && state.queue_generation != current_generation) {
    return Freshness::stale_generation;
  }
  if (state.observed_at > now) {
    const Nanos ahead = state.observed_at - now;
    if (policy.max_future_skew <= 0 || ahead > policy.max_future_skew) {
      return Freshness::future_timestamp;
    }
  }
  const Nanos age = occupancy_age(now, state.observed_at);
  if (policy.max_age > 0 && age > policy.max_age) {
    return Freshness::stale_age;
  }
  return Freshness::fresh;
}

bool occupancy_authoritative(const OccupancyState& state) noexcept { return state.present && state.freshness == Freshness::fresh; }

void invalidate_occupancy(OccupancyState& state, Freshness reason) noexcept {
  if (!state.present) {
    state.freshness = Freshness::unknown;
    return;
  }
  state.freshness = reason;
}

}  // namespace qf
