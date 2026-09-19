// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
#include "qf/time.hpp"

namespace qf {

IClock::~IClock() = default;

SteadyClock::SteadyClock() noexcept : origin_(std::chrono::steady_clock::now()) {}

Nanos SteadyClock::now_nanos() const noexcept {
  const auto delta = std::chrono::steady_clock::now() - origin_;
  return static_cast<Nanos>(std::chrono::duration_cast<std::chrono::nanoseconds>(delta).count());
}

}  // namespace qf
