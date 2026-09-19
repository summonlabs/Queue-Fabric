// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <cstdint>

namespace qf {

/// Small deterministic PRNG (splitmix64). All randomized behaviour in Queue
/// Fabric - fixture generation, seeded property tests, benchmark populations,
/// boot identifiers - draws from an explicitly seeded instance so that every
/// run is reproducible.
class Rng {
 public:
  explicit Rng(std::uint64_t seed) noexcept : state_(seed) {}

  [[nodiscard]] std::uint64_t next_u64() noexcept {
    state_ += 0x9E3779B97F4A7C15ull;
    std::uint64_t z = state_;
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ull;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBull;
    return z ^ (z >> 31);
  }

  [[nodiscard]] std::uint32_t next_u32() noexcept { return static_cast<std::uint32_t>(next_u64() >> 32); }

  /// Uniform value in [0, bound). Returns 0 when bound == 0.
  [[nodiscard]] std::uint64_t next_bounded(std::uint64_t bound) noexcept {
    if (bound == 0) {
      return 0;
    }
    return next_u64() % bound;
  }

  [[nodiscard]] bool next_bool() noexcept { return (next_u64() & 1u) != 0u; }

  [[nodiscard]] std::uint64_t state() const noexcept { return state_; }

 private:
  std::uint64_t state_;
};

}  // namespace qf
