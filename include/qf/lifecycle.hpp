// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <cstdint>

#include "qf/status.hpp"

namespace qf {

/// Queue lifecycle. Exactly one lifecycle state is authoritative at a time.
enum class Lifecycle : std::uint8_t {
  declared = 0,   ///< Identity and definition exist; not yet validated.
  validated = 1,  ///< Definition, class binding and thresholds verified.
  active = 2,     ///< Accepting occupancy and mutations.
  draining = 3,   ///< Drain requested; drain completion not yet proven.
  quiesced = 4,   ///< Drain proven complete; occupancy authoritative at zero.
  retired = 5,    ///< Terminal. No fresh mutation is ever accepted.
  failed = 6,     ///< Faulted; only retirement (cleanup) remains.
};

const char* to_string(Lifecycle value) noexcept;

/// Applicability flags. These are orthogonal to lifecycle: a queue can be
/// active and stale, or draining and fenced.
enum class Applicability : std::uint8_t {
  none = 0,
  stale = 1u << 0,   ///< Evidence (occupancy/drain/backend) needs revalidation.
  fenced = 1u << 1,  ///< Mutations refused until an unfence by an authorized owner.
};

[[nodiscard]] constexpr Applicability operator|(Applicability a, Applicability b) noexcept {
  return static_cast<Applicability>(static_cast<std::uint8_t>(a) | static_cast<std::uint8_t>(b));
}
[[nodiscard]] constexpr Applicability operator&(Applicability a, Applicability b) noexcept {
  return static_cast<Applicability>(static_cast<std::uint8_t>(a) & static_cast<std::uint8_t>(b));
}
[[nodiscard]] constexpr bool has_flag(Applicability value, Applicability flag) noexcept {
  return (static_cast<std::uint8_t>(value) & static_cast<std::uint8_t>(flag)) != 0;
}
[[nodiscard]] constexpr Applicability clear_flag(Applicability value, Applicability flag) noexcept {
  return static_cast<Applicability>(static_cast<std::uint8_t>(value) & ~static_cast<std::uint8_t>(flag));
}
[[nodiscard]] std::string to_string(Applicability value);

/// Why a state change was permitted or refused. Rules are named so that the
/// explanation surface can quote the exact rule that justified a decision.
struct TransitionDecision {
  bool allowed{false};
  const char* rule{"unset"};
  Code code{Code::internal};
};

/// Validates a lifecycle transition. Retired is terminal; quiescence requires
/// that the queue passed through draining; reactivation requires revalidation.
[[nodiscard]] TransitionDecision check_transition(Lifecycle from, Lifecycle to) noexcept;

[[nodiscard]] constexpr bool is_terminal(Lifecycle value) noexcept { return value == Lifecycle::retired; }

/// A queue in these states can never accept a fresh mutation.
[[nodiscard]] constexpr bool accepts_mutation(Lifecycle value, Applicability applicability) noexcept {
  if (value == Lifecycle::retired || value == Lifecycle::failed) {
    return false;
  }
  if (has_flag(applicability, Applicability::fenced)) {
    return false;
  }
  return true;
}

}  // namespace qf
