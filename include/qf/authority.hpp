// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "qf/ids.hpp"
#include "qf/lifecycle.hpp"
#include "qf/limits.hpp"
#include "qf/status.hpp"

namespace qf {

/// Mutation capabilities. Authority is granted per resource owner and is always
/// evaluated against a specific epoch; a grant from a superseded epoch is not
/// authority.
enum class Capability : std::uint32_t {
  none = 0,
  declare_topology = 1u << 0,
  declare_queue = 1u << 1,
  validate_queue = 1u << 2,
  activate_queue = 1u << 3,
  rebind_class = 1u << 4,
  set_thresholds = 1u << 5,
  ingest_occupancy = 1u << 6,
  request_drain = 1u << 7,
  submit_evidence = 1u << 8,
  quiesce_queue = 1u << 9,
  retire_queue = 1u << 10,
  fence_queue = 1u << 11,
  unfence_queue = 1u << 12,
  fail_queue = 1u << 13,
  transfer_ownership = 1u << 14,
  acknowledge_backend = 1u << 15,
  admin = 1u << 30,
  all = 0x7FFFFFFFu,
};

const char* to_string(Capability value) noexcept;

[[nodiscard]] constexpr std::uint32_t to_mask(Capability value) noexcept {
  return static_cast<std::uint32_t>(value);
}

[[nodiscard]] constexpr bool has_capability(std::uint32_t mask, Capability value) noexcept {
  return (mask & to_mask(value)) == to_mask(value);
}

/// Grant of capabilities to one owner for one resource.
struct AuthorityGrant {
  OwnerId owner{};
  std::uint32_t capabilities{0};

  friend bool operator==(const AuthorityGrant& a, const AuthorityGrant& b) noexcept {
    return a.owner == b.owner && a.capabilities == b.capabilities;
  }
};

/// Authority vector for a resource: the epoch it belongs to, the owning
/// identity, and the explicit capability grants.
struct AuthorityVector {
  ResourceId resource{};
  OwnerId owner{};
  Epoch epoch{};
  std::vector<AuthorityGrant> grants{};

  [[nodiscard]] std::uint32_t capabilities_of(OwnerId principal) const noexcept {
    for (const auto& grant : grants) {
      if (grant.owner == principal) {
        return grant.capabilities;
      }
    }
    return 0;
  }
};

/// Result of authorizing one mutation request. Always carries the named rule
/// that produced the decision.
struct AuthorityDecision {
  bool allowed{false};
  Capability required{Capability::none};
  Code code{Code::internal};
  const char* rule{"unset"};
};

/// Authorizes a principal for one capability.
///
/// Evaluation order is fixed and deterministic so that explanations are
/// reproducible: epoch currency, then capability grant, then fence state, then
/// lifecycle terminality. Each stage returns its own named rule.
[[nodiscard]] AuthorityDecision authorize(const AuthorityVector& authority,
                                          OwnerId principal,
                                          Capability required,
                                          Epoch presented_epoch,
                                          Applicability applicability,
                                          Lifecycle lifecycle) noexcept;

}  // namespace qf
