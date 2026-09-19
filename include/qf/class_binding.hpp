// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <cstdint>
#include <string>

#include "qf/ids.hpp"
#include "qf/limits.hpp"
#include "qf/provenance.hpp"
#include "qf/status.hpp"

namespace qf {

/// Reference to a quality-of-service marking. Queue Fabric stores the reference
/// only; it never implements packet scheduling or rate behaviour.
struct QosRef {
  std::uint32_t priority_code{0};
  std::uint32_t traffic_class{0};
  std::string group{};

  friend bool operator==(const QosRef& a, const QosRef& b) noexcept {
    return a.priority_code == b.priority_code && a.traffic_class == b.traffic_class && a.group == b.group;
  }
};

/// Authoritative scheduling-class definition.
struct SchedulingClassDef {
  ClassId id{};
  ResourceId resource{};
  std::string name{};
  std::uint32_t priority{0};
  QosRef qos{};
  Generation generation{};
  Provenance declared_by{};

  [[nodiscard]] bool valid() const noexcept {
    return id.valid() && resource.valid() && generation.valid() && !name.empty() && name.size() <= limits::kMaxNameChars;
  }
};

/// Exact-generation binding of a queue to a scheduling class.
///
/// A binding is only usable while it names the class generation that is
/// currently authoritative for the resource. Rebinding is an explicit,
/// generation-checked mutation; a stale binding can never authorize a decision.
struct ClassBinding {
  ClassId id{};
  Generation generation{};

  [[nodiscard]] bool valid() const noexcept { return id.valid() && generation.valid(); }

  friend bool operator==(const ClassBinding& a, const ClassBinding& b) noexcept {
    return a.id == b.id && a.generation == b.generation;
  }
  friend bool operator!=(const ClassBinding& a, const ClassBinding& b) noexcept { return !(a == b); }
};

/// Reference to a buffer pool. Queue Fabric never allocates buffers; it records
/// which pool a queue is associated with and the pool generation observed.
struct PoolRef {
  PoolId id{};
  Generation generation{};
  std::uint64_t capacity_bytes{0};

  [[nodiscard]] bool valid() const noexcept { return id.valid() && generation.valid(); }

  friend bool operator==(const PoolRef& a, const PoolRef& b) noexcept {
    return a.id == b.id && a.generation == b.generation && a.capacity_bytes == b.capacity_bytes;
  }
  friend bool operator!=(const PoolRef& a, const PoolRef& b) noexcept { return !(a == b); }
};

/// Authoritative buffer-pool definition (identity and capacity only).
struct BufferPoolDef {
  PoolId id{};
  ResourceId resource{};
  std::string name{};
  std::uint64_t capacity_bytes{0};
  Generation generation{};
  Provenance declared_by{};
};

}  // namespace qf
