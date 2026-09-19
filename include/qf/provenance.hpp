// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>

#include "qf/hash.hpp"
#include "qf/ids.hpp"
#include "qf/limits.hpp"
#include "qf/status.hpp"

namespace qf {

/// Provenance attached to every externally supplied fact. Nothing enters the
/// fabric without an identified publisher, node, incarnation, epoch, boot and
/// per-publisher sequence.
struct Provenance {
  PublisherId publisher{};
  NodeId node{};
  Incarnation incarnation{};
  Epoch epoch{};
  Sequence sequence{};
  BootId boot{};

  [[nodiscard]] bool valid() const noexcept {
    return publisher.valid() && node.valid() && incarnation.valid() && sequence.valid() && boot.valid();
  }

  [[nodiscard]] std::uint64_t fingerprint() const noexcept {
    std::uint64_t seed = 0x51ED270B5F1F0E1Bull;
    hash_mix(seed, publisher.value());
    hash_mix(seed, node.value());
    hash_mix(seed, incarnation.value());
    hash_mix(seed, epoch.value());
    hash_mix(seed, sequence.value());
    hash_mix(seed, boot.hi);
    hash_mix(seed, boot.lo);
    return seed;
  }
};

/// Classification of an incoming provenance record relative to what the tracker
/// has already observed from that publisher.
enum class ProvenanceClass : std::uint8_t {
  accepted = 0,        ///< First record, newer incarnation, or newer sequence.
  duplicate,           ///< Same publisher, incarnation, boot and sequence: replay.
  stale_incarnation,   ///< Publisher incarnation went backwards.
  stale_boot,          ///< Same incarnation but a different boot identifier.
  stale_epoch,         ///< Epoch went backwards for a known incarnation.
  invalid,             ///< Structurally incomplete provenance.
  capacity_exceeded,   ///< Publisher table is full; new publishers rejected.
};

const char* to_string(ProvenanceClass value) noexcept;

/// Tracks the newest provenance observed per publisher.
///
/// The tracker is the replay and stale-authority gate: duplicate sequences are
/// reported as duplicates (and handled idempotently upstream), while older
/// incarnations, foreign boots and lagging epochs are refused outright. The
/// table is bounded; a full table rejects unknown publishers instead of growing
/// without limit.
class ProvenanceTracker {
 public:
  struct PublisherState {
    Incarnation incarnation{};
    BootId boot{};
    Epoch epoch{};
    Sequence last_sequence{};
    std::uint64_t accepted{0};
    std::uint64_t duplicates{0};
    std::uint64_t rejected{0};
  };

  explicit ProvenanceTracker(std::size_t max_publishers = limits::kMaxProvenanceNodes) noexcept
      : max_publishers_(max_publishers) {}

  [[nodiscard]] ProvenanceClass classify(const Provenance& provenance) const noexcept;
  Status observe(const Provenance& provenance);

  [[nodiscard]] const PublisherState* find(PublisherId publisher) const noexcept;
  [[nodiscard]] std::size_t publisher_count() const noexcept { return states_.size(); }
  [[nodiscard]] std::size_t max_publishers() const noexcept { return max_publishers_; }
  void clear() noexcept { states_.clear(); }

 private:
  std::unordered_map<PublisherId, PublisherState> states_;
  std::size_t max_publishers_;
};

}  // namespace qf
