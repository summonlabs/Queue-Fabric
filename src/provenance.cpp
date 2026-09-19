// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
#include "qf/provenance.hpp"

namespace qf {

const char* to_string(ProvenanceClass value) noexcept {
  switch (value) {
    case ProvenanceClass::accepted: return "accepted";
    case ProvenanceClass::duplicate: return "duplicate";
    case ProvenanceClass::stale_incarnation: return "stale_incarnation";
    case ProvenanceClass::stale_boot: return "stale_boot";
    case ProvenanceClass::stale_epoch: return "stale_epoch";
    case ProvenanceClass::invalid: return "invalid";
    case ProvenanceClass::capacity_exceeded: return "capacity_exceeded";
  }
  return "unknown";
}

ProvenanceClass ProvenanceTracker::classify(const Provenance& provenance) const noexcept {
  if (!provenance.valid()) {
    return ProvenanceClass::invalid;
  }
  const auto it = states_.find(provenance.publisher);
  if (it == states_.end()) {
    return states_.size() >= max_publishers_ ? ProvenanceClass::capacity_exceeded : ProvenanceClass::accepted;
  }
  const PublisherState& state = it->second;
  if (provenance.incarnation < state.incarnation) {
    return ProvenanceClass::stale_incarnation;
  }
  if (provenance.incarnation == state.incarnation) {
    if (provenance.boot != state.boot) {
      return ProvenanceClass::stale_boot;
    }
    if (provenance.epoch < state.epoch) {
      return ProvenanceClass::stale_epoch;
    }
    if (provenance.sequence <= state.last_sequence) {
      return ProvenanceClass::duplicate;
    }
    return ProvenanceClass::accepted;
  }
  // A newer incarnation supersedes the previous one; its sequence space is new.
  return ProvenanceClass::accepted;
}

Status ProvenanceTracker::observe(const Provenance& provenance) {
  const ProvenanceClass classification = classify(provenance);
  switch (classification) {
    case ProvenanceClass::invalid:
      return Status{Code::malformed_input, "provenance is structurally incomplete"};
    case ProvenanceClass::capacity_exceeded:
      return Status{Code::capacity_exceeded, "publisher table is full"};
    case ProvenanceClass::stale_incarnation:
      return Status{Code::stale_incarnation, "publisher incarnation is superseded"};
    case ProvenanceClass::stale_boot:
      return Status{Code::stale_boot, "boot identifier does not match publisher incarnation"};
    case ProvenanceClass::stale_epoch:
      return Status{Code::stale_epoch, "provenance epoch is behind the observed epoch"};
    default:
      break;
  }

  PublisherState& state = states_[provenance.publisher];
  if (classification == ProvenanceClass::duplicate) {
    ++state.duplicates;
    return Status{Code::replay_detected, "duplicate sequence from publisher"};
  }
  if (provenance.incarnation != state.incarnation) {
    state.incarnation = provenance.incarnation;
    state.boot = provenance.boot;
    state.last_sequence = Sequence{};
  }
  state.boot = provenance.boot;
  state.epoch = provenance.epoch;
  state.last_sequence = provenance.sequence;
  ++state.accepted;
  return Status{};
}

const ProvenanceTracker::PublisherState* ProvenanceTracker::find(PublisherId publisher) const noexcept {
  const auto it = states_.find(publisher);
  return it == states_.end() ? nullptr : &it->second;
}

}  // namespace qf
