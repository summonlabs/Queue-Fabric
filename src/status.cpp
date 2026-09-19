// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
#include "qf/status.hpp"

namespace qf {

const char* to_string(Code code) noexcept {
  switch (code) {
    case Code::ok: return "ok";
    case Code::invalid_argument: return "invalid_argument";
    case Code::out_of_range: return "out_of_range";
    case Code::not_found: return "not_found";
    case Code::already_exists: return "already_exists";
    case Code::capacity_exceeded: return "capacity_exceeded";
    case Code::malformed_input: return "malformed_input";
    case Code::integrity_failure: return "integrity_failure";
    case Code::unsupported: return "unsupported";
    case Code::stale_generation: return "stale_generation";
    case Code::stale_epoch: return "stale_epoch";
    case Code::stale_incarnation: return "stale_incarnation";
    case Code::stale_boot: return "stale_boot";
    case Code::stale_occupancy: return "stale_occupancy";
    case Code::replay_detected: return "replay_detected";
    case Code::conflict: return "conflict";
    case Code::authority_denied: return "authority_denied";
    case Code::lifecycle_violation: return "lifecycle_violation";
    case Code::fence_violation: return "fence_violation";
    case Code::threshold_exceeded: return "threshold_exceeded";
    case Code::occupancy_overflow: return "occupancy_overflow";
    case Code::drain_incomplete: return "drain_incomplete";
    case Code::backend_mismatch: return "backend_mismatch";
    case Code::not_durable: return "not_durable";
    case Code::ambiguous_outcome: return "ambiguous_outcome";
    case Code::shutting_down: return "shutting_down";
    case Code::cancelled: return "cancelled";
    case Code::busy: return "busy";
    case Code::internal: return "internal";
  }
  return "unknown";
}

Status::Status(Code code, std::string_view message) : code_(code) {
  if (code == Code::ok) {
    return;
  }
  const std::size_t limit = limits::kMaxMessageChars;
  if (message.size() > limit) {
    message_.assign(message.substr(0, limit));
  } else {
    message_.assign(message);
  }
}

std::string Status::to_string() const {
  const char* name = qf::to_string(code_);
  if (message_.empty()) {
    return std::string(name);
  }
  std::string out(name);
  out += ": ";
  out += message_;
  return out;
}

}  // namespace qf
