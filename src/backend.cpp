// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
#include "qf/backend.hpp"

namespace qf {

BackendVerification verify_backend_applied(const BackendApplied& applied,
                                           const BackendIncarnation& live,
                                           Epoch current_epoch,
                                           Generation current_queue_generation,
                                           ClassBinding current_class) noexcept {
  if (!applied.known) {
    return BackendVerification{false, Code::backend_mismatch, "no-backend-acknowledgement"};
  }
  if (!live.alive) {
    return BackendVerification{false, Code::stale_incarnation, "backend-incarnation-not-live"};
  }
  if (applied.backend != live.backend) {
    return BackendVerification{false, Code::backend_mismatch, "backend-identity-mismatch"};
  }
  if (applied.backend_incarnation != live.incarnation) {
    return BackendVerification{false, Code::stale_incarnation, "backend-incarnation-superseded"};
  }
  if (applied.epoch != current_epoch) {
    return BackendVerification{false, Code::stale_epoch, "acknowledgement-epoch-superseded"};
  }
  if (applied.queue_generation != current_queue_generation) {
    return BackendVerification{false, Code::stale_generation, "acknowledgement-bound-to-older-queue-generation"};
  }
  if (applied.applied_class != current_class) {
    return BackendVerification{false, Code::stale_generation, "acknowledgement-bound-to-older-class-binding"};
  }
  return BackendVerification{true, Code::ok, "backend-acknowledgement-verifiable"};
}

}  // namespace qf
