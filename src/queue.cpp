// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
#include "qf/queue.hpp"

namespace qf {

const AttemptMemo* QueueRecord::find_attempt(const AttemptId& attempt) const noexcept {
  for (const auto& entry : memo) {
    if (entry.attempt == attempt) {
      return &entry;
    }
  }
  return nullptr;
}

void QueueRecord::remember(const AttemptMemo& entry, std::size_t capacity) {
  if (capacity == 0) {
    return;
  }
  for (auto& existing : memo) {
    if (existing.attempt == entry.attempt) {
      existing = entry;
      return;
    }
  }
  if (memo.size() >= capacity) {
    memo.erase(memo.begin());
  }
  memo.push_back(entry);
}

}  // namespace qf
