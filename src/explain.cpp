// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
#include "qf/explain.hpp"

#include <string>

namespace qf {

namespace {

constexpr const char* kTruncated = "\n... explanation truncated at configured bound";

void append(std::string& target, std::string_view text) {
  if (target.size() >= limits::kMaxExplainBytes) {
    return;
  }
  const std::size_t room = limits::kMaxExplainBytes - target.size();
  target.append(text.substr(0, room));
}

void append_field(std::string& target, const char* key, std::string_view value) {
  append(target, key);
  append(target, "=");
  append(target, value);
  append(target, " ");
}

std::string hex(std::uint64_t value) {
  static constexpr char kDigits[] = "0123456789abcdef";
  std::string out(18, '0');
  out[0] = '0';
  out[1] = 'x';
  for (int i = 0; i < 16; ++i) {
    out[2 + static_cast<std::size_t>(i)] = kDigits[(value >> (4 * (15 - i))) & 0xFu];
  }
  return out;
}

std::string decimal(std::uint64_t value) { return std::to_string(value); }

}  // namespace

std::string Explanation::render() const {
  std::string out;
  out.reserve(1024);

  append(out, "queue ");
  append(out, hex(queue.value()));
  append(out, "\n  identity: ");
  append_field(out, "name", name);
  append_field(out, "resource", hex(resource.value()));
  append_field(out, "lifecycle", to_string(lifecycle));
  append_field(out, "applicability", to_string(applicability));
  append_field(out, "generation", hex(generation.value()));
  append_field(out, "owner", hex(owner.value()));
  append(out, "\n  binding: ");
  append_field(out, "class", hex(cls.id.value()));
  append_field(out, "class_generation", hex(cls.generation.value()));
  append_field(out, "pool", hex(pool.id.value()));
  append_field(out, "pool_generation", hex(pool.generation.value()));
  append_field(out, "pool_capacity_bytes", decimal(pool.capacity_bytes));
  append(out, "\n  authority: ");
  append_field(out, "epoch", hex(authority_epoch.value()));
  append_field(out, "principal_capabilities", hex(principal_capabilities));
  append(out, "\n  occupancy: ");
  append_field(out, "present", occupancy.present ? "true" : "false");
  append_field(out, "bytes", decimal(occupancy.bytes));
  append_field(out, "packets", decimal(occupancy.packets));
  append_field(out, "observed_at_ns", decimal(static_cast<std::uint64_t>(occupancy.observed_at)));
  append_field(out, "age_ns", decimal(static_cast<std::uint64_t>(occupancy.age)));
  append_field(out, "sequence", hex(occupancy.sequence.value()));
  append_field(out, "freshness", to_string(occupancy.freshness));
  append(out, "\n  thresholds: ");
  append_field(out, "low_watermark_bytes", decimal(thresholds.low_watermark_bytes));
  append_field(out, "high_watermark_bytes", decimal(thresholds.high_watermark_bytes));
  append_field(out, "max_occupancy_bytes", decimal(thresholds.max_occupancy_bytes));
  append_field(out, "max_packets", decimal(thresholds.max_packets));
  append_field(out, "stale_after_ns", decimal(static_cast<std::uint64_t>(thresholds.occupancy_stale_after)));
  append_field(out, "band", to_string(threshold_eval.band));
  append_field(out, "admission", threshold_eval.admission_authorized ? "authorized" : "refused");
  append_field(out, "headroom_bytes", decimal(threshold_eval.headroom_bytes));
  append_field(out, "threshold_rule", threshold_eval.rule);
  append(out, "\n  fence: ");
  append_field(out, "fenced", fence.fenced ? "true" : "false");
  append_field(out, "token", decimal(fence.token));
  append_field(out, "epoch", hex(fence.epoch.value()));
  append_field(out, "reason", fence_reason);
  append(out, "\n  drain: ");
  append_field(out, "phase", to_string(drain.phase));
  append_field(out, "attempt", drain.attempt.to_string());
  append_field(out, "requested_at_ns", decimal(static_cast<std::uint64_t>(drain.requested_at)));
  append_field(out, "evidence_records", decimal(drain.evidence.records));
  append_field(out, "evidence_revalidated", drain.evidence.revalidated ? "true" : "false");
  append_field(out, "completable", drain_assessment.completable ? "true" : "false");
  append_field(out, "drain_rule", drain_assessment.rule);
  append(out, "\n  backend: ");
  append_field(out, "known", applied.known ? "true" : "false");
  append_field(out, "backend", hex(applied.backend.value()));
  append_field(out, "backend_incarnation", hex(applied.backend_incarnation.value()));
  append_field(out, "ack_epoch", hex(applied.epoch.value()));
  append_field(out, "verified", backend_verification.verified ? "true" : "false");
  append_field(out, "verification_rule", backend_verification.rule);
  append(out, "\n  mutation: ");
  append_field(out, "pending_attempt", pending_attempt.to_string());
  append_field(out, "pending_kind", pending_kind);
  append_field(out, "last_attempt", last_attempt.to_string());
  append_field(out, "last_rule", last_rule);
  append_field(out, "last_status", last_status.to_string());
  append_field(out, "applied", decimal(mutation_count));
  append_field(out, "replayed", decimal(replayed_count));
  append_field(out, "rejected", decimal(rejected_count));
  append_field(out, "overflow_events", decimal(overflow_events));
  append_field(out, "staleness_events", decimal(staleness_events));
  append(out, "\n  reasons: ");
  append_field(out, "stale_reason", stale_reason);
  append_field(out, "fence_reason", fence_reason);
  append_field(out, "failure_reason", failure_reason);

  if (out.size() >= limits::kMaxExplainBytes) {
    out.resize(limits::kMaxExplainBytes - 64);
    out += kTruncated;
  }
  return out;
}

std::string Explanation::to_json_like() const {
  std::string out;
  out.reserve(512);
  append(out, "{\"queue\":\"");
  append(out, hex(queue.value()));
  append(out, "\",\"name\":\"");
  append(out, name);
  append(out, "\",\"lifecycle\":\"");
  append(out, to_string(lifecycle));
  append(out, "\",\"applicability\":\"");
  append(out, to_string(applicability));
  append(out, "\",\"generation\":\"");
  append(out, hex(generation.value()));
  append(out, "\",\"class\":\"");
  append(out, hex(cls.id.value()));
  append(out, "\",\"class_generation\":\"");
  append(out, hex(cls.generation.value()));
  append(out, "\",\"occupancy_freshness\":\"");
  append(out, to_string(occupancy.freshness));
  append(out, "\",\"occupancy_bytes\":");
  append(out, decimal(occupancy.bytes));
  append(out, ",\"band\":\"");
  append(out, to_string(threshold_eval.band));
  append(out, "\",\"admission\":");
  append(out, threshold_eval.admission_authorized ? "true" : "false");
  append(out, ",\"drain_phase\":\"");
  append(out, to_string(drain.phase));
  append(out, "\",\"drain_completable\":");
  append(out, drain_assessment.completable ? "true" : "false");
  append(out, ",\"fenced\":");
  append(out, fence.fenced ? "true" : "false");
  append(out, ",\"backend_verified\":");
  append(out, backend_verification.verified ? "true" : "false");
  append(out, ",\"stale_reason\":\"");
  append(out, stale_reason);
  append(out, "\",\"last_rule\":\"");
  append(out, last_rule);
  append(out, "\"}");
  return out;
}

}  // namespace qf
