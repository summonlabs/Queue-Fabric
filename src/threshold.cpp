// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
#include "qf/threshold.hpp"

#include "qf/checked.hpp"

namespace qf {

const char* to_string(ThresholdBand value) noexcept {
  switch (value) {
    case ThresholdBand::unknown: return "unknown";
    case ThresholdBand::below_low: return "below_low";
    case ThresholdBand::nominal: return "nominal";
    case ThresholdBand::above_high: return "above_high";
    case ThresholdBand::at_limit: return "at_limit";
    case ThresholdBand::exceeded: return "exceeded";
  }
  return "unknown";
}

ThresholdEvaluation evaluate_thresholds(const Thresholds& thresholds,
                                        std::uint64_t observed_bytes,
                                        std::uint64_t observed_packets,
                                        bool evidence_fresh) noexcept {
  ThresholdEvaluation out{};
  out.headroom_bytes = saturating_sub(thresholds.max_occupancy_bytes, observed_bytes);

  // Missing or stale evidence is never positive authority.
  if (!evidence_fresh) {
    out.band = ThresholdBand::unknown;
    out.admission_authorized = false;
    out.code = Code::stale_occupancy;
    out.rule = "stale-evidence-cannot-authorize-admission";
    return out;
  }

  const bool bytes_over = observed_bytes > thresholds.max_occupancy_bytes;
  const bool packets_over = thresholds.max_packets != 0 && observed_packets > thresholds.max_packets;
  if (bytes_over || packets_over) {
    out.band = ThresholdBand::exceeded;
    out.admission_authorized = false;
    out.code = Code::threshold_exceeded;
    out.rule = bytes_over ? "observed-bytes-above-configured-maximum" : "observed-packets-above-configured-maximum";
    return out;
  }
  if (observed_bytes >= thresholds.max_occupancy_bytes) {
    out.band = ThresholdBand::at_limit;
    out.admission_authorized = false;
    out.code = Code::threshold_exceeded;
    out.rule = "occupancy-at-configured-maximum";
    return out;
  }
  if (observed_bytes >= thresholds.high_watermark_bytes) {
    out.band = ThresholdBand::above_high;
    out.admission_authorized = true;
    out.code = Code::ok;
    out.rule = "occupancy-above-high-watermark-within-limit";
    return out;
  }
  if (observed_bytes <= thresholds.low_watermark_bytes) {
    out.band = ThresholdBand::below_low;
    out.admission_authorized = true;
    out.code = Code::ok;
    out.rule = "occupancy-below-low-watermark";
    return out;
  }
  out.band = ThresholdBand::nominal;
  out.admission_authorized = true;
  out.code = Code::ok;
  out.rule = "occupancy-nominal";
  return out;
}

}  // namespace qf
