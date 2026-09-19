// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <cstdint>
#include <string>

#include "qf/status.hpp"
#include "qf/time.hpp"

namespace qf {

/// Configured queue limits. A configured threshold is never observed occupancy:
/// these values only exist to be compared against measured evidence.
struct Thresholds {
  std::uint64_t low_watermark_bytes{0};
  std::uint64_t high_watermark_bytes{0};
  std::uint64_t max_occupancy_bytes{0};
  std::uint64_t max_packets{0};
  Nanos occupancy_stale_after{0};  ///< 0 means "fall back to the fabric policy".

  [[nodiscard]] bool consistent() const noexcept {
    return low_watermark_bytes <= high_watermark_bytes && high_watermark_bytes <= max_occupancy_bytes;
  }

  friend bool operator==(const Thresholds& a, const Thresholds& b) noexcept {
    return a.low_watermark_bytes == b.low_watermark_bytes && a.high_watermark_bytes == b.high_watermark_bytes &&
           a.max_occupancy_bytes == b.max_occupancy_bytes && a.max_packets == b.max_packets &&
           a.occupancy_stale_after == b.occupancy_stale_after;
  }
  friend bool operator!=(const Thresholds& a, const Thresholds& b) noexcept { return !(a == b); }
};

/// Band of the configured threshold ladder that observed occupancy falls into.
enum class ThresholdBand : std::uint8_t {
  unknown = 0,   ///< No fresh evidence; nothing is authorized.
  below_low = 1,
  nominal = 2,
  above_high = 3,
  at_limit = 4,
  exceeded = 5,  ///< Observed occupancy above the configured maximum.
};

const char* to_string(ThresholdBand value) noexcept;

/// Result of comparing configured limits with (possibly stale) observation.
struct ThresholdEvaluation {
  ThresholdBand band{ThresholdBand::unknown};
  bool admission_authorized{false};
  std::uint64_t headroom_bytes{0};
  Code code{Code::stale_occupancy};
  const char* rule{"unset"};
};

/// Evaluates observed occupancy against configured thresholds.
///
/// Stale or absent evidence yields ThresholdBand::unknown with admission
/// refused: missing evidence never becomes positive authority.
[[nodiscard]] ThresholdEvaluation evaluate_thresholds(const Thresholds& thresholds,
                                                      std::uint64_t observed_bytes,
                                                      std::uint64_t observed_packets,
                                                      bool evidence_fresh) noexcept;

}  // namespace qf
