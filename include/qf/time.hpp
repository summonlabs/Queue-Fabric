// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <chrono>
#include <cstdint>

namespace qf {

/// Nanoseconds on a monotonic timeline. Wall-clock time is deliberately not
/// used for freshness, ordering or authority decisions.
using Nanos = std::int64_t;

/// Injected time source. Every component that needs "now" takes a reference to
/// this interface so that tests and benchmarks can drive time explicitly.
class IClock {
 public:
  IClock() = default;
  IClock(const IClock&) = delete;
  IClock& operator=(const IClock&) = delete;
  virtual ~IClock();

  [[nodiscard]] virtual Nanos now_nanos() const noexcept = 0;
};

/// Monotonic wall time since construction of the process-level clock.
class SteadyClock final : public IClock {
 public:
  SteadyClock() noexcept;
  [[nodiscard]] Nanos now_nanos() const noexcept override;

 private:
  std::chrono::steady_clock::time_point origin_;
};

/// Manually advanced clock for deterministic tests.
class ManualClock final : public IClock {
 public:
  explicit ManualClock(Nanos start = 0) noexcept : now_(start) {}
  [[nodiscard]] Nanos now_nanos() const noexcept override { return now_; }
  void advance(Nanos delta) noexcept { now_ += delta; }
  void set(Nanos value) noexcept { now_ = value; }

 private:
  Nanos now_{0};
};

}  // namespace qf
