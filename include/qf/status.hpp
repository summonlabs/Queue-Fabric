// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <cstdint>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <utility>

#include "qf/limits.hpp"

namespace qf {

/// Machine-readable outcome of an operation.
///
/// Codes are stable and are part of the explanation surface: every rejected
/// operation reports one of these plus a bounded human-readable reason.
enum class Code : std::uint16_t {
  ok = 0,
  invalid_argument,
  out_of_range,
  not_found,
  already_exists,
  capacity_exceeded,
  malformed_input,
  integrity_failure,
  unsupported,
  stale_generation,
  stale_epoch,
  stale_incarnation,
  stale_boot,
  stale_occupancy,
  replay_detected,
  conflict,
  authority_denied,
  lifecycle_violation,
  fence_violation,
  threshold_exceeded,
  occupancy_overflow,
  drain_incomplete,
  backend_mismatch,
  not_durable,
  ambiguous_outcome,
  shutting_down,
  cancelled,
  busy,
  internal,
};

/// Stable lowercase identifier for a code (used in logs, tests and explanations).
const char* to_string(Code code) noexcept;

/// Human-readable, length-bounded status value.
///
/// Messages are truncated to limits::kMaxMessageChars so that a hostile or
/// buggy caller can never inflate explanation size.
class Status {
 public:
  Status() noexcept = default;
  Status(Code code, std::string_view message = {});

  /// Explicit success value. Named distinctly from the ok() predicate.
  [[nodiscard]] static Status success() noexcept { return Status{}; }

  [[nodiscard]] bool ok() const noexcept { return code_ == Code::ok; }
  [[nodiscard]] Code code() const noexcept { return code_; }
  [[nodiscard]] const std::string& message() const noexcept { return message_; }

  /// "code" or "code: message".
  [[nodiscard]] std::string to_string() const;

  friend bool operator==(const Status& a, const Status& b) noexcept {
    return a.code_ == b.code_ && a.message_ == b.message_;
  }
  friend bool operator!=(const Status& a, const Status& b) noexcept { return !(a == b); }

 private:
  Code code_{Code::ok};
  std::string message_{};
};

inline std::ostream& operator<<(std::ostream& os, const Status& s) { return os << s.to_string(); }

/// Value-or-status return type. Either holds a value or a non-ok Status.
template <class T>
class Result {
 public:
  Result(T value) : value_(std::move(value)) {}          // NOLINT(google-explicit-constructor)
  Result(Status status) : status_(std::move(status)) {}  // NOLINT(google-explicit-constructor)

  [[nodiscard]] bool ok() const noexcept { return value_.has_value(); }
  explicit operator bool() const noexcept { return ok(); }

  [[nodiscard]] const Status& status() const noexcept { return status_; }

  /// Precondition: ok(). Behaviour is checked in debug builds by callers that
  /// use value_or / value with a fallback.
  [[nodiscard]] const T& value() const& { return *value_; }
  [[nodiscard]] T& value() & { return *value_; }
  [[nodiscard]] T&& value() && { return std::move(*value_); }

  [[nodiscard]] const T* operator->() const { return &*value_; }
  [[nodiscard]] T* operator->() { return &*value_; }
  [[nodiscard]] const T& operator*() const& { return *value_; }
  [[nodiscard]] T& operator*() & { return *value_; }

  template <class U>
  [[nodiscard]] T value_or(U&& fallback) const {
    return value_.has_value() ? *value_ : static_cast<T>(std::forward<U>(fallback));
  }

 private:
  std::optional<T> value_{};
  Status status_{};
};

}  // namespace qf
