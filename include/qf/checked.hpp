// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <cstddef>
#include <cstdint>
#include <limits>
#include <type_traits>

namespace qf {

template <class T>
  requires std::is_unsigned_v<T>
[[nodiscard]] constexpr bool add_overflow(T a, T b, T& out) noexcept {
  if (b > static_cast<T>(std::numeric_limits<T>::max() - a)) {
    return true;
  }
  out = static_cast<T>(a + b);
  return false;
}

template <class T>
  requires std::is_unsigned_v<T>
[[nodiscard]] constexpr bool sub_underflow(T a, T b, T& out) noexcept {
  if (b > a) {
    return true;
  }
  out = static_cast<T>(a - b);
  return false;
}

template <class T>
  requires std::is_unsigned_v<T>
[[nodiscard]] constexpr bool mul_overflow(T a, T b, T& out) noexcept {
  if (a == 0 || b == 0) {
    out = 0;
    return false;
  }
  if (a > static_cast<T>(std::numeric_limits<T>::max() / b)) {
    return true;
  }
  out = static_cast<T>(a * b);
  return false;
}

template <class T>
[[nodiscard]] constexpr T saturating_add(T a, T b) noexcept {
  T out{};
  if (add_overflow(a, b, out)) {
    return std::numeric_limits<T>::max();
  }
  return out;
}

template <class T>
[[nodiscard]] constexpr T saturating_sub(T a, T b) noexcept {
  T out{};
  if (sub_underflow(a, b, out)) {
    return T{0};
  }
  return out;
}

template <class T>
[[nodiscard]] constexpr T saturating_mul(T a, T b) noexcept {
  T out{};
  if (mul_overflow(a, b, out)) {
    return std::numeric_limits<T>::max();
  }
  return out;
}

[[nodiscard]] constexpr bool add_overflow_i64(std::int64_t a, std::int64_t b, std::int64_t& out) noexcept {
  if (b > 0 && a > (std::numeric_limits<std::int64_t>::max() - b)) {
    return true;
  }
  if (b < 0 && a < (std::numeric_limits<std::int64_t>::min() - b)) {
    return true;
  }
  out = a + b;
  return false;
}

[[nodiscard]] constexpr std::int64_t saturating_add_i64(std::int64_t a, std::int64_t b) noexcept {
  std::int64_t out = 0;
  if (add_overflow_i64(a, b, out)) {
    return b > 0 ? std::numeric_limits<std::int64_t>::max() : std::numeric_limits<std::int64_t>::min();
  }
  return out;
}

[[nodiscard]] constexpr std::int64_t saturating_sub_i64(std::int64_t a, std::int64_t b) noexcept {
  if (b == std::numeric_limits<std::int64_t>::min()) {
    return a >= 0 ? std::numeric_limits<std::int64_t>::max() : 0;
  }
  return saturating_add_i64(a, -b);
}

template <class To, class From>
[[nodiscard]] constexpr bool narrow_cast(From value, To& out) noexcept {
  static_assert(std::is_integral_v<From> && std::is_integral_v<To>, "integral types required");
  if constexpr (std::is_signed_v<From> == std::is_signed_v<To>) {
    if (value < static_cast<From>(std::numeric_limits<To>::min()) ||
        value > static_cast<From>(std::numeric_limits<To>::max())) {
      return false;
    }
  } else if constexpr (std::is_signed_v<From>) {
    if (value < 0) {
      return false;
    }
    using ufrom = std::make_unsigned_t<From>;
    if (static_cast<ufrom>(value) > static_cast<ufrom>(std::numeric_limits<To>::max())) {
      return false;
    }
  } else {
    if (value > static_cast<From>(std::numeric_limits<To>::max())) {
      return false;
    }
  }
  out = static_cast<To>(value);
  return true;
}

[[nodiscard]] constexpr bool align_up(std::uint64_t value, std::uint64_t alignment, std::uint64_t& out) noexcept {
  if (alignment == 0) {
    return true;
  }
  const std::uint64_t remainder = value % alignment;
  if (remainder == 0) {
    out = value;
    return false;
  }
  return add_overflow(value, alignment - remainder, out);
}

}  // namespace qf
