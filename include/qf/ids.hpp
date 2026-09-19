// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <cstdint>
#include <functional>
#include <ostream>
#include <string>
#include <string_view>

#include "qf/rng.hpp"
#include "qf/status.hpp"

namespace qf {

/// Tag types. Each identity kind is a distinct C++ type: a queue identity can
/// never be silently used where a resource identity is required.
namespace tags {
struct resource_tag {};
struct queue_tag {};
struct class_tag {};
struct pool_tag {};
struct policy_tag {};
struct owner_tag {};
struct backend_tag {};
struct publisher_tag {};
struct node_tag {};
struct generation_tag {};
struct epoch_tag {};
struct sequence_tag {};
struct incarnation_tag {};
}  // namespace tags

namespace detail {

inline std::string format_hex16(std::uint64_t value) {
  static constexpr char kDigits[] = "0123456789abcdef";
  std::string out(18, '0');
  out[0] = '0';
  out[1] = 'x';
  for (int i = 0; i < 16; ++i) {
    out[2 + static_cast<std::size_t>(i)] = kDigits[(value >> (4 * (15 - i))) & 0xFu];
  }
  return out;
}

inline bool parse_hex16(std::string_view text, std::uint64_t& out) noexcept {
  if (text.size() > 2 && text[0] == '0' && (text[1] == 'x' || text[1] == 'X')) {
    text.remove_prefix(2);
  }
  if (text.empty() || text.size() > 16) {
    return false;
  }
  std::uint64_t value = 0;
  for (const char ch : text) {
    std::uint64_t digit = 0;
    if (ch >= '0' && ch <= '9') {
      digit = static_cast<std::uint64_t>(ch - '0');
    } else if (ch >= 'a' && ch <= 'f') {
      digit = static_cast<std::uint64_t>(ch - 'a' + 10);
    } else if (ch >= 'A' && ch <= 'F') {
      digit = static_cast<std::uint64_t>(ch - 'A' + 10);
    } else {
      return false;
    }
    value = (value << 4) | digit;
  }
  out = value;
  return true;
}

inline bool parse_u64(std::string_view text, std::uint64_t& out) noexcept {
  if (text.empty() || text.size() > 20) {
    return false;
  }
  std::uint64_t value = 0;
  for (const char ch : text) {
    if (ch < '0' || ch > '9') {
      return false;
    }
    const std::uint64_t digit = static_cast<std::uint64_t>(ch - '0');
    if (value > (0xFFFFFFFFFFFFFFFFull - digit) / 10ull) {
      return false;
    }
    value = value * 10ull + digit;
  }
  out = value;
  return true;
}

}  // namespace detail

/// Strongly typed 64-bit identity. Value 0 is the invalid/absent identity.
template <class Tag>
class Id {
 public:
  using tag_type = Tag;

  constexpr Id() noexcept = default;
  constexpr explicit Id(std::uint64_t value) noexcept : value_(value) {}

  [[nodiscard]] static constexpr Id from_value(std::uint64_t value) noexcept { return Id{value}; }

  [[nodiscard]] constexpr std::uint64_t value() const noexcept { return value_; }
  [[nodiscard]] constexpr bool valid() const noexcept { return value_ != 0; }

  [[nodiscard]] std::string to_string() const { return detail::format_hex16(value_); }

  [[nodiscard]] static Result<Id> parse(std::string_view text) {
    std::uint64_t value = 0;
    if (!detail::parse_hex16(text, value)) {
      return Status{Code::malformed_input, "identity is not a hexadecimal 64-bit value"};
    }
    return Id{value};
  }

  friend constexpr bool operator==(Id a, Id b) noexcept { return a.value_ == b.value_; }
  friend constexpr bool operator!=(Id a, Id b) noexcept { return a.value_ != b.value_; }
  friend constexpr bool operator<(Id a, Id b) noexcept { return a.value_ < b.value_; }

 private:
  std::uint64_t value_{0};
};

template <class Tag>
inline std::ostream& operator<<(std::ostream& os, Id<Tag> id) {
  return os << id.to_string();
}

/// Monotonic counter with checked overflow. Used for generations, epochs,
/// sequences and incarnations; all of them must fail closed at the ceiling
/// rather than wrap.
template <class Tag>
class Counter {
 public:
  using tag_type = Tag;

  constexpr Counter() noexcept = default;
  constexpr explicit Counter(std::uint64_t value) noexcept : value_(value) {}

  [[nodiscard]] static constexpr Counter from_value(std::uint64_t value) noexcept { return Counter{value}; }

  [[nodiscard]] constexpr std::uint64_t value() const noexcept { return value_; }
  [[nodiscard]] constexpr bool valid() const noexcept { return value_ != 0; }

  [[nodiscard]] Result<Counter> next() const {
    if (value_ == 0xFFFFFFFFFFFFFFFFull) {
      return Status{Code::capacity_exceeded, "counter exhausted"};
    }
    return Counter{value_ + 1ull};
  }

  [[nodiscard]] std::string to_string() const { return detail::format_hex16(value_); }

  [[nodiscard]] static Result<Counter> parse(std::string_view text) {
    std::uint64_t value = 0;
    if (!detail::parse_hex16(text, value)) {
      return Status{Code::malformed_input, "counter is not a hexadecimal 64-bit value"};
    }
    return Counter{value};
  }

  friend constexpr bool operator==(Counter a, Counter b) noexcept { return a.value_ == b.value_; }
  friend constexpr bool operator!=(Counter a, Counter b) noexcept { return a.value_ != b.value_; }
  friend constexpr bool operator<(Counter a, Counter b) noexcept { return a.value_ < b.value_; }
  friend constexpr bool operator<=(Counter a, Counter b) noexcept { return a.value_ <= b.value_; }
  friend constexpr bool operator>(Counter a, Counter b) noexcept { return a.value_ > b.value_; }
  friend constexpr bool operator>=(Counter a, Counter b) noexcept { return a.value_ >= b.value_; }

 private:
  std::uint64_t value_{0};
};

template <class Tag>
inline std::ostream& operator<<(std::ostream& os, Counter<Tag> counter) {
  return os << counter.to_string();
}

using ResourceId = Id<tags::resource_tag>;
using QueueId = Id<tags::queue_tag>;
using ClassId = Id<tags::class_tag>;
using PoolId = Id<tags::pool_tag>;
using PolicyId = Id<tags::policy_tag>;
using OwnerId = Id<tags::owner_tag>;
using BackendId = Id<tags::backend_tag>;
using PublisherId = Id<tags::publisher_tag>;
using NodeId = Id<tags::node_tag>;

using Generation = Counter<tags::generation_tag>;
using Epoch = Counter<tags::epoch_tag>;
using Sequence = Counter<tags::sequence_tag>;
using Incarnation = Counter<tags::incarnation_tag>;

/// Boot identifier: fresh for every process start. Two independent 64-bit
/// halves make accidental reuse across restarts practically impossible, and an
/// explicit seed keeps randomized tests reproducible.
struct BootId {
  std::uint64_t hi{0};
  std::uint64_t lo{0};

  [[nodiscard]] bool valid() const noexcept { return (hi | lo) != 0; }

  [[nodiscard]] static BootId generate(Rng& rng) noexcept {
    BootId boot{};
    boot.hi = rng.next_u64();
    boot.lo = rng.next_u64();
    if (!boot.valid()) {
      boot.lo = 1;
    }
    return boot;
  }

  [[nodiscard]] std::string to_string() const {
    return detail::format_hex16(hi) + ":" + detail::format_hex16(lo);
  }

  [[nodiscard]] static Result<BootId> parse(std::string_view text) {
    const std::size_t separator = text.find(':');
    if (separator == std::string_view::npos) {
      return Status{Code::malformed_input, "boot id must be <hi>:<lo>"};
    }
    BootId boot{};
    if (!detail::parse_hex16(text.substr(0, separator), boot.hi) ||
        !detail::parse_hex16(text.substr(separator + 1), boot.lo)) {
      return Status{Code::malformed_input, "boot id halves must be hexadecimal"};
    }
    return boot;
  }

  friend bool operator==(const BootId& a, const BootId& b) noexcept { return a.hi == b.hi && a.lo == b.lo; }
  friend bool operator!=(const BootId& a, const BootId& b) noexcept { return !(a == b); }
  friend bool operator<(const BootId& a, const BootId& b) noexcept {
    return a.hi != b.hi ? a.hi < b.hi : a.lo < b.lo;
  }
};

/// Attempt identity: publisher-scoped monotonic counter. Every mutation and
/// every occupancy publication carries one, which is what makes duplicate
/// delivery detectable and idempotent replay exact.
struct AttemptId {
  PublisherId publisher{};
  std::uint64_t counter{0};

  [[nodiscard]] bool valid() const noexcept { return publisher.valid() && counter != 0; }

  [[nodiscard]] std::string to_string() const {
    return publisher.to_string() + ":" + std::to_string(counter);
  }

  [[nodiscard]] static Result<AttemptId> parse(std::string_view text) {
    const std::size_t separator = text.find(':');
    if (separator == std::string_view::npos) {
      return Status{Code::malformed_input, "attempt id must be <publisher>:<counter>"};
    }
    auto publisher = PublisherId::parse(text.substr(0, separator));
    if (!publisher.ok()) {
      return publisher.status();
    }
    std::uint64_t counter = 0;
    if (!detail::parse_u64(text.substr(separator + 1), counter)) {
      return Status{Code::malformed_input, "attempt counter must be decimal"};
    }
    return AttemptId{publisher.value(), counter};
  }

  friend bool operator==(const AttemptId& a, const AttemptId& b) noexcept {
    return a.publisher == b.publisher && a.counter == b.counter;
  }
  friend bool operator!=(const AttemptId& a, const AttemptId& b) noexcept { return !(a == b); }
  friend bool operator<(const AttemptId& a, const AttemptId& b) noexcept {
    return a.publisher != b.publisher ? a.publisher < b.publisher : a.counter < b.counter;
  }
};

inline std::ostream& operator<<(std::ostream& os, const AttemptId& id) { return os << id.to_string(); }

}  // namespace qf

namespace std {

template <class Tag>
struct hash<qf::Id<Tag>> {
  std::size_t operator()(const qf::Id<Tag>& id) const noexcept {
    return static_cast<std::size_t>(id.value() * 0x9E3779B97F4A7C15ull);
  }
};

template <class Tag>
struct hash<qf::Counter<Tag>> {
  std::size_t operator()(const qf::Counter<Tag>& counter) const noexcept {
    return static_cast<std::size_t>(counter.value() * 0xBF58476D1CE4E5B9ull);
  }
};

template <>
struct hash<qf::BootId> {
  std::size_t operator()(const qf::BootId& boot) const noexcept {
    return static_cast<std::size_t>(boot.hi ^ (boot.lo * 0x94D049BB133111EBull));
  }
};

template <>
struct hash<qf::AttemptId> {
  std::size_t operator()(const qf::AttemptId& attempt) const noexcept {
    return static_cast<std::size_t>(attempt.publisher.value() * 0x9E3779B97F4A7C15ull) ^
           static_cast<std::size_t>(attempt.counter * 0xBF58476D1CE4E5B9ull);
  }
};

}  // namespace std
