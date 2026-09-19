// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
#include "qf/lifecycle.hpp"

#include <string>

namespace qf {

const char* to_string(Lifecycle value) noexcept {
  switch (value) {
    case Lifecycle::declared: return "declared";
    case Lifecycle::validated: return "validated";
    case Lifecycle::active: return "active";
    case Lifecycle::draining: return "draining";
    case Lifecycle::quiesced: return "quiesced";
    case Lifecycle::retired: return "retired";
    case Lifecycle::failed: return "failed";
  }
  return "unknown";
}

std::string to_string(Applicability value) {
  if (value == Applicability::none) {
    return "none";
  }
  std::string out;
  if (has_flag(value, Applicability::stale)) {
    out += "stale";
  }
  if (has_flag(value, Applicability::fenced)) {
    if (!out.empty()) {
      out += "+";
    }
    out += "fenced";
  }
  return out;
}

namespace {

struct TransitionRule {
  Lifecycle from;
  Lifecycle to;
  const char* rule;
};

// The complete transition table. Anything not listed is refused, including
// every transition out of retired.
constexpr TransitionRule kRules[] = {
    {Lifecycle::declared, Lifecycle::validated, "declared-to-validated"},
    {Lifecycle::declared, Lifecycle::retired, "abandoned-before-validation"},
    {Lifecycle::declared, Lifecycle::failed, "declaration-failed"},
    {Lifecycle::validated, Lifecycle::active, "validated-to-active"},
    {Lifecycle::validated, Lifecycle::retired, "retired-before-activation"},
    {Lifecycle::validated, Lifecycle::failed, "validation-failed"},
    {Lifecycle::active, Lifecycle::draining, "active-must-drain-before-quiesce"},
    {Lifecycle::active, Lifecycle::failed, "activation-failed"},
    {Lifecycle::draining, Lifecycle::quiesced, "drain-proven-complete"},
    {Lifecycle::draining, Lifecycle::active, "drain-cancelled-by-revalidation"},
    {Lifecycle::draining, Lifecycle::failed, "drain-failed"},
    {Lifecycle::quiesced, Lifecycle::active, "reactivation-requires-revalidation"},
    {Lifecycle::quiesced, Lifecycle::retired, "quiesced-to-retired"},
    {Lifecycle::quiesced, Lifecycle::failed, "quiesced-fault"},
    {Lifecycle::failed, Lifecycle::retired, "failed-cleanup-retire"},
};

}  // namespace

TransitionDecision check_transition(Lifecycle from, Lifecycle to) noexcept {
  if (from == to) {
    return TransitionDecision{true, "no-op-idempotent", Code::ok};
  }
  for (const auto& rule : kRules) {
    if (rule.from == from && rule.to == to) {
      return TransitionDecision{true, rule.rule, Code::ok};
    }
  }
  if (is_terminal(from)) {
    return TransitionDecision{false, "retired-is-terminal", Code::lifecycle_violation};
  }
  return TransitionDecision{false, "transition-not-permitted", Code::lifecycle_violation};
}

}  // namespace qf
