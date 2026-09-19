// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
//
// Minimal test framework. No external dependency, deterministic ordering, and
// no timeouts anywhere: a test runs until it finishes or the process dies.
#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include "qf/status.hpp"

namespace qftest {

/// Uniform way to obtain an outcome code from either a Status or a Result.
[[nodiscard]] inline qf::Code code_of(const qf::Status& status) noexcept { return status.code(); }

template <class T>
[[nodiscard]] inline qf::Code code_of(const qf::Result<T>& result) noexcept {
  return result.ok() ? qf::Code::ok : result.status().code();
}

struct Failure {
  std::string message;
};

struct TestCase {
  std::string suite;
  std::string name;
  std::function<void()> body;
};

class Registry {
 public:
  static Registry& instance();

  void add(std::string suite, std::string name, std::function<void()> body);
  [[nodiscard]] const std::vector<TestCase>& cases() const noexcept { return cases_; }

  /// Runs every case whose suite or name contains one of the filters (empty
  /// filter runs everything). Returns the number of failed cases.
  int run(const std::vector<std::string>& filters);

  void note(std::string text);

 private:
  std::vector<TestCase> cases_{};
  std::vector<std::string> notes_{};
};

struct Registrar {
  Registrar(const char* suite, const char* name, void (*body)());
};

void report_check(bool passed, const char* expression, const char* file, int line);
void report_require(bool passed, const char* expression, const char* file, int line);

/// Records the test binary path so tests can locate sibling executables.
void set_program_path(const char* path);

/// Directory containing the Queue Fabric tool executables that ship next to the
/// test binary (qf_coordinator, qf_worker). Empty when they cannot be found.
[[nodiscard]] std::string tools_directory();

/// Executable path for a tool by base name, or an empty string when missing.
[[nodiscard]] std::string tool_path(const std::string& base_name);

[[nodiscard]] std::uint64_t checks_run() noexcept;
[[nodiscard]] std::uint64_t checks_failed() noexcept;

}  // namespace qftest

#define QF_TEST(suite_name, case_name)                                                       \
  static void qf_test_##suite_name##_##case_name();                                          \
  static const ::qftest::Registrar qf_registrar_##suite_name##_##case_name(                  \
      #suite_name, #case_name, &qf_test_##suite_name##_##case_name);                         \
  static void qf_test_##suite_name##_##case_name()

#define QF_CHECK(expression) ::qftest::report_check((expression), #expression, __FILE__, __LINE__)
#define QF_REQUIRE(expression) ::qftest::report_require((expression), #expression, __FILE__, __LINE__)

#define QF_CHECK_EQ(lhs, rhs) QF_CHECK((lhs) == (rhs))
#define QF_REQUIRE_EQ(lhs, rhs) QF_REQUIRE((lhs) == (rhs))
#define QF_CHECK_CODE(result, expected) QF_CHECK(::qftest::code_of(result) == (expected))
