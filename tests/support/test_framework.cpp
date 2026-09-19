// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
#include "test_framework.hpp"

#include <algorithm>
#include <cstdio>
#include <exception>
#include <filesystem>
#include <system_error>
#include <utility>

namespace qftest {

namespace {

std::uint64_t g_checks_run = 0;
std::uint64_t g_checks_failed = 0;
std::string g_program_path{};

}  // namespace

void set_program_path(const char* path) {
  if (path != nullptr) {
    g_program_path = path;
  }
}

std::string tools_directory() {
  if (g_program_path.empty()) {
    return {};
  }
  std::error_code error;
  const auto binary = std::filesystem::absolute(g_program_path, error);
  if (error) {
    return {};
  }
  const auto directory = binary.parent_path();
  const auto candidate = directory.parent_path() / "tools";
  if (std::filesystem::exists(candidate, error)) {
    return candidate.string();
  }
  return {};
}

std::string tool_path(const std::string& base_name) {
  const std::string directory = tools_directory();
  if (directory.empty()) {
    return {};
  }
  const auto candidate = std::filesystem::path(directory) / base_name;
  std::error_code error;
  if (std::filesystem::exists(candidate, error)) {
    return candidate.string();
  }
  return {};
}

Registry& Registry::instance() {
  static Registry registry;
  return registry;
}

void Registry::add(std::string suite, std::string name, std::function<void()> body) {
  cases_.push_back(TestCase{std::move(suite), std::move(name), std::move(body)});
}

void Registry::note(std::string text) { notes_.push_back(std::move(text)); }

Registrar::Registrar(const char* suite, const char* name, void (*body)()) {
  Registry::instance().add(suite, name, body);
}

void report_check(bool passed, const char* expression, const char* file, int line) {
  ++g_checks_run;
  if (!passed) {
    ++g_checks_failed;
    std::string message = std::string(file) + ":" + std::to_string(line) + ": check failed: " + expression;
    throw Failure{std::move(message)};
  }
}

void report_require(bool passed, const char* expression, const char* file, int line) {
  report_check(passed, expression, file, line);
}

std::uint64_t checks_run() noexcept { return g_checks_run; }
std::uint64_t checks_failed() noexcept { return g_checks_failed; }

int Registry::run(const std::vector<std::string>& filters) {
  std::size_t executed = 0;
  std::size_t failed = 0;
  for (const auto& test : cases_) {
    if (!filters.empty()) {
      bool matches = false;
      for (const auto& filter : filters) {
        if (test.suite.find(filter) != std::string::npos || test.name.find(filter) != std::string::npos) {
          matches = true;
          break;
        }
      }
      if (!matches) {
        continue;
      }
    }
    ++executed;
    const std::uint64_t failed_before = g_checks_failed;
    try {
      test.body();
    } catch (const Failure& failure) {
      std::printf("FAIL %s.%s\n     %s\n", test.suite.c_str(), test.name.c_str(), failure.message.c_str());
      ++failed;
      continue;
    } catch (const std::exception& error) {
      std::printf("FAIL %s.%s\n     unexpected exception: %s\n", test.suite.c_str(), test.name.c_str(), error.what());
      ++failed;
      continue;
    } catch (...) {
      std::printf("FAIL %s.%s\n     unexpected non-standard exception\n", test.suite.c_str(), test.name.c_str());
      ++failed;
      continue;
    }
    if (g_checks_failed != failed_before) {
      std::printf("FAIL %s.%s\n     %llu checks failed\n", test.suite.c_str(), test.name.c_str(),
                  static_cast<unsigned long long>(g_checks_failed - failed_before));
      ++failed;
      continue;
    }
    std::printf("pass %s.%s\n", test.suite.c_str(), test.name.c_str());
  }

  std::printf("\n%llu cases executed, %llu failed, %llu checks run, %llu checks failed\n",
              static_cast<unsigned long long>(executed), static_cast<unsigned long long>(failed),
              static_cast<unsigned long long>(g_checks_run), static_cast<unsigned long long>(g_checks_failed));
  for (const auto& note : notes_) {
    std::printf("note: %s\n", note.c_str());
  }
  return static_cast<int>(failed);
}

}  // namespace qftest
