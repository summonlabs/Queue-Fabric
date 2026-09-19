// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "qf/status.hpp"

namespace qf {

/// Real operating-system child process.
///
/// Used by the multiprocess proof surface: workers and coordinators are started
/// as independent processes, killed hard, and restarted to demonstrate
/// incarnation fencing, epoch advancement and crash recovery.
class ChildProcess {
 public:
  ChildProcess() = default;
  ~ChildProcess();

  ChildProcess(const ChildProcess&) = delete;
  ChildProcess& operator=(const ChildProcess&) = delete;
  ChildProcess(ChildProcess&& other) noexcept;
  ChildProcess& operator=(ChildProcess&& other) noexcept;

  /// Starts a program with arguments. The first element is the executable path.
  static Result<ChildProcess> spawn(const std::vector<std::string>& argv, const std::string& working_dir = {});

  /// Hard termination (TerminateProcess / SIGKILL). No cleanup runs in the
  /// child, which is exactly what the crash-recovery proof requires.
  Status terminate();

  /// Waits for the process to exit and returns its exit code.
  Result<int> wait();

  /// Waits up to a bounded number of milliseconds. Returns true when the
  /// process has exited; the exit code is then available through wait().
  Result<bool> wait_for(std::uint32_t timeout_ms);

  [[nodiscard]] bool running() const;
  [[nodiscard]] std::uint64_t pid() const noexcept { return pid_; }

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_{};
  std::uint64_t pid_{0};
};

/// Reads a small text file into memory (bounded). Used for readiness handshake
/// files written by child processes.
Result<std::string> read_text_file(const std::string& path, std::size_t max_bytes = 4096);

/// Writes a small text file atomically.
Status write_text_file(const std::string& path, std::string_view text);

/// Waits until a file exists and is non-empty, polling with a bounded number of
/// attempts. Returns the file content; this is a readiness handshake, not a
/// test timeout.
Result<std::string> wait_for_file(const std::string& path, std::uint32_t attempts, std::uint32_t sleep_ms);

}  // namespace qf
