// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
#include "qf/process.hpp"

#include <chrono>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <system_error>
#include <thread>
#include <vector>

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#else
#include <csignal>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

namespace qf {

struct ChildProcess::Impl {
#ifdef _WIN32
  HANDLE process{nullptr};
  DWORD exit_code{0};
  bool exited{false};
#else
  int pid{-1};
  int status{0};
  bool exited{false};
#endif
};

ChildProcess::~ChildProcess() {
  if (impl_ != nullptr && running()) {
    terminate();
    wait();
  }
}

ChildProcess::ChildProcess(ChildProcess&& other) noexcept : impl_(std::move(other.impl_)), pid_(other.pid_) {
  other.pid_ = 0;
}

ChildProcess& ChildProcess::operator=(ChildProcess&& other) noexcept {
  if (this != &other) {
    impl_ = std::move(other.impl_);
    pid_ = other.pid_;
    other.pid_ = 0;
  }
  return *this;
}

#ifdef _WIN32

Result<ChildProcess> ChildProcess::spawn(const std::vector<std::string>& argv, const std::string& working_dir) {
  if (argv.empty()) {
    return Status{Code::invalid_argument, "spawn requires a program"};
  }
  std::string command_line;
  for (const auto& argument : argv) {
    if (!command_line.empty()) {
      command_line.push_back(' ');
    }
    command_line.push_back('"');
    command_line += argument;
    command_line.push_back('"');
  }
  std::vector<char> mutable_command(command_line.begin(), command_line.end());
  mutable_command.push_back('\0');

  STARTUPINFOA startup{};
  startup.cb = sizeof(startup);
  PROCESS_INFORMATION info{};
  const char* directory = working_dir.empty() ? nullptr : working_dir.c_str();
  const BOOL created = CreateProcessA(nullptr, mutable_command.data(), nullptr, nullptr, FALSE,
                                      CREATE_NO_WINDOW, nullptr, directory, &startup, &info);
  if (created == FALSE) {
    return Status{Code::not_found, "child process could not be started"};
  }
  CloseHandle(info.hThread);
  ChildProcess child{};
  child.impl_ = std::make_unique<Impl>();
  child.impl_->process = info.hProcess;
  child.pid_ = static_cast<std::uint64_t>(info.dwProcessId);
  return child;
}

Status ChildProcess::terminate() {
  if (impl_ == nullptr || impl_->process == nullptr) {
    return Status{Code::not_found, "no child process"};
  }
  if (impl_->exited) {
    return Status{};
  }
  if (TerminateProcess(impl_->process, 137) == FALSE) {
    return Status{Code::internal, "child process could not be terminated"};
  }
  return Status{};
}

Result<int> ChildProcess::wait() {
  if (impl_ == nullptr || impl_->process == nullptr) {
    return Status{Code::not_found, "no child process"};
  }
  if (!impl_->exited) {
    WaitForSingleObject(impl_->process, INFINITE);
    DWORD code = 0;
    if (GetExitCodeProcess(impl_->process, &code) == FALSE) {
      return Status{Code::internal, "exit code could not be read"};
    }
    impl_->exit_code = code;
    impl_->exited = true;
  }
  return static_cast<int>(impl_->exit_code);
}

Result<bool> ChildProcess::wait_for(std::uint32_t timeout_ms) {
  if (impl_ == nullptr || impl_->process == nullptr) {
    return Status{Code::not_found, "no child process"};
  }
  if (impl_->exited) {
    return true;
  }
  const DWORD result = WaitForSingleObject(impl_->process, timeout_ms);
  if (result == WAIT_OBJECT_0) {
    DWORD code = 0;
    if (GetExitCodeProcess(impl_->process, &code) == FALSE) {
      return Status{Code::internal, "exit code could not be read"};
    }
    impl_->exit_code = code;
    impl_->exited = true;
    return true;
  }
  return false;
}

bool ChildProcess::running() const {
  if (impl_ == nullptr || impl_->process == nullptr || impl_->exited) {
    return false;
  }
  DWORD code = 0;
  if (GetExitCodeProcess(impl_->process, &code) == FALSE) {
    return false;
  }
  if (code == STILL_ACTIVE) {
    return true;
  }
  impl_->exit_code = code;
  impl_->exited = true;
  return false;
}

#else

Result<ChildProcess> ChildProcess::spawn(const std::vector<std::string>& argv, const std::string& working_dir) {
  if (argv.empty()) {
    return Status{Code::invalid_argument, "spawn requires a program"};
  }
  std::vector<char*> raw;
  raw.reserve(argv.size() + 1);
  for (const auto& argument : argv) {
    raw.push_back(const_cast<char*>(argument.c_str()));
  }
  raw.push_back(nullptr);
  const pid_t pid = ::fork();
  if (pid < 0) {
    return Status{Code::internal, "fork failed"};
  }
  if (pid == 0) {
    if (!working_dir.empty()) {
      if (::chdir(working_dir.c_str()) != 0) {
        ::_exit(127);
      }
    }
    ::execv(raw[0], raw.data());
    ::_exit(127);
  }
  ChildProcess child{};
  child.impl_ = std::make_unique<Impl>();
  child.impl_->pid = static_cast<int>(pid);
  child.pid_ = static_cast<std::uint64_t>(pid);
  return child;
}

Status ChildProcess::terminate() {
  if (impl_ == nullptr || impl_->pid <= 0) {
    return Status{Code::not_found, "no child process"};
  }
  if (impl_->exited) {
    return Status{};
  }
  if (::kill(impl_->pid, SIGKILL) != 0) {
    return Status{Code::internal, "child process could not be terminated"};
  }
  return Status{};
}

Result<int> ChildProcess::wait() {
  if (impl_ == nullptr || impl_->pid <= 0) {
    return Status{Code::not_found, "no child process"};
  }
  if (!impl_->exited) {
    int status = 0;
    if (::waitpid(impl_->pid, &status, 0) < 0) {
      return Status{Code::internal, "waitpid failed"};
    }
    impl_->status = status;
    impl_->exited = true;
  }
  if (WIFEXITED(impl_->status)) {
    return WEXITSTATUS(impl_->status);
  }
  if (WIFSIGNALED(impl_->status)) {
    return 128 + WTERMSIG(impl_->status);
  }
  return 0;
}

Result<bool> ChildProcess::wait_for(std::uint32_t timeout_ms) {
  if (impl_ == nullptr || impl_->pid <= 0) {
    return Status{Code::not_found, "no child process"};
  }
  if (impl_->exited) {
    return true;
  }
  int status = 0;
  const pid_t result = ::waitpid(impl_->pid, &status, WNOHANG);
  if (result == 0) {
    return false;
  }
  if (result < 0) {
    return Status{Code::internal, "waitpid failed"};
  }
  impl_->status = status;
  impl_->exited = true;
  (void)timeout_ms;
  return true;
}

bool ChildProcess::running() const {
  if (impl_ == nullptr || impl_->pid <= 0 || impl_->exited) {
    return false;
  }
  int status = 0;
  const pid_t result = ::waitpid(impl_->pid, &status, WNOHANG);
  if (result == 0) {
    return true;
  }
  if (result > 0) {
    impl_->status = status;
    impl_->exited = true;
  }
  return false;
}

#endif

Result<std::string> read_text_file(const std::string& path, std::size_t max_bytes) {
  std::ifstream stream(path, std::ios::binary);
  if (!stream) {
    return Status{Code::not_found, "file could not be opened"};
  }
  std::string content;
  content.resize(max_bytes);
  stream.read(content.data(), static_cast<std::streamsize>(max_bytes));
  const auto read_bytes = static_cast<std::size_t>(stream.gcount());
  content.resize(read_bytes);
  if (read_bytes == 0) {
    return Status{Code::not_found, "file is empty"};
  }
  return content;
}

Status write_text_file(const std::string& path, std::string_view text) {
  const std::string temporary = path + ".tmp";
  {
    std::ofstream stream(temporary, std::ios::binary | std::ios::trunc);
    if (!stream) {
      return Status{Code::internal, "temporary file could not be created"};
    }
    stream.write(text.data(), static_cast<std::streamsize>(text.size()));
    stream.flush();
    if (!stream) {
      return Status{Code::internal, "temporary file could not be written"};
    }
  }
  std::error_code error;
  std::filesystem::rename(temporary, path, error);
  if (error) {
    return Status{Code::internal, "file could not be replaced atomically"};
  }
  return Status{};
}

Result<std::string> wait_for_file(const std::string& path, std::uint32_t attempts, std::uint32_t sleep_ms) {
  for (std::uint32_t attempt = 0; attempt < attempts; ++attempt) {
    auto content = read_text_file(path);
    if (content.ok()) {
      return content.value();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
  }
  return Status{Code::not_found, "handshake file did not appear"};
}

}  // namespace qf
