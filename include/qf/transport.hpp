// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <cstdint>
#include <span>
#include <string>

#include "qf/bytes.hpp"
#include "qf/limits.hpp"
#include "qf/status.hpp"

namespace qf {

/// Byte-stream transport. Implementations move opaque bytes; framing, message
/// interpretation and admission control live above this interface.
class ITransport {
 public:
  ITransport() = default;
  ITransport(const ITransport&) = delete;
  ITransport& operator=(const ITransport&) = delete;
  virtual ~ITransport();

  [[nodiscard]] virtual bool open() const noexcept = 0;

  /// Sends all bytes or fails. Partial sends are completed internally.
  virtual Status send(std::span<const std::byte> data) = 0;

  /// Receives up to buffer.size() bytes. A successful zero-length result means
  /// the peer closed the connection.
  virtual Result<std::size_t> receive(std::span<std::byte> buffer) = 0;

  /// Non-blocking readiness check used by the single-threaded coordinator loop.
  [[nodiscard]] virtual bool readable(std::uint32_t timeout_ms) const = 0;

  virtual void close() noexcept = 0;
  [[nodiscard]] virtual std::string peer() const = 0;
  [[nodiscard]] virtual std::uint64_t bytes_sent() const noexcept = 0;
  [[nodiscard]] virtual std::uint64_t bytes_received() const noexcept = 0;
};

/// Process-wide socket subsystem lifetime. Safe to call repeatedly.
Status network_init();
void network_shutdown();

/// Real TCP socket transport (IPv4 loopback or any local address).
class TcpTransport final : public ITransport {
 public:
  TcpTransport() = default;
  ~TcpTransport() override;

  TcpTransport(const TcpTransport&) = delete;
  TcpTransport& operator=(const TcpTransport&) = delete;
  TcpTransport(TcpTransport&& other) noexcept;
  TcpTransport& operator=(TcpTransport&& other) noexcept;

  /// Connects to host:port.
  static Result<TcpTransport> connect(std::string_view host, std::uint16_t port);

  /// Wraps an already accepted socket.
  static TcpTransport adopt(std::intptr_t handle, std::string peer);

  [[nodiscard]] bool open() const noexcept override;
  Status send(std::span<const std::byte> data) override;
  Result<std::size_t> receive(std::span<std::byte> buffer) override;
  [[nodiscard]] bool readable(std::uint32_t timeout_ms) const override;
  void close() noexcept override;
  [[nodiscard]] std::string peer() const override;
  [[nodiscard]] std::uint64_t bytes_sent() const noexcept override { return bytes_sent_; }
  [[nodiscard]] std::uint64_t bytes_received() const noexcept override { return bytes_received_; }

  /// Disables Nagle batching so small control messages are not delayed.
  Status set_no_delay();

  /// Enables TCP keepalive; used by long-lived coordinator/worker links.
  Status set_keepalive();

 private:
  std::intptr_t handle_{-1};
  std::string peer_{};
  std::uint64_t bytes_sent_{0};
  std::uint64_t bytes_received_{0};
};

/// Listening socket. Binds, listens and accepts into TcpTransport.
class TcpListener {
 public:
  TcpListener() = default;
  ~TcpListener();

  TcpListener(const TcpListener&) = delete;
  TcpListener& operator=(const TcpListener&) = delete;
  TcpListener(TcpListener&& other) noexcept;
  TcpListener& operator=(TcpListener&& other) noexcept;

  /// Binds to host:port. Port 0 selects an ephemeral port.
  static Result<TcpListener> bind(std::string_view host, std::uint16_t port, std::size_t backlog);

  [[nodiscard]] bool open() const noexcept;
  [[nodiscard]] std::uint16_t port() const noexcept;

  /// Accepts one pending connection if one is ready within the timeout.
  [[nodiscard]] Result<TcpTransport> accept(std::uint32_t timeout_ms);

  void close() noexcept;

 private:
  std::intptr_t handle_{-1};
  std::uint16_t port_{0};
};

}  // namespace qf
