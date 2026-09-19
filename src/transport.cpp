// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
#include "qf/transport.hpp"

#include <array>
#include <cstring>
#include <mutex>
#include <string>
#include <utility>

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#include <cerrno>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#endif

namespace qf {

namespace {

#ifdef _WIN32
constexpr std::intptr_t kInvalidHandle = static_cast<std::intptr_t>(INVALID_SOCKET);
using socket_length_t = int;
using socket_handle_t = SOCKET;
#else
constexpr std::intptr_t kInvalidHandle = -1;
using socket_length_t = socklen_t;
using socket_handle_t = int;
#endif

std::once_flag g_network_once;
bool g_network_ok = false;
std::string g_network_error = "network subsystem was never initialised";

void close_handle(std::intptr_t handle) noexcept {
  if (handle == kInvalidHandle) {
    return;
  }
  const auto raw = static_cast<socket_handle_t>(handle);
#ifdef _WIN32
  ::closesocket(raw);
#else
  ::close(raw);
#endif
}

void initialize_network() {
#ifdef _WIN32
  WSADATA data{};
  const int rc = WSAStartup(MAKEWORD(2, 2), &data);
  if (rc != 0) {
    g_network_error = "WSAStartup failed";
    g_network_ok = false;
    return;
  }
#endif
  g_network_ok = true;
  g_network_error.clear();
}

std::string last_socket_error() {
#ifdef _WIN32
  return "socket error " + std::to_string(WSAGetLastError());
#else
  return std::string("socket error ") + std::strerror(errno);
#endif
}

bool would_block() noexcept {
#ifdef _WIN32
  const int error = WSAGetLastError();
  return error == WSAEWOULDBLOCK || error == WSAETIMEDOUT;
#else
  return errno == EWOULDBLOCK || errno == EAGAIN || errno == EINTR;
#endif
}

std::string describe_peer(const sockaddr_storage& address) {
  std::array<char, INET6_ADDRSTRLEN> text{};
  std::uint16_t port = 0;
  if (address.ss_family == AF_INET) {
    const auto* v4 = reinterpret_cast<const sockaddr_in*>(&address);
    if (::inet_ntop(AF_INET, &v4->sin_addr, text.data(), static_cast<socket_length_t>(text.size())) == nullptr) {
      return "unknown";
    }
    port = ntohs(v4->sin_port);
  } else if (address.ss_family == AF_INET6) {
    const auto* v6 = reinterpret_cast<const sockaddr_in6*>(&address);
    if (::inet_ntop(AF_INET6, &v6->sin6_addr, text.data(), static_cast<socket_length_t>(text.size())) == nullptr) {
      return "unknown";
    }
    port = ntohs(v6->sin6_port);
  } else {
    return "unknown";
  }
  return std::string(text.data()) + ":" + std::to_string(port);
}

}  // namespace

ITransport::~ITransport() = default;

Status network_init() {
  std::call_once(g_network_once, initialize_network);
  if (!g_network_ok) {
    return Status{Code::unsupported, g_network_error};
  }
  return Status{};
}

void network_shutdown() {
#ifdef _WIN32
  WSACleanup();
#endif
}

TcpTransport::~TcpTransport() { close(); }

TcpTransport::TcpTransport(TcpTransport&& other) noexcept
    : handle_(other.handle_), peer_(std::move(other.peer_)), bytes_sent_(other.bytes_sent_),
      bytes_received_(other.bytes_received_) {
  other.handle_ = kInvalidHandle;
  other.bytes_sent_ = 0;
  other.bytes_received_ = 0;
}

TcpTransport& TcpTransport::operator=(TcpTransport&& other) noexcept {
  if (this != &other) {
    close();
    handle_ = other.handle_;
    peer_ = std::move(other.peer_);
    bytes_sent_ = other.bytes_sent_;
    bytes_received_ = other.bytes_received_;
    other.handle_ = kInvalidHandle;
    other.bytes_sent_ = 0;
    other.bytes_received_ = 0;
  }
  return *this;
}

TcpTransport TcpTransport::adopt(std::intptr_t handle, std::string peer) {
  TcpTransport transport{};
  transport.handle_ = handle;
  transport.peer_ = std::move(peer);
  return transport;
}

Result<TcpTransport> TcpTransport::connect(std::string_view host, std::uint16_t port) {
  const Status init = network_init();
  if (!init.ok()) {
    return init;
  }
  std::string host_text(host);
  std::string port_text = std::to_string(port);
  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_protocol = IPPROTO_TCP;
  addrinfo* results = nullptr;
  const int rc = ::getaddrinfo(host_text.c_str(), port_text.c_str(), &hints, &results);
  if (rc != 0 || results == nullptr) {
    return Status{Code::not_found, "address could not be resolved"};
  }
  Status failure{Code::not_found, "no reachable address"};
  for (addrinfo* entry = results; entry != nullptr; entry = entry->ai_next) {
    const auto raw = ::socket(entry->ai_family, entry->ai_socktype, entry->ai_protocol);
    if (raw == static_cast<socket_handle_t>(kInvalidHandle)) {
      failure = Status{Code::internal, last_socket_error()};
      continue;
    }
    if (::connect(raw, entry->ai_addr, static_cast<socket_length_t>(entry->ai_addrlen)) == 0) {
      sockaddr_storage local{};
      socket_length_t local_length = static_cast<socket_length_t>(sizeof(local));
      std::string peer = host_text + ":" + port_text;
      if (::getpeername(raw, reinterpret_cast<sockaddr*>(&local), &local_length) == 0) {
        peer = describe_peer(local);
      }
      ::freeaddrinfo(results);
      return TcpTransport::adopt(static_cast<std::intptr_t>(raw), std::move(peer));
    }
    failure = Status{Code::not_found, last_socket_error()};
    close_handle(static_cast<std::intptr_t>(raw));
  }
  ::freeaddrinfo(results);
  return failure;
}

bool TcpTransport::open() const noexcept { return handle_ != kInvalidHandle; }

Status TcpTransport::send(std::span<const std::byte> data) {
  if (!open()) {
    return Status{Code::internal, "transport is closed"};
  }
  std::size_t offset = 0;
  while (offset < data.size()) {
    const std::size_t remaining = data.size() - offset;
    const std::size_t chunk = remaining > 1u << 20 ? 1u << 20 : remaining;
#ifdef _WIN32
    const int sent = ::send(static_cast<socket_handle_t>(handle_),
                            reinterpret_cast<const char*>(data.data() + offset), static_cast<int>(chunk), 0);
#else
    const auto sent = ::send(static_cast<socket_handle_t>(handle_), data.data() + offset, chunk, MSG_NOSIGNAL);
#endif
    if (sent <= 0) {
      if (would_block()) {
        continue;
      }
      return Status{Code::internal, last_socket_error()};
    }
    offset += static_cast<std::size_t>(sent);
    bytes_sent_ += static_cast<std::uint64_t>(sent);
  }
  return Status{};
}

Result<std::size_t> TcpTransport::receive(std::span<std::byte> buffer) {
  if (!open()) {
    return Status{Code::internal, "transport is closed"};
  }
  if (buffer.empty()) {
    return std::size_t{0};
  }
#ifdef _WIN32
  const int received = ::recv(static_cast<socket_handle_t>(handle_), reinterpret_cast<char*>(buffer.data()),
                              static_cast<int>(buffer.size()), 0);
#else
  const auto received = ::recv(static_cast<socket_handle_t>(handle_), buffer.data(), buffer.size(), 0);
#endif
  if (received < 0) {
    if (would_block()) {
      return std::size_t{0};
    }
    return Status{Code::internal, last_socket_error()};
  }
  bytes_received_ += static_cast<std::uint64_t>(received);
  return static_cast<std::size_t>(received);
}

bool TcpTransport::readable(std::uint32_t timeout_ms) const {
  if (!open()) {
    return true;
  }
  fd_set read_set;
  FD_ZERO(&read_set);
  FD_SET(static_cast<socket_handle_t>(handle_), &read_set);
  timeval timeout{};
  timeout.tv_sec = static_cast<long>(timeout_ms / 1000u);
  timeout.tv_usec = static_cast<long>((timeout_ms % 1000u) * 1000u);
#ifdef _WIN32
  const int rc = ::select(0, &read_set, nullptr, nullptr, &timeout);
#else
  const int rc = ::select(static_cast<int>(handle_) + 1, &read_set, nullptr, nullptr, &timeout);
#endif
  return rc > 0;
}

void TcpTransport::close() noexcept {
  close_handle(handle_);
  handle_ = kInvalidHandle;
}

std::string TcpTransport::peer() const { return peer_; }

Status TcpTransport::set_no_delay() {
  if (!open()) {
    return Status{Code::internal, "transport is closed"};
  }
  const int enabled = 1;
  if (::setsockopt(static_cast<socket_handle_t>(handle_), IPPROTO_TCP, TCP_NODELAY,
                   reinterpret_cast<const char*>(&enabled), sizeof(enabled)) != 0) {
    return Status{Code::internal, last_socket_error()};
  }
  return Status{};
}

Status TcpTransport::set_keepalive() {
  if (!open()) {
    return Status{Code::internal, "transport is closed"};
  }
  const int enabled = 1;
  if (::setsockopt(static_cast<socket_handle_t>(handle_), SOL_SOCKET, SO_KEEPALIVE,
                   reinterpret_cast<const char*>(&enabled), sizeof(enabled)) != 0) {
    return Status{Code::internal, last_socket_error()};
  }
  return Status{};
}

TcpListener::~TcpListener() { close(); }

TcpListener::TcpListener(TcpListener&& other) noexcept : handle_(other.handle_), port_(other.port_) {
  other.handle_ = kInvalidHandle;
  other.port_ = 0;
}

TcpListener& TcpListener::operator=(TcpListener&& other) noexcept {
  if (this != &other) {
    close();
    handle_ = other.handle_;
    port_ = other.port_;
    other.handle_ = kInvalidHandle;
    other.port_ = 0;
  }
  return *this;
}

Result<TcpListener> TcpListener::bind(std::string_view host, std::uint16_t port, std::size_t backlog) {
  const Status init = network_init();
  if (!init.ok()) {
    return init;
  }
  std::string host_text(host);
  std::string port_text = std::to_string(port);
  addrinfo hints{};
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_protocol = IPPROTO_TCP;
  hints.ai_flags = AI_PASSIVE;
  addrinfo* results = nullptr;
  const int rc = ::getaddrinfo(host_text.empty() ? nullptr : host_text.c_str(), port_text.c_str(), &hints, &results);
  if (rc != 0 || results == nullptr) {
    return Status{Code::invalid_argument, "listen address could not be resolved"};
  }
  Status failure{Code::internal, "listen socket could not be created"};
  for (addrinfo* entry = results; entry != nullptr; entry = entry->ai_next) {
    const auto raw = ::socket(entry->ai_family, entry->ai_socktype, entry->ai_protocol);
    if (raw == static_cast<socket_handle_t>(kInvalidHandle)) {
      failure = Status{Code::internal, last_socket_error()};
      continue;
    }
    const int reuse = 1;
    ::setsockopt(raw, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<const char*>(&reuse), sizeof(reuse));
    if (::bind(raw, entry->ai_addr, static_cast<socket_length_t>(entry->ai_addrlen)) != 0) {
      failure = Status{Code::internal, last_socket_error()};
      close_handle(static_cast<std::intptr_t>(raw));
      continue;
    }
    const std::size_t effective_backlog = backlog == 0 ? limits::kMaxConnections : backlog;
    if (::listen(raw, static_cast<int>(effective_backlog)) != 0) {
      failure = Status{Code::internal, last_socket_error()};
      close_handle(static_cast<std::intptr_t>(raw));
      continue;
    }
    sockaddr_storage local{};
    socket_length_t local_length = static_cast<socket_length_t>(sizeof(local));
    std::uint16_t bound_port = port;
    if (::getsockname(raw, reinterpret_cast<sockaddr*>(&local), &local_length) == 0 && local.ss_family == AF_INET) {
      bound_port = ntohs(reinterpret_cast<const sockaddr_in*>(&local)->sin_port);
    }
    ::freeaddrinfo(results);
    TcpListener listener{};
    listener.handle_ = static_cast<std::intptr_t>(raw);
    listener.port_ = bound_port;
    return listener;
  }
  ::freeaddrinfo(results);
  return failure;
}

bool TcpListener::open() const noexcept { return handle_ != kInvalidHandle; }

std::uint16_t TcpListener::port() const noexcept { return port_; }

Result<TcpTransport> TcpListener::accept(std::uint32_t timeout_ms) {
  if (!open()) {
    return Status{Code::internal, "listener is closed"};
  }
  fd_set read_set;
  FD_ZERO(&read_set);
  FD_SET(static_cast<socket_handle_t>(handle_), &read_set);
  timeval timeout{};
  timeout.tv_sec = static_cast<long>(timeout_ms / 1000u);
  timeout.tv_usec = static_cast<long>((timeout_ms % 1000u) * 1000u);
#ifdef _WIN32
  const int ready = ::select(0, &read_set, nullptr, nullptr, &timeout);
#else
  const int ready = ::select(static_cast<int>(handle_) + 1, &read_set, nullptr, nullptr, &timeout);
#endif
  if (ready <= 0) {
    return Status{Code::not_found, "no pending connection"};
  }
  sockaddr_storage remote{};
  socket_length_t remote_length = static_cast<socket_length_t>(sizeof(remote));
  const auto raw = ::accept(static_cast<socket_handle_t>(handle_), reinterpret_cast<sockaddr*>(&remote), &remote_length);
  if (raw == static_cast<socket_handle_t>(kInvalidHandle)) {
    return Status{Code::internal, last_socket_error()};
  }
  return TcpTransport::adopt(static_cast<std::intptr_t>(raw), describe_peer(remote));
}

void TcpListener::close() noexcept {
  close_handle(handle_);
  handle_ = kInvalidHandle;
}

}  // namespace qf
