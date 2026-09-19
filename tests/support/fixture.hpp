// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
//
// Shared test fixture: a deterministic clock, an explicitly seeded publisher
// and a helper set for topology plus queue lifecycle walking.
#pragma once

#include <memory>
#include <string>

#include "qf/fabric.hpp"
#include "qf/journal.hpp"
#include "support/test_framework.hpp"

namespace qftest {

struct Fixture {
  qf::ManualClock clock{};
  std::unique_ptr<qf::FileJournal> file_journal{};
  std::unique_ptr<qf::MemoryJournal> memory_journal{};
  std::unique_ptr<qf::Fabric> fabric{};
  std::string journal_path{};

  qf::OwnerId owner{qf::OwnerId::from_value(0xAA01)};
  qf::OwnerId stranger{qf::OwnerId::from_value(0xBB02)};
  qf::PublisherId publisher{qf::PublisherId::from_value(0xCC03)};
  qf::NodeId node{qf::NodeId::from_value(0xDD04)};
  qf::BootId boot{0x1234, 0x5678};

  qf::ResourceId resource{};
  qf::ClassId class_id{};
  qf::Generation class_generation{};
  qf::PoolId pool_id{};
  qf::Generation pool_generation{};

  std::uint64_t sequence{0};
  std::uint64_t incarnation{1};

  /// Creates a fixture. When journal_path is empty the fabric is in-memory and
  /// non-durable unless config.durable was requested explicitly. Bootstrap
  /// declares the standard resource/class/pool topology unless suppressed, which
  /// is what a restart onto an existing journal requires.
  explicit Fixture(qf::FabricConfig config = {},
                   std::string path = {},
                   qf::IEventSink* sink = nullptr,
                   bool bootstrap = true);

  [[nodiscard]] qf::Provenance provenance();
  [[nodiscard]] qf::MutationContext context();

  /// Models a restarted publisher process: a fresh incarnation and boot
  /// identifier, exactly what a real restart produces.
  void new_incarnation();

  void bootstrap_topology(std::string_view resource_name = "fabric0");
  qf::QueueId declare_queue(std::string_view name,
                            std::uint64_t low = 1024,
                            std::uint64_t high = 4096,
                            std::uint64_t maximum = 8192,
                            std::uint64_t max_packets = 4096);

  /// Declares, validates and activates a queue, returning its identity.
  qf::QueueId activate_queue(std::string_view name, std::uint64_t maximum = 8192);

  [[nodiscard]] qf::Generation generation_of(qf::QueueId queue) const;
  [[nodiscard]] qf::OccupancySample sample(qf::QueueId queue, std::uint64_t bytes, std::uint64_t packets = 0);
};

/// Creates a clean scratch directory for durable-state tests.
[[nodiscard]] std::string scratch_directory(const std::string& name);

/// Returns the code of a failed result, or Code::ok when it succeeded.
template <class T>
[[nodiscard]] qf::Code failure_code(const qf::Result<T>& result) {
  return result.ok() ? qf::Code::ok : result.status().code();
}

}  // namespace qftest
