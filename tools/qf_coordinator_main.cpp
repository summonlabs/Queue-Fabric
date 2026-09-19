// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
//
// Coordinator process. Used by the multiprocess proof surface and by operators.
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <string>
#include <system_error>

#include "qf/coordinator.hpp"
#include "qf/process.hpp"

namespace {

struct Options {
  std::string directory{"."};
  std::string ready_file{};
  std::string stop_file{};
  std::string host{"127.0.0.1"};
  std::uint16_t port{0};
  bool bootstrap{true};
  bool durable{true};
  std::int64_t heartbeat_deadline_ms{5000};
  std::uint64_t max_cycles{0};
  std::uint64_t compact_every{250};
  std::string name{"coordinator"};
};

bool parse(int argc, char** argv, Options& options) {
  for (int index = 1; index < argc; ++index) {
    const std::string argument = argv[index];
    bool consumed = false;
    const auto next = [&](std::string& target) {
      if (index + 1 < argc) {
        target = argv[++index];
        consumed = true;
      }
    };
    if (argument == "--dir") {
      next(options.directory);
    } else if (argument == "--ready-file") {
      next(options.ready_file);
    } else if (argument == "--stop-file") {
      next(options.stop_file);
    } else if (argument == "--host") {
      next(options.host);
    } else if (argument == "--port") {
      std::string value;
      next(value);
      if (consumed) {
        options.port = static_cast<std::uint16_t>(std::strtoul(value.c_str(), nullptr, 10));
      }
    } else if (argument == "--heartbeat-deadline-ms") {
      std::string value;
      next(value);
      if (consumed) {
        options.heartbeat_deadline_ms = std::strtoll(value.c_str(), nullptr, 10);
      }
    } else if (argument == "--max-cycles") {
      std::string value;
      next(value);
      if (consumed) {
        options.max_cycles = std::strtoull(value.c_str(), nullptr, 10);
      }
    } else if (argument == "--compact-every") {
      std::string value;
      next(value);
      if (consumed) {
        options.compact_every = std::strtoull(value.c_str(), nullptr, 10);
      }
    } else if (argument == "--name") {
      next(options.name);
    } else if (argument == "--no-bootstrap") {
      options.bootstrap = false;
    } else if (argument == "--no-durability") {
      options.durable = false;
    } else {
      std::fprintf(stderr, "unknown argument: %s\n", argument.c_str());
      return false;
    }
    (void)consumed;
  }
  return true;
}

std::string hex(std::uint64_t value) {
  static constexpr char kDigits[] = "0123456789abcdef";
  std::string out(18, '0');
  out[0] = '0';
  out[1] = 'x';
  for (int i = 0; i < 16; ++i) {
    out[2 + static_cast<std::size_t>(i)] = kDigits[(value >> (4 * (15 - i))) & 0xFu];
  }
  return out;
}

}  // namespace

int main(int argc, char** argv) {
  Options options{};
  if (!parse(argc, argv, options)) {
    return 2;
  }

  std::error_code error;
  std::filesystem::create_directories(options.directory, error);

  qf::SteadyClock clock{};
  qf::FabricConfig fabric_config{};
  fabric_config.durable = options.durable;
  fabric_config.policy.default_thresholds.low_watermark_bytes = 1024;
  fabric_config.policy.default_thresholds.high_watermark_bytes = 4096;
  fabric_config.policy.default_thresholds.max_occupancy_bytes = 8192;
  fabric_config.policy.default_thresholds.max_packets = 4096;
  fabric_config.policy.occupancy_stale_after = 2000000000ll;
  fabric_config.occupancy.max_age = 2000000000ll;

  qf::CoordinatorConfig config{};
  config.host = options.host;
  config.port = options.port;
  config.fabric = fabric_config;
  config.journal_path = options.durable ? (options.directory + "/fabric.qfjournal") : std::string{};
  config.sync_on_commit = true;
  config.heartbeat_deadline = options.heartbeat_deadline_ms * 1000000ll;

  qf::CoordinatorServer server{config, clock};
  const qf::Status started = server.start();
  if (!started.ok()) {
    std::fprintf(stderr, "start failed: %s\n", started.to_string().c_str());
    return 3;
  }
  qf::Fabric& fabric = server.fabric();

  const qf::OwnerId owner = qf::OwnerId::from_value(0x1000);
  qf::Provenance provenance{};
  provenance.publisher = qf::PublisherId::from_value(1);
  provenance.node = qf::NodeId::from_value(1);
  provenance.incarnation = qf::Incarnation::from_value(1);
  provenance.epoch = fabric.epoch();
  provenance.sequence = qf::Sequence::from_value(1);
  provenance.boot = fabric.boot();

  std::string ready;
  if (options.bootstrap) {
    // Bootstrap is idempotent: a restart onto restored durable state reuses the
    // existing topology instead of redeclaring it (which would advance class
    // and pool generations and invalidate every queue binding).
    qf::ResourceId resource{};
    const auto existing = fabric.find_resource("fabric0");
    if (existing.ok()) {
      resource = existing.value();
    } else {
      auto declared = fabric.declare_resource("fabric0", owner, provenance);
      if (!declared.ok()) {
        std::fprintf(stderr, "bootstrap resource failed: %s\n", declared.status().to_string().c_str());
        return 4;
      }
      resource = declared.value();
    }
    const auto current = fabric.resource_record(resource);
    if (!current.ok()) {
      std::fprintf(stderr, "bootstrap resource lookup failed: %s\n", current.status().to_string().c_str());
      return 5;
    }
    if (current.value().classes.empty()) {
      provenance.sequence = qf::Sequence::from_value(2);
      auto cls = fabric.declare_class(resource, "gold", 7, qf::QosRef{}, owner, fabric.epoch(), provenance);
      if (!cls.ok()) {
        std::fprintf(stderr, "bootstrap class failed: %s\n", cls.status().to_string().c_str());
        return 6;
      }
    }
    if (current.value().pools.empty()) {
      provenance.sequence = qf::Sequence::from_value(3);
      auto pool = fabric.declare_pool(resource, "pool0", 1u << 20, owner, fabric.epoch(), provenance);
      if (!pool.ok()) {
        std::fprintf(stderr, "bootstrap pool failed: %s\n", pool.status().to_string().c_str());
        return 7;
      }
    }
    const auto resource_record = fabric.resource_record(resource);
    const std::uint64_t class_id = resource_record.ok() && !resource_record.value().classes.empty()
                                       ? resource_record.value().classes[0].id.value()
                                       : 0;
    const std::uint64_t class_generation = resource_record.ok() && !resource_record.value().classes.empty()
                                               ? resource_record.value().classes[0].generation.value()
                                               : 0;
    const std::uint64_t pool_id = resource_record.ok() && !resource_record.value().pools.empty()
                                      ? resource_record.value().pools[0].id.value()
                                      : 0;
    const std::uint64_t pool_generation = resource_record.ok() && !resource_record.value().pools.empty()
                                              ? resource_record.value().pools[0].generation.value()
                                              : 0;
    ready += "port=" + std::to_string(server.port()) + "\n";
    ready += "epoch=" + hex(fabric.epoch().value()) + "\n";
    ready += "boot=" + fabric.boot().to_string() + "\n";
    ready += "resource=" + hex(resource.value()) + "\n";
    ready += "class=" + hex(class_id) + "\n";
    ready += "class_generation=" + hex(class_generation) + "\n";
    ready += "pool=" + hex(pool_id) + "\n";
    ready += "pool_generation=" + hex(pool_generation) + "\n";
    ready += "owner=" + hex(owner.value()) + "\n";
    ready += "journal=" + config.journal_path + "\n";
  } else {
    ready += "port=" + std::to_string(server.port()) + "\n";
    ready += "epoch=" + hex(fabric.epoch().value()) + "\n";
    ready += "boot=" + fabric.boot().to_string() + "\n";
    ready += "journal=" + config.journal_path + "\n";
  }
  if (!options.ready_file.empty()) {
    const qf::Status written = qf::write_text_file(options.ready_file, ready);
    if (!written.ok()) {
      std::fprintf(stderr, "ready file failed: %s\n", written.to_string().c_str());
      return 7;
    }
  }
  std::fputs(ready.c_str(), stdout);
  std::fflush(stdout);

  std::uint64_t cycles = 0;
  while (!server.stopped()) {
    const qf::Status cycle = server.run_once(20);
    if (!cycle.ok() && cycle.code() != qf::Code::shutting_down) {
      std::fprintf(stderr, "cycle failed: %s\n", cycle.to_string().c_str());
      break;
    }
    ++cycles;
    if (options.max_cycles != 0 && cycles >= options.max_cycles) {
      break;
    }
    // Bound journal growth and replay time with a periodic checkpoint. The
    // checkpoint refuses to run while a backend attempt is reserved.
    if (options.compact_every != 0 && cycles % options.compact_every == 0) {
      const qf::Status compacted = fabric.compact();
      if (!compacted.ok() && compacted.code() != qf::Code::busy) {
        std::fprintf(stderr, "checkpoint failed: %s\n", compacted.to_string().c_str());
      }
    }
    if (!options.stop_file.empty() && std::filesystem::exists(options.stop_file, error)) {
      break;
    }
  }

  const qf::CoordinatorStats final_stats = server.stats();
  const qf::RecoveryReport recovery = fabric.recovery_report();
  std::string summary;
  summary += "cycles=" + std::to_string(cycles) + "\n";
  summary += "epoch=" + hex(fabric.epoch().value()) + "\n";
  summary += "queues=" + std::to_string(final_stats.queues) + "\n";
  summary += "occupancy_accepted=" + std::to_string(final_stats.occupancy_accepted) + "\n";
  summary += "occupancy_rejected=" + std::to_string(final_stats.occupancy_rejected) + "\n";
  summary += "occupancy_overflow=" + std::to_string(final_stats.occupancy_overflow) + "\n";
  summary += "mutations_applied=" + std::to_string(final_stats.mutations_applied) + "\n";
  summary += "mutations_rejected=" + std::to_string(final_stats.mutations_rejected) + "\n";
  summary += "stale_epoch_rejections=" + std::to_string(final_stats.stale_epoch_rejections) + "\n";
  summary += "stale_incarnation_rejections=" + std::to_string(final_stats.stale_incarnation_rejections) + "\n";
  summary += "revalidations_required=" + std::to_string(final_stats.revalidations_required) + "\n";
  summary += "connections_accepted=" + std::to_string(final_stats.connections_accepted) + "\n";
  summary += "connections_lost=" + std::to_string(final_stats.connections_lost) + "\n";
  summary += "liveness_expirations=" + std::to_string(final_stats.liveness_expirations) + "\n";
  summary += "orphaned_begins=" + std::to_string(recovery.orphaned_begins) + "\n";
  summary += "records_usable=" + std::to_string(recovery.records_usable) + "\n";
  summary += "records_truncated=" + std::to_string(recovery.records_truncated) + "\n";
  summary += "queues_restored=" + std::to_string(recovery.queues_restored) + "\n";
  summary += "queues_revalidated=" + std::to_string(recovery.queues_revalidated) + "\n";
  summary += "previous_epoch=" + hex(recovery.previous_epoch.value()) + "\n";
  summary += "previous_boot=" + recovery.previous_boot.to_string() + "\n";
  summary += "current_boot=" + fabric.boot().to_string() + "\n";
  const qf::Status written = qf::write_text_file(options.directory + "/final.txt", summary);
  if (!written.ok()) {
    std::fprintf(stderr, "summary file failed: %s\n", written.to_string().c_str());
  }
  std::fputs(summary.c_str(), stdout);

  server.stop();
  fabric.shutdown();
  return 0;
}
