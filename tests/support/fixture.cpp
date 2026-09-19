// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
#include "support/fixture.hpp"

#include <filesystem>
#include <system_error>
#include <utility>

namespace qftest {

std::string scratch_directory(const std::string& name) {
  std::error_code error;
  const auto base = std::filesystem::temp_directory_path(error) / "qf-tests";
  std::filesystem::create_directories(base, error);
  const auto directory = base / name;
  std::filesystem::remove_all(directory, error);
  std::filesystem::create_directories(directory, error);
  return directory.string();
}

Fixture::Fixture(qf::FabricConfig config, std::string path, qf::IEventSink* sink, bool bootstrap)
    : journal_path(std::move(path)) {
  qf::IJournal* journal = nullptr;
  if (!journal_path.empty()) {
    qf::JournalConfig journal_config{};
    journal_config.path = journal_path;
    journal_config.sync_on_commit = true;
    file_journal = std::make_unique<qf::FileJournal>(journal_config);
    journal = file_journal.get();
    config.durable = true;
  }
  fabric = std::make_unique<qf::Fabric>(config, clock, journal, sink);
  const qf::Status recovered = fabric->recover();
  QF_REQUIRE(recovered.ok());
  if (bootstrap) {
    bootstrap_topology();
  } else {
    const auto resources = fabric->stats();
    QF_REQUIRE(resources.resources >= 1);
    const auto record = fabric->resource_record(qf::ResourceId::from_value(1));
    QF_REQUIRE(record.ok());
    resource = record.value().id;
    QF_REQUIRE(!record.value().classes.empty());
    QF_REQUIRE(!record.value().pools.empty());
    class_id = record.value().classes.front().id;
    class_generation = record.value().classes.front().generation;
    pool_id = record.value().pools.front().id;
    pool_generation = record.value().pools.front().generation;
  }
}

qf::Provenance Fixture::provenance() {
  ++sequence;
  qf::Provenance value{};
  value.publisher = publisher;
  value.node = node;
  value.incarnation = qf::Incarnation::from_value(incarnation);
  value.epoch = fabric->epoch();
  value.sequence = qf::Sequence::from_value(sequence);
  value.boot = boot;
  return value;
}

qf::MutationContext Fixture::context() {
  qf::MutationContext ctx{};
  ctx.principal = owner;
  ctx.provenance = provenance();
  ctx.epoch = fabric->epoch();
  return ctx;
}

void Fixture::new_incarnation() {
  ++incarnation;
  boot = qf::BootId{0x2000 + incarnation, 0x3000 + incarnation};
  sequence = 0;
}

void Fixture::bootstrap_topology(std::string_view resource_name) {
  auto declared = fabric->declare_resource(resource_name, owner, provenance());
  QF_REQUIRE(declared.ok());
  resource = declared.value();
  auto cls = fabric->declare_class(resource, "gold", 7, qf::QosRef{}, owner, fabric->epoch(), provenance());
  QF_REQUIRE(cls.ok());
  class_id = cls.value();
  auto pool = fabric->declare_pool(resource, "pool0", 1u << 20, owner, fabric->epoch(), provenance());
  QF_REQUIRE(pool.ok());
  pool_id = pool.value();
  const auto record = fabric->resource_record(resource);
  QF_REQUIRE(record.ok());
  QF_REQUIRE(!record.value().classes.empty());
  QF_REQUIRE(!record.value().pools.empty());
  class_generation = record.value().classes.front().generation;
  pool_generation = record.value().pools.front().generation;
}

qf::QueueId Fixture::declare_queue(std::string_view name,
                                   std::uint64_t low,
                                   std::uint64_t high,
                                   std::uint64_t maximum,
                                   std::uint64_t max_packets) {
  qf::Thresholds thresholds{};
  thresholds.low_watermark_bytes = low;
  thresholds.high_watermark_bytes = high;
  thresholds.max_occupancy_bytes = maximum;
  thresholds.max_packets = max_packets;
  auto outcome = fabric->declare_queue(context(), resource, std::string(name), 64,
                                       qf::ClassBinding{class_id, class_generation},
                                       qf::PoolRef{pool_id, pool_generation, 1u << 20}, thresholds);
  QF_REQUIRE(outcome.ok());
  return outcome.value().queue;
}

qf::QueueId Fixture::activate_queue(std::string_view name, std::uint64_t maximum) {
  const qf::QueueId queue = declare_queue(name, 1024, 4096, maximum);
  auto validated = fabric->validate_queue(context(), queue, generation_of(queue));
  QF_REQUIRE(validated.ok());
  auto activated = fabric->activate_queue(context(), queue, generation_of(queue));
  QF_REQUIRE(activated.ok());
  return queue;
}

qf::Generation Fixture::generation_of(qf::QueueId queue) const {
  const auto record = fabric->queue_record(queue);
  QF_REQUIRE(record.ok());
  return record.value().def.generation;
}

qf::OccupancySample Fixture::sample(qf::QueueId queue, std::uint64_t bytes, std::uint64_t packets) {
  qf::OccupancySample value{};
  value.queue = queue;
  value.queue_generation = generation_of(queue);
  value.bytes = bytes;
  value.packets = packets;
  value.epoch = fabric->epoch();
  value.sequence = qf::Sequence::from_value(sequence + 1);
  value.observed_at = clock.now_nanos();
  value.provenance = provenance();
  return value;
}

}  // namespace qftest
