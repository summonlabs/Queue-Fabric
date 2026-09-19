// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
//
// Durability: journalling, crash simulation, torn tails, integrity failures,
// compaction and restart semantics.
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "qf/fabric.hpp"
#include "qf/journal.hpp"
#include "qf/serialize.hpp"
#include "support/fixture.hpp"
#include "support/test_framework.hpp"

using namespace qf;
using qftest::Fixture;

namespace {

std::vector<std::byte> read_file(const std::string& path) {
  std::ifstream stream(path, std::ios::binary);
  QF_REQUIRE(stream.good());
  std::vector<std::byte> data;
  char byte = 0;
  while (stream.get(byte)) {
    data.push_back(static_cast<std::byte>(static_cast<unsigned char>(byte)));
  }
  return data;
}

void write_file(const std::string& path, const std::vector<std::byte>& data) {
  std::ofstream stream(path, std::ios::binary | std::ios::trunc);
  QF_REQUIRE(stream.good());
  stream.write(reinterpret_cast<const char*>(data.data()), static_cast<std::streamsize>(data.size()));
}

}  // namespace

QF_TEST(durability, durable_mutations_survive_restart) {
  const std::string directory = qftest::scratch_directory("restart");
  const std::string journal_path = directory + "/fabric.qfjournal";
  QueueId queue{};
  {
    FabricConfig config{};
    Fixture fixture{config, journal_path};
    queue = fixture.activate_queue("q-durable");
    QF_REQUIRE(fixture.fabric->ingest_occupancy(fixture.context(), fixture.sample(queue, 512)).ok());
    const auto stats = fixture.fabric->stats();
    QF_CHECK_EQ(stats.journal_records > 0, true);
  }

  FabricConfig config{};
  Fixture restarted{config, journal_path, nullptr, false};
  const RecoveryReport report = restarted.fabric->recovery_report();
  QF_CHECK(report.journal_present);
  QF_CHECK(report.header_valid);
  QF_CHECK_EQ(report.queues_restored, 1ull);
  QF_CHECK_EQ(report.status.code(), Code::ok);
  QF_CHECK(restarted.fabric->epoch().value() >= 2);
  QF_CHECK_EQ(report.previous_epoch.value() + 1, restarted.fabric->epoch().value());

  const auto record = restarted.fabric->queue_record(queue);
  QF_REQUIRE(record.ok());
  QF_CHECK(record.value().lifecycle == Lifecycle::active);
  QF_CHECK_EQ(record.value().def.name, std::string("q-durable"));
  QF_CHECK_EQ(record.value().def.generation.value(), 1ull);

  // Occupancy changes are not journalled: without a compacted snapshot the
  // restarted queue holds no observation at all, and an absent observation is
  // never authority. The compaction test covers the retained-but-stale path.
  QF_CHECK(!record.value().occupancy.present);
  QF_CHECK(record.value().occupancy.freshness == Freshness::unknown);
  auto explanation = restarted.fabric->explain(queue);
  QF_REQUIRE(explanation.ok());
  QF_CHECK(!explanation.value().threshold_eval.admission_authorized);
  QF_CHECK(explanation.value().threshold_eval.band == ThresholdBand::unknown);

  // The restored queue accepts new evidence under the new epoch.
  QF_REQUIRE(restarted.fabric->ingest_occupancy(restarted.context(), restarted.sample(queue, 256)).ok());
  explanation = restarted.fabric->explain(queue);
  QF_REQUIRE(explanation.ok());
  QF_CHECK(explanation.value().threshold_eval.admission_authorized);

  std::vector<std::string> violations;
  QF_CHECK_EQ(restarted.fabric->validate_invariants(violations), 0ull);
}

QF_TEST(durability, retired_queues_stay_retired_across_restart) {
  const std::string directory = qftest::scratch_directory("retired");
  const std::string journal_path = directory + "/fabric.qfjournal";
  QueueId queue{};
  {
    Fixture fixture{{}, journal_path};
    queue = fixture.declare_queue("q-retired");
    QF_REQUIRE(fixture.fabric->retire_queue(fixture.context(), queue, fixture.generation_of(queue)).ok());
  }
  Fixture restarted{{}, journal_path, nullptr, false};
  const auto record = restarted.fabric->queue_record(queue);
  QF_REQUIRE(record.ok());
  QF_CHECK(record.value().lifecycle == Lifecycle::retired);
  auto refused = restarted.fabric->ingest_occupancy(restarted.context(), restarted.sample(queue, 4));
  QF_CHECK_CODE(refused, Code::lifecycle_violation);
}

QF_TEST(durability, unfinished_attempt_is_reported_as_ambiguous) {
  const std::string directory = qftest::scratch_directory("orphan");
  const std::string journal_path = directory + "/fabric.qfjournal";
  QueueId queue{};
  Generation generation{};
  {
    Fixture fixture{{}, journal_path};
    queue = fixture.activate_queue("q-orphan");
    generation = fixture.generation_of(queue);
  }

  // Simulate a crash between the durable begin record and the durable commit.
  {
    JournalConfig journal_config{};
    journal_config.path = journal_path;
    journal_config.sync_on_commit = true;
    FileJournal journal{journal_config};
    QF_REQUIRE(journal.open().ok());
    MutationRequest request{};
    request.kind = MutationKind::retire_queue;
    request.queue = queue;
    request.expected_generation = generation;
    request.expected_epoch = Epoch::from_value(1);
    request.principal = OwnerId::from_value(0xAA01);
    request.provenance.publisher = PublisherId::from_value(0xCC03);
    request.provenance.node = NodeId::from_value(0xDD04);
    request.provenance.incarnation = Incarnation::from_value(1);
    request.provenance.epoch = Epoch::from_value(1);
    request.provenance.sequence = Sequence::from_value(999);
    request.provenance.boot = BootId{0x1234, 0x5678};
    request.attempt = AttemptId{request.provenance.publisher, 999};
    const Bytes encoded = encode_mutation(request);
    QF_REQUIRE(!encoded.empty());
    QF_REQUIRE(journal.append(JournalRecordType::begin_attempt, encoded).ok());
    QF_REQUIRE(journal.sync().ok());
  }

  Fixture restarted{{}, journal_path, nullptr, false};
  const RecoveryReport report = restarted.fabric->recovery_report();
  QF_CHECK_EQ(report.orphaned_begins, 1ull);
  QF_CHECK(report.notes.size() > 0);

  // The unfinished attempt never committed, so the queue is still active, and
  // its evidence requires revalidation because the outcome was ambiguous.
  const auto record = restarted.fabric->queue_record(queue);
  QF_REQUIRE(record.ok());
  QF_CHECK(record.value().lifecycle == Lifecycle::active);
  QF_CHECK(record.value().stale());
}

QF_TEST(durability, torn_tail_is_dropped_and_reported) {
  const std::string directory = qftest::scratch_directory("torn");
  const std::string journal_path = directory + "/fabric.qfjournal";
  QueueId first{};
  QueueId second{};
  {
    Fixture fixture{{}, journal_path};
    first = fixture.activate_queue("q-torn-a");
    second = fixture.activate_queue("q-torn-b");
  }
  const std::vector<std::byte> intact = read_file(journal_path);
  QF_REQUIRE(intact.size() > 40);

  // Truncate inside the final record: recovery keeps the valid prefix and
  // reports the torn tail instead of guessing.
  std::vector<std::byte> torn(intact.begin(), intact.end() - 5);
  write_file(journal_path, torn);
  {
    Fixture restarted{{}, journal_path, nullptr, false};
    const RecoveryReport report = restarted.fabric->recovery_report();
    QF_CHECK(report.status.ok());
    QF_CHECK(report.records_truncated >= 1);
    QF_CHECK(!report.notes.empty());
    // The torn record was the activation of the second queue: both declarations
    // are inside the valid prefix, but the torn mutation must have had no
    // effect at all.
    const auto first_record = restarted.fabric->queue_record(first);
    QF_REQUIRE(first_record.ok());
    QF_CHECK(first_record.value().lifecycle == Lifecycle::active);
    const auto second_record = restarted.fabric->queue_record(second);
    QF_REQUIRE(second_record.ok());
    QF_CHECK(second_record.value().lifecycle != Lifecycle::active);
  }
}

QF_TEST(durability, header_damage_is_refused) {
  const std::string directory = qftest::scratch_directory("header");
  const std::string journal_path = directory + "/fabric.qfjournal";
  {
    Fixture fixture{{}, journal_path};
    fixture.activate_queue("q-header");
  }
  std::vector<std::byte> data = read_file(journal_path);
  data[0] = std::byte{0x00};  // magic damage
  write_file(journal_path, data);

  ManualClock clock{};
  JournalConfig journal_config{};
  journal_config.path = journal_path;
  journal_config.sync_on_commit = true;
  FileJournal journal{journal_config};
  const Status opened = journal.open();
  QF_CHECK_CODE(opened, Code::integrity_failure);
  QF_CHECK(!journal.recovery().header_valid);

  // The fabric refuses to serve on a damaged store instead of starting empty.
  Fabric fabric{{}, clock, &journal, nullptr};
  const Status recovered = fabric.recover();
  QF_CHECK_CODE(recovered, Code::integrity_failure);
}

QF_TEST(durability, mid_file_corruption_is_distinguished_from_a_torn_tail) {
  const std::string directory = qftest::scratch_directory("corrupt");
  const std::string journal_path = directory + "/fabric.qfjournal";
  {
    Fixture fixture{{}, journal_path};
    fixture.activate_queue("q-corrupt");
    fixture.activate_queue("q-corrupt-2");
  }
  std::vector<std::byte> data = read_file(journal_path);
  // Flip a byte inside the first record payload while leaving later records
  // structurally valid: this is corruption, not a torn write.
  data[40 + 12] = static_cast<std::byte>(static_cast<unsigned char>(data[40 + 12]) ^ 0x5A);
  write_file(journal_path, data);

  FileJournal journal{JournalConfig{journal_path, true, limits::kMaxJournalFileBytes, limits::kMaxJournalRecordBytes}};
  const Status opened = journal.open();
  QF_CHECK_CODE(opened, Code::integrity_failure);
  QF_CHECK(journal.recovery().records_corrupt >= 1);
  QF_CHECK_EQ(journal.recovery().records_truncated, 0ull);
}

QF_TEST(durability, compaction_preserves_authoritative_state) {
  const std::string directory = qftest::scratch_directory("compact");
  const std::string journal_path = directory + "/fabric.qfjournal";
  QueueId active{};
  QueueId retired{};
  struct Expected {
    Generation generation{};
    std::uint64_t bytes{0};
    bool bytes_present{false};
  };
  Expected before{};
  {
    Fixture fixture{{}, journal_path};
    active = fixture.activate_queue("q-compact-a");
    retired = fixture.declare_queue("q-compact-b");
    QF_REQUIRE(fixture.fabric->retire_queue(fixture.context(), retired, fixture.generation_of(retired)).ok());
    QF_REQUIRE(fixture.fabric->ingest_occupancy(fixture.context(), fixture.sample(active, 777, 7)).ok());
    QF_REQUIRE(fixture.fabric->compact().ok());
    QF_CHECK_EQ(fixture.fabric->pending_plans().size(), 0ull);
    const auto record = fixture.fabric->queue_record(active);
    QF_REQUIRE(record.ok());
    before.generation = record.value().def.generation;
    before.bytes = record.value().occupancy.bytes;
    before.bytes_present = record.value().occupancy.present;
  }

  Fixture restarted{{}, journal_path, nullptr, false};
  const auto record = restarted.fabric->queue_record(active);
  QF_REQUIRE(record.ok());
  QF_CHECK_EQ(record.value().def.generation.value(), before.generation.value());
  QF_CHECK_EQ(record.value().occupancy.bytes, before.bytes);
  QF_CHECK_EQ(record.value().occupancy.present, before.bytes_present);
  QF_CHECK(record.value().occupancy.freshness == Freshness::stale_restart);
  const auto retired_record = restarted.fabric->queue_record(retired);
  QF_REQUIRE(retired_record.ok());
  QF_CHECK(retired_record.value().lifecycle == Lifecycle::retired);

  // After compaction the journal still accepts new work. The restarted process
  // publishes under a fresh incarnation, which is what a real restart does.
  restarted.new_incarnation();
  QF_REQUIRE(restarted.fabric->ingest_occupancy(restarted.context(), restarted.sample(active, 64)).ok());
  const auto stats = restarted.fabric->stats();
  QF_CHECK(stats.journal_records > 0);
}

QF_TEST(durability, failed_sync_never_acknowledges_a_mutation) {
  ManualClock clock{};
  MemoryJournal journal{};
  QF_REQUIRE(journal.open().ok());
  FabricConfig config{};
  config.durable = true;
  Fabric fabric{config, clock, &journal, nullptr};
  QF_REQUIRE(fabric.recover().ok());

  OwnerId owner = OwnerId::from_value(1);
  Provenance provenance{};
  provenance.publisher = PublisherId::from_value(1);
  provenance.node = NodeId::from_value(1);
  provenance.incarnation = Incarnation::from_value(1);
  provenance.epoch = fabric.epoch();
  provenance.sequence = Sequence::from_value(1);
  provenance.boot = BootId{1, 1};
  auto resource = fabric.declare_resource("r0", owner, provenance);
  QF_REQUIRE(resource.ok());

  journal.set_sync_failure(true);
  provenance.sequence = Sequence::from_value(2);
  auto cls = fabric.declare_class(resource.value(), "gold", 1, QosRef{}, owner, fabric.epoch(), provenance);
  QF_CHECK_CODE(cls, Code::not_durable);
  // Nothing was half-applied.
  const auto record = fabric.resource_record(resource.value());
  QF_REQUIRE(record.ok());
  QF_CHECK_EQ(record.value().classes.size(), 0ull);

  journal.set_sync_failure(false);
  provenance.sequence = Sequence::from_value(3);
  cls = fabric.declare_class(resource.value(), "gold", 1, QosRef{}, owner, fabric.epoch(), provenance);
  QF_REQUIRE(cls.ok());
}

QF_TEST(durability, durable_queue_mutation_requires_a_journal) {
  ManualClock clock{};
  FabricConfig config{};
  config.durable = true;
  Fabric fabric{config, clock, nullptr, nullptr};
  QF_REQUIRE(fabric.recover().ok());
  Provenance provenance{};
  provenance.publisher = PublisherId::from_value(1);
  provenance.node = NodeId::from_value(1);
  provenance.incarnation = Incarnation::from_value(1);
  provenance.epoch = fabric.epoch();
  provenance.sequence = Sequence::from_value(1);
  provenance.boot = BootId{1, 1};
  auto declared = fabric.declare_resource("r0", OwnerId::from_value(1), provenance);
  QF_CHECK_CODE(declared, Code::not_durable);
}

QF_TEST(durability, shutdown_stops_accepting_and_preserves_committed_state) {
  const std::string directory = qftest::scratch_directory("shutdown");
  const std::string journal_path = directory + "/fabric.qfjournal";
  QueueId queue{};
  {
    Fixture fixture{{}, journal_path};
    queue = fixture.activate_queue("q-shutdown");
    QF_REQUIRE(fixture.fabric->shutdown().ok());
    QF_CHECK(fixture.fabric->shutting_down());
    auto refused = fixture.fabric->ingest_occupancy(fixture.context(), fixture.sample(queue, 4));
    QF_CHECK_CODE(refused, Code::shutting_down);
    QF_REQUIRE(fixture.fabric->shutdown().ok());
  }
  Fixture restarted{{}, journal_path, nullptr, false};
  const auto record = restarted.fabric->queue_record(queue);
  QF_REQUIRE(record.ok());
  QF_CHECK(record.value().lifecycle == Lifecycle::active);
}
