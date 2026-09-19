// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
//
// Foundation units: identities, checked arithmetic, hashing, byte codec,
// provenance tracking, lifecycle table, thresholds, occupancy freshness,
// authority, drain assessment, framing and protocol codecs.
#include <string>
#include <vector>

#include "qf/authority.hpp"
#include "qf/bytes.hpp"
#include "qf/checked.hpp"
#include "qf/drain.hpp"
#include "qf/frame.hpp"
#include "qf/hash.hpp"
#include "qf/ids.hpp"
#include "qf/lifecycle.hpp"
#include "qf/occupancy.hpp"
#include "qf/protocol.hpp"
#include "qf/serialize.hpp"
#include "qf/threshold.hpp"
#include "support/fixture.hpp"
#include "support/test_framework.hpp"

using namespace qf;

QF_TEST(identity, strong_types_and_parsing) {
  const QueueId queue = QueueId::from_value(0x1234);
  const ResourceId resource = ResourceId::from_value(0x1234);
  QF_CHECK(queue.valid());
  QF_CHECK_EQ(queue.to_string(), std::string("0x0000000000001234"));
  QF_CHECK(queue.value() == resource.value());

  auto parsed = QueueId::parse("0x00000000000000ff");
  QF_REQUIRE(parsed.ok());
  QF_CHECK_EQ(parsed.value().value(), 0xffull);
  QF_CHECK(!QueueId::parse("not-hex").ok());
  QF_CHECK(!QueueId::parse("0x00000000000000ff00").ok());
  QF_CHECK(!QueueId().valid());

  const AttemptId attempt{PublisherId::from_value(9), 42};
  auto round_trip = AttemptId::parse(attempt.to_string());
  QF_REQUIRE(round_trip.ok());
  QF_CHECK(round_trip.value() == attempt);

  const BootId boot{0xAB, 0xCD};
  auto boot_round_trip = BootId::parse(boot.to_string());
  QF_REQUIRE(boot_round_trip.ok());
  QF_CHECK(boot_round_trip.value() == boot);
  QF_CHECK(!BootId::parse("0xAB").ok());
}

QF_TEST(identity, counters_fail_closed_at_the_ceiling) {
  const Generation top = Generation::from_value(0xFFFFFFFFFFFFFFFFull);
  QF_CHECK(!top.next().ok());
  QF_CHECK_CODE(top.next(), Code::capacity_exceeded);
  const Generation one = Generation::from_value(1);
  auto next = one.next();
  QF_REQUIRE(next.ok());
  QF_CHECK_EQ(next.value().value(), 2ull);
  QF_CHECK(one < next.value());
}

QF_TEST(checked_arithmetic, detects_overflow) {
  std::uint64_t out = 0;
  QF_CHECK(!add_overflow<std::uint64_t>(1, 2, out));
  QF_CHECK_EQ(out, 3ull);
  QF_CHECK(add_overflow<std::uint64_t>(0xFFFFFFFFFFFFFFFFull, 1, out));
  QF_CHECK(sub_underflow<std::uint64_t>(1, 2, out));
  QF_CHECK(mul_overflow<std::uint64_t>(0xFFFFFFFFFFFFFFFFull, 2, out));
  QF_CHECK(!mul_overflow<std::uint64_t>(0xFFFFFFFFull, 0xFFFFFFFFull, out));
  QF_CHECK_EQ(out, 0xFFFFFFFE00000001ull);
  QF_CHECK(!mul_overflow<std::uint64_t>(0, 0xFFFFFFFFFFFFFFFFull, out));
  QF_CHECK_EQ(out, 0ull);
  QF_CHECK_EQ(saturating_add<std::uint64_t>(0xFFFFFFFFFFFFFFFFull, 5), 0xFFFFFFFFFFFFFFFFull);
  QF_CHECK_EQ(saturating_sub<std::uint64_t>(5, 9), 0ull);
  std::int64_t signed_out = 0;
  QF_CHECK(add_overflow_i64(INT64_MAX, 1, signed_out));
  QF_CHECK_EQ(saturating_add_i64(INT64_MAX, 1), INT64_MAX);
  std::uint16_t narrowed = 0;
  QF_CHECK(!narrow_cast<std::uint16_t>(70000u, narrowed));
  QF_CHECK(narrow_cast<std::uint16_t>(65535u, narrowed));
  QF_CHECK_EQ(narrowed, 65535);
}

QF_TEST(hash, crc32c_known_vectors_and_fingerprints) {
  // CRC-32C of "123456789" is 0xE3069283.
  QF_CHECK_EQ(crc32c(std::string_view{"123456789"}), 0xE3069283u);
  QF_CHECK_EQ(crc32c(std::string_view{""}), 0u);
  const std::string_view text = "queue-fabric";
  QF_CHECK_EQ(crc32c(text), crc32c(text.data(), text.size()));
  std::uint64_t seed_a = 1;
  std::uint64_t seed_b = 2;
  hash_mix(seed_a, std::string_view{"alpha"});
  hash_mix(seed_b, std::string_view{"beta"});
  QF_CHECK(seed_a != seed_b);
}

QF_TEST(bytes, writer_reader_round_trip_and_bounds) {
  ByteWriter writer(256);
  writer.put_u8(0xAB);
  writer.put_u16(0xBEEF);
  writer.put_u32(0xDEADBEEF);
  writer.put_u64(0x0123456789ABCDEFull);
  writer.put_i64(-42);
  writer.put_string("queue");
  writer.put_bool(true);
  QF_CHECK(!writer.failed());
  const Bytes encoded = writer.take();

  ByteReader reader(encoded);
  QF_CHECK_EQ(reader.get_u8(), 0xAB);
  QF_CHECK_EQ(reader.get_u16(), 0xBEEFu);
  QF_CHECK_EQ(reader.get_u32(), 0xDEADBEEFu);
  QF_CHECK_EQ(reader.get_u64(), 0x0123456789ABCDEFull);
  QF_CHECK_EQ(reader.get_i64(), -42);
  QF_CHECK_EQ(reader.get_string(), std::string("queue"));
  QF_CHECK(reader.get_bool());
  QF_CHECK(!reader.failed());
  QF_CHECK_EQ(reader.remaining(), 0ull);

  // Truncation latches a failure and never reads past the end.
  ByteReader truncated(std::span<const std::byte>(encoded.data(), 3));
  (void)truncated.get_u64();
  QF_CHECK(truncated.failed());

  ByteWriter small(4);
  small.put_u32(1);
  small.put_u32(2);
  QF_CHECK(small.failed());

  // Strings longer than the configured bound are refused rather than truncated
  // silently.
  ByteWriter long_writer(1024);
  long_writer.put_string(std::string(600, 'x'));
  QF_CHECK(long_writer.failed());
}

QF_TEST(provenance, tracker_classifies_replay_and_staleness) {
  ProvenanceTracker tracker{4};
  Provenance base{};
  base.publisher = PublisherId::from_value(1);
  base.node = NodeId::from_value(1);
  base.incarnation = Incarnation::from_value(2);
  base.epoch = Epoch::from_value(3);
  base.sequence = Sequence::from_value(10);
  base.boot = BootId{7, 7};

  QF_CHECK(tracker.classify(base) == ProvenanceClass::accepted);
  QF_REQUIRE(tracker.observe(base).ok());
  QF_CHECK(tracker.classify(base) == ProvenanceClass::duplicate);

  Provenance newer = base;
  newer.sequence = Sequence::from_value(11);
  QF_CHECK(tracker.classify(newer) == ProvenanceClass::accepted);
  QF_REQUIRE(tracker.observe(newer).ok());
  QF_CHECK(tracker.observe(base).code() == Code::replay_detected);

  Provenance older_incarnation = base;
  older_incarnation.incarnation = Incarnation::from_value(1);
  older_incarnation.sequence = Sequence::from_value(99);
  QF_CHECK(tracker.classify(older_incarnation) == ProvenanceClass::stale_incarnation);

  Provenance foreign_boot = base;
  foreign_boot.sequence = Sequence::from_value(12);
  foreign_boot.boot = BootId{9, 9};
  QF_CHECK(tracker.classify(foreign_boot) == ProvenanceClass::stale_boot);

  Provenance lagging_epoch = base;
  lagging_epoch.sequence = Sequence::from_value(12);
  lagging_epoch.epoch = Epoch::from_value(2);
  QF_CHECK(tracker.classify(lagging_epoch) == ProvenanceClass::stale_epoch);

  Provenance fresh_incarnation = base;
  fresh_incarnation.incarnation = Incarnation::from_value(3);
  fresh_incarnation.sequence = Sequence::from_value(1);
  QF_CHECK(tracker.classify(fresh_incarnation) == ProvenanceClass::accepted);
  QF_REQUIRE(tracker.observe(fresh_incarnation).ok());

  Provenance unknown{};
  QF_CHECK(tracker.classify(unknown) == ProvenanceClass::invalid);

  // A full publisher table refuses unknown publishers instead of growing.
  ProvenanceTracker bounded{1};
  QF_REQUIRE(bounded.observe(base).ok());
  Provenance extra{};
  extra.publisher = PublisherId::from_value(77);
  extra.node = NodeId::from_value(1);
  extra.incarnation = Incarnation::from_value(1);
  extra.epoch = Epoch::from_value(1);
  extra.sequence = Sequence::from_value(1);
  extra.boot = BootId{1, 1};
  QF_CHECK(bounded.classify(extra) == ProvenanceClass::capacity_exceeded);
  QF_CHECK(bounded.observe(extra).code() == Code::capacity_exceeded);
  QF_CHECK_EQ(bounded.publisher_count(), 1ull);
}

QF_TEST(lifecycle, transition_table_is_closed) {
  QF_CHECK(check_transition(Lifecycle::declared, Lifecycle::validated).allowed);
  QF_CHECK(check_transition(Lifecycle::validated, Lifecycle::active).allowed);
  QF_CHECK(check_transition(Lifecycle::active, Lifecycle::draining).allowed);
  QF_CHECK(check_transition(Lifecycle::draining, Lifecycle::quiesced).allowed);
  QF_CHECK(check_transition(Lifecycle::quiesced, Lifecycle::retired).allowed);
  QF_CHECK(check_transition(Lifecycle::quiesced, Lifecycle::active).allowed);
  QF_CHECK(check_transition(Lifecycle::failed, Lifecycle::retired).allowed);

  QF_CHECK(!check_transition(Lifecycle::declared, Lifecycle::active).allowed);
  QF_CHECK(!check_transition(Lifecycle::validated, Lifecycle::quiesced).allowed);
  QF_CHECK(!check_transition(Lifecycle::active, Lifecycle::quiesced).allowed);
  QF_CHECK(!check_transition(Lifecycle::retired, Lifecycle::active).allowed);
  QF_CHECK(!check_transition(Lifecycle::retired, Lifecycle::validated).allowed);
  QF_CHECK_EQ(std::string(check_transition(Lifecycle::retired, Lifecycle::active).rule),
              std::string("retired-is-terminal"));

  QF_CHECK(is_terminal(Lifecycle::retired));
  QF_CHECK(!is_terminal(Lifecycle::failed));
  QF_CHECK(accepts_mutation(Lifecycle::active, Applicability::none));
  QF_CHECK(!accepts_mutation(Lifecycle::active, Applicability::fenced));
  QF_CHECK(!accepts_mutation(Lifecycle::retired, Applicability::none));
  QF_CHECK(!accepts_mutation(Lifecycle::failed, Applicability::none));

  Applicability flags = Applicability::none;
  flags = flags | Applicability::stale;
  QF_CHECK(has_flag(flags, Applicability::stale));
  QF_CHECK(!has_flag(flags, Applicability::fenced));
  flags = clear_flag(flags | Applicability::fenced, Applicability::fenced);
  QF_CHECK(has_flag(flags, Applicability::stale));
  QF_CHECK(!has_flag(flags, Applicability::fenced));
  QF_CHECK_EQ(to_string(flags), std::string("stale"));
}

QF_TEST(thresholds, bands_and_stale_evidence) {
  Thresholds thresholds{};
  thresholds.low_watermark_bytes = 100;
  thresholds.high_watermark_bytes = 500;
  thresholds.max_occupancy_bytes = 1000;
  thresholds.max_packets = 50;
  QF_CHECK(thresholds.consistent());

  auto evaluation = evaluate_thresholds(thresholds, 50, 1, true);
  QF_CHECK(evaluation.band == ThresholdBand::below_low);
  QF_CHECK(evaluation.admission_authorized);
  QF_CHECK_EQ(evaluation.headroom_bytes, 950ull);

  evaluation = evaluate_thresholds(thresholds, 300, 1, true);
  QF_CHECK(evaluation.band == ThresholdBand::nominal);

  evaluation = evaluate_thresholds(thresholds, 700, 1, true);
  QF_CHECK(evaluation.band == ThresholdBand::above_high);
  QF_CHECK(evaluation.admission_authorized);

  evaluation = evaluate_thresholds(thresholds, 1000, 1, true);
  QF_CHECK(evaluation.band == ThresholdBand::at_limit);
  QF_CHECK(!evaluation.admission_authorized);
  QF_CHECK_EQ(evaluation.headroom_bytes, 0ull);

  evaluation = evaluate_thresholds(thresholds, 1001, 1, true);
  QF_CHECK(evaluation.band == ThresholdBand::exceeded);
  QF_CHECK(!evaluation.admission_authorized);
  QF_CHECK_EQ(std::string(evaluation.rule), std::string("observed-bytes-above-configured-maximum"));

  evaluation = evaluate_thresholds(thresholds, 10, 51, true);
  QF_CHECK(evaluation.band == ThresholdBand::exceeded);
  QF_CHECK_EQ(std::string(evaluation.rule), std::string("observed-packets-above-configured-maximum"));

  // Missing or stale evidence is never positive authority.
  evaluation = evaluate_thresholds(thresholds, 10, 1, false);
  QF_CHECK(evaluation.band == ThresholdBand::unknown);
  QF_CHECK(!evaluation.admission_authorized);
  QF_CHECK_CODE(Status{evaluation.code}, Code::stale_occupancy);

  Thresholds inconsistent{};
  inconsistent.low_watermark_bytes = 500;
  inconsistent.high_watermark_bytes = 100;
  QF_CHECK(!inconsistent.consistent());
}

QF_TEST(occupancy, freshness_rules) {
  OccupancyPolicy policy{};
  policy.max_age = 1000;
  policy.max_future_skew = 10;

  OccupancyState state{};
  QF_CHECK(classify_freshness(state, 0, policy, Generation::from_value(1)) == Freshness::unknown);
  QF_CHECK(!occupancy_authoritative(state));

  state.present = true;
  state.queue_generation = Generation::from_value(1);
  state.observed_at = 500;
  QF_CHECK(classify_freshness(state, 1000, policy, Generation::from_value(1)) == Freshness::fresh);
  state.freshness = Freshness::fresh;
  QF_CHECK(occupancy_authoritative(state));

  QF_CHECK(classify_freshness(state, 1501, policy, Generation::from_value(1)) == Freshness::stale_age);
  QF_CHECK(classify_freshness(state, 1000, policy, Generation::from_value(2)) == Freshness::stale_generation);

  OccupancyState future = state;
  future.observed_at = 2000;
  QF_CHECK(classify_freshness(future, 1000, policy, Generation::from_value(1)) == Freshness::future_timestamp);
  future.observed_at = 1005;
  QF_CHECK(classify_freshness(future, 1000, policy, Generation::from_value(1)) == Freshness::fresh);

  OccupancyState restarted = state;
  restarted.freshness = Freshness::stale_restart;
  QF_CHECK(classify_freshness(restarted, 1000, policy, Generation::from_value(1)) == Freshness::stale_restart);
  invalidate_occupancy(restarted, Freshness::stale_epoch);
  QF_CHECK(restarted.freshness == Freshness::stale_epoch);
  QF_CHECK_EQ(occupancy_age(100, 40), 60);
  QF_CHECK_EQ(occupancy_age(40, 100), 0);
}

QF_TEST(authority, deterministic_evaluation_order) {
  AuthorityVector authority{};
  authority.resource = ResourceId::from_value(1);
  authority.owner = OwnerId::from_value(0xAA);
  authority.epoch = Epoch::from_value(5);
  authority.grants.push_back(AuthorityGrant{OwnerId::from_value(0xAA), static_cast<std::uint32_t>(Capability::all)});
  authority.grants.push_back(AuthorityGrant{OwnerId::from_value(0xBB), to_mask(Capability::ingest_occupancy)});

  auto decision = authorize(authority, OwnerId::from_value(0xBB), Capability::ingest_occupancy, Epoch::from_value(5),
                            Applicability::none, Lifecycle::active);
  QF_CHECK(decision.allowed);
  QF_CHECK_EQ(std::string(decision.rule), std::string("authorized"));

  decision = authorize(authority, OwnerId::from_value(0xBB), Capability::retire_queue, Epoch::from_value(5),
                       Applicability::none, Lifecycle::active);
  QF_CHECK(!decision.allowed);
  QF_CHECK_CODE(Status{decision.code}, Code::authority_denied);
  QF_CHECK_EQ(std::string(decision.rule), std::string("capability-not-granted"));

  decision = authorize(authority, OwnerId::from_value(0xAA), Capability::retire_queue, Epoch::from_value(4),
                       Applicability::none, Lifecycle::active);
  QF_CHECK(!decision.allowed);
  QF_CHECK_CODE(Status{decision.code}, Code::stale_epoch);

  decision = authorize(authority, OwnerId::from_value(0xAA), Capability::ingest_occupancy, Epoch::from_value(5),
                       Applicability::fenced, Lifecycle::active);
  QF_CHECK(!decision.allowed);
  QF_CHECK_CODE(Status{decision.code}, Code::fence_violation);

  decision = authorize(authority, OwnerId::from_value(0xAA), Capability::ingest_occupancy, Epoch::from_value(5),
                       Applicability::none, Lifecycle::retired);
  QF_CHECK(!decision.allowed);
  QF_CHECK_CODE(Status{decision.code}, Code::lifecycle_violation);

  decision = authorize(authority, OwnerId::from_value(0xAA), Capability::unfence_queue, Epoch::from_value(5),
                       Applicability::fenced, Lifecycle::active);
  QF_CHECK(decision.allowed);
}

QF_TEST(drain, completion_requires_complete_evidence) {
  OccupancyPolicy policy{};
  policy.max_age = 1000;
  BackendIncarnation live{};
  live.backend = BackendId::from_value(1);
  live.resource = ResourceId::from_value(1);
  live.incarnation = Incarnation::from_value(4);
  live.epoch = Epoch::from_value(2);
  live.alive = true;

  OccupancyState occupancy{};
  occupancy.present = true;
  occupancy.bytes = 0;
  occupancy.packets = 0;
  occupancy.observed_at = 200;
  occupancy.freshness = Freshness::fresh;
  occupancy.queue_generation = Generation::from_value(1);

  DrainState drain{};
  QF_CHECK(!assess_drain(drain, occupancy, &live, Epoch::from_value(2), policy, 300).completable);
  QF_CHECK_EQ(std::string(assess_drain(drain, occupancy, &live, Epoch::from_value(2), policy, 300).rule),
              std::string("no-drain-requested"));

  drain.phase = DrainPhase::requested;
  drain.requested_at = 100;
  drain.evidence.records = 1;
  auto assessment = assess_drain(drain, occupancy, &live, Epoch::from_value(2), policy, 300);
  QF_CHECK(!assessment.completable);
  QF_CHECK_EQ(std::string(assessment.rule), std::string("backend-did-not-accept-drain-request"));

  drain.evidence.backend.request_accepted = true;
  drain.evidence.backend.reported_drained = true;
  drain.evidence.backend.backend = live.backend;
  drain.evidence.backend.backend_incarnation = live.incarnation;
  drain.evidence.backend.epoch = Epoch::from_value(2);
  assessment = assess_drain(drain, occupancy, &live, Epoch::from_value(2), policy, 300);
  QF_CHECK(!assessment.completable);
  QF_CHECK_EQ(std::string(assessment.rule), std::string("no-zero-occupancy-observation"));

  drain.evidence.occupancy_zero_observed = true;
  drain.evidence.zero_observed_at = 200;
  assessment = assess_drain(drain, occupancy, &live, Epoch::from_value(2), policy, 300);
  QF_CHECK(assessment.completable);
  QF_CHECK_EQ(std::string(assessment.rule), std::string("drain-evidence-complete-and-verifiable"));

  // Occupancy that is not zero keeps the drain incomplete.
  OccupancyState busy = occupancy;
  busy.bytes = 64;
  assessment = assess_drain(drain, busy, &live, Epoch::from_value(2), policy, 300);
  QF_CHECK(!assessment.completable);
  QF_CHECK_EQ(std::string(assessment.rule), std::string("occupancy-is-not-zero"));

  // A stale zero-occupancy observation cannot justify completion either.
  OccupancyState stale = occupancy;
  stale.freshness = Freshness::stale_age;
  assessment = assess_drain(drain, stale, &live, Epoch::from_value(2), policy, 300);
  QF_CHECK(!assessment.completable);
  QF_CHECK_EQ(std::string(assessment.rule), std::string("occupancy-evidence-not-fresh"));

  // A superseded backend incarnation cannot report drain completion.
  BackendIncarnation gone = live;
  gone.incarnation = Incarnation::from_value(5);
  assessment = assess_drain(drain, occupancy, &gone, Epoch::from_value(2), policy, 300);
  QF_CHECK(!assessment.completable);
  QF_CHECK_EQ(std::string(assessment.rule), std::string("drain-evidence-backend-incarnation-superseded"));

  // Evidence collected before a restart is never live authority.
  drain.evidence.revalidated = false;
  assessment = assess_drain(drain, occupancy, &live, Epoch::from_value(2), policy, 300);
  QF_CHECK(!assessment.completable);
  QF_CHECK_EQ(std::string(assessment.rule), std::string("drain-evidence-requires-revalidation-after-restart"));
}

QF_TEST(frame, codec_rejects_malformed_streams) {
  Frame frame{};
  frame.type = 7;
  frame.sequence = 42;
  frame.payload = Bytes{std::byte{1}, std::byte{2}, std::byte{3}};
  const Bytes encoded = encode_frame(frame);
  QF_REQUIRE(!encoded.empty());

  FrameDecoder decoder{};
  QF_REQUIRE(decoder.push(std::span<const std::byte>(encoded.data(), 5)).ok());
  auto partial = decoder.next();
  QF_CHECK_CODE(partial, Code::not_found);
  QF_REQUIRE(decoder.push(std::span<const std::byte>(encoded.data() + 5, encoded.size() - 5)).ok());
  auto complete = decoder.next();
  QF_REQUIRE(complete.ok());
  QF_CHECK_EQ(complete.value().type, static_cast<std::uint16_t>(7));
  QF_CHECK_EQ(complete.value().sequence, 42u);
  QF_CHECK_EQ(complete.value().payload.size(), 3ull);
  QF_CHECK(decoder.next().status().code() == Code::not_found);
  QF_CHECK_EQ(decoder.frames_decoded(), 1ull);

  // Bit damage in the payload is refused.
  Bytes damaged = encoded;
  damaged.back() = static_cast<std::byte>(damaged.back() ^ std::byte{0xFF});
  FrameDecoder corrupt{};
  QF_REQUIRE(corrupt.push(damaged).ok());
  auto result = corrupt.next();
  QF_CHECK_CODE(result, Code::integrity_failure);
  QF_CHECK(corrupt.failed());

  // Bad magic is refused.
  Bytes wrong_magic = encoded;
  wrong_magic[0] = std::byte{0};
  FrameDecoder magic{};
  QF_REQUIRE(magic.push(wrong_magic).ok());
  QF_CHECK_CODE(magic.next(), Code::malformed_input);

  // An oversized declared length is refused before allocating.
  Bytes oversized = encoded;
  oversized[16] = std::byte{0xFF};
  oversized[17] = std::byte{0xFF};
  oversized[18] = std::byte{0xFF};
  oversized[19] = std::byte{0x7F};
  FrameDecoder huge{1024};
  QF_REQUIRE(huge.push(oversized).ok());
  QF_CHECK_CODE(huge.next(), Code::capacity_exceeded);

  // Truncated header and truncated payload stay incomplete, never guessed.
  FrameDecoder short_stream{};
  QF_REQUIRE(short_stream.push(std::span<const std::byte>(encoded.data(), 4)).ok());
  QF_CHECK_CODE(short_stream.next(), Code::not_found);
  QF_CHECK(!short_stream.failed());
}

QF_TEST(protocol, message_round_trip_and_hostile_input) {
  Message hello{};
  hello.type = MessageType::hello;
  hello.sequence = 3;
  hello.hello.role = Role::worker;
  hello.hello.node = NodeId::from_value(5);
  hello.hello.incarnation = Incarnation::from_value(2);
  hello.hello.boot = BootId{1, 2};
  hello.hello.epoch = Epoch::from_value(9);
  hello.hello.name = "w1";
  const Bytes encoded = encode_message(hello);
  QF_REQUIRE(!encoded.empty());
  auto decoded = decode_message(encoded);
  QF_REQUIRE(decoded.ok());
  QF_CHECK(decoded.value().type == MessageType::hello);
  QF_CHECK_EQ(decoded.value().hello.name, std::string("w1"));
  QF_CHECK(decoded.value().hello.boot == hello.hello.boot);

  // Trailing bytes are refused.
  Bytes extended = encoded;
  extended.push_back(std::byte{0});
  QF_CHECK_CODE(decode_message(extended), Code::malformed_input);

  // Truncation is refused.
  QF_CHECK(!decode_message(std::span<const std::byte>(encoded.data(), encoded.size() - 1)).ok());

  // Unknown message types are refused; a truncated header is refused first.
  Bytes unknown{std::byte{0x7F}, std::byte{0x00}, std::byte{0}, std::byte{0}, std::byte{0}, std::byte{0}};
  QF_CHECK_CODE(decode_message(unknown), Code::unsupported);
  Bytes truncated_header{std::byte{0x7F}, std::byte{0x00}};
  QF_CHECK_CODE(decode_message(truncated_header), Code::malformed_input);

  // Oversized occupancy batches are refused at decode time.
  ByteWriter writer(64);
  writer.put_u16(static_cast<std::uint16_t>(MessageType::occupancy_batch));
  writer.put_u32(0);
  writer.put_u32(static_cast<std::uint32_t>(limits::kMaxOccupancyBatch) + 1u);
  QF_CHECK_CODE(decode_message(writer.take()), Code::malformed_input);

  // A batch of real samples survives the round trip with exact provenance.
  Message batch{};
  batch.type = MessageType::occupancy_batch;
  batch.sequence = 1;
  OccupancySample sample{};
  sample.queue = QueueId::from_value(4);
  sample.queue_generation = Generation::from_value(1);
  sample.bytes = 1234;
  sample.packets = 12;
  sample.epoch = Epoch::from_value(2);
  sample.sequence = Sequence::from_value(8);
  sample.observed_at = 999;
  sample.provenance.publisher = PublisherId::from_value(1);
  sample.provenance.node = NodeId::from_value(2);
  sample.provenance.incarnation = Incarnation::from_value(3);
  sample.provenance.epoch = Epoch::from_value(2);
  sample.provenance.sequence = Sequence::from_value(8);
  sample.provenance.boot = BootId{4, 5};
  batch.occupancy.samples.push_back(sample);
  const Bytes batch_bytes = encode_message(batch);
  auto batch_decoded = decode_message(batch_bytes);
  QF_REQUIRE(batch_decoded.ok());
  QF_REQUIRE_EQ(batch_decoded.value().occupancy.samples.size(), 1ull);
  QF_CHECK_EQ(batch_decoded.value().occupancy.samples[0].bytes, 1234ull);
  QF_CHECK(batch_decoded.value().occupancy.samples[0].provenance.boot == sample.provenance.boot);
}

QF_TEST(serialization, mutation_and_state_records_round_trip) {
  MutationRequest request{};
  request.kind = MutationKind::declare_queue;
  request.attempt = AttemptId{PublisherId::from_value(3), 5};
  request.expected_epoch = Epoch::from_value(1);
  request.principal = OwnerId::from_value(9);
  request.provenance.publisher = PublisherId::from_value(3);
  request.provenance.node = NodeId::from_value(1);
  request.provenance.incarnation = Incarnation::from_value(1);
  request.provenance.epoch = Epoch::from_value(1);
  request.provenance.sequence = Sequence::from_value(5);
  request.provenance.boot = BootId{1, 1};
  DeclareQueuePayload payload{};
  payload.resource = ResourceId::from_value(2);
  payload.name = "q0";
  payload.depth = 32;
  payload.cls = ClassBinding{ClassId::from_value(1), Generation::from_value(2)};
  payload.pool = PoolRef{PoolId::from_value(1), Generation::from_value(3), 4096};
  payload.thresholds.max_occupancy_bytes = 8192;
  request.payload = payload;

  const Bytes encoded = encode_mutation(request);
  QF_REQUIRE(!encoded.empty());
  auto decoded = decode_mutation(encoded);
  QF_REQUIRE(decoded.ok());
  QF_CHECK(decoded.value().kind == MutationKind::declare_queue);
  QF_CHECK_EQ(decoded.value().attempt, request.attempt);
  QF_CHECK_EQ(std::get<DeclareQueuePayload>(decoded.value().payload).name, std::string("q0"));
  QF_CHECK_EQ(decoded.value().fingerprint(), request.fingerprint());

  // A tampered effect is never mistaken for the original: the fingerprint of
  // the decoded record changes, so idempotency checks reject it as a conflict.
  Bytes damaged = encoded;
  damaged.back() = static_cast<std::byte>(damaged.back() ^ std::byte{0x40});
  auto damaged_decoded = decode_mutation(damaged);
  QF_CHECK(!damaged_decoded.ok() || damaged_decoded.value().fingerprint() != request.fingerprint());

  // Truncated records are refused.
  QF_CHECK(!decode_mutation(std::span<const std::byte>(encoded.data(), encoded.size() - 1)).ok());

  const EpochState epoch_state{BootId{9, 9}, Epoch::from_value(4), Incarnation::from_value(2)};
  const Bytes epoch_bytes = encode_epoch_state(epoch_state);
  auto epoch_decoded = decode_epoch_state(epoch_bytes);
  QF_REQUIRE(epoch_decoded.ok());
  QF_CHECK(epoch_decoded.value().boot == epoch_state.boot);
  QF_CHECK_EQ(epoch_decoded.value().epoch.value(), 4ull);
  QF_CHECK(!decode_epoch_state(std::span<const std::byte>(epoch_bytes.data(), 4)).ok());
}
