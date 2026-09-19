// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
//
// End-to-end queue lifecycle through the fabric: declaration, validation,
// activation, drain with evidence, quiescence, retirement.
#include <string>

#include "qf/fabric.hpp"
#include "support/fixture.hpp"
#include "support/test_framework.hpp"

using namespace qf;
using qftest::Fixture;

namespace {

/// Walks a queue to quiescence with real evidence: a live backend incarnation,
/// a verified acknowledgement, a drain request and a fresh zero observation.
void walk_to_quiescence(Fixture& fixture, QueueId queue, BackendId& backend, Incarnation& incarnation) {
  auto registered = fixture.fabric->register_backend(fixture.resource, Incarnation::from_value(3), fixture.owner,
                                                     fixture.fabric->epoch(), fixture.provenance());
  QF_REQUIRE(registered.ok());
  backend = registered.value();
  incarnation = Incarnation::from_value(3);

  const Generation generation = fixture.generation_of(queue);
  BackendApplied applied{};
  applied.known = true;
  applied.backend = backend;
  applied.backend_incarnation = incarnation;
  applied.epoch = fixture.fabric->epoch();
  applied.sequence = Sequence::from_value(1);
  applied.applied_at = fixture.clock.now_nanos();
  applied.applied_class = ClassBinding{fixture.class_id, fixture.class_generation};
  applied.queue_generation = generation;
  applied.applied_high_watermark_bytes = 4096;
  applied.applied_max_occupancy_bytes = 8192;
  auto ack = fixture.fabric->acknowledge_backend(fixture.context(), queue, generation, applied);
  QF_REQUIRE(ack.ok());

  auto drained = fixture.fabric->request_drain(fixture.context(), queue, generation);
  QF_REQUIRE(drained.ok());
  QF_CHECK_EQ(std::string(drained.value().rule), std::string("drain-requested-not-drained"));

  auto zero = fixture.fabric->ingest_occupancy(fixture.context(), fixture.sample(queue, 0, 0));
  QF_REQUIRE(zero.ok());

  DrainEvidence evidence{};
  evidence.records = 2;
  evidence.backend.request_accepted = true;
  evidence.backend.reported_drained = true;
  evidence.backend.backend = backend;
  evidence.backend.backend_incarnation = incarnation;
  evidence.backend.epoch = fixture.fabric->epoch();
  evidence.backend.sequence = Sequence::from_value(2);
  evidence.backend.observed_at = fixture.clock.now_nanos();
  evidence.occupancy_zero_observed = true;
  evidence.zero_observed_at = fixture.clock.now_nanos();
  evidence.zero_sequence = Sequence::from_value(1);
  auto submitted = fixture.fabric->submit_drain_evidence(fixture.context(), queue, generation, evidence);
  QF_REQUIRE(submitted.ok());

  auto quiesced = fixture.fabric->quiesce_queue(fixture.context(), queue, generation);
  QF_REQUIRE(quiesced.ok());
  QF_CHECK(quiesced.value().lifecycle == Lifecycle::quiesced);
}

}  // namespace

QF_TEST(lifecycle, full_walk_to_retirement) {
  Fixture fixture{};
  const QueueId queue = fixture.declare_queue("q-full");
  QF_CHECK(fixture.generation_of(queue).value() == 1);

  auto record = fixture.fabric->queue_record(queue);
  QF_REQUIRE(record.ok());
  QF_CHECK(record.value().lifecycle == Lifecycle::declared);

  // Activation before validation is refused: the table has no such edge.
  auto premature = fixture.fabric->activate_queue(fixture.context(), queue, fixture.generation_of(queue));
  QF_CHECK_CODE(premature, Code::lifecycle_violation);

  auto validated = fixture.fabric->validate_queue(fixture.context(), queue, fixture.generation_of(queue));
  QF_REQUIRE(validated.ok());
  QF_CHECK(validated.value().lifecycle == Lifecycle::validated);

  auto activated = fixture.fabric->activate_queue(fixture.context(), queue, fixture.generation_of(queue));
  QF_REQUIRE(activated.ok());
  QF_CHECK(activated.value().lifecycle == Lifecycle::active);

  // A queue cannot jump straight to quiescence, and an active queue cannot be
  // retired without draining first.
  auto skipped_drain = fixture.fabric->quiesce_queue(fixture.context(), queue, fixture.generation_of(queue));
  QF_CHECK_CODE(skipped_drain, Code::lifecycle_violation);
  auto premature_retire = fixture.fabric->retire_queue(fixture.context(), queue, fixture.generation_of(queue));
  QF_CHECK_CODE(premature_retire, Code::lifecycle_violation);

  BackendId backend{};
  Incarnation incarnation{};
  walk_to_quiescence(fixture, queue, backend, incarnation);

  auto retired = fixture.fabric->retire_queue(fixture.context(), queue, fixture.generation_of(queue));
  QF_REQUIRE(retired.ok());
  QF_CHECK(retired.value().lifecycle == Lifecycle::retired);

  // Retirement is terminal: no fresh mutation is ever accepted again.
  auto after_retire = fixture.fabric->ingest_occupancy(fixture.context(), fixture.sample(queue, 8));
  QF_CHECK_CODE(after_retire, Code::lifecycle_violation);
  auto reactivate = fixture.fabric->activate_queue(fixture.context(), queue, fixture.generation_of(queue));
  QF_CHECK_CODE(reactivate, Code::lifecycle_violation);

  record = fixture.fabric->queue_record(queue);
  QF_REQUIRE(record.ok());
  QF_CHECK(!record.value().occupancy.present);
  QF_CHECK(record.value().drain.phase == DrainPhase::idle);
}

QF_TEST(lifecycle, drain_requested_is_not_drained) {
  Fixture fixture{};
  const QueueId queue = fixture.activate_queue("q-drain");
  auto registered = fixture.fabric->register_backend(fixture.resource, Incarnation::from_value(1), fixture.owner,
                                                     fixture.fabric->epoch(), fixture.provenance());
  QF_REQUIRE(registered.ok());

  auto requested = fixture.fabric->request_drain(fixture.context(), queue, fixture.generation_of(queue));
  QF_REQUIRE(requested.ok());

  // Quiescence without evidence is refused, and the reason names the rule.
  auto quiesce = fixture.fabric->quiesce_queue(fixture.context(), queue, fixture.generation_of(queue));
  QF_CHECK_CODE(quiesce, Code::drain_incomplete);

  auto explanation = fixture.fabric->explain(queue);
  QF_REQUIRE(explanation.ok());
  QF_CHECK(explanation.value().drain.phase == DrainPhase::requested);
  QF_CHECK(!explanation.value().drain_assessment.completable);

  // A drain request may be cancelled by revalidation.
  auto cancelled = fixture.fabric->cancel_drain(fixture.context(), queue, fixture.generation_of(queue));
  QF_REQUIRE(cancelled.ok());
  QF_CHECK(cancelled.value().lifecycle == Lifecycle::active);
  auto record = fixture.fabric->queue_record(queue);
  QF_REQUIRE(record.ok());
  QF_CHECK(record.value().drain.phase == DrainPhase::idle);
}

QF_TEST(lifecycle, drain_completion_requires_fresh_zero_occupancy) {
  Fixture fixture{};
  const QueueId queue = fixture.activate_queue("q-evidence");
  auto registered = fixture.fabric->register_backend(fixture.resource, Incarnation::from_value(4), fixture.owner,
                                                     fixture.fabric->epoch(), fixture.provenance());
  QF_REQUIRE(registered.ok());
  const Generation generation = fixture.generation_of(queue);

  DrainEvidence evidence{};
  evidence.records = 1;
  evidence.backend.request_accepted = true;
  evidence.backend.reported_drained = true;
  evidence.backend.backend = registered.value();
  evidence.backend.backend_incarnation = Incarnation::from_value(4);
  evidence.backend.epoch = fixture.fabric->epoch();

  // Evidence submitted before any drain request is refused.
  auto early = fixture.fabric->submit_drain_evidence(fixture.context(), queue, generation, evidence);
  QF_CHECK_CODE(early, Code::drain_incomplete);

  QF_REQUIRE(fixture.fabric->request_drain(fixture.context(), queue, generation).ok());

  // Backend evidence naming an incarnation that is not live is refused.
  DrainEvidence stale_evidence = evidence;
  stale_evidence.backend.backend_incarnation = Incarnation::from_value(99);
  auto stale = fixture.fabric->submit_drain_evidence(fixture.context(), queue, generation, stale_evidence);
  QF_CHECK_CODE(stale, Code::stale_incarnation);

  QF_REQUIRE(fixture.fabric->submit_drain_evidence(fixture.context(), queue, generation, evidence).ok());
  auto quiesce = fixture.fabric->quiesce_queue(fixture.context(), queue, generation);
  QF_CHECK_CODE(quiesce, Code::drain_incomplete);

  // Occupancy observed above zero keeps the drain incomplete.
  QF_REQUIRE(fixture.fabric->ingest_occupancy(fixture.context(), fixture.sample(queue, 128)).ok());
  quiesce = fixture.fabric->quiesce_queue(fixture.context(), queue, generation);
  QF_CHECK_CODE(quiesce, Code::drain_incomplete);

  // A fresh zero observation, a verified acknowledgement and complete backend
  // evidence are all required before quiescence is granted.
  QF_REQUIRE(fixture.fabric->ingest_occupancy(fixture.context(), fixture.sample(queue, 0, 0)).ok());
  quiesce = fixture.fabric->quiesce_queue(fixture.context(), queue, generation);
  QF_CHECK_CODE(quiesce, Code::backend_mismatch);

  BackendApplied applied{};
  applied.known = true;
  applied.backend = registered.value();
  applied.backend_incarnation = Incarnation::from_value(4);
  applied.epoch = fixture.fabric->epoch();
  applied.applied_class = ClassBinding{fixture.class_id, fixture.class_generation};
  applied.queue_generation = generation;
  QF_REQUIRE(fixture.fabric->acknowledge_backend(fixture.context(), queue, generation, applied).ok());

  auto completed = fixture.fabric->quiesce_queue(fixture.context(), queue, generation);
  QF_REQUIRE(completed.ok());
  QF_CHECK(completed.value().lifecycle == Lifecycle::quiesced);
  auto explanation = fixture.fabric->explain(queue);
  QF_REQUIRE(explanation.ok());
  QF_CHECK(explanation.value().drain_assessment.completable);
  QF_CHECK(explanation.value().backend_verification.verified);
}

QF_TEST(lifecycle, quiesced_queue_can_be_reactivated_with_revalidation) {
  Fixture fixture{};
  const QueueId queue = fixture.activate_queue("q-reactivate");
  BackendId backend{};
  Incarnation incarnation{};
  walk_to_quiescence(fixture, queue, backend, incarnation);

  auto reactivated = fixture.fabric->activate_queue(fixture.context(), queue, fixture.generation_of(queue));
  QF_REQUIRE(reactivated.ok());
  QF_CHECK(reactivated.value().lifecycle == Lifecycle::active);

  // Re-activation does not resurrect the completed drain evidence.
  auto record = fixture.fabric->queue_record(queue);
  QF_REQUIRE(record.ok());
  QF_CHECK(record.value().drain.phase == DrainPhase::completed);
  auto explanation = fixture.fabric->explain(queue);
  QF_REQUIRE(explanation.ok());
  QF_CHECK(explanation.value().lifecycle == Lifecycle::active);
}

QF_TEST(lifecycle, failed_queue_accepts_only_cleanup) {
  Fixture fixture{};
  const QueueId queue = fixture.activate_queue("q-failing");
  auto failed = fixture.fabric->fail_queue(fixture.context(), queue, fixture.generation_of(queue), "hardware fault");
  QF_REQUIRE(failed.ok());
  QF_CHECK(failed.value().lifecycle == Lifecycle::failed);

  auto explanation = fixture.fabric->explain(queue);
  QF_REQUIRE(explanation.ok());
  QF_CHECK_EQ(explanation.value().failure_reason, std::string("hardware fault"));

  auto mutation = fixture.fabric->ingest_occupancy(fixture.context(), fixture.sample(queue, 4));
  QF_CHECK_CODE(mutation, Code::lifecycle_violation);

  auto retired = fixture.fabric->retire_queue(fixture.context(), queue, fixture.generation_of(queue));
  QF_REQUIRE(retired.ok());
  QF_CHECK(retired.value().lifecycle == Lifecycle::retired);
}

QF_TEST(lifecycle, class_rebinding_advances_the_definition_generation) {
  Fixture fixture{};
  const QueueId queue = fixture.activate_queue("q-rebind");
  QF_REQUIRE(fixture.fabric->ingest_occupancy(fixture.context(), fixture.sample(queue, 64)).ok());
  const Generation before = fixture.generation_of(queue);

  // A second class generation exists after the class is redeclared.
  auto redeclared = fixture.fabric->declare_class(fixture.resource, "gold", 9, QosRef{}, fixture.owner,
                                                  fixture.fabric->epoch(), fixture.provenance());
  QF_REQUIRE(redeclared.ok());
  const auto resource = fixture.fabric->resource_record(fixture.resource);
  QF_REQUIRE(resource.ok());
  const Generation new_class_generation = resource.value().classes.front().generation;
  QF_CHECK(new_class_generation.value() == fixture.class_generation.value() + 1);

  // The old binding is no longer exact, so the queue cannot be validated again
  // until it is rebound.
  auto stale_rebind = fixture.fabric->rebind_class(fixture.context(), queue, before,
                                                   ClassBinding{fixture.class_id, fixture.class_generation});
  QF_CHECK_CODE(stale_rebind, Code::stale_generation);

  auto rebound = fixture.fabric->rebind_class(fixture.context(), queue, before,
                                              ClassBinding{fixture.class_id, new_class_generation});
  QF_REQUIRE(rebound.ok());
  QF_CHECK(rebound.value().generation.value() == before.value() + 1);

  // Generation-bound evidence does not survive the rebinding.
  const auto record = fixture.fabric->queue_record(queue);
  QF_REQUIRE(record.ok());
  QF_CHECK(record.value().occupancy.freshness == Freshness::stale_generation);
  QF_CHECK(record.value().stale());
  auto explanation = fixture.fabric->explain(queue);
  QF_REQUIRE(explanation.ok());
  QF_CHECK(!explanation.value().threshold_eval.admission_authorized);
  QF_CHECK(explanation.value().threshold_eval.band == ThresholdBand::unknown);
}

QF_TEST(lifecycle, thresholds_update_is_generation_bound) {
  Fixture fixture{};
  const QueueId queue = fixture.activate_queue("q-thresholds");
  const Generation before = fixture.generation_of(queue);

  Thresholds updated{};
  updated.low_watermark_bytes = 2048;
  updated.high_watermark_bytes = 6144;
  updated.max_occupancy_bytes = 16384;
  updated.max_packets = 2048;

  // An inconsistent ladder is refused outright.
  Thresholds broken = updated;
  broken.high_watermark_bytes = 1;
  auto inconsistent = fixture.fabric->set_thresholds(fixture.context(), queue, before, broken);
  QF_CHECK_CODE(inconsistent, Code::invalid_argument);

  auto applied = fixture.fabric->set_thresholds(fixture.context(), queue, before, updated);
  QF_REQUIRE(applied.ok());
  QF_CHECK(applied.value().generation.value() == before.value() + 1);

  // A stale generation is refused: the caller must re-read the queue.
  auto stale = fixture.fabric->set_thresholds(fixture.context(), queue, before, updated);
  QF_CHECK_CODE(stale, Code::stale_generation);

  auto explanation = fixture.fabric->explain(queue);
  QF_REQUIRE(explanation.ok());
  QF_CHECK_EQ(explanation.value().thresholds.max_occupancy_bytes, 16384ull);
}
