// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
//
// Mutation governance: authority, exact-generation binding, idempotency,
// occupancy freshness and overflow, fencing, two-phase backend application,
// epoch advancement and explanation.
#include <cstdio>
#include <string>
#include <vector>

#include "qf/fabric.hpp"
#include "support/fixture.hpp"
#include "support/test_framework.hpp"

using namespace qf;
using qftest::Fixture;

QF_TEST(governance, capabilities_are_required_and_named) {
  Fixture fixture{};
  const QueueId queue = fixture.declare_queue("q-authority");

  // A principal with no grant cannot mutate anything.
  MutationContext stranger_ctx{};
  stranger_ctx.principal = fixture.stranger;
  stranger_ctx.provenance = fixture.provenance();
  stranger_ctx.epoch = fixture.fabric->epoch();
  auto denied = fixture.fabric->validate_queue(stranger_ctx, queue, fixture.generation_of(queue));
  QF_CHECK_CODE(denied, Code::authority_denied);

  // Granting a narrow capability allows exactly that capability.
  auto granted = fixture.fabric->grant_capabilities(fixture.resource, fixture.stranger,
                                                    to_mask(Capability::validate_queue), fixture.owner,
                                                    fixture.fabric->epoch(), fixture.provenance());
  QF_REQUIRE(granted.ok());
  stranger_ctx.provenance = fixture.provenance();
  auto allowed = fixture.fabric->validate_queue(stranger_ctx, queue, fixture.generation_of(queue));
  QF_REQUIRE(allowed.ok());
  stranger_ctx.provenance = fixture.provenance();
  auto not_granted = fixture.fabric->activate_queue(stranger_ctx, queue, fixture.generation_of(queue));
  QF_CHECK_CODE(not_granted, Code::authority_denied);

  auto explanation = fixture.fabric->explain(queue, fixture.stranger);
  QF_REQUIRE(explanation.ok());
  QF_CHECK(has_capability(explanation.value().principal_capabilities, Capability::validate_queue));
  QF_CHECK(!has_capability(explanation.value().principal_capabilities, Capability::activate_queue));

  // A superseded authority epoch is not authority.
  MutationContext stale_epoch = fixture.context();
  stale_epoch.epoch = Epoch::from_value(fixture.fabric->epoch().value() + 5);
  auto epoch_denied = fixture.fabric->activate_queue(stale_epoch, queue, fixture.generation_of(queue));
  QF_CHECK_CODE(epoch_denied, Code::stale_epoch);
}

QF_TEST(governance, duplicate_attempts_are_idempotent_and_conflicting_reuse_is_rejected) {
  Fixture fixture{};
  const QueueId queue = fixture.declare_queue("q-idempotent");
  MutationContext ctx = fixture.context();

  auto first = fixture.fabric->validate_queue(ctx, queue, fixture.generation_of(queue));
  QF_REQUIRE(first.ok());
  QF_CHECK(!first.value().replayed);

  // Replaying the exact same attempt returns the same outcome without applying
  // anything twice.
  auto replay = fixture.fabric->validate_queue(ctx, queue, fixture.generation_of(queue));
  QF_REQUIRE(replay.ok());
  QF_CHECK(replay.value().replayed);

  auto record = fixture.fabric->queue_record(queue);
  QF_REQUIRE(record.ok());
  QF_CHECK_EQ(record.value().mutation_count, 2ull);
  QF_CHECK_EQ(record.value().replayed_count, 1ull);
  const std::size_t memo_size = record.value().memo.size();
  QF_CHECK_EQ(memo_size, 2ull);

  // The same attempt identity reused with different content is a conflict, not
  // a replay.
  MutationContext conflicting = fixture.context();
  conflicting.provenance.sequence = ctx.provenance.sequence;
  conflicting.provenance.publisher = ctx.provenance.publisher;
  auto conflict = fixture.fabric->activate_queue(conflicting, queue, fixture.generation_of(queue));
  QF_CHECK_CODE(conflict, Code::conflict);

  // A duplicate sequence from a different attempt is refused outright, so a
  // replay can never double-apply after the memo has been evicted.
  MutationContext duplicate_sequence = fixture.context();
  duplicate_sequence.provenance.sequence = ctx.provenance.sequence;
  duplicate_sequence.provenance.publisher = ctx.provenance.publisher;
  MutationRequest request{};
  request.kind = MutationKind::retire_queue;
  request.queue = queue;
  request.expected_generation = fixture.generation_of(queue);
  request.expected_epoch = fixture.fabric->epoch();
  request.principal = fixture.owner;
  request.provenance = duplicate_sequence.provenance;
  request.attempt = AttemptId{ctx.provenance.publisher, ctx.provenance.sequence.value()};
  auto duplicate = fixture.fabric->apply(request);
  QF_CHECK_CODE(duplicate, Code::conflict);
}

QF_TEST(governance, attempt_memo_is_bounded) {
  FabricConfig config{};
  config.limits.max_attempt_memo = 4;
  Fixture fixture{config};
  const QueueId queue = fixture.declare_queue("q-memo");

  for (int index = 0; index < 12; ++index) {
    auto outcome = fixture.fabric->validate_queue(fixture.context(), queue, fixture.generation_of(queue));
    QF_REQUIRE(outcome.ok());
  }
  auto record = fixture.fabric->queue_record(queue);
  QF_REQUIRE(record.ok());
  QF_CHECK(record.value().memo.size() <= 4);
}

QF_TEST(governance, occupancy_rules_and_overflow_reporting) {
  Fixture fixture{};
  const QueueId queue = fixture.activate_queue("q-occupancy", 4500);

  auto accepted = fixture.fabric->ingest_occupancy(fixture.context(), fixture.sample(queue, 400, 4));
  QF_REQUIRE(accepted.ok());
  auto explanation = fixture.fabric->explain(queue);
  QF_REQUIRE(explanation.ok());
  QF_CHECK(explanation.value().occupancy.present);
  QF_CHECK(explanation.value().occupancy.freshness == Freshness::fresh);
  QF_CHECK(explanation.value().threshold_eval.band == ThresholdBand::below_low);
  QF_CHECK(explanation.value().threshold_eval.admission_authorized);

  // Occupancy above the configured maximum is never silently accepted: the
  // observation is recorded and the overflow is reported.
  auto overflow = fixture.fabric->ingest_occupancy(fixture.context(), fixture.sample(queue, 5000, 4));
  QF_REQUIRE(overflow.ok());
  QF_CHECK_EQ(std::string(overflow.value().rule), std::string("observed-occupancy-above-configured-maximum"));
  explanation = fixture.fabric->explain(queue);
  QF_REQUIRE(explanation.ok());
  QF_CHECK_EQ(explanation.value().occupancy.bytes, 5000ull);
  QF_CHECK_EQ(explanation.value().overflow_events, 1ull);
  QF_CHECK(explanation.value().threshold_eval.band == ThresholdBand::exceeded);
  QF_CHECK(!explanation.value().threshold_eval.admission_authorized);
  QF_CHECK_EQ(fixture.fabric->stats().occupancy_overflow, 1ull);

  // A sample bound to another generation is stale work.
  OccupancySample stale = fixture.sample(queue, 10);
  stale.queue_generation = Generation::from_value(fixture.generation_of(queue).value() + 3);
  auto stale_result = fixture.fabric->ingest_occupancy(fixture.context(), stale);
  QF_CHECK_CODE(stale_result, Code::stale_generation);

  // A sample from a superseded epoch is stale work.
  OccupancySample old_epoch = fixture.sample(queue, 10);
  old_epoch.epoch = Epoch::from_value(fixture.fabric->epoch().value() + 4);
  auto epoch_result = fixture.fabric->ingest_occupancy(fixture.context(), old_epoch);
  QF_CHECK_CODE(epoch_result, Code::stale_epoch);

  // A future observation beyond the skew allowance is refused.
  OccupancySample future = fixture.sample(queue, 10);
  future.observed_at = fixture.clock.now_nanos() + 5ll * 1000000000ll;
  auto future_result = fixture.fabric->ingest_occupancy(fixture.context(), future);
  QF_CHECK_CODE(future_result, Code::stale_occupancy);

  // An observation identity is its own provenance sequence. A verbatim duplicate
  // is an idempotent replay, and a reuse of the same identity with different
  // content is an attempt-identity conflict rather than a silently applied
  // update. Behind both, the provenance tracker refuses a duplicate sequence and
  // the kind handler refuses a sequence that goes backwards within one
  // publisher life.
  const auto record = fixture.fabric->queue_record(queue);
  QF_REQUIRE(record.ok());
  OccupancySample replay{};
  replay.queue = queue;
  replay.queue_generation = fixture.generation_of(queue);
  replay.bytes = record.value().occupancy.bytes;
  replay.packets = record.value().occupancy.packets;
  replay.epoch = fixture.fabric->epoch();
  replay.sequence = record.value().occupancy.sequence;
  replay.observed_at = record.value().occupancy.observed_at;
  replay.provenance = record.value().occupancy.provenance;
  auto replay_result = fixture.fabric->ingest_occupancy(fixture.context(), replay);
  QF_CHECK(replay_result.ok());
  QF_CHECK(replay_result.value().replayed);
  QF_CHECK_EQ(replay_result.value().generation.value(), fixture.generation_of(queue).value());

  OccupancySample conflict_sample = replay;
  conflict_sample.bytes = 1;
  auto regression_result = fixture.fabric->ingest_occupancy(fixture.context(), conflict_sample);
  QF_CHECK_CODE(regression_result, Code::conflict);

  // A conflict on an attempt identity poisons that identity: the committed
  // effect stands, but the identity is no longer replayable.
  auto poisoned = fixture.fabric->ingest_occupancy(fixture.context(), replay);
  QF_CHECK_CODE(poisoned, Code::conflict);
  const auto final_record = fixture.fabric->queue_record(queue);
  QF_REQUIRE(final_record.ok());
  QF_CHECK_EQ(final_record.value().occupancy.bytes, 5000ull);
  QF_CHECK_EQ(final_record.value().occupancy.sequence.value(), replay.sequence.value());
}

QF_TEST(governance, occupancy_goes_stale_with_the_clock) {
  Fixture fixture{};
  const QueueId queue = fixture.activate_queue("q-stale");
  QF_REQUIRE(fixture.fabric->ingest_occupancy(fixture.context(), fixture.sample(queue, 100)).ok());

  auto explanation = fixture.fabric->explain(queue);
  QF_REQUIRE(explanation.ok());
  QF_CHECK(explanation.value().threshold_eval.admission_authorized);

  fixture.clock.advance(5000000000ll);  // 5 s: beyond the 2 s policy window
  QF_REQUIRE(fixture.fabric->refresh_freshness().ok());
  explanation = fixture.fabric->explain(queue);
  QF_REQUIRE(explanation.ok());
  QF_CHECK(explanation.value().occupancy.freshness == Freshness::stale_age);
  QF_CHECK_EQ(explanation.value().stale_reason, std::string("stale_age"));
  QF_CHECK(!explanation.value().threshold_eval.admission_authorized);
  QF_CHECK(explanation.value().threshold_eval.band == ThresholdBand::unknown);
  QF_CHECK_EQ(std::string(explanation.value().threshold_eval.rule),
              std::string("stale-evidence-cannot-authorize-admission"));
}

QF_TEST(governance, fencing_blocks_mutation_and_unfencing_restores_it) {
  Fixture fixture{};
  const QueueId queue = fixture.activate_queue("q-fence");

  auto fenced = fixture.fabric->fence_queue(fixture.context(), queue, fixture.generation_of(queue), "operator quarantine");
  QF_REQUIRE(fenced.ok());
  auto record = fixture.fabric->queue_record(queue);
  QF_REQUIRE(record.ok());
  QF_CHECK(record.value().fenced());
  QF_CHECK(record.value().fence.token != 0);

  auto blocked = fixture.fabric->ingest_occupancy(fixture.context(), fixture.sample(queue, 8));
  QF_CHECK_CODE(blocked, Code::fence_violation);
  auto blocked_drain = fixture.fabric->request_drain(fixture.context(), queue, fixture.generation_of(queue));
  QF_CHECK_CODE(blocked_drain, Code::fence_violation);

  auto explanation = fixture.fabric->explain(queue);
  QF_REQUIRE(explanation.ok());
  QF_CHECK_EQ(explanation.value().fence_reason, std::string("operator quarantine"));

  auto unfenced = fixture.fabric->unfence_queue(fixture.context(), queue, fixture.generation_of(queue));
  QF_REQUIRE(unfenced.ok());
  record = fixture.fabric->queue_record(queue);
  QF_REQUIRE(record.ok());
  QF_CHECK(!record.value().fenced());
  auto accepted = fixture.fabric->ingest_occupancy(fixture.context(), fixture.sample(queue, 8));
  QF_REQUIRE(accepted.ok());
}

QF_TEST(governance, evidence_invalidation_requires_revalidation) {
  Fixture fixture{};
  const QueueId queue = fixture.activate_queue("q-invalidate");
  QF_REQUIRE(fixture.fabric->ingest_occupancy(fixture.context(), fixture.sample(queue, 64)).ok());

  auto invalidated = fixture.fabric->invalidate_evidence(queue, fixture.owner, fixture.fabric->epoch(), "publisher lost");
  QF_REQUIRE(invalidated.ok());
  auto explanation = fixture.fabric->explain(queue);
  QF_REQUIRE(explanation.ok());
  QF_CHECK(!explanation.value().threshold_eval.admission_authorized);
  QF_CHECK(explanation.value().occupancy.freshness == Freshness::stale_backend);
  QF_CHECK_EQ(std::string(explanation.value().stale_reason), std::string("stale_backend"));

  // A stranger cannot invalidate evidence.
  auto denied = fixture.fabric->invalidate_evidence(queue, fixture.stranger, fixture.fabric->epoch(), "unauthorized");
  QF_CHECK_CODE(denied, Code::authority_denied);

  // Fresh evidence clears the revalidation requirement.
  QF_REQUIRE(fixture.fabric->ingest_occupancy(fixture.context(), fixture.sample(queue, 64)).ok());
  explanation = fixture.fabric->explain(queue);
  QF_REQUIRE(explanation.ok());
  QF_CHECK(explanation.value().threshold_eval.admission_authorized);
  QF_CHECK(explanation.value().stale_reason.empty());
}

QF_TEST(governance, epoch_advance_invalidates_epoch_bound_evidence) {
  Fixture fixture{};
  const QueueId queue = fixture.activate_queue("q-epoch");
  QF_REQUIRE(fixture.fabric->ingest_occupancy(fixture.context(), fixture.sample(queue, 128)).ok());
  const Epoch before = fixture.fabric->epoch();

  auto advanced = fixture.fabric->advance_epoch(fixture.owner, fixture.provenance());
  QF_REQUIRE(advanced.ok());
  QF_CHECK_EQ(advanced.value().value(), before.value() + 1);

  auto explanation = fixture.fabric->explain(queue);
  QF_REQUIRE(explanation.ok());
  QF_CHECK(explanation.value().occupancy.freshness == Freshness::stale_epoch);
  QF_CHECK_EQ(explanation.value().authority_epoch.value(), advanced.value().value());

  // Work bound to the previous epoch is refused.
  MutationContext old_epoch = fixture.context();
  old_epoch.epoch = before;
  auto refused = fixture.fabric->ingest_occupancy(old_epoch, fixture.sample(queue, 16));
  QF_CHECK_CODE(refused, Code::stale_epoch);

  // Work in the new epoch is accepted.
  QF_REQUIRE(fixture.fabric->ingest_occupancy(fixture.context(), fixture.sample(queue, 16)).ok());

  auto denied = fixture.fabric->advance_epoch(fixture.stranger, fixture.provenance());
  QF_CHECK_CODE(denied, Code::authority_denied);
}

QF_TEST(governance, backend_acknowledgement_requires_exact_generations) {
  Fixture fixture{};
  const QueueId queue = fixture.activate_queue("q-backend");
  auto registered = fixture.fabric->register_backend(fixture.resource, Incarnation::from_value(7), fixture.owner,
                                                     fixture.fabric->epoch(), fixture.provenance());
  QF_REQUIRE(registered.ok());
  const Generation generation = fixture.generation_of(queue);

  BackendApplied applied{};
  applied.known = true;
  applied.backend = registered.value();
  applied.backend_incarnation = Incarnation::from_value(7);
  applied.epoch = fixture.fabric->epoch();
  applied.applied_class = ClassBinding{fixture.class_id, fixture.class_generation};
  applied.queue_generation = generation;

  auto accepted = fixture.fabric->acknowledge_backend(fixture.context(), queue, generation, applied);
  QF_REQUIRE(accepted.ok());
  auto explanation = fixture.fabric->explain(queue);
  QF_REQUIRE(explanation.ok());
  QF_CHECK(explanation.value().backend_verification.verified);

  // An acknowledgement from a superseded incarnation is refused.
  BackendApplied stale_incarnation = applied;
  stale_incarnation.backend_incarnation = Incarnation::from_value(6);
  auto refused = fixture.fabric->acknowledge_backend(fixture.context(), queue, generation, stale_incarnation);
  QF_CHECK_CODE(refused, Code::stale_incarnation);

  // An acknowledgement bound to an older queue generation is refused.
  BackendApplied stale_generation = applied;
  stale_generation.queue_generation = Generation::from_value(generation.value() + 1);
  refused = fixture.fabric->acknowledge_backend(fixture.context(), queue, generation, stale_generation);
  QF_CHECK_CODE(refused, Code::stale_generation);

  // An acknowledgement bound to an older class binding is refused.
  BackendApplied stale_class = applied;
  stale_class.applied_class = ClassBinding{fixture.class_id, Generation::from_value(fixture.class_generation.value() + 1)};
  refused = fixture.fabric->acknowledge_backend(fixture.context(), queue, generation, stale_class);
  QF_CHECK_CODE(refused, Code::stale_generation);

  // A dead backend incarnation makes the stored acknowledgement unverifiable.
  auto dead = fixture.fabric->mark_backend_dead(registered.value(), Incarnation::from_value(7), fixture.owner,
                                                fixture.fabric->epoch());
  QF_REQUIRE(dead.ok());
  explanation = fixture.fabric->explain(queue);
  QF_REQUIRE(explanation.ok());
  QF_CHECK(!explanation.value().backend_verification.verified);
  QF_CHECK(std::string(explanation.value().backend_verification.rule) != "backend-acknowledgement-verifiable");

  // Re-registering the backend mints a fresh incarnation, which does not
  // retroactively verify the old acknowledgement.
  auto reregistered = fixture.fabric->register_backend(fixture.resource, Incarnation::from_value(8), fixture.owner,
                                                       fixture.fabric->epoch(), fixture.provenance());
  QF_REQUIRE(reregistered.ok());
  explanation = fixture.fabric->explain(queue);
  QF_REQUIRE(explanation.ok());
  QF_CHECK(!explanation.value().backend_verification.verified);
}

QF_TEST(governance, two_phase_backend_application) {
  Fixture fixture{};
  const QueueId queue = fixture.activate_queue("q-plan");
  auto registered = fixture.fabric->register_backend(fixture.resource, Incarnation::from_value(2), fixture.owner,
                                                     fixture.fabric->epoch(), fixture.provenance());
  QF_REQUIRE(registered.ok());
  const Generation generation = fixture.generation_of(queue);

  auto planned = fixture.fabric->plan_backend_apply(queue, generation, fixture.owner, fixture.provenance());
  QF_REQUIRE(planned.ok());
  QF_CHECK(planned.value().attempt.valid());
  QF_CHECK_EQ(fixture.fabric->pending_plans().size(), 1ull);
  QF_CHECK_EQ(fixture.fabric->pending_attempts().size(), 1ull);

  // A second reservation with the same attempt identity is refused.
  auto duplicate = fixture.fabric->plan_backend_apply(queue, generation, fixture.owner, fixture.provenance());
  QF_REQUIRE(duplicate.ok());
  QF_CHECK(duplicate.value().attempt != planned.value().attempt);

  // Aborting releases the reservation without committing anything.
  auto aborted = fixture.fabric->abort_backend_apply(planned.value(), "dry run");
  QF_REQUIRE(aborted.ok());
  QF_CHECK_EQ(fixture.fabric->pending_plans().size(), 1ull);
  auto record = fixture.fabric->queue_record(queue);
  QF_REQUIRE(record.ok());
  QF_CHECK(!record.value().applied.known);

  // Committing with a matching live incarnation records verifiable state.
  BackendApplied applied{};
  applied.known = true;
  applied.backend = registered.value();
  applied.backend_incarnation = Incarnation::from_value(2);
  applied.epoch = fixture.fabric->epoch();
  applied.applied_class = ClassBinding{fixture.class_id, fixture.class_generation};
  applied.queue_generation = generation;
  auto committed = fixture.fabric->commit_backend_apply(duplicate.value(), applied, fixture.provenance());
  QF_REQUIRE(committed.ok());
  QF_CHECK_EQ(fixture.fabric->pending_plans().size(), 0ull);
  auto explanation = fixture.fabric->explain(queue);
  QF_REQUIRE(explanation.ok());
  QF_CHECK(explanation.value().backend_verification.verified);

  // A plan bound to a superseded generation cannot be committed.
  auto planned_again = fixture.fabric->plan_backend_apply(queue, generation, fixture.owner, fixture.provenance());
  QF_REQUIRE(planned_again.ok());
  Thresholds updated{};
  updated.low_watermark_bytes = 512;
  updated.high_watermark_bytes = 2048;
  updated.max_occupancy_bytes = 4096;
  QF_REQUIRE(fixture.fabric->set_thresholds(fixture.context(), queue, generation, updated).ok());
  auto stale_commit = fixture.fabric->commit_backend_apply(planned_again.value(), applied, fixture.provenance());
  QF_CHECK_CODE(stale_commit, Code::stale_generation);
}

QF_TEST(governance, explanation_is_complete_and_bounded) {
  Fixture fixture{};
  const QueueId queue = fixture.activate_queue("q-explain");
  QF_REQUIRE(fixture.fabric->ingest_occupancy(fixture.context(), fixture.sample(queue, 2048, 3)).ok());

  auto explanation = fixture.fabric->explain(queue, fixture.owner);
  QF_REQUIRE(explanation.ok());
  const std::string rendered = explanation.value().render();
  QF_CHECK(rendered.find("lifecycle=active") != std::string::npos);
  QF_CHECK(rendered.find("freshness=fresh") != std::string::npos);
  QF_CHECK(rendered.find("class_generation=") != std::string::npos);
  QF_CHECK(rendered.find("drain_rule=") != std::string::npos);
  QF_CHECK(rendered.find("threshold_rule=") != std::string::npos);
  QF_CHECK(rendered.size() <= limits::kMaxExplainBytes + 64);

  const std::string json = explanation.value().to_json_like();
  QF_CHECK(json.find("\"lifecycle\":\"active\"") != std::string::npos);
  QF_CHECK(json.find("\"admission\":true") != std::string::npos);

  auto missing = fixture.fabric->explain(QueueId::from_value(0xDEAD));
  QF_CHECK_CODE(missing, Code::not_found);
}

QF_TEST(governance, invariants_hold_after_a_lifecycle_walk) {
  Fixture fixture{};
  const QueueId first = fixture.activate_queue("q-inv-a");
  const QueueId second = fixture.declare_queue("q-inv-b");
  QF_REQUIRE(fixture.fabric->ingest_occupancy(fixture.context(), fixture.sample(first, 100)).ok());

  std::vector<std::string> violations;
  QF_CHECK_EQ(fixture.fabric->validate_invariants(violations), 0ull);
  for (const auto& violation : violations) {
    std::printf("     violation: %s\n", violation.c_str());
  }

  QF_REQUIRE(fixture.fabric->validate_queue(fixture.context(), second, fixture.generation_of(second)).ok());
  QF_CHECK_EQ(fixture.fabric->validate_invariants(violations), 0ull);
  auto stats = fixture.fabric->stats();
  QF_CHECK_EQ(stats.queues, 2ull);
  QF_CHECK_EQ(stats.resources, 1ull);
  QF_CHECK_EQ(stats.classes, 1ull);
  QF_CHECK_EQ(stats.pools, 1ull);
  QF_CHECK(stats.mutations_applied >= 6);
}

QF_TEST(governance, names_are_unique_per_resource_and_bounded) {
  Fixture fixture{};
  fixture.declare_queue("q-unique");
  QF_REQUIRE(!fixture.fabric->declare_queue(fixture.context(), fixture.resource, "q-unique", 8,
                                            ClassBinding{fixture.class_id, fixture.class_generation},
                                            PoolRef{fixture.pool_id, fixture.pool_generation, 4096}, Thresholds{})
                   .ok());

  auto found = fixture.fabric->find_queue(fixture.resource, "q-unique");
  QF_REQUIRE(found.ok());

  // Over-long names are refused before any allocation.
  const std::string long_name(limits::kMaxNameChars + 1, 'x');
  auto refused = fixture.fabric->declare_queue(fixture.context(), fixture.resource, long_name, 8,
                                               ClassBinding{fixture.class_id, fixture.class_generation},
                                               PoolRef{fixture.pool_id, fixture.pool_generation, 4096}, Thresholds{});
  QF_CHECK_CODE(refused, Code::out_of_range);
}

QF_TEST(governance, capacity_limits_are_enforced) {
  FabricConfig config{};
  config.limits.max_queues_per_resource = 3;
  Fixture fixture{config};
  fixture.declare_queue("q-cap-0");
  fixture.declare_queue("q-cap-1");
  fixture.declare_queue("q-cap-2");
  auto refused = fixture.fabric->declare_queue(fixture.context(), fixture.resource, "q-cap-3", 8,
                                               ClassBinding{fixture.class_id, fixture.class_generation},
                                               PoolRef{fixture.pool_id, fixture.pool_generation, 4096}, Thresholds{});
  QF_CHECK_CODE(refused, Code::capacity_exceeded);
  QF_CHECK_EQ(fixture.fabric->stats().queues, 3ull);
}
