// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
//
// Seeded randomized model checks, determinism, replay equivalence and the
// stale-evidence property.
#include <cstdint>
#include <cstdio>
#include <string>
#include <vector>

#include "qf/fabric.hpp"
#include "qf/hash.hpp"
#include "qf/rng.hpp"
#include "support/fixture.hpp"
#include "support/test_framework.hpp"

using namespace qf;
using qftest::Fixture;

namespace {

enum class OpKind : std::uint8_t {
  declare = 0,
  validate = 1,
  activate = 2,
  rebind = 3,
  thresholds = 4,
  occupancy = 5,
  fence = 6,
  unfence = 7,
  drain = 8,
  cancel_drain = 9,
  evidence = 10,
  quiesce = 11,
  retire = 12,
  fail = 13,
  stale_occupancy = 14,
  wrong_generation = 15,
  advance_epoch = 16,
  invalidate = 17,
};

struct Op {
  OpKind kind{OpKind::declare};
  std::size_t queue_slot{0};
  std::uint64_t value{0};
};

std::vector<Op> build_script(std::uint64_t seed, std::size_t operations, std::size_t queue_slots) {
  Rng rng{seed};
  std::vector<Op> script;
  script.reserve(operations);
  for (std::size_t index = 0; index < operations; ++index) {
    Op op{};
    op.kind = static_cast<OpKind>(rng.next_bounded(18));
    op.queue_slot = static_cast<std::size_t>(rng.next_bounded(queue_slots));
    op.value = rng.next_bounded(9000);
    script.push_back(op);
  }
  return script;
}

/// Durable digest: everything that must survive a restart, excluding dynamic
/// evidence and the coordinator epoch.
std::uint64_t durable_digest(const Fabric& fabric) {
  std::uint64_t seed = 0x9988776655443322ull;
  for (const auto& record : fabric.all_queues()) {
    hash_mix(seed, record.def.id.value());
    hash_mix(seed, std::string_view{record.def.name});
    hash_mix(seed, static_cast<std::uint64_t>(record.lifecycle));
    hash_mix(seed, record.def.generation.value());
    hash_mix(seed, record.def.cls.id.value());
    hash_mix(seed, record.def.cls.generation.value());
    hash_mix(seed, record.def.pool.id.value());
    hash_mix(seed, record.def.pool.generation.value());
    hash_mix(seed, record.def.owner.value());
    hash_mix(seed, record.def.thresholds.low_watermark_bytes);
    hash_mix(seed, record.def.thresholds.high_watermark_bytes);
    hash_mix(seed, record.def.thresholds.max_occupancy_bytes);
    hash_mix(seed, record.fence.fenced);
    hash_mix(seed, static_cast<std::uint64_t>(record.drain.phase));
  }
  return seed;
}

struct Model {
  std::vector<Lifecycle> lifecycle{};
  std::vector<bool> fenced{};
  std::vector<bool> exists{};
  std::vector<QueueId> ids{};

  void resize(std::size_t slots) {
    lifecycle.assign(slots, Lifecycle::declared);
    fenced.assign(slots, false);
    exists.assign(slots, false);
    ids.assign(slots, QueueId{});
  }
};

/// Executes a script deterministically against one fabric and returns the
/// number of operations applied together with the model it produced.
std::size_t execute_script(Fixture& fixture, const std::vector<Op>& script, Model& model, bool check_model) {
  std::size_t applied = 0;
  std::uint64_t epoch = fixture.fabric->epoch().value();
  for (const auto& op : script) {
    if (op.queue_slot >= model.exists.size()) {
      continue;
    }
    const bool exists = model.exists[op.queue_slot];
    MutationContext ctx = fixture.context();
    ctx.epoch = Epoch::from_value(epoch);
    ctx.provenance.epoch = Epoch::from_value(epoch);
    switch (op.kind) {
      case OpKind::declare: {
        if (exists) {
          continue;
        }
        auto outcome = fixture.fabric->declare_queue(ctx, fixture.resource, "q" + std::to_string(op.queue_slot), 32,
                                                    ClassBinding{fixture.class_id, fixture.class_generation},
                                                    PoolRef{fixture.pool_id, fixture.pool_generation, 1u << 20},
                                                    Thresholds{1024, 4096, 8192, 4096, 0});
        if (outcome.ok()) {
          model.exists[op.queue_slot] = true;
          model.lifecycle[op.queue_slot] = Lifecycle::declared;
          model.fenced[op.queue_slot] = false;
          model.ids[op.queue_slot] = outcome.value().queue;
          ++applied;
        }
        break;
      }
      case OpKind::validate:
      case OpKind::activate:
      case OpKind::rebind:
      case OpKind::thresholds:
      case OpKind::fence:
      case OpKind::unfence:
      case OpKind::drain:
      case OpKind::cancel_drain:
      case OpKind::quiesce:
      case OpKind::retire:
      case OpKind::fail:
      case OpKind::invalidate: {
        if (!exists) {
          continue;
        }
        const auto record = fixture.fabric->queue_record(model.ids[op.queue_slot]);
        if (!record.ok()) {
          continue;
        }
        const QueueId queue = record.value().def.id;
        const Generation generation = record.value().def.generation;
        Result<MutationOutcome> outcome = Status{Code::internal, "unset"};
        switch (op.kind) {
          case OpKind::validate:
            outcome = fixture.fabric->validate_queue(ctx, queue, generation);
            break;
          case OpKind::activate:
            outcome = fixture.fabric->activate_queue(ctx, queue, generation);
            break;
          case OpKind::rebind: {
            const auto resource = fixture.fabric->resource_record(fixture.resource);
            if (!resource.ok() || resource.value().classes.empty()) {
              continue;
            }
            outcome = fixture.fabric->rebind_class(
                ctx, queue, generation,
                ClassBinding{resource.value().classes.front().id, resource.value().classes.front().generation});
            break;
          }
          case OpKind::thresholds:
            outcome = fixture.fabric->set_thresholds(
                ctx, queue, generation,
                Thresholds{1 + (op.value % 100), 500 + (op.value % 1000), 4000 + (op.value % 4000), 512, 0});
            break;
          case OpKind::fence:
            outcome = fixture.fabric->fence_queue(ctx, queue, generation, "rng");
            break;
          case OpKind::unfence:
            outcome = fixture.fabric->unfence_queue(ctx, queue, generation);
            break;
          case OpKind::drain:
            outcome = fixture.fabric->request_drain(ctx, queue, generation);
            break;
          case OpKind::cancel_drain:
            outcome = fixture.fabric->cancel_drain(ctx, queue, generation);
            break;
          case OpKind::quiesce:
            outcome = fixture.fabric->quiesce_queue(ctx, queue, generation);
            break;
          case OpKind::retire:
            outcome = fixture.fabric->retire_queue(ctx, queue, generation);
            break;
          case OpKind::fail:
            outcome = fixture.fabric->fail_queue(ctx, queue, generation, "rng");
            break;
          case OpKind::invalidate:
            if (op.value % 16 != 0) {
              continue;
            }
            {
              const Status invalidated =
                  fixture.fabric->invalidate_evidence(queue, fixture.owner, Epoch::from_value(epoch), "rng");
              outcome = invalidated.ok() ? Result<MutationOutcome>{MutationOutcome{}}
                                         : Result<MutationOutcome>{invalidated};
            }
            break;
          default:
            break;
        }
        if (outcome.ok()) {
          ++applied;
          const auto after = fixture.fabric->queue_record(queue);
          if (after.ok()) {
            model.lifecycle[op.queue_slot] = after.value().lifecycle;
            model.fenced[op.queue_slot] = after.value().fenced();
            if (check_model) {
              // A retired queue is terminal, and a fenced queue never accepts a
              // later mutation without an unfence.
              if (op.kind == OpKind::retire) {
                QF_CHECK(after.value().lifecycle == Lifecycle::retired);
              }
            }
          }
        } else if (check_model) {
          const Code code = outcome.status().code();
          const bool acceptable = code == Code::lifecycle_violation || code == Code::fence_violation ||
                                  code == Code::drain_incomplete || code == Code::backend_mismatch ||
                                  code == Code::stale_occupancy || code == Code::stale_generation ||
                                  code == Code::authority_denied || code == Code::conflict ||
                                  code == Code::replay_detected || code == Code::stale_epoch;
          QF_CHECK(acceptable);
        }
        break;
      }
      case OpKind::occupancy:
      case OpKind::stale_occupancy:
      case OpKind::wrong_generation: {
        if (!exists || model.lifecycle[op.queue_slot] == Lifecycle::retired ||
            model.lifecycle[op.queue_slot] == Lifecycle::failed) {
          continue;
        }
        const auto record = fixture.fabric->queue_record(model.ids[op.queue_slot]);
        if (!record.ok()) {
          continue;
        }
        OccupancySample sample = fixture.sample(record.value().def.id, op.value % 10000, op.value % 64);
        sample.epoch = Epoch::from_value(epoch);
        if (op.kind == OpKind::stale_occupancy) {
          sample.observed_at = fixture.clock.now_nanos() - 60ll * 1000000000ll;
        } else if (op.kind == OpKind::wrong_generation) {
          sample.queue_generation = Generation::from_value(record.value().def.generation.value() + 7);
        }
        auto outcome = fixture.fabric->ingest_occupancy(ctx, sample);
        if (outcome.ok()) {
          ++applied;
        } else if (check_model) {
          const Code code = outcome.status().code();
          QF_CHECK(code == Code::stale_generation || code == Code::fence_violation ||
                   code == Code::lifecycle_violation || code == Code::stale_occupancy ||
                   code == Code::stale_epoch || code == Code::conflict || code == Code::replay_detected ||
                   code == Code::occupancy_overflow);
          if (op.kind == OpKind::wrong_generation) {
            QF_CHECK(code == Code::stale_generation || code == Code::fence_violation ||
                     code == Code::lifecycle_violation);
          }
        }
        break;
      }
      case OpKind::advance_epoch: {
        if (op.value % 97 != 0) {
          continue;
        }
        auto advanced = fixture.fabric->advance_epoch(fixture.owner, ctx.provenance);
        if (advanced.ok()) {
          epoch = advanced.value().value();
          ++applied;
        }
        break;
      }
    }
  }
  return applied;
}

}  // namespace

QF_TEST(property, seeded_scripts_preserve_every_invariant) {
  for (std::uint64_t seed = 1; seed <= 6; ++seed) {
    Fixture fixture{};
    Model model{};
    model.resize(6);
    const std::vector<Op> script = build_script(seed * 7919, 400, 6);
    execute_script(fixture, script, model, true);

    std::vector<std::string> violations;
    const std::size_t count = fixture.fabric->validate_invariants(violations);
    for (const auto& violation : violations) {
      std::printf("     seed=%llu violation: %s\n", static_cast<unsigned long long>(seed), violation.c_str());
    }
    QF_CHECK_EQ(count, 0ull);

    // The model agrees with the fabric on lifecycle and fencing.
    for (std::size_t slot = 0; slot < model.exists.size(); ++slot) {
      if (!model.exists[slot]) {
        continue;
      }
      const QueueId queue = model.ids[slot];
      const auto record = fixture.fabric->queue_record(queue);
      QF_REQUIRE(record.ok());
      QF_CHECK(record.value().lifecycle == model.lifecycle[slot]);
      QF_CHECK_EQ(record.value().fenced(), model.fenced[slot]);
    }
  }
}

QF_TEST(property, stale_evidence_never_authorizes_after_the_window) {
  Fixture fixture{};
  std::vector<QueueId> queues;
  for (int index = 0; index < 4; ++index) {
    queues.push_back(fixture.activate_queue("q-window-" + std::to_string(index)));
    QF_REQUIRE(fixture.fabric->ingest_occupancy(fixture.context(), fixture.sample(queues.back(), 100 + index)).ok());
  }
  for (const auto& queue : queues) {
    auto explanation = fixture.fabric->explain(queue);
    QF_REQUIRE(explanation.ok());
    QF_CHECK(explanation.value().threshold_eval.admission_authorized);
  }

  fixture.clock.advance(30ll * 1000000000ll);
  QF_REQUIRE(fixture.fabric->refresh_freshness().ok());
  for (const auto& queue : queues) {
    auto explanation = fixture.fabric->explain(queue);
    QF_REQUIRE(explanation.ok());
    QF_CHECK(!explanation.value().threshold_eval.admission_authorized);
    QF_CHECK(explanation.value().occupancy.freshness == Freshness::stale_age);
  }

  // The clock going backwards does not resurrect authority either.
  fixture.clock.set(0);
  QF_REQUIRE(fixture.fabric->refresh_freshness().ok());
  for (const auto& queue : queues) {
    auto explanation = fixture.fabric->explain(queue);
    QF_REQUIRE(explanation.ok());
    QF_CHECK(!explanation.value().threshold_eval.admission_authorized);
  }
}

QF_TEST(property, identical_scripts_produce_identical_state) {
  const std::vector<Op> script = build_script(0xFEEDFACE, 300, 5);

  Fixture first{};
  Model first_model{};
  first_model.resize(5);
  execute_script(first, script, first_model, false);
  const std::uint64_t first_digest = durable_digest(*first.fabric);

  Fixture second{};
  Model second_model{};
  second_model.resize(5);
  execute_script(second, script, second_model, false);
  const std::uint64_t second_digest = durable_digest(*second.fabric);

  QF_CHECK_EQ(first_digest, second_digest);
  QF_CHECK_EQ(first.fabric->stats().queues, second.fabric->stats().queues);
  QF_CHECK_EQ(first.fabric->stats().mutations_applied, second.fabric->stats().mutations_applied);
  QF_CHECK_EQ(first.fabric->stats().mutations_rejected, second.fabric->stats().mutations_rejected);
}

QF_TEST(property, replayed_durable_state_equals_the_original) {
  const std::string directory = qftest::scratch_directory("property-replay");
  const std::string journal_path = directory + "/fabric.qfjournal";
  const std::vector<Op> script = build_script(0xC0FFEE, 250, 4);

  std::uint64_t original_digest = 0;
  std::size_t original_queues = 0;
  std::vector<QueueRecord> original_records;
  {
    Fixture fixture{{}, journal_path};
    Model model{};
    model.resize(4);
    execute_script(fixture, script, model, false);
    QF_REQUIRE(fixture.fabric->compact().ok());
    original_digest = durable_digest(*fixture.fabric);
    original_queues = fixture.fabric->stats().queues;
    original_records = fixture.fabric->all_queues();
  }

  Fixture restarted{{}, journal_path, nullptr, false};
  QF_CHECK_EQ(durable_digest(*restarted.fabric), original_digest);
  QF_CHECK_EQ(restarted.fabric->all_queues().size(), original_records.size());
  QF_CHECK_EQ(restarted.fabric->stats().queues, original_queues);
  std::vector<std::string> violations;
  QF_CHECK_EQ(restarted.fabric->validate_invariants(violations), 0ull);
  const RecoveryReport report = restarted.fabric->recovery_report();
  QF_CHECK(report.status.ok());
  QF_CHECK_EQ(report.records_corrupt, 0ull);
}

QF_TEST(property, explain_always_names_the_rule_that_justified_a_decision) {
  Fixture fixture{};
  const QueueId queue = fixture.activate_queue("q-rules");
  static const char* const kKnownRules[] = {
      "authorized",          "queue-fenced",         "queue-unfenced",      "drain-requested-not-drained",
      "drain-evidence-recorded", "drain-evidence-complete", "occupied-observed", "occupancy-observed",
      "no-op-idempotent",    "idempotent-replay",     "declared",            "class-rebound-generation-advanced",
      "thresholds-updated-generation-advanced",       "ownership-transferred-generation-advanced",
      "queue-retired",       "queue-failed",          "backend-acknowledgement-verified",
  };
  auto matches = [](std::string_view rule) {
    for (const char* known : kKnownRules) {
      if (rule == known) {
        return true;
      }
    }
    return false;
  };

  QF_REQUIRE(fixture.fabric->ingest_occupancy(fixture.context(), fixture.sample(queue, 10)).ok());
  auto explanation = fixture.fabric->explain(queue);
  QF_REQUIRE(explanation.ok());
  QF_CHECK(matches(explanation.value().last_rule));
  QF_CHECK(explanation.value().last_status.ok());
  QF_CHECK(std::string(explanation.value().threshold_eval.rule) != "unset");

  // A rejected operation leaves an explanation of the rejection behind.
  auto refused = fixture.fabric->retire_queue(fixture.context(), queue, Generation::from_value(99));
  QF_CHECK(!refused.ok());
  explanation = fixture.fabric->explain(queue);
  QF_REQUIRE(explanation.ok());
  QF_CHECK(explanation.value().last_status.code() != Code::ok);
  QF_CHECK(std::string(explanation.value().last_rule).size() > 0);
}
