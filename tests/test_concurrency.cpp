// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
//
// Concurrency: parallel occupancy ingestion, lifecycle races, retire-versus-
// mutation, concurrent explanation, event-sink re-entrancy and shutdown.
//
// Every worker is a std::jthread so that a failed check can never leave a
// joinable thread behind: the thread joins during unwinding and the failure is
// reported instead of terminating the process.
#include <atomic>
#include <cstdio>
#include <string>
#include <thread>
#include <vector>

#include "qf/fabric.hpp"
#include "support/fixture.hpp"
#include "support/test_framework.hpp"

using namespace qf;
using qftest::Fixture;

namespace {

/// Event sink that re-enters the fabric from the callback. The fabric must not
/// hold its lock while emitting, otherwise this deadlocks immediately.
class ReentrantSink final : public IEventSink {
 public:
  void attach(Fabric& fabric) noexcept { fabric_ = &fabric; }

  void on_event(const Event& event) override {
    ++events;
    if (event.queue.valid() && fabric_ != nullptr) {
      auto explanation = fabric_->explain(event.queue);
      if (explanation.ok()) {
        ++explained;
      }
    }
    if (fabric_ != nullptr) {
      last_queue_count = fabric_->stats().queues;
    }
    if (event.kind == EventKind::occupancy_overflow) {
      ++overflows;
    }
  }

  Fabric* fabric_{nullptr};
  std::atomic<std::uint64_t> events{0};
  std::atomic<std::uint64_t> explained{0};
  std::atomic<std::uint64_t> overflows{0};
  std::atomic<std::size_t> last_queue_count{0};
};

/// One publisher identity per thread: sequences are per publisher, so concurrent
/// publishers never collide.
struct Publisher {
  PublisherId id{};
  NodeId node{};
  Incarnation incarnation{};
  BootId boot{};
  Sequence sequence{Sequence::from_value(0)};

  Provenance next(const Fabric& fabric) {
    auto advanced = sequence.next();
    if (advanced.ok()) {
      sequence = advanced.value();
    }
    Provenance provenance{};
    provenance.publisher = id;
    provenance.node = node;
    provenance.incarnation = incarnation;
    provenance.epoch = fabric.epoch();
    provenance.sequence = sequence;
    provenance.boot = boot;
    return provenance;
  }
};

Publisher make_publisher(std::uint64_t index) {
  Publisher publisher{};
  publisher.id = PublisherId::from_value(0x5000 + index);
  publisher.node = NodeId::from_value(0x6000 + index);
  publisher.incarnation = Incarnation::from_value(1);
  publisher.boot = BootId{index + 1, 0x77};
  return publisher;
}

}  // namespace

QF_TEST(concurrency, parallel_occupancy_ingestion_loses_nothing) {
  Fixture fixture{};
  const QueueId queue = fixture.activate_queue("q-parallel");
  const Generation generation = fixture.generation_of(queue);
  const Epoch epoch = fixture.fabric->epoch();

  constexpr int kThreads = 8;
  constexpr int kIterations = 250;
  std::atomic<int> accepted{0};
  std::atomic<int> rejected{0};

  std::vector<std::jthread> threads;
  threads.reserve(kThreads);
  for (int index = 0; index < kThreads; ++index) {
    threads.emplace_back([&, index]() {
      Publisher publisher = make_publisher(static_cast<std::uint64_t>(index));
      for (int iteration = 0; iteration < kIterations; ++iteration) {
        OccupancySample sample{};
        sample.queue = queue;
        sample.queue_generation = generation;
        sample.bytes = static_cast<std::uint64_t>(100 + index * 10 + iteration);
        sample.packets = 1;
        sample.epoch = epoch;
        const Provenance provenance = publisher.next(*fixture.fabric);
        sample.sequence = provenance.sequence;
        sample.provenance = provenance;
        sample.observed_at = fixture.clock.now_nanos();
        MutationContext context{};
        context.principal = fixture.owner;
        context.provenance = provenance;
        context.epoch = epoch;
        auto outcome = fixture.fabric->ingest_occupancy(context, sample);
        if (outcome.ok()) {
          accepted.fetch_add(1);
        } else {
          rejected.fetch_add(1);
        }
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }

  QF_CHECK_EQ(accepted.load(), kThreads * kIterations);
  QF_CHECK_EQ(rejected.load(), 0);
  const auto stats = fixture.fabric->stats();
  QF_CHECK_EQ(stats.occupancy_accepted, static_cast<std::uint64_t>(kThreads * kIterations));

  const auto record = fixture.fabric->queue_record(queue);
  QF_REQUIRE(record.ok());
  QF_CHECK(record.value().occupancy.present);
  QF_CHECK(record.value().occupancy.bytes >= 100);

  std::vector<std::string> violations;
  QF_CHECK_EQ(fixture.fabric->validate_invariants(violations), 0ull);
}

QF_TEST(concurrency, parallel_lifecycle_and_queries_stay_consistent) {
  Fixture fixture{};
  constexpr int kQueues = 24;
  std::vector<QueueId> queues;
  queues.reserve(kQueues);
  for (int index = 0; index < kQueues; ++index) {
    queues.push_back(fixture.declare_queue("q-life-" + std::to_string(index)));
  }
  const ResourceId resource = fixture.resource;
  const ClassBinding binding{fixture.class_id, fixture.class_generation};
  const PoolRef pool{fixture.pool_id, fixture.pool_generation, 1u << 20};
  const Epoch epoch = fixture.fabric->epoch();
  const OwnerId owner = fixture.owner;

  std::atomic<int> activations{0};
  std::atomic<int> explanations{0};
  std::vector<std::jthread> threads;
  threads.reserve(kQueues + 4);

  for (int index = 0; index < kQueues; ++index) {
    threads.emplace_back([&, index]() {
      Publisher publisher = make_publisher(0x100 + static_cast<std::uint64_t>(index));
      MutationContext context{};
      context.principal = owner;
      context.epoch = epoch;
      context.provenance = publisher.next(*fixture.fabric);
      const QueueId queue = queues[static_cast<std::size_t>(index)];
      auto validated = fixture.fabric->validate_queue(context, queue, Generation::from_value(1));
      if (!validated.ok()) {
        return;
      }
      context.provenance = publisher.next(*fixture.fabric);
      auto activated = fixture.fabric->activate_queue(context, queue, Generation::from_value(1));
      if (activated.ok()) {
        activations.fetch_add(1);
      }
    });
  }
  for (int index = 0; index < 4; ++index) {
    threads.emplace_back([&]() {
      for (int iteration = 0; iteration < 50; ++iteration) {
        for (const auto& queue : queues) {
          auto explanation = fixture.fabric->explain(queue);
          if (explanation.ok()) {
            explanations.fetch_add(1);
          }
        }
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
  QF_CHECK_EQ(activations.load(), kQueues);
  QF_CHECK(explanations.load() > 0);
  std::vector<std::string> violations;
  QF_CHECK_EQ(fixture.fabric->validate_invariants(violations), 0ull);
  QF_CHECK(resource.valid());
  QF_CHECK(binding.valid());
  QF_CHECK(pool.valid());
}

QF_TEST(concurrency, retirement_wins_over_concurrent_occupancy) {
  Fixture fixture{};
  // A validated queue accepts occupancy but can be retired directly, which is
  // the race this test exercises.
  const QueueId queue = fixture.declare_queue("q-retire-race");
  QF_REQUIRE(fixture.fabric->validate_queue(fixture.context(), queue, fixture.generation_of(queue)).ok());
  const Generation generation = fixture.generation_of(queue);
  const Epoch epoch = fixture.fabric->epoch();
  const OwnerId owner = fixture.owner;

  std::atomic<int> accepted{0};
  std::atomic<int> refused{0};
  std::atomic<int> successes_after_first_refusal{0};
  std::atomic<bool> saw_refusal{false};
  std::atomic<bool> stop{false};

  std::jthread publisher([&]() {
    Publisher identity = make_publisher(0x7000);
    for (int iteration = 0; iteration < 200000 && !stop.load(); ++iteration) {
      OccupancySample sample{};
      sample.queue = queue;
      sample.queue_generation = generation;
      sample.bytes = 64;
      sample.packets = 1;
      sample.epoch = epoch;
      const Provenance provenance = identity.next(*fixture.fabric);
      sample.sequence = provenance.sequence;
      sample.provenance = provenance;
      sample.observed_at = fixture.clock.now_nanos();
      MutationContext context{};
      context.principal = owner;
      context.provenance = provenance;
      context.epoch = epoch;
      auto outcome = fixture.fabric->ingest_occupancy(context, sample);
      if (outcome.ok()) {
        accepted.fetch_add(1);
        if (saw_refusal.load()) {
          successes_after_first_refusal.fetch_add(1);
        }
      } else {
        refused.fetch_add(1);
        saw_refusal.store(true);
      }
    }
  });

  // Wait until the publisher is demonstrably live, then retire the queue while
  // publications are still in flight.
  while (accepted.load() < 16) {
    std::this_thread::yield();
  }
  const MutationContext retire_context = fixture.context();
  auto retired_outcome = fixture.fabric->retire_queue(retire_context, queue, generation);
  QF_REQUIRE(retired_outcome.ok());

  // The publisher keeps publishing until it has observed a run of refusals.
  while (refused.load() < 32) {
    std::this_thread::yield();
  }
  stop.store(true);
  publisher.join();

  const auto record = fixture.fabric->queue_record(queue);
  QF_REQUIRE(record.ok());
  QF_CHECK(record.value().lifecycle == Lifecycle::retired);
  QF_CHECK(!record.value().occupancy.present);
  QF_CHECK(accepted.load() > 0);
  QF_CHECK(refused.load() >= 32);
  // Retirement is a hard cut: once a publication has been refused, no later
  // publication may ever succeed.
  QF_CHECK_EQ(successes_after_first_refusal.load(), 0);
  std::vector<std::string> violations;
  QF_CHECK_EQ(fixture.fabric->validate_invariants(violations), 0ull);
}

QF_TEST(concurrency, event_sink_reentrancy_never_deadlocks) {
  // The sink re-enters the fabric that emits to it. If the fabric emitted while
  // holding its lock this would deadlock rather than fail.
  ReentrantSink sink{};
  ManualClock clock{};
  Fabric observed{{}, clock, nullptr, &sink};
  sink.attach(observed);
  QF_REQUIRE(observed.recover().ok());

  const OwnerId owner = OwnerId::from_value(1);
  Provenance provenance{};
  provenance.publisher = PublisherId::from_value(1);
  provenance.node = NodeId::from_value(1);
  provenance.incarnation = Incarnation::from_value(1);
  provenance.epoch = observed.epoch();
  provenance.sequence = Sequence::from_value(1);
  provenance.boot = BootId{1, 1};
  auto resource = observed.declare_resource("r0", owner, provenance);
  QF_REQUIRE(resource.ok());
  provenance.sequence = Sequence::from_value(2);
  QF_REQUIRE(observed.declare_class(resource.value(), "gold", 1, QosRef{}, owner, observed.epoch(), provenance).ok());
  provenance.sequence = Sequence::from_value(3);
  QF_REQUIRE(observed.declare_pool(resource.value(), "p0", 4096, owner, observed.epoch(), provenance).ok());
  const auto record = observed.resource_record(resource.value());
  QF_REQUIRE(record.ok());

  MutationContext context{};
  context.principal = owner;
  context.epoch = observed.epoch();
  provenance.sequence = Sequence::from_value(4);
  context.provenance = provenance;
  auto queue = observed.declare_queue(context, resource.value(), "q-events", 8,
                                     ClassBinding{record.value().classes.front().id, record.value().classes.front().generation},
                                     PoolRef{record.value().pools.front().id, record.value().pools.front().generation, 4096},
                                     Thresholds{});
  QF_REQUIRE(queue.ok());
  provenance.sequence = Sequence::from_value(5);
  context.provenance = provenance;
  QF_REQUIRE(observed.validate_queue(context, queue.value().queue, queue.value().generation).ok());
  provenance.sequence = Sequence::from_value(6);
  context.provenance = provenance;
  QF_REQUIRE(observed.activate_queue(context, queue.value().queue, queue.value().generation).ok());

  QF_CHECK(sink.events.load() >= 6);
  QF_CHECK(sink.explained.load() >= 4);
  QF_CHECK_EQ(sink.last_queue_count.load(), 1ull);

  // Tighten the limits and publish above them: the overflow event must reach
  // the sink, and the sink must still be able to re-enter.
  Thresholds tight{};
  tight.low_watermark_bytes = 1;
  tight.high_watermark_bytes = 2;
  tight.max_occupancy_bytes = 4;
  provenance.sequence = Sequence::from_value(7);
  context.provenance = provenance;
  auto tightened = observed.set_thresholds(context, queue.value().queue, queue.value().generation, tight);
  QF_REQUIRE(tightened.ok());
  // The threshold update advanced the definition generation, so evidence must
  // bind to the new generation.
  QF_CHECK(tightened.value().generation.value() == queue.value().generation.value() + 1);

  OccupancySample sample{};
  sample.queue = queue.value().queue;
  sample.queue_generation = tightened.value().generation;
  sample.bytes = 1000;
  sample.epoch = observed.epoch();
  provenance.sequence = Sequence::from_value(8);
  sample.sequence = provenance.sequence;
  sample.provenance = provenance;
  context.provenance = provenance;
  auto ingested = observed.ingest_occupancy(context, sample);
  QF_REQUIRE(ingested.ok());
  QF_CHECK_EQ(sink.overflows.load(), 1ull);
}

QF_TEST(concurrency, shutdown_refuses_new_work_and_preserves_committed_state) {
  Fixture fixture{};
  const QueueId queue = fixture.activate_queue("q-shutdown-race");
  const Generation generation = fixture.generation_of(queue);
  const Epoch epoch = fixture.fabric->epoch();
  const OwnerId owner = fixture.owner;

  std::atomic<int> committed{0};
  std::atomic<int> refused{0};
  std::atomic<bool> stop{false};

  std::jthread worker([&]() {
    Publisher identity = make_publisher(0x8000);
    for (int iteration = 0; iteration < 200000 && !stop.load(); ++iteration) {
      OccupancySample sample{};
      sample.queue = queue;
      sample.queue_generation = generation;
      sample.bytes = 32;
      sample.epoch = epoch;
      const Provenance provenance = identity.next(*fixture.fabric);
      sample.sequence = provenance.sequence;
      sample.provenance = provenance;
      MutationContext context{};
      context.principal = owner;
      context.provenance = provenance;
      context.epoch = epoch;
      auto outcome = fixture.fabric->ingest_occupancy(context, sample);
      if (outcome.ok()) {
        committed.fetch_add(1);
      } else if (outcome.status().code() == Code::shutting_down) {
        refused.fetch_add(1);
        return;
      }
    }
  });

  while (committed.load() < 32) {
    std::this_thread::yield();
  }
  QF_REQUIRE(fixture.fabric->shutdown().ok());
  worker.join();
  stop.store(true);

  QF_CHECK(committed.load() > 0);
  QF_CHECK(refused.load() > 0);
  const auto stats = fixture.fabric->stats();
  QF_CHECK_EQ(stats.occupancy_accepted, static_cast<std::uint64_t>(committed.load()));
  QF_CHECK_EQ(stats.pending_attempts, 0ull);
  QF_CHECK_EQ(fixture.fabric->pending_plans().size(), 0ull);
  QF_CHECK(fixture.fabric->shutting_down());
}

QF_TEST(concurrency, parallel_epoch_advance_is_serialised_and_monotonic) {
  Fixture fixture{};
  constexpr int kThreads = 6;
  std::atomic<int> successes{0};
  const Epoch start = fixture.fabric->epoch();
  const OwnerId owner = fixture.owner;

  std::vector<std::jthread> threads;
  threads.reserve(kThreads);
  for (int index = 0; index < kThreads; ++index) {
    threads.emplace_back([&, index]() {
      Publisher identity = make_publisher(0x900 + static_cast<std::uint64_t>(index));
      auto advanced = fixture.fabric->advance_epoch(owner, identity.next(*fixture.fabric));
      if (advanced.ok()) {
        successes.fetch_add(1);
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
  QF_CHECK_EQ(successes.load(), kThreads);
  QF_CHECK_EQ(fixture.fabric->epoch().value(), start.value() + kThreads);
}
