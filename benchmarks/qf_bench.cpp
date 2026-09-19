// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
//
// Synthetic queue-state mutation and evaluation throughput harness.
//
// Everything measured here is completed work: a mutation counts only after the
// fabric has committed it (or rejected it) through its full governance path.
// Populations are synthetic; these numbers say nothing about physical networks.
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <system_error>
#include <cstdlib>
#include <string>
#include <thread>
#include <vector>

#include "qf/fabric.hpp"
#include "qf/rng.hpp"

namespace {

using Clock = std::chrono::steady_clock;

double seconds_since(Clock::time_point start) {
  return std::chrono::duration<double>(Clock::now() - start).count();
}

struct Label {
  std::string name;
  std::string classification;
};

void report(const Label& label, const char* scenario, std::uint64_t operations, double seconds, std::uint64_t extra) {
  const double rate = seconds > 0 ? static_cast<double>(operations) / seconds : 0.0;
  std::printf("%-28s %-34s ops=%-10llu seconds=%-8.3f ops_per_second=%-14.0f %llu\n", scenario,
              label.classification.c_str(), static_cast<unsigned long long>(operations), seconds, rate,
              static_cast<unsigned long long>(extra));
  std::fflush(stdout);
}

struct Population {
  std::unique_ptr<qf::Fabric> fabric{};
  qf::ManualClock clock{};
  qf::ResourceId resource{};
  qf::ClassId class_id{};
  qf::Generation class_generation{};
  qf::PoolId pool_id{};
  qf::Generation pool_generation{};
  qf::OwnerId owner{qf::OwnerId::from_value(1)};
  qf::PublisherId publisher{qf::PublisherId::from_value(2)};
  std::uint64_t sequence{0};

  qf::Provenance next() {
    ++sequence;
    qf::Provenance provenance{};
    provenance.publisher = publisher;
    provenance.node = qf::NodeId::from_value(3);
    provenance.incarnation = qf::Incarnation::from_value(1);
    provenance.epoch = fabric->epoch();
    provenance.sequence = qf::Sequence::from_value(sequence);
    provenance.boot = qf::BootId{1, 1};
    return provenance;
  }
};

void bootstrap(Population& population, std::uint64_t classes, std::uint64_t pools) {
  qf::FabricConfig config{};
  config.boot_seed = 0x5150ull;
  population.fabric = std::make_unique<qf::Fabric>(config, population.clock, nullptr, nullptr);
  if (!population.fabric->recover().ok()) {
    std::fprintf(stderr, "recover failed\n");
    std::exit(2);
  }
  auto resource = population.fabric->declare_resource("fabric0", population.owner, population.next());
  population.resource = resource.value();
  for (std::uint64_t index = 0; index < classes; ++index) {
    auto cls = population.fabric->declare_class(population.resource, "class" + std::to_string(index),
                                                static_cast<std::uint32_t>(index % 8), qf::QosRef{}, population.owner,
                                                population.fabric->epoch(), population.next());
    if (index == 0) {
      population.class_id = cls.value();
    }
  }
  for (std::uint64_t index = 0; index < pools; ++index) {
    auto pool = population.fabric->declare_pool(population.resource, "pool" + std::to_string(index), 1u << 20,
                                                population.owner, population.fabric->epoch(), population.next());
    if (index == 0) {
      population.pool_id = pool.value();
    }
  }
  const auto record = population.fabric->resource_record(population.resource);
  population.class_generation = record.value().classes.front().generation;
  population.pool_generation = record.value().pools.front().generation;
}

std::vector<qf::QueueId> declare_queues(Population& population, std::uint64_t count) {
  std::vector<qf::QueueId> queues;
  queues.reserve(static_cast<std::size_t>(count));
  for (std::uint64_t index = 0; index < count; ++index) {
    qf::MutationContext ctx{};
    ctx.principal = population.owner;
    ctx.provenance = population.next();
    ctx.epoch = population.fabric->epoch();
    qf::Thresholds thresholds{};
    thresholds.low_watermark_bytes = 1024;
    thresholds.high_watermark_bytes = 4096;
    thresholds.max_occupancy_bytes = 8192;
    thresholds.max_packets = 4096;
    auto outcome = population.fabric->declare_queue(
        ctx, population.resource, "q" + std::to_string(index), 64,
        qf::ClassBinding{population.class_id, population.class_generation},
        qf::PoolRef{population.pool_id, population.pool_generation, 1u << 20}, thresholds);
    if (!outcome.ok()) {
      std::fprintf(stderr, "declare failed at %llu: %s\n", static_cast<unsigned long long>(index),
                   outcome.status().to_string().c_str());
      std::exit(3);
    }
    queues.push_back(outcome.value().queue);
  }
  return queues;
}

qf::QueueId activate(Population& population, qf::QueueId queue, qf::Generation generation) {
  qf::MutationContext ctx{};
  ctx.principal = population.owner;
  ctx.provenance = population.next();
  ctx.epoch = population.fabric->epoch();
  auto validated = population.fabric->validate_queue(ctx, queue, generation);
  if (!validated.ok()) {
    std::exit(4);
  }
  ctx.provenance = population.next();
  auto activated = population.fabric->activate_queue(ctx, queue, generation);
  if (!activated.ok()) {
    std::exit(5);
  }
  return queue;
}

/// Scenario 1: occupancy mutation throughput over a synthetic queue population.
void scenario_occupancy_fan_in(const std::vector<std::uint64_t>& counts) {
  for (const std::uint64_t count : counts) {
    Population population{};
    bootstrap(population, 1, 1);
    const std::vector<qf::QueueId> queues = declare_queues(population, count);
    for (const auto& queue : queues) {
      activate(population, queue, qf::Generation::from_value(1));
    }
    const std::uint64_t rounds = 4;
    const auto start = Clock::now();
    std::uint64_t applied = 0;
    for (std::uint64_t round = 0; round < rounds; ++round) {
      for (const auto& queue : queues) {
        qf::MutationContext ctx{};
        ctx.principal = population.owner;
        ctx.provenance = population.next();
        ctx.epoch = population.fabric->epoch();
        qf::OccupancySample sample{};
        sample.queue = queue;
        sample.queue_generation = qf::Generation::from_value(1);
        sample.bytes = 512 + (round % 4) * 64;
        sample.packets = 4;
        sample.epoch = population.fabric->epoch();
        sample.sequence = ctx.provenance.sequence;
        sample.provenance = ctx.provenance;
        sample.observed_at = population.clock.now_nanos();
        auto outcome = population.fabric->ingest_occupancy(ctx, sample);
        if (outcome.ok()) {
          ++applied;
        }
      }
    }
    const double seconds = seconds_since(start);
    report({"synthetic occupancy mutation", "SYNTHETIC (in-process, synthetic queues)"},
           "occupancy fan-in", applied, seconds, count);
  }
}

/// Scenario 2: evaluation (explanation) throughput over many classes.
void scenario_evaluation(const std::vector<std::uint64_t>& class_counts) {
  for (const std::uint64_t classes : class_counts) {
    Population population{};
    bootstrap(population, classes, 1);
    const std::vector<qf::QueueId> queues = declare_queues(population, 64);
    for (const auto& queue : queues) {
      activate(population, queue, qf::Generation::from_value(1));
    }
    const std::uint64_t evaluations = 20000;
    const auto start = Clock::now();
    std::uint64_t completed = 0;
    for (std::uint64_t index = 0; index < evaluations; ++index) {
      auto explanation = population.fabric->explain(queues[static_cast<std::size_t>(index % queues.size())]);
      if (explanation.ok()) {
        const std::string rendered = explanation.value().render();
        if (!rendered.empty()) {
          ++completed;
        }
      }
    }
    const double seconds = seconds_since(start);
    report({"synthetic state evaluation", "SYNTHETIC (in-process, synthetic classes)"}, "state evaluation", completed,
           seconds, classes);
  }
}

/// Scenario 3: full lifecycle churn (declare, validate, activate, acknowledge,
/// drain, evidence, quiesce, retire) with real evidence at every step.
void scenario_lifecycle_churn(std::uint64_t rounds) {
  Population population{};
  bootstrap(population, 1, 1);
  qf::MutationContext backend_ctx{};
  backend_ctx.principal = population.owner;
  backend_ctx.provenance = population.next();
  backend_ctx.epoch = population.fabric->epoch();
  auto backend = population.fabric->register_backend(population.resource, qf::Incarnation::from_value(5),
                                                     population.owner, population.fabric->epoch(),
                                                     backend_ctx.provenance);
  if (!backend.ok()) {
    std::exit(6);
  }

  const auto start = Clock::now();
  std::uint64_t transitions = 0;
  for (std::uint64_t round = 0; round < rounds; ++round) {
    qf::MutationContext ctx{};
    ctx.principal = population.owner;
    ctx.provenance = population.next();
    ctx.epoch = population.fabric->epoch();
    auto declared = population.fabric->declare_queue(
        ctx, population.resource, "churn" + std::to_string(round), 16,
        qf::ClassBinding{population.class_id, population.class_generation},
        qf::PoolRef{population.pool_id, population.pool_generation, 1u << 20}, qf::Thresholds{});
    if (!declared.ok()) {
      std::exit(7);
    }
    const qf::QueueId queue = declared.value().queue;
    const qf::Generation generation = declared.value().generation;
    ++transitions;

    ctx.provenance = population.next();
    if (population.fabric->validate_queue(ctx, queue, generation).ok()) {
      ++transitions;
    }
    ctx.provenance = population.next();
    if (population.fabric->activate_queue(ctx, queue, generation).ok()) {
      ++transitions;
    }

    qf::BackendApplied applied{};
    applied.known = true;
    applied.backend = backend.value();
    applied.backend_incarnation = qf::Incarnation::from_value(5);
    applied.epoch = population.fabric->epoch();
    applied.applied_class = qf::ClassBinding{population.class_id, population.class_generation};
    applied.queue_generation = generation;
    ctx.provenance = population.next();
    if (population.fabric->acknowledge_backend(ctx, queue, generation, applied).ok()) {
      ++transitions;
    }

    ctx.provenance = population.next();
    if (population.fabric->request_drain(ctx, queue, generation).ok()) {
      ++transitions;
    }

    qf::OccupancySample zero{};
    zero.queue = queue;
    zero.queue_generation = generation;
    zero.bytes = 0;
    zero.packets = 0;
    zero.epoch = population.fabric->epoch();
    ctx.provenance = population.next();
    zero.sequence = ctx.provenance.sequence;
    zero.provenance = ctx.provenance;
    zero.observed_at = population.clock.now_nanos();
    if (population.fabric->ingest_occupancy(ctx, zero).ok()) {
      ++transitions;
    }

    qf::DrainEvidence evidence{};
    evidence.records = 2;
    evidence.backend.request_accepted = true;
    evidence.backend.reported_drained = true;
    evidence.backend.backend = backend.value();
    evidence.backend.backend_incarnation = qf::Incarnation::from_value(5);
    evidence.backend.epoch = population.fabric->epoch();
    evidence.occupancy_zero_observed = true;
    evidence.zero_observed_at = zero.observed_at;
    evidence.zero_sequence = zero.sequence;
    ctx.provenance = population.next();
    if (population.fabric->submit_drain_evidence(ctx, queue, generation, evidence).ok()) {
      ++transitions;
    }

    ctx.provenance = population.next();
    if (population.fabric->quiesce_queue(ctx, queue, generation).ok()) {
      ++transitions;
    }
    ctx.provenance = population.next();
    if (population.fabric->retire_queue(ctx, queue, generation).ok()) {
      ++transitions;
    }
  }
  const double seconds = seconds_since(start);
  report({"synthetic lifecycle churn", "SYNTHETIC (in-process, synthetic queues)"}, "lifecycle churn", rounds, seconds,
         transitions);
  std::printf("    lifecycle note: %llu committed transitions across %llu complete lifecycles, %llu queues retired\n",
              static_cast<unsigned long long>(transitions), static_cast<unsigned long long>(rounds),
              static_cast<unsigned long long>(population.fabric->stats().queues));
}

/// Scenario 4: durable commit throughput with a real journal and fsync.
void scenario_durable(const std::string& directory, std::uint64_t operations) {
  // A fresh journal per run so the harness is repeatable; the file is removed
  // again when the scenario completes.
  const std::string journal_path =
      directory + "/bench-" + std::to_string(static_cast<unsigned long long>(std::chrono::steady_clock::now().time_since_epoch().count())) + ".qfjournal";
  qf::JournalConfig journal_config{};
  journal_config.path = journal_path;
  journal_config.sync_on_commit = true;
  qf::FileJournal journal{journal_config};
  qf::ManualClock clock{};
  qf::FabricConfig config{};
  config.durable = true;
  qf::Fabric fabric{config, clock, &journal, nullptr};
  if (!fabric.recover().ok()) {
    std::fprintf(stderr, "durable recover failed\n");
    std::exit(7);
  }
  Population population{};
  population.fabric = nullptr;
  const qf::OwnerId owner = qf::OwnerId::from_value(1);
  qf::PublisherId publisher = qf::PublisherId::from_value(2);
  std::uint64_t sequence = 0;
  const auto next = [&]() {
    ++sequence;
    qf::Provenance provenance{};
    provenance.publisher = publisher;
    provenance.node = qf::NodeId::from_value(3);
    provenance.incarnation = qf::Incarnation::from_value(1);
    provenance.epoch = fabric.epoch();
    provenance.sequence = qf::Sequence::from_value(sequence);
    provenance.boot = qf::BootId{1, 1};
    return provenance;
  };
  auto resource = fabric.declare_resource("fabric0", owner, next());
  if (!resource.ok()) {
    std::exit(8);
  }
  auto cls = fabric.declare_class(resource.value(), "gold", 1, qf::QosRef{}, owner, fabric.epoch(), next());
  auto pool = fabric.declare_pool(resource.value(), "pool0", 1u << 20, owner, fabric.epoch(), next());
  if (!cls.ok() || !pool.ok()) {
    std::exit(9);
  }
  const auto record = fabric.resource_record(resource.value());
  const qf::ClassBinding binding{record.value().classes.front().id, record.value().classes.front().generation};
  const qf::PoolRef pool_ref{record.value().pools.front().id, record.value().pools.front().generation, 1u << 20};

  const auto start = Clock::now();
  std::uint64_t committed = 0;
  for (std::uint64_t index = 0; index < operations; ++index) {
    qf::MutationContext ctx{};
    ctx.principal = owner;
    ctx.provenance = next();
    ctx.epoch = fabric.epoch();
    auto declared = fabric.declare_queue(ctx, resource.value(), "d" + std::to_string(index), 8, binding, pool_ref,
                                         qf::Thresholds{});
    if (!declared.ok()) {
      std::fprintf(stderr, "durable declare failed: %s\n", declared.status().to_string().c_str());
      std::exit(10);
    }
    const qf::QueueId queue = declared.value().queue;
    ctx.provenance = next();
    if (fabric.validate_queue(ctx, queue, declared.value().generation).ok()) {
      ++committed;
    }
  }
  const double seconds = seconds_since(start);
  report({"synthetic durable commit", "SYNTHETIC (real fsync journal on local storage)"}, "durable commit", operations,
         seconds, committed);
  std::printf("    durable note: %llu fsync-backed transactions, journal=%s\n",
              static_cast<unsigned long long>(fabric.stats().journal_records),
              std::filesystem::path(journal_path).filename().string().c_str());
  fabric.shutdown();
  journal.close();
  std::error_code cleanup_error;
  std::filesystem::remove(journal_path, cleanup_error);
}

/// Scenario 5: evaluation under concurrent occupancy mutation.
void scenario_concurrent(std::uint64_t threads_count, std::uint64_t operations_per_thread) {
  Population population{};
  bootstrap(population, 1, 1);
  const std::vector<qf::QueueId> queues = declare_queues(population, 16);
  for (const auto& queue : queues) {
    activate(population, queue, qf::Generation::from_value(1));
  }
  std::atomic<std::uint64_t> completed{0};
  const auto start = Clock::now();
  std::vector<std::jthread> threads;
  threads.reserve(static_cast<std::size_t>(threads_count));
  for (std::uint64_t index = 0; index < threads_count; ++index) {
    threads.emplace_back([&, index]() {
      qf::PublisherId publisher = qf::PublisherId::from_value(0x100 + index);
      std::uint64_t sequence = 0;
      for (std::uint64_t operation = 0; operation < operations_per_thread; ++operation) {
        ++sequence;
        qf::Provenance provenance{};
        provenance.publisher = publisher;
        provenance.node = qf::NodeId::from_value(0x200 + index);
        provenance.incarnation = qf::Incarnation::from_value(1);
        provenance.epoch = population.fabric->epoch();
        provenance.sequence = qf::Sequence::from_value(sequence);
        provenance.boot = qf::BootId{index + 1, 0x55};
        qf::MutationContext ctx{};
        ctx.principal = population.owner;
        ctx.provenance = provenance;
        ctx.epoch = population.fabric->epoch();
        qf::OccupancySample sample{};
        sample.queue = queues[static_cast<std::size_t>(operation % queues.size())];
        sample.queue_generation = qf::Generation::from_value(1);
        sample.bytes = 128;
        sample.packets = 1;
        sample.epoch = population.fabric->epoch();
        sample.sequence = provenance.sequence;
        sample.provenance = provenance;
        sample.observed_at = population.clock.now_nanos();
        if (population.fabric->ingest_occupancy(ctx, sample).ok()) {
          completed.fetch_add(1);
        }
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
  const double seconds = seconds_since(start);
  report({"synthetic concurrent mutation", "SYNTHETIC (real threads, synthetic queues)"}, "concurrent mutation",
         completed.load(), seconds, threads_count);
}

}  // namespace

int main(int argc, char** argv) {
  std::string directory = ".";
  for (int index = 1; index < argc; ++index) {
    const std::string argument = argv[index];
    if (argument == "--dir" && index + 1 < argc) {
      directory = argv[++index];
    }
  }
  std::printf("Queue Fabric benchmark harness\n");
  std::printf("classification: all measurements are SYNTHETIC in-process populations\n");
  std::printf("no physical network, NIC, switch, RDMA or hardware behaviour is measured\n\n");

  scenario_occupancy_fan_in({1000, 10000, 50000});
  scenario_evaluation({1, 64, 1024});
  scenario_lifecycle_churn(2000);
  scenario_durable(directory, 400);
  scenario_concurrent(8, 20000);
  std::printf("\nbenchmark complete\n");
  return 0;
}
