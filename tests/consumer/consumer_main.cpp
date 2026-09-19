// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
//
// Downstream consumer of the installed Queue Fabric package. It exercises the
// documented public surface only: topology, queue lifecycle, occupancy
// ingestion, explanation, invariants and the framed transport headers.
#include <cstdio>
#include <string>
#include <vector>

#include "qf/coordinator.hpp"
#include "qf/fabric.hpp"
#include "qf/frame.hpp"
#include "qf/journal.hpp"
#include "qf/version.hpp"
#include "qf/worker.hpp"

int main() {
  using namespace qf;

  if (std::string(kVersionString) != "1.0.0") {
    std::printf("unexpected version string\n");
    return 1;
  }

  ManualClock clock{};
  FabricConfig config{};
  config.boot_seed = 0xC0FFEE;
  Fabric fabric{config, clock, nullptr, nullptr};
  if (!fabric.recover().ok()) {
    std::printf("recover failed\n");
    return 1;
  }

  const OwnerId owner = OwnerId::from_value(0x1234);
  Provenance provenance{};
  provenance.publisher = PublisherId::from_value(1);
  provenance.node = NodeId::from_value(1);
  provenance.incarnation = Incarnation::from_value(1);
  provenance.epoch = fabric.epoch();
  provenance.sequence = Sequence::from_value(1);
  provenance.boot = BootId{1, 1};

  auto resource = fabric.declare_resource("consumer0", owner, provenance);
  if (!resource.ok()) {
    std::printf("declare_resource failed: %s\n", resource.status().to_string().c_str());
    return 1;
  }
  provenance.sequence = Sequence::from_value(2);
  auto cls = fabric.declare_class(resource.value(), "gold", 5, QosRef{}, owner, fabric.epoch(), provenance);
  provenance.sequence = Sequence::from_value(3);
  auto pool = fabric.declare_pool(resource.value(), "pool0", 1u << 20, owner, fabric.epoch(), provenance);
  if (!cls.ok() || !pool.ok()) {
    std::printf("topology declaration failed\n");
    return 1;
  }
  const auto resource_record = fabric.resource_record(resource.value());
  if (!resource_record.ok()) {
    return 1;
  }

  MutationContext context{};
  context.principal = owner;
  context.epoch = fabric.epoch();
  provenance.sequence = Sequence::from_value(4);
  context.provenance = provenance;

  Thresholds thresholds{};
  thresholds.low_watermark_bytes = 1024;
  thresholds.high_watermark_bytes = 4096;
  thresholds.max_occupancy_bytes = 8192;
  thresholds.max_packets = 1024;

  auto declared = fabric.declare_queue(
      context, resource.value(), "consumer-q0", 32,
      ClassBinding{resource_record.value().classes.front().id, resource_record.value().classes.front().generation},
      PoolRef{resource_record.value().pools.front().id, resource_record.value().pools.front().generation, 1u << 20},
      thresholds);
  if (!declared.ok()) {
    std::printf("declare_queue failed: %s\n", declared.status().to_string().c_str());
    return 1;
  }
  const QueueId queue = declared.value().queue;
  const Generation generation = declared.value().generation;

  provenance.sequence = Sequence::from_value(5);
  context.provenance = provenance;
  if (!fabric.validate_queue(context, queue, generation).ok()) {
    return 1;
  }
  provenance.sequence = Sequence::from_value(6);
  context.provenance = provenance;
  if (!fabric.activate_queue(context, queue, generation).ok()) {
    return 1;
  }

  OccupancySample sample{};
  sample.queue = queue;
  sample.queue_generation = generation;
  sample.bytes = 2048;
  sample.packets = 8;
  sample.epoch = fabric.epoch();
  provenance.sequence = Sequence::from_value(7);
  sample.sequence = provenance.sequence;
  sample.provenance = provenance;
  sample.observed_at = clock.now_nanos();
  context.provenance = provenance;
  if (!fabric.ingest_occupancy(context, sample).ok()) {
    return 1;
  }

  auto explanation = fabric.explain(queue, owner);
  if (!explanation.ok()) {
    return 1;
  }
  const std::string rendered = explanation.value().render();
  if (rendered.find("lifecycle=active") == std::string::npos) {
    std::printf("explanation did not report the active lifecycle\n");
    return 1;
  }
  if (!explanation.value().threshold_eval.admission_authorized) {
    std::printf("fresh occupancy did not authorize admission\n");
    return 1;
  }

  std::vector<std::string> violations;
  if (fabric.validate_invariants(violations) != 0) {
    std::printf("invariant violation reported by the fabric\n");
    return 1;
  }

  // The transport and protocol headers are part of the installed surface too.
  const Status network = network_init();
  if (!network.ok()) {
    std::printf("network subsystem unavailable\n");
    return 1;
  }
  Frame frame{};
  frame.type = 1;
  frame.sequence = 1;
  frame.payload = Bytes{std::byte{1}, std::byte{2}};
  const Bytes encoded = encode_frame(frame);
  FrameDecoder decoder{};
  if (encoded.empty() || !decoder.push(encoded).ok()) {
    return 1;
  }
  auto decoded = decoder.next();
  if (!decoded.ok() || decoded.value().payload.size() != 2) {
    return 1;
  }
  network_shutdown();

  std::printf("consumer ok: version=%s queues=%llu lifecycle=%s band=%s\n", std::string(kVersionString).c_str(),
              static_cast<unsigned long long>(fabric.stats().queues), to_string(explanation.value().lifecycle),
              to_string(explanation.value().threshold_eval.band));
  return 0;
}
