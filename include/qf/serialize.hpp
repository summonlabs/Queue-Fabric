// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <cstdint>
#include <span>

#include "qf/bytes.hpp"
#include "qf/fabric.hpp"
#include "qf/journal.hpp"
#include "qf/mutation.hpp"
#include "qf/queue.hpp"

namespace qf {

/// Durable epoch/boot record. Persisted so that a restart is always observable
/// as an epoch advance, and so a worker from a previous coordinator life can be
/// told apart from a current one.
struct EpochState {
  BootId boot{};
  Epoch epoch{};
  Incarnation coordinator_incarnation{};
};

Bytes encode_resource_state(const ResourceRecord& resource);
Result<ResourceRecord> decode_resource_state(std::span<const std::byte> data);

Bytes encode_queue_state(const QueueRecord& queue);
Result<QueueRecord> decode_queue_state(std::span<const std::byte> data);

Bytes encode_mutation(const MutationRequest& request);
Result<MutationRequest> decode_mutation(std::span<const std::byte> data);

Bytes encode_epoch_state(const EpochState& state);
Result<EpochState> decode_epoch_state(std::span<const std::byte> data);

void encode_provenance(ByteWriter& writer, const Provenance& provenance);
Provenance decode_provenance(ByteReader& reader);

}  // namespace qf
