// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <cstddef>
#include <cstdint>

namespace qf::limits {

/// Every externally influenced collection, payload and explanation is bounded.
/// These are hard limits enforced by the runtime, not documentation hints.

// Fabric topology.
inline constexpr std::size_t kMaxResources = 4096;
inline constexpr std::size_t kMaxQueuesPerResource = 65536;
inline constexpr std::size_t kMaxQueuesTotal = kMaxResources * std::size_t{16};
inline constexpr std::size_t kMaxClassesPerResource = 4096;
inline constexpr std::size_t kMaxPoolsPerResource = 4096;
inline constexpr std::size_t kMaxGrantsPerResource = 256;
inline constexpr std::size_t kMaxPolicies = 1024;

// Identifiers and text.
inline constexpr std::size_t kMaxNameChars = 128;
inline constexpr std::size_t kMaxMessageChars = 192;
inline constexpr std::size_t kMaxReasonChars = 96;
inline constexpr std::size_t kMaxProvenanceNodes = 4096;

// Mutation governance.
inline constexpr std::size_t kMaxAttemptMemoPerQueue = 64;
inline constexpr std::size_t kMaxPendingAttempts = 4096;
inline constexpr std::size_t kMaxBatchedMutations = 4096;

// Occupancy.
inline constexpr std::size_t kMaxOccupancyBatch = 4096;
inline constexpr std::size_t kMaxDrainEvidenceRecords = 16;

// Durability.
inline constexpr std::size_t kMaxJournalRecordBytes = 1u << 20;   // 1 MiB
inline constexpr std::size_t kMaxSnapshotQueues = kMaxQueuesTotal;
/// Journal size ceiling. Kept 64-bit so the bound is identical on 32- and
/// 64-bit targets (a 32-bit size_t cannot represent it).
inline constexpr std::uint64_t kMaxJournalFileBytes = 1ull << 32;  // 4 GiB hard stop

// Transport.
inline constexpr std::size_t kMaxFramePayloadBytes = 1u << 20;  // 1 MiB
inline constexpr std::size_t kMaxFrameBufferBytes = (1u << 20) + 64;
inline constexpr std::size_t kMaxConnections = 256;
inline constexpr std::size_t kMaxWorkerBacklog = 1024;
inline constexpr std::size_t kMaxSubscribeQueues = 4096;

// Explanation surface.
inline constexpr std::size_t kMaxExplainEntries = 128;
inline constexpr std::size_t kMaxExplainBytes = 16u * 1024u;

// Timing (nanoseconds).
inline constexpr std::int64_t kNanosPerSecond = 1000000000;
inline constexpr std::int64_t kMaxFreshnessWindowNanos = 24ll * 3600ll * kNanosPerSecond;
inline constexpr std::int64_t kDefaultMaxFutureSkewNanos = 1000000000ll;  // 1 s

}  // namespace qf::limits
