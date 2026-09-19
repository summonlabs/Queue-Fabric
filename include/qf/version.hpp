// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <cstdint>
#include <string_view>

#define QF_VERSION_MAJOR 1
#define QF_VERSION_MINOR 0
#define QF_VERSION_PATCH 0

namespace qf {

/// Semantic version of the Queue Fabric runtime.
inline constexpr int kVersionMajor = QF_VERSION_MAJOR;
inline constexpr int kVersionMinor = QF_VERSION_MINOR;
inline constexpr int kVersionPatch = QF_VERSION_PATCH;

/// Version string, e.g. "1.0.0".
inline constexpr std::string_view kVersionString = "1.0.0";

/// Durable format version. Bumped whenever persistent state changes shape in a
/// way that older readers cannot interpret. Persisted state carries this value
/// and recovery refuses formats it does not understand.
inline constexpr std::uint16_t kDurableFormatVersion = 1;

/// Wire protocol version for framed coordinator/worker transport.
inline constexpr std::uint16_t kProtocolVersion = 1;

}  // namespace qf
