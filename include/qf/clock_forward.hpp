// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
#pragma once

// Convenience header for translation units that need the clock interface but
// not the full fabric. Kept separate so transport-facing headers stay small.
#include "qf/time.hpp"
