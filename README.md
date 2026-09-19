# Queue Fabric

Open-source, vendor-neutral C++20 runtime for generation-bound queue lifecycle, occupancy,
scheduling-class authority, thresholds, ownership, and queue-state governance across fabric
resources.

Queue Fabric answers one question with evidence instead of convention:

> Given authoritative queue definitions, resource ownership, scheduling classes, occupancy
> evidence, thresholds, policy and current generations, which queues exist, what state is
> authoritative, who may mutate them, and when must a queue be drained, fenced, revalidated,
> retired, or rejected as stale?

Version 1.0.0. Apache License 2.0.

## Systems boundary

Queue Fabric owns:

* queue identity, scope and uniqueness;
* queue lifecycle state and its legal transitions;
* scheduling-class binding at exact generation, and buffer-pool references;
* occupancy-state ingestion, freshness classification and threshold evaluation;
* ownership, capability authority vectors and epochs;
* mutation governance: validation, idempotency, fencing, journalling, commit;
* drain requests, drain evidence and quiescence proof;
* explanation of every authoritative decision.

Queue Fabric does **not** own and does not implement: packet scheduling algorithms, bandwidth
arbitration, rate enforcement, shared buffer allocation, congestion synthesis, microburst,
incast or hotspot logic, pacing, backpressure, topology or path authority, or physical device
programming. Scheduling classes are stored as policy references; queue depth is a configured
attribute; a backend acknowledgement is recorded as a claim, never as applied effect. Device
programming is reachable only through an explicit backend identity.

## Model

Strong types separate every identity kind: `ResourceId`, `QueueId`,
`ClassId`, `PoolId`, `OwnerId`, `BackendId`,
`PublisherId`, `NodeId`, `AttemptId`, plus the counters
`Generation`, `Epoch`, `Sequence`, `Incarnation` and the
`BootId` of a process life. A queue identity cannot be passed where a resource identity
is required, and a generation cannot be assigned to an epoch.

Lifecycle states: DECLARED, VALIDATED, ACTIVE, DRAINING, QUIESCED, RETIRED, FAILED. Applicability
flags STALE and FENCED are orthogonal to lifecycle: a queue can be ACTIVE and STALE, or DRAINING
and FENCED.

The transition table is closed. Anything not listed is refused with a named rule, retirement is
terminal, an active queue must drain before it can be quiesced or retired, and quiescence is
granted only from DRAINING with complete evidence.

## Invariants enforced by the runtime

1. Queue identity is unique within its resource scope; names are never recycled.
2. Class binding is exact-generation: a binding that does not name the current class generation
   cannot validate, activate or rebind.
3. Retired and fenced queues refuse fresh mutation, with a named rule and an explanation.
4. Stale occupancy cannot authorize any decision: threshold evaluation returns band UNKNOWN and
   refuses admission whenever evidence is absent, aged out, generation-mismatched,
   epoch-superseded, backend-invalidated or restored after a restart.
5. Drain completion requires evidence: an accepted backend request, a drained report from the
   live backend incarnation at the current epoch, and a fresh zero-occupancy observation taken
   after the request. "Drain requested" is never "drained".
6. Duplicate mutation attempts are idempotent: a repeated attempt returns the original outcome
   and never re-applies. Reusing an attempt identity with different content is a conflict.
7. Occupancy never silently overflows: an observation above the configured maximum is recorded
   exactly as observed, counted as an overflow event, reported through the event sink, and never
   admitted.
8. Backend acknowledgement is not applied effect: an acknowledgement is verifiable only against
   the live backend incarnation, the current epoch, the current queue generation and the current
   class binding.
9. Every accepted mutation and every rejection records the named rule that produced it, and
   `Fabric::explain` renders identity, lifecycle, binding, occupancy freshness,
   thresholds, ownership, authority vector, pending mutation, drain assessment and stale/fence
   reasons within a bounded size.

## Durability

Durable state is a versioned, CRC-32C checked, append-only journal with a 40-byte header
(magic, format version, boot, epoch, header checksum) and CRC-checked records. The mutation path
is write-ahead: validate, bind authority, plan, reserve, journal a begin record, journal the
commit record, `fsync`, then publish in memory and only then acknowledge. A mutation
that cannot be made durable is refused with `not_durable` and leaves no partial effect.

Recovery distinguishes:

* durable configuration and history (resources, classes, pools, ownership, authority, fences);
* committed authoritative state (queue definitions, lifecycle, generations, memos);
* unfinished attempts (a durable begin with no durable commit) reported as ambiguous, with the
  affected queue marked as requiring revalidation;
* evidence requiring revalidation: restored occupancy is marked `stale_restart`, drain
  evidence loses its revalidation flag, backend incarnations are never restored as live.

A torn tail is dropped and reported. Mid-file corruption is refused outright: the fabric fails
closed rather than starting from a partially trusted journal. Compaction rewrites the store
atomically through a temporary file plus rename and refuses to run while a backend attempt is
reserved.

Restart always advances the coordinator epoch, so every acknowledgement and observation bound to
the previous epoch stops being verifiable.

## Distributed operation

Where distributed, Queue Fabric uses real OS processes and a real framed TCP transport: a length
prefixed frame with magic, protocol version, sequence and CRC-32C over header and payload, carried
over a socket transport with bounded buffers. Malformed, truncated, oversized and
checksum-failing frames are refused; the decoder never trusts a declared length before bounding it.

A single-threaded coordinator admits workers by `hello` (node, incarnation, boot, epoch),
refuses an absent or superseded epoch, fences a node that reconnects with an older incarnation,
ingests occupancy batches, applies mutations, and detects worker loss by connection close or
liveness deadline. On loss it invalidates the evidence that worker published instead of trusting
it. The coordinator holds no lock while emitting events and never calls into transport or user code
from inside its state critical section.

## Building

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build
ctest --test-dir build --output-on-failure
```

Requirements: CMake 3.25+, a C++20 compiler, Threads. Options:
`QF_BUILD_TESTS`, `QF_BUILD_BENCHMARKS`, `QF_BUILD_TOOLS`,
`QF_WERROR` (default ON), `QF_SANITIZE_ADDRESS`, `QF_ANALYZE`.

The library builds clean under MSVC `/W4 /WX` in Debug, Release and RelWithDebInfo, and
under GCC/Clang `-Wall -Wextra -Wpedantic -Wshadow -Werror`.

## Installing and consuming

```sh
cmake -S . -B build -DCMAKE_INSTALL_PREFIX=/your/prefix
cmake --build build --target install
```

```cmake
find_package(qf 1.0 CONFIG REQUIRED)
target_link_libraries(my_app PRIVATE qf::queue_fabric)
```

`tests/consumer` is an independent downstream project that validates exactly this path;
it is not part of the Queue Fabric build.

## Minimal example

```cpp
qf::ManualClock clock;
qf::Fabric fabric{qf::FabricConfig{}, clock};
fabric.recover();

auto resource = fabric.declare_resource("fabric0", owner, provenance);
auto cls      = fabric.declare_class(resource.value(), "gold", 7, {}, owner, fabric.epoch(), provenance);
auto pool     = fabric.declare_pool(resource.value(), "pool0", 1u << 20, owner, fabric.epoch(), provenance);

qf::MutationContext ctx{owner, provenance, fabric.epoch()};
auto declared = fabric.declare_queue(ctx, resource.value(), "q0", 64,
                                     {cls.value(), qf::Generation::from_value(1)},
                                     {pool.value(), qf::Generation::from_value(1), 1u << 20},
                                     qf::Thresholds{1024, 4096, 8192, 4096, 0});
fabric.validate_queue(ctx, declared.value().queue, declared.value().generation);
fabric.activate_queue(ctx, declared.value().queue, declared.value().generation);

auto explanation = fabric.explain(declared.value().queue, owner);
std::puts(explanation.value().render().c_str());
```

## Tests

`tests/` contains 65 cases and 10911 checks covering units, integration, durability,
concurrency, adversarial input and a real multiprocess proof surface. No test timeout is
configured anywhere; a hang is treated as a defect rather than masked.

* foundation: identities, checked arithmetic, CRC-32C, byte codec, provenance tracking, lifecycle
  table, thresholds, occupancy freshness, authority evaluation order, drain assessment, frame and
  protocol codecs, record serialization;
* lifecycle: full walk to retirement, drain-versus-evidence, illegal transitions, class rebinding
  and threshold updates advancing the definition generation;
* governance: capability enforcement, epoch and fence gating, idempotent replay and conflicting
  reuse, occupancy rules and overflow reporting, evidence invalidation, epoch advance, backend
  verification, two-phase application, explanation completeness, self-audit invariants, capacity
  bounds;
* durability: restart survival, retirement across restart, unfinished-attempt ambiguity, torn
  tails, header damage, mid-file corruption, compaction, failed sync never acknowledging, journal
  requirement, shutdown accounting;
* adversarial: seeded frame fuzzing (2000 mutated frames), random message bodies (4000),
  corrupted mutation records (3000), hostile mutations, oversized batches, transport misuse;
* concurrency: 8-thread occupancy ingestion, parallel lifecycle with concurrent explanation,
  retirement racing publications, event-sink re-entrancy, shutdown under load, parallel epoch
  advance;
* property: seeded randomized scripts against a model, determinism of identical scripts, replay
  equivalence of durable state, stale evidence never authorizing, rule naming;
* multiprocess: two workers publishing over TCP, a worker killed hard, a worker presenting a
  stale epoch, coordinator killed hard and restarted with an advanced epoch, post-restart stale
  rejection, fresh worker admitted, graceful shutdown.

## Benchmarks

`benchmarks/qf_bench.cpp` measures completed work only; a mutation is counted after the
fabric has committed or rejected it through the full governance path. Every population is
synthetic and in-process. These numbers say nothing about physical networks or hardware.

Measured on the development machine (MSVC 19.44, Release, single socket, x86-64):

| Scenario | Population | Result |
| --- | --- | --- |
| Occupancy mutation | 1 000 queues | ~2.9 M ops/s |
| Occupancy mutation | 10 000 queues | ~3.1 M ops/s |
| Occupancy mutation | 50 000 queues | ~1.7 M ops/s |
| State evaluation (explain + render) | 1 / 64 / 1024 classes | ~0.3-0.5 M ops/s |
| Lifecycle churn (full lifecycle with evidence) | 2 000 queues | ~148 k lifecycles/s, 18 000 committed transitions |
| Durable commit (real fsync journal) | 400 transactions | ~577 transactions/s, 1 604 records |
| Concurrent mutation | 8 threads, 16 queues | ~1.39 M ops/s |

## Proof surface: REAL, SYNTHETIC, UNSUPPORTED

REAL:

* queue lifecycle, authority, occupancy, threshold, drain, fence and retirement semantics;
* durable journal with CRC-32C integrity, fsync commit, torn-tail and corruption handling,
  compaction, restart recovery and epoch advancement;
* framed TCP transport across real operating-system processes, including hard process kill,
  incarnation fencing and stale-epoch rejection;
* multiprocess tests that spawn the shipped `qf_coordinator` and `qf_worker`
  executables, kill them with TerminateProcess-equivalent semantics and verify recovery;
* install/export package validated by an independent downstream `find_package` consumer;
* AddressSanitizer run of the entire suite (32-bit x86 runtime; see limitations);
* MSVC `/analyze` with zero first-party findings.

SYNTHETIC:

* all benchmark populations and rates; throughput numbers describe in-process data structures;
* occupancy samples, queue populations and lifecycle churn generated by the harness.

UNSUPPORTED (explicitly not implemented, not tested, and not claimed):

* physical switches, NICs, DPUs, RDMA, NVLink, optical or multi-node network validation;
* packet scheduling, rate enforcement, congestion control, pacing or buffer allocation;
* hardware register programming of any kind;
* multi-host coordination beyond the single coordinator plus framed TCP workers demonstrated here;
* queue state restoration for a backend that does not re-register after a restart.

## Limitations

* Occupancy changes are not journalled. A durable snapshot records the last observed occupancy as
  evidence, and every restored observation is marked `stale_restart` and cannot
  authorize. A restart without compaction restores no occupancy at all.
* Changing thresholds, the class binding or ownership advances the queue definition generation and
  therefore requires re-establishing generation-bound evidence. This is deliberate and strict.
* The coordinator is single threaded by design; the fabric itself is thread safe with a single
  internal mutex and never calls user code while holding it.
* AddressSanitizer is exercised through the 32-bit x86 runtime because the Visual Studio
  installation used for validation does not include the x86-64 ASan runtime component. The full
  suite passes there with no findings; a 64-bit sanitizer run is not claimed.
* Static analysis is MSVC `/analyze`. Two findings are reported inside the Windows SDK
  header `ws2tcpip.h` (C6101); no first-party finding was reported.

## Repository layout

```
include/qf/      public headers - one concern per header
src/             implementation
tools/           qf_coordinator and qf_worker executables
tests/           test suite plus the downstream consumer fixture
benchmarks/      synthetic throughput harness
cmake/           package configuration template
```

## License

Apache License 2.0. Copyright 2026 Summon Software Labs. No telemetry transmission.
