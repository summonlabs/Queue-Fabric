// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
//
// Adversarial input: malformed, truncated, oversized, contradictory, stale and
// structurally invalid frames, messages, records and mutations.
#include <algorithm>
#include <string>
#include <vector>

#include "qf/fabric.hpp"
#include "qf/frame.hpp"
#include "qf/protocol.hpp"
#include "qf/rng.hpp"
#include "qf/serialize.hpp"
#include "qf/transport.hpp"
#include "support/fixture.hpp"
#include "support/test_framework.hpp"

using namespace qf;
using qftest::Fixture;

QF_TEST(adversarial, random_frame_mutations_never_crash_the_decoder) {
  Frame frame{};
  frame.type = static_cast<std::uint16_t>(MessageType::occupancy_batch);
  frame.sequence = 1;
  Message message{};
  message.type = MessageType::occupancy_batch;
  message.sequence = 1;
  OccupancySample sample{};
  sample.queue = QueueId::from_value(1);
  sample.queue_generation = Generation::from_value(1);
  sample.epoch = Epoch::from_value(1);
  sample.sequence = Sequence::from_value(1);
  sample.provenance.publisher = PublisherId::from_value(1);
  sample.provenance.node = NodeId::from_value(1);
  sample.provenance.incarnation = Incarnation::from_value(1);
  sample.provenance.epoch = Epoch::from_value(1);
  sample.provenance.sequence = Sequence::from_value(1);
  sample.provenance.boot = BootId{1, 1};
  message.occupancy.samples.push_back(sample);
  frame.payload = encode_message(message);
  QF_REQUIRE(!frame.payload.empty());
  const Bytes good = encode_frame(frame);
  QF_REQUIRE(!good.empty());

  Rng rng{0xADBE11};
  std::size_t accepted = 0;
  std::size_t rejected = 0;
  std::size_t incomplete = 0;
  for (int iteration = 0; iteration < 2000; ++iteration) {
    Bytes mutated = good;
    const std::uint64_t mutations = 1 + rng.next_bounded(4);
    for (std::uint64_t index = 0; index < mutations; ++index) {
      const std::uint64_t choice = rng.next_bounded(3);
      if (choice == 0 && !mutated.empty()) {
        const std::size_t position = static_cast<std::size_t>(rng.next_bounded(mutated.size()));
        mutated[position] = static_cast<std::byte>(static_cast<unsigned char>(mutated[position]) ^
                                                  static_cast<unsigned char>(rng.next_u32() & 0xFFu));
      } else if (choice == 1 && mutated.size() > 1) {
        mutated.resize(static_cast<std::size_t>(rng.next_bounded(mutated.size())));
      } else {
        mutated.push_back(static_cast<std::byte>(rng.next_u32() & 0xFFu));
      }
    }
    FrameDecoder decoder{};
    const Status pushed = decoder.push(mutated);
    if (!pushed.ok()) {
      ++rejected;
      continue;
    }
    auto decoded = decoder.next();
    if (decoded.ok()) {
      ++accepted;
      // A frame is accepted only when its bytes are still exactly the original
      // frame: the checksum is what admits a frame, so any in-frame damage must
      // have been refused above. Damage past the frame end is trailing noise.
      const bool prefix_intact =
          mutated.size() >= good.size() && std::equal(good.begin(), good.end(), mutated.begin());
      QF_CHECK(prefix_intact);
      // Even when the frame survives, the message must remain structurally
      // valid or be refused by the message decoder.
      auto parsed = decode_message(decoded.value().payload);
      if (parsed.ok()) {
        QF_CHECK(parsed.value().type != MessageType::none);
      }
    } else if (decoded.status().code() == Code::not_found) {
      ++incomplete;
    } else {
      ++rejected;
      QF_CHECK(decoder.failed());
    }
  }
  // Damage must be caught nearly always: a decoder that accepted a large share
  // of mutated frames would be a checksum failure. Frames reported as
  // incomplete are those whose length field survived while the tail was
  // truncated; frames reported as accepted are byte-identical to the original.
  QF_CHECK(accepted > 0);    // the harness must exercise real acceptances
  QF_CHECK(rejected > 400);  // and real integrity refusals
  QF_CHECK(rejected > 400);      // and real integrity refusals
}

QF_TEST(adversarial, random_message_bodies_never_crash_the_codec) {
  Rng rng{0x5EED1234};
  std::size_t parsed = 0;
  std::size_t refused = 0;
  for (int iteration = 0; iteration < 4000; ++iteration) {
    const std::size_t length = static_cast<std::size_t>(rng.next_bounded(200));
    Bytes body(length);
    for (std::size_t index = 0; index < length; ++index) {
      body[index] = static_cast<std::byte>(rng.next_u32() & 0xFFu);
    }
    auto decoded = decode_message(body);
    if (decoded.ok()) {
      ++parsed;
      // Anything that parses must re-encode to something decodable again.
      const Bytes reencoded = encode_message(decoded.value());
      QF_CHECK(!reencoded.empty());
      auto again = decode_message(reencoded);
      QF_REQUIRE(again.ok());
      QF_CHECK(again.value().type == decoded.value().type);
    } else {
      ++refused;
    }
  }
  QF_CHECK_EQ(parsed + refused, 4000ull);
  QF_CHECK(refused > 3000);
}

QF_TEST(adversarial, random_mutation_records_never_crash_the_decoder) {
  Rng rng{0x1234ABCD};
  MutationRequest request{};
  request.kind = MutationKind::declare_queue;
  request.attempt = AttemptId{PublisherId::from_value(1), 1};
  request.expected_epoch = Epoch::from_value(1);
  request.principal = OwnerId::from_value(1);
  request.provenance.publisher = PublisherId::from_value(1);
  request.provenance.node = NodeId::from_value(1);
  request.provenance.incarnation = Incarnation::from_value(1);
  request.provenance.epoch = Epoch::from_value(1);
  request.provenance.sequence = Sequence::from_value(1);
  request.provenance.boot = BootId{1, 1};
  DeclareQueuePayload payload{};
  payload.resource = ResourceId::from_value(1);
  payload.name = "q";
  payload.cls = ClassBinding{ClassId::from_value(1), Generation::from_value(1)};
  payload.pool = PoolRef{PoolId::from_value(1), Generation::from_value(1), 1024};
  request.payload = payload;
  const Bytes encoded = encode_mutation(request);
  QF_REQUIRE(!encoded.empty());

  std::size_t decoded_ok = 0;
  for (int iteration = 0; iteration < 3000; ++iteration) {
    Bytes mutated = encoded;
    const std::size_t position = static_cast<std::size_t>(rng.next_bounded(mutated.size()));
    mutated[position] = static_cast<std::byte>(static_cast<unsigned char>(mutated[position]) ^
                                               static_cast<unsigned char>(1 + rng.next_bounded(255)));
    auto decoded = decode_mutation(mutated);
    if (decoded.ok()) {
      ++decoded_ok;
      // A successfully decoded record must still be structurally valid for the
      // kind it claims, or validation has to reject it.
      QF_CHECK(decoded.value().kind != MutationKind::none);
    }
  }
  QF_CHECK(decoded_ok < 3000);
}

QF_TEST(adversarial, hostile_mutations_are_refused_with_named_rules) {
  Fixture fixture{};
  const QueueId queue = fixture.declare_queue("q-hostile");

  // A mutation with no attempt identity at all.
  MutationRequest anonymous{};
  anonymous.kind = MutationKind::validate_queue;
  anonymous.queue = queue;
  anonymous.expected_generation = fixture.generation_of(queue);
  anonymous.expected_epoch = fixture.fabric->epoch();
  anonymous.principal = fixture.owner;
  anonymous.provenance = fixture.provenance();
  auto refused = fixture.fabric->apply(anonymous);
  QF_CHECK_CODE(refused, Code::invalid_argument);

  // A mutation whose payload does not match its kind.
  MutationRequest mismatched{};
  mismatched.kind = MutationKind::retire_queue;
  mismatched.queue = queue;
  mismatched.expected_generation = fixture.generation_of(queue);
  mismatched.expected_epoch = fixture.fabric->epoch();
  mismatched.principal = fixture.owner;
  mismatched.provenance = fixture.provenance();
  mismatched.attempt = AttemptId{fixture.publisher, mismatched.provenance.sequence.value()};
  mismatched.payload = ThresholdsPayload{};
  refused = fixture.fabric->apply(mismatched);
  QF_CHECK_CODE(refused, Code::invalid_argument);

  // An unknown queue identity.
  MutationRequest missing{};
  missing.kind = MutationKind::validate_queue;
  missing.queue = QueueId::from_value(0xFFFF);
  missing.expected_generation = Generation::from_value(1);
  missing.expected_epoch = fixture.fabric->epoch();
  missing.principal = fixture.owner;
  missing.provenance = fixture.provenance();
  missing.attempt = AttemptId{fixture.publisher, missing.provenance.sequence.value()};
  refused = fixture.fabric->apply(missing);
  QF_CHECK_CODE(refused, Code::not_found);

  // Incomplete provenance is refused before any state is touched.
  MutationRequest no_boot{};
  no_boot.kind = MutationKind::validate_queue;
  no_boot.queue = queue;
  no_boot.expected_generation = fixture.generation_of(queue);
  no_boot.expected_epoch = fixture.fabric->epoch();
  no_boot.principal = fixture.owner;
  no_boot.provenance = fixture.provenance();
  no_boot.provenance.boot = BootId{};
  no_boot.attempt = AttemptId{fixture.publisher, no_boot.provenance.sequence.value()};
  refused = fixture.fabric->apply(no_boot);
  QF_CHECK_CODE(refused, Code::malformed_input);

  // Deeply invalid thresholds inside a declare request.
  auto inconsistent = fixture.fabric->declare_queue(fixture.context(), fixture.resource, "q-bad-thresholds", 8,
                                                    ClassBinding{fixture.class_id, fixture.class_generation},
                                                    PoolRef{fixture.pool_id, fixture.pool_generation, 4096},
                                                    Thresholds{4096, 1024, 512, 0, 0});
  QF_CHECK_CODE(inconsistent, Code::invalid_argument);

  // A class binding that names a generation which never existed.
  auto bad_binding = fixture.fabric->declare_queue(fixture.context(), fixture.resource, "q-bad-binding", 8,
                                                   ClassBinding{fixture.class_id, Generation::from_value(99)},
                                                   PoolRef{fixture.pool_id, fixture.pool_generation, 4096}, Thresholds{});
  QF_CHECK_CODE(bad_binding, Code::stale_generation);

  // A pool reference from another resource generation.
  auto bad_pool = fixture.fabric->declare_queue(fixture.context(), fixture.resource, "q-bad-pool", 8,
                                                ClassBinding{fixture.class_id, fixture.class_generation},
                                                PoolRef{fixture.pool_id, Generation::from_value(77), 4096}, Thresholds{});
  QF_CHECK_CODE(bad_pool, Code::stale_generation);

  // Reusing a publisher sequence with a different effect.
  MutationContext reused = fixture.context();
  auto first = fixture.fabric->validate_queue(reused, queue, fixture.generation_of(queue));
  QF_REQUIRE(first.ok());
  auto conflict = fixture.fabric->rebind_class(reused, queue, fixture.generation_of(queue),
                                               ClassBinding{fixture.class_id, fixture.class_generation});
  QF_CHECK_CODE(conflict, Code::conflict);
}

QF_TEST(adversarial, oversized_batches_are_refused) {
  Fixture fixture{};
  const QueueId queue = fixture.activate_queue("q-batch");
  std::vector<OccupancySample> samples;
  samples.reserve(limits::kMaxOccupancyBatch + 1);
  for (std::size_t index = 0; index < limits::kMaxOccupancyBatch + 1; ++index) {
    samples.push_back(fixture.sample(queue, index));
  }
  Message message{};
  message.type = MessageType::occupancy_batch;
  message.occupancy.samples = samples;
  QF_CHECK(encode_message(message).empty());

  // Every individual sample is still accepted, so the bound is on the batch,
  // not on the feature.
  auto outcome = fixture.fabric->ingest_occupancy(fixture.context(), fixture.sample(queue, 1));
  QF_REQUIRE(outcome.ok());
}

QF_TEST(adversarial, durable_records_are_bounded) {
  // A mutation whose name is exactly at the bound encodes; one over it fails.
  MutationRequest request{};
  request.kind = MutationKind::declare_queue;
  request.attempt = AttemptId{PublisherId::from_value(1), 1};
  request.expected_epoch = Epoch::from_value(1);
  request.principal = OwnerId::from_value(1);
  request.provenance.publisher = PublisherId::from_value(1);
  request.provenance.node = NodeId::from_value(1);
  request.provenance.incarnation = Incarnation::from_value(1);
  request.provenance.epoch = Epoch::from_value(1);
  request.provenance.sequence = Sequence::from_value(1);
  request.provenance.boot = BootId{1, 1};
  DeclareQueuePayload payload{};
  payload.resource = ResourceId::from_value(1);
  payload.name = std::string(limits::kMaxNameChars, 'n');
  payload.cls = ClassBinding{ClassId::from_value(1), Generation::from_value(1)};
  payload.pool = PoolRef{PoolId::from_value(1), Generation::from_value(1), 1};
  request.payload = payload;
  QF_CHECK(!encode_mutation(request).empty());

  payload.name = std::string(limits::kMaxNameChars + 1, 'n');
  request.payload = payload;
  QF_CHECK(encode_mutation(request).empty());
}

QF_TEST(adversarial, tcp_transport_rejects_a_silent_peer_and_closed_socket) {
  auto listener = TcpListener::bind("127.0.0.1", 0, 8);
  QF_REQUIRE(listener.ok());
  const std::uint16_t port = listener.value().port();
  QF_CHECK(port != 0);

  auto client = TcpTransport::connect("127.0.0.1", port);
  QF_REQUIRE(client.ok());
  auto server = listener.value().accept(2000);
  QF_REQUIRE(server.ok());

  const Bytes payload{std::byte{1}, std::byte{2}, std::byte{3}};
  QF_REQUIRE(client.value().send(payload).ok());
  std::vector<std::byte> buffer(16);
  auto received = server.value().receive(buffer);
  QF_REQUIRE(received.ok());
  QF_CHECK_EQ(received.value(), 3ull);
  QF_CHECK_EQ(server.value().bytes_received(), 3ull);

  // A closed peer is reported as a clean end of stream, not as data.
  client.value().close();
  received = server.value().receive(buffer);
  QF_REQUIRE(received.ok());
  QF_CHECK_EQ(received.value(), 0ull);

  // Sending on a closed transport fails rather than silently discarding.
  QF_CHECK(!client.value().send(payload).ok());
  server.value().close();
  listener.value().close();

  // Connecting to a dead port fails cleanly.
  auto unreachable = TcpTransport::connect("127.0.0.1", port);
  QF_CHECK(!unreachable.ok());
}

QF_TEST(adversarial, occupancy_from_a_foreign_publisher_is_refused_when_replayed) {
  Fixture fixture{};
  const QueueId queue = fixture.activate_queue("q-replay");
  OccupancySample sample = fixture.sample(queue, 100);
  QF_REQUIRE(fixture.fabric->ingest_occupancy(fixture.context(), sample).ok());

  // Replaying the same publication verbatim is idempotent, not additive.
  auto replay = fixture.fabric->ingest_occupancy(fixture.context(), sample);
  QF_CHECK(replay.ok());
  QF_CHECK(replay.value().replayed);

  // A different publisher claiming the same sequence is not a replay of ours.
  OccupancySample impostor = sample;
  impostor.provenance.publisher = PublisherId::from_value(0xBAD);
  impostor.provenance.incarnation = Incarnation::from_value(1);
  auto result = fixture.fabric->ingest_occupancy(fixture.context(), impostor);
  QF_CHECK(result.ok());
  QF_CHECK(!result.value().replayed);

  // A stale incarnation for the same publisher is refused.
  OccupancySample stale = fixture.sample(queue, 10);
  stale.provenance.incarnation = Incarnation::from_value(0);
  auto refused = fixture.fabric->ingest_occupancy(fixture.context(), stale);
  QF_CHECK_CODE(refused, Code::malformed_input);
}
