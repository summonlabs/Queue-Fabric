// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
//
// Worker process. Publishes occupancy for queues of one resource over a real
// framed TCP transport and reports its outcomes to a result file.
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <thread>
#include <vector>

#include "qf/process.hpp"
#include "qf/worker.hpp"

namespace {

struct Options {
  std::string host{"127.0.0.1"};
  std::uint16_t port{0};
  std::uint64_t node{1};
  std::uint64_t incarnation{1};
  std::uint64_t epoch{0};  ///< 0 means "no epoch known": admission is then refused.
  std::uint64_t resource{0};
  std::uint64_t class_id{0};
  std::uint64_t class_generation{1};
  std::uint64_t pool{0};
  std::uint64_t pool_generation{1};
  std::uint64_t owner{0};
  std::uint64_t queues{1};
  std::uint64_t publishes{4};
  std::uint64_t batch{1};
  std::uint64_t occupancy_bytes{512};
  std::uint64_t occupancy_packets{4};
  std::uint64_t interval_us{0};
  std::uint64_t stay_alive_ms{0};
  std::uint64_t heartbeat_every{0};
  std::string result_file{};
  std::string name{"worker"};
  std::string mode{"publish"};
  std::vector<std::uint64_t> existing_queues{};
};

bool parse(int argc, char** argv, Options& options) {
  for (int index = 1; index < argc; ++index) {
    const std::string argument = argv[index];
    bool consumed = false;
    const auto next = [&](std::string& target) {
      if (index + 1 < argc) {
        target = argv[++index];
        consumed = true;
      }
    };
    if (argument == "--host") {
      next(options.host);
    } else if (argument == "--name") {
      next(options.name);
    } else if (argument == "--mode") {
      next(options.mode);
    } else if (argument == "--result-file") {
      next(options.result_file);
    } else if (argument.rfind("--", 0) == 0) {
      std::string value;
      next(value);
      if (!consumed) {
        std::fprintf(stderr, "missing value for %s\n", argument.c_str());
        return false;
      }
      const auto number = [&](std::uint64_t& target) {
        target = std::strtoull(value.c_str(), nullptr, 0);
      };
      if (argument == "--port") {
        options.port = static_cast<std::uint16_t>(std::strtoul(value.c_str(), nullptr, 10));
      } else if (argument == "--node") {
        number(options.node);
      } else if (argument == "--incarnation") {
        number(options.incarnation);
      } else if (argument == "--epoch") {
        number(options.epoch);
      } else if (argument == "--resource") {
        number(options.resource);
      } else if (argument == "--class") {
        number(options.class_id);
      } else if (argument == "--class-generation") {
        number(options.class_generation);
      } else if (argument == "--pool") {
        number(options.pool);
      } else if (argument == "--pool-generation") {
        number(options.pool_generation);
      } else if (argument == "--owner") {
        number(options.owner);
      } else if (argument == "--queues") {
        number(options.queues);
      } else if (argument == "--queue") {
        std::uint64_t queue = 0;
        number(queue);
        options.existing_queues.push_back(queue);
      } else if (argument == "--publishes") {
        number(options.publishes);
      } else if (argument == "--batch") {
        number(options.batch);
      } else if (argument == "--occupancy-bytes") {
        number(options.occupancy_bytes);
      } else if (argument == "--occupancy-packets") {
        number(options.occupancy_packets);
      } else if (argument == "--interval-us") {
        number(options.interval_us);
      } else if (argument == "--stay-alive-ms") {
        number(options.stay_alive_ms);
      } else if (argument == "--heartbeat-every") {
        number(options.heartbeat_every);
      } else {
        std::fprintf(stderr, "unknown argument: %s\n", argument.c_str());
        return false;
      }
    } else {
      std::fprintf(stderr, "unknown argument: %s\n", argument.c_str());
      return false;
    }
  }
  return true;
}

std::string hex(std::uint64_t value) {
  static constexpr char kDigits[] = "0123456789abcdef";
  std::string out(18, '0');
  out[0] = '0';
  out[1] = 'x';
  for (int i = 0; i < 16; ++i) {
    out[2 + static_cast<std::size_t>(i)] = kDigits[(value >> (4 * (15 - i))) & 0xFu];
  }
  return out;
}

}  // namespace

int main(int argc, char** argv) {
  Options options{};
  if (!parse(argc, argv, options)) {
    return 2;
  }

  qf::SteadyClock clock{};
  const qf::PublisherId publisher = qf::PublisherId::from_value(options.node * 16 + 1);
  qf::WorkerConfig config{};
  config.host = options.host;
  config.port = options.port;
  config.node = qf::NodeId::from_value(options.node);
  config.incarnation = qf::Incarnation::from_value(options.incarnation);
  config.epoch = qf::Epoch::from_value(options.epoch);
  config.boot = qf::BootId{options.incarnation, options.node};
  config.name = options.name;

  qf::WorkerClient client{config, clock};
  std::string result;
  const auto finish = [&](int code) {
    if (!options.result_file.empty()) {
      const qf::Status written = qf::write_text_file(options.result_file, result);
      if (!written.ok()) {
        std::fprintf(stderr, "result file failed: %s\n", written.to_string().c_str());
      }
    }
    std::fputs(result.c_str(), stdout);
    std::fflush(stdout);
    client.close();
    return code;
  };

  const qf::Status connected = client.connect();
  if (!connected.ok()) {
    result = "connect=failed code=" + std::string(qf::to_string(connected.code())) + "\n";
    return finish(3);
  }
  auto ack = client.handshake();
  if (!ack.ok()) {
    result = "handshake=error code=" + std::string(qf::to_string(ack.status().code())) + "\n";
    return finish(4);
  }
  result += std::string("handshake=") + (ack.value().accepted ? "accepted" : "rejected") + "\n";
  result += "handshake_code=" + std::string(qf::to_string(ack.value().code)) + "\n";
  result += "handshake_message=" + ack.value().message + "\n";
  result += "coordinator_epoch=" + hex(ack.value().coordinator_epoch.value()) + "\n";
  if (!ack.value().accepted) {
    return finish(0);
  }

  if (options.mode == "stall") {
    const std::uint64_t millis = options.stay_alive_ms == 0 ? 200 : options.stay_alive_ms;
    std::this_thread::sleep_for(std::chrono::milliseconds(millis));
    result += "mode=stall\n";
    return finish(0);
  }

  const qf::Epoch epoch = ack.value().coordinator_epoch;
  qf::MutationContext context{};
  context.principal = qf::OwnerId::from_value(options.owner);
  context.epoch = epoch;
  qf::Provenance provenance{};
  provenance.publisher = publisher;
  provenance.node = config.node;
  provenance.incarnation = config.incarnation;
  provenance.epoch = epoch;
  provenance.boot = config.boot;

  std::vector<qf::QueueId> queues;
  for (const auto raw : options.existing_queues) {
    queues.push_back(qf::QueueId::from_value(raw));
  }

  std::uint64_t declared = 0;
  std::uint64_t declare_rejected = 0;
  std::uint64_t sequence_counter = 1;
  for (std::uint64_t index = 0; index < options.queues; ++index) {
    provenance.sequence = qf::Sequence::from_value(sequence_counter);
    context.provenance = provenance;
    qf::MutationRequest request{};
    request.kind = qf::MutationKind::declare_queue;
    request.attempt = qf::AttemptId{publisher, sequence_counter};
    request.expected_epoch = epoch;
    request.principal = context.principal;
    request.provenance = provenance;
    qf::DeclareQueuePayload payload{};
    payload.resource = qf::ResourceId::from_value(options.resource);
    payload.name = options.name + "-q" + std::to_string(index);
    payload.depth = 64;
    payload.cls.id = qf::ClassId::from_value(options.class_id);
    payload.cls.generation = qf::Generation::from_value(options.class_generation);
    payload.pool.id = qf::PoolId::from_value(options.pool);
    payload.pool.generation = qf::Generation::from_value(options.pool_generation);
    payload.pool.capacity_bytes = 1u << 20;
    payload.thresholds.low_watermark_bytes = 1024;
    payload.thresholds.high_watermark_bytes = 4096;
    payload.thresholds.max_occupancy_bytes = 8192;
    payload.thresholds.max_packets = 4096;
    request.payload = std::move(payload);
    ++sequence_counter;
    auto response = client.send_mutation(request);
    if (response.ok() && response.value().code == qf::Code::ok) {
      ++declared;
      if (response.value().queue.valid()) {
        queues.push_back(response.value().queue);
      }
    } else {
      ++declare_rejected;
      if (result.find("first_declare_error") == std::string::npos && response.ok()) {
        result += "first_declare_error=" + std::string(qf::to_string(response.value().code)) + "\n";
      }
    }
  }
  result += "declared=" + std::to_string(declared) + "\n";
  result += "declare_rejected=" + std::to_string(declare_rejected) + "\n";
  result += "queue_count=" + std::to_string(queues.size()) + "\n";
  if (!queues.empty()) {
    result += "first_queue=" + hex(queues.front().value()) + "\n";
  }

  std::uint64_t published = 0;
  std::uint64_t publish_failures = 0;
  std::vector<qf::OccupancySample> batch;
  const auto flush = [&]() {
    if (batch.empty()) {
      return;
    }
    const qf::Status sent = client.publish(batch);
    if (sent.ok()) {
      published += batch.size();
    } else {
      publish_failures += batch.size();
    }
    batch.clear();
  };

  for (std::uint64_t round = 0; round < options.publishes && !queues.empty(); ++round) {
    for (const auto& queue : queues) {
      qf::OccupancySample sample{};
      sample.queue = queue;
      sample.queue_generation = qf::Generation::from_value(1);
      sample.bytes = options.occupancy_bytes;
      sample.packets = options.occupancy_packets;
      sample.epoch = epoch;
      sample.sequence = qf::Sequence::from_value(sequence_counter);
      provenance.sequence = sample.sequence;
      ++sequence_counter;
      sample.observed_at = clock.now_nanos();
      sample.provenance = provenance;
      batch.push_back(sample);
      if (batch.size() >= options.batch) {
        flush();
      }
    }
    flush();
    if (options.interval_us != 0) {
      std::this_thread::sleep_for(std::chrono::microseconds(options.interval_us));
    }
    if (options.heartbeat_every != 0 && (round + 1) % options.heartbeat_every == 0) {
      client.send_heartbeat();
    }
  }
  result += "published=" + std::to_string(published) + "\n";
  result += "publish_failures=" + std::to_string(publish_failures) + "\n";
  result += "frames_sent=" + std::to_string(client.frames_sent()) + "\n";
  result += "frames_rejected=" + std::to_string(client.frames_rejected()) + "\n";

  if (options.stay_alive_ms != 0) {
    std::uint64_t elapsed = 0;
    while (elapsed < options.stay_alive_ms) {
      const std::uint64_t slice = std::min<std::uint64_t>(10, options.stay_alive_ms - elapsed);
      std::this_thread::sleep_for(std::chrono::milliseconds(slice));
      elapsed += slice;
      if (options.heartbeat_every != 0) {
        client.send_heartbeat();
      }
    }
  }
  return finish(0);
}
