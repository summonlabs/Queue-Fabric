// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
//
// Real multiprocess proof surface: coordinator and workers are independent OS
// processes communicating over a real framed TCP transport. Workers are killed
// hard, incarnations are fenced, the coordinator is restarted with an advanced
// epoch, and stale epochs are refused. Nothing here is simulated in-process.
#include <chrono>
#include <cstdio>
#include <map>
#include <string>
#include <thread>
#include <vector>

#include "qf/process.hpp"
#include "qf/worker.hpp"
#include "support/fixture.hpp"
#include "support/test_framework.hpp"

using namespace qf;

namespace {

using KeyValues = std::map<std::string, std::string>;

KeyValues parse_key_values(const std::string& text) {
  KeyValues values;
  std::string key;
  std::string value;
  bool in_key = true;
  for (const char character : text) {
    if (character == '\n') {
      if (!key.empty()) {
        values[key] = value;
      }
      key.clear();
      value.clear();
      in_key = true;
      continue;
    }
    if (character == '=' && in_key) {
      in_key = false;
      continue;
    }
    if (in_key) {
      key.push_back(character);
    } else {
      value.push_back(character);
    }
  }
  if (!key.empty()) {
    values[key] = value;
  }
  return values;
}

std::string field(const KeyValues& values, const std::string& key) {
  const auto found = values.find(key);
  return found == values.end() ? std::string{} : found->second;
}

std::uint64_t number(const KeyValues& values, const std::string& key) {
  return std::strtoull(field(values, key).c_str(), nullptr, 0);
}

}  // namespace

QF_TEST(multiprocess, coordinator_worker_epoch_and_restart_closure) {
  const std::string coordinator_exe = qftest::tool_path("qf_coordinator.exe");
  const std::string worker_exe = qftest::tool_path("qf_worker.exe");
  QF_REQUIRE(!coordinator_exe.empty());
  QF_REQUIRE(!worker_exe.empty());

  const std::string directory = qftest::scratch_directory("multiprocess");
  const std::string ready_file = directory + "/ready.txt";
  const std::string stop_file = directory + "/stop.txt";
  const std::string summary_file = directory + "/final.txt";

  // ---- phase 1: start the coordinator as a real child process --------------
  auto coordinator = ChildProcess::spawn({coordinator_exe, "--dir", directory, "--ready-file", ready_file,
                                          "--heartbeat-deadline-ms", "5000"});
  QF_REQUIRE(coordinator.ok());
  QF_CHECK(coordinator.value().pid() != 0);

  auto ready_text = wait_for_file(ready_file, 600, 25);
  QF_REQUIRE(ready_text.ok());
  const KeyValues ready = parse_key_values(ready_text.value());
  const std::uint16_t port = static_cast<std::uint16_t>(number(ready, "port"));
  const std::string epoch_hex = field(ready, "epoch");
  const std::string resource = field(ready, "resource");
  const std::string class_id = field(ready, "class");
  const std::string class_generation = field(ready, "class_generation");
  const std::string pool_id = field(ready, "pool");
  const std::string pool_generation = field(ready, "pool_generation");
  const std::string owner = field(ready, "owner");
  QF_CHECK(port != 0);
  QF_CHECK(!epoch_hex.empty());
  QF_CHECK(!resource.empty());

  SteadyClock clock{};

  // ---- phase 2: two workers declare queues and publish occupancy -----------
  for (int index = 0; index < 2; ++index) {
    const std::string result_file = directory + "/worker" + std::to_string(index) + ".txt";
    std::vector<std::string> arguments{worker_exe,
                                       "--host", "127.0.0.1",
                                       "--port", std::to_string(port),
                                       "--node", std::to_string(index + 1),
                                       "--incarnation", "1",
                                       "--epoch", epoch_hex,
                                       "--resource", resource,
                                       "--class", class_id,
                                       "--class-generation", class_generation,
                                       "--pool", pool_id,
                                       "--pool-generation", pool_generation,
                                       "--owner", owner,
                                       "--queues", "2",
                                       "--publishes", "3",
                                       "--name", "w" + std::to_string(index),
                                       "--result-file", result_file};
    auto worker = ChildProcess::spawn(arguments);
    QF_REQUIRE(worker.ok());
    const auto exit_code = worker.value().wait();
    QF_REQUIRE(exit_code.ok());
    QF_CHECK_EQ(exit_code.value(), 0);
    auto text = read_text_file(result_file);
    QF_REQUIRE(text.ok());
    const KeyValues values = parse_key_values(text.value());
    QF_CHECK_EQ(field(values, "handshake"), std::string("accepted"));
    QF_CHECK_EQ(number(values, "declared"), 2ull);
    QF_CHECK_EQ(number(values, "published"), 6ull);
    QF_CHECK_EQ(number(values, "publish_failures"), 0ull);
  }

  // ---- phase 3: a monitor observes the committed work ----------------------
  {
    WorkerConfig config{};
    config.host = "127.0.0.1";
    config.port = port;
    config.role = Role::monitor;
    config.node = NodeId::from_value(0x900);
    config.incarnation = Incarnation::from_value(1);
    config.boot = BootId{0x900, 0x901};
    config.name = "monitor";
    WorkerClient monitor{config, clock};
    QF_REQUIRE(monitor.connect().ok());
    auto ack = monitor.handshake();
    QF_REQUIRE(ack.ok());
    QF_CHECK(ack.value().accepted);
    auto stats = monitor.request_stats();
    QF_REQUIRE(stats.ok());
    QF_CHECK_EQ(stats.value().queues, 4ull);
    QF_CHECK(stats.value().occupancy_accepted >= 12);
    QF_CHECK_EQ(stats.value().occupancy_rejected, 0ull);
    QF_CHECK(stats.value().connections_accepted >= 2);
    monitor.close();
  }

  // ---- phase 4: a stale epoch is refused -----------------------------------
  {
    const std::string result_file = directory + "/stale-epoch.txt";
    auto worker = ChildProcess::spawn({worker_exe,
                                       "--host", "127.0.0.1",
                                       "--port", std::to_string(port),
                                       "--node", "77",
                                       "--incarnation", "1",
                                       "--epoch", "0x0000000000000000",
                                       "--resource", resource,
                                       "--class", class_id,
                                       "--class-generation", class_generation,
                                       "--pool", pool_id,
                                       "--pool-generation", pool_generation,
                                       "--owner", owner,
                                       "--queues", "1",
                                       "--publishes", "1",
                                       "--name", "stale",
                                       "--result-file", result_file});
    QF_REQUIRE(worker.ok());
    QF_REQUIRE(worker.value().wait().ok());
    auto text = read_text_file(result_file);
    QF_REQUIRE(text.ok());
    const KeyValues values = parse_key_values(text.value());
    QF_CHECK_EQ(field(values, "handshake"), std::string("rejected"));
    QF_CHECK_EQ(field(values, "handshake_code"), std::string("stale_epoch"));
  }

  // ---- phase 5: a worker is killed hard, its evidence must be invalidated --
  {
    const std::string result_file = directory + "/long-lived.txt";
    auto worker = ChildProcess::spawn({worker_exe,
                                       "--host", "127.0.0.1",
                                       "--port", std::to_string(port),
                                       "--node", "5",
                                       "--incarnation", "1",
                                       "--epoch", epoch_hex,
                                       "--resource", resource,
                                       "--class", class_id,
                                       "--class-generation", class_generation,
                                       "--pool", pool_id,
                                       "--pool-generation", pool_generation,
                                       "--owner", owner,
                                       "--queues", "1",
                                       "--publishes", "2",
                                       "--heartbeat-every", "1",
                                       "--stay-alive-ms", "60000",
                                       "--name", "victim",
                                       "--result-file", result_file});
    QF_REQUIRE(worker.ok());

    // Wait until the victim's publications are committed.
    bool observed = false;
    for (int attempt = 0; attempt < 400 && !observed; ++attempt) {
      WorkerConfig config{};
      config.host = "127.0.0.1";
      config.port = port;
      config.role = Role::monitor;
      config.node = NodeId::from_value(0x901);
      config.incarnation = Incarnation::from_value(1);
      config.boot = BootId{0x901, 0x902};
      WorkerClient monitor{config, clock};
      if (monitor.connect().ok() && monitor.handshake().ok()) {
        auto stats = monitor.request_stats();
        if (stats.ok() && stats.value().queues >= 5) {
          observed = true;
        }
      }
      monitor.close();
      if (!observed) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
      }
    }
    QF_CHECK(observed);

    // Hard kill: no destructors, no farewell message.
    QF_REQUIRE(worker.value().terminate().ok());
    const auto exit_code = worker.value().wait();
    QF_REQUIRE(exit_code.ok());
    QF_CHECK(exit_code.value() != 0);

    // The coordinator must notice the loss and invalidate the evidence.
    bool invalidated = false;
    for (int attempt = 0; attempt < 400 && !invalidated; ++attempt) {
      WorkerConfig config{};
      config.host = "127.0.0.1";
      config.port = port;
      config.role = Role::monitor;
      config.node = NodeId::from_value(0x902);
      config.incarnation = Incarnation::from_value(1);
      config.boot = BootId{0x902, 0x903};
      WorkerClient monitor{config, clock};
      if (monitor.connect().ok() && monitor.handshake().ok()) {
        auto stats = monitor.request_stats();
        if (stats.ok() && stats.value().connections_lost >= 1 && stats.value().revalidations_required >= 1) {
          invalidated = true;
        }
      }
      monitor.close();
      if (!invalidated) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
      }
    }
    QF_CHECK(invalidated);
  }

  // ---- phase 6: the coordinator is killed hard and restarted ---------------
  QF_REQUIRE(coordinator.value().terminate().ok());
  const auto killed = coordinator.value().wait();
  QF_REQUIRE(killed.ok());

  const std::string ready_file_2 = directory + "/ready2.txt";
  auto restarted = ChildProcess::spawn({coordinator_exe, "--dir", directory, "--ready-file", ready_file_2,
                                        "--stop-file", stop_file, "--heartbeat-deadline-ms", "5000"});
  QF_REQUIRE(restarted.ok());
  auto ready_text_2 = wait_for_file(ready_file_2, 600, 25);
  QF_REQUIRE(ready_text_2.ok());
  const KeyValues ready_2 = parse_key_values(ready_text_2.value());
  const std::uint16_t port_2 = static_cast<std::uint16_t>(number(ready_2, "port"));
  const std::string epoch_2_hex = field(ready_2, "epoch");
  QF_CHECK(port_2 != 0);
  QF_CHECK(!epoch_2_hex.empty());
  QF_CHECK(epoch_2_hex != epoch_hex);

  // Durable queues survived, and every restored queue requires revalidation.
  {
    WorkerConfig config{};
    config.host = "127.0.0.1";
    config.port = port_2;
    config.role = Role::monitor;
    config.node = NodeId::from_value(0x903);
    config.incarnation = Incarnation::from_value(1);
    config.boot = BootId{0x903, 0x904};
    WorkerClient monitor{config, clock};
    QF_REQUIRE(monitor.connect().ok());
    auto ack = monitor.handshake();
    QF_REQUIRE(ack.ok());
    QF_CHECK(ack.value().accepted);
    QF_CHECK_EQ(ack.value().coordinator_epoch.value(), number(ready_2, "epoch"));
    auto stats = monitor.request_stats();
    QF_REQUIRE(stats.ok());
    QF_CHECK(stats.value().queues >= 4);
    monitor.close();
  }

  // A worker still holding the previous epoch is refused after the restart.
  {
    const std::string result_file = directory + "/post-restart-stale.txt";
    auto worker = ChildProcess::spawn({worker_exe,
                                       "--host", "127.0.0.1",
                                       "--port", std::to_string(port_2),
                                       "--node", "8",
                                       "--incarnation", "1",
                                       "--epoch", epoch_hex,
                                       "--resource", resource,
                                       "--class", class_id,
                                       "--class-generation", class_generation,
                                       "--pool", pool_id,
                                       "--pool-generation", pool_generation,
                                       "--owner", owner,
                                       "--queues", "1",
                                       "--publishes", "1",
                                       "--name", "post",
                                       "--result-file", result_file});
    QF_REQUIRE(worker.ok());
    QF_REQUIRE(worker.value().wait().ok());
    auto text = read_text_file(result_file);
    QF_REQUIRE(text.ok());
    const KeyValues values = parse_key_values(text.value());
    QF_CHECK_EQ(field(values, "handshake"), std::string("rejected"));
    QF_CHECK_EQ(field(values, "handshake_code"), std::string("stale_epoch"));
  }

  // A worker with the new epoch is admitted and can publish again.
  {
    const std::string result_file = directory + "/post-restart-fresh.txt";
    auto worker = ChildProcess::spawn({worker_exe,
                                       "--host", "127.0.0.1",
                                       "--port", std::to_string(port_2),
                                       "--node", "9",
                                       "--incarnation", "2",
                                       "--epoch", epoch_2_hex,
                                       "--resource", resource,
                                       "--class", class_id,
                                       "--class-generation", class_generation,
                                       "--pool", pool_id,
                                       "--pool-generation", pool_generation,
                                       "--owner", owner,
                                       "--queues", "1",
                                       "--publishes", "2",
                                       "--name", "fresh",
                                       "--result-file", result_file});
    QF_REQUIRE(worker.ok());
    QF_REQUIRE(worker.value().wait().ok());
    auto text = read_text_file(result_file);
    QF_REQUIRE(text.ok());
    const KeyValues values = parse_key_values(text.value());
    QF_CHECK_EQ(field(values, "handshake"), std::string("accepted"));
    QF_CHECK_EQ(number(values, "published"), 2ull);
  }

  // ---- phase 7: graceful shutdown through the protocol ---------------------
  {
    WorkerConfig config{};
    config.host = "127.0.0.1";
    config.port = port_2;
    config.role = Role::monitor;
    config.node = NodeId::from_value(0x904);
    config.incarnation = Incarnation::from_value(1);
    config.boot = BootId{0x904, 0x905};
    WorkerClient monitor{config, clock};
    QF_REQUIRE(monitor.connect().ok());
    QF_REQUIRE(monitor.handshake().ok());
    QF_REQUIRE(monitor.send_shutdown("test closure").ok());
    monitor.close();
  }
  const auto summary_exit = restarted.value().wait();
  QF_REQUIRE(summary_exit.ok());
  QF_CHECK_EQ(summary_exit.value(), 0);

  auto summary_text = read_text_file(summary_file);
  QF_REQUIRE(summary_text.ok());
  const KeyValues summary = parse_key_values(summary_text.value());
  // The restarted coordinator is a new process: its own counters cover only its
  // own life, while the restored durable state is reported separately.
  QF_CHECK(number(summary, "queues") >= 4);
  QF_CHECK(number(summary, "occupancy_accepted") >= 2);
  QF_CHECK(number(summary, "queues_restored") >= 4);
  QF_CHECK(number(summary, "queues_revalidated") >= 4);
  QF_CHECK(number(summary, "records_usable") > 0);
  QF_CHECK(field(summary, "previous_boot") != field(summary, "current_boot"));
  // One stale-epoch refusal was counted by the killed coordinator and one by
  // the restarted process; the restarted process must have seen at least one.
  QF_CHECK(number(summary, "stale_epoch_rejections") >= 1);
  QF_CHECK_EQ(number(summary, "orphaned_begins"), 0ull);
}
