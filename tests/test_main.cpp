// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

#include "qf/transport.hpp"
#include "support/test_framework.hpp"

int main(int argc, char** argv) {
  qftest::set_program_path(argc > 0 ? argv[0] : nullptr);
  std::vector<std::string> filters;
  for (int index = 1; index < argc; ++index) {
    const std::string argument = argv[index];
    if (argument == "--list") {
      for (const auto& test : qftest::Registry::instance().cases()) {
        std::printf("%s.%s\n", test.suite.c_str(), test.name.c_str());
      }
      return 0;
    }
    if (argument.rfind("--filter=", 0) == 0) {
      filters.push_back(argument.substr(9));
      continue;
    }
    filters.push_back(argument);
  }

  const qf::Status network = qf::network_init();
  if (!network.ok()) {
    std::printf("note: network subsystem unavailable: %s\n", network.to_string().c_str());
  }

  const int failed = qftest::Registry::instance().run(filters);
  qf::network_shutdown();
  return failed == 0 ? 0 : 1;
}
