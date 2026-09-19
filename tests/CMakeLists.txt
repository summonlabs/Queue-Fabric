add_executable(qf_tests
  support/test_framework.cpp
  support/fixture.cpp
  test_foundation.cpp
  test_lifecycle.cpp
  test_governance.cpp
  test_durability.cpp
  test_adversarial.cpp
  test_concurrency.cpp
  test_property.cpp
  test_multiprocess.cpp
  test_main.cpp)
target_include_directories(qf_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
qf_apply_strict(qf_tests)
target_link_libraries(qf_tests PRIVATE qf::queue_fabric)

# No test timeouts are configured anywhere in this project: tests run to
# completion and a hang is treated as a defect rather than masked.
add_test(NAME qf_tests COMMAND qf_tests)
