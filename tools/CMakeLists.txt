add_executable(qf_coordinator qf_coordinator_main.cpp)
qf_apply_strict(qf_coordinator)
target_link_libraries(qf_coordinator PRIVATE qf::queue_fabric)

add_executable(qf_worker qf_worker_main.cpp)
qf_apply_strict(qf_worker)
target_link_libraries(qf_worker PRIVATE qf::queue_fabric)
