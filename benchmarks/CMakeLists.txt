add_executable(qf_bench qf_bench.cpp)
qf_apply_strict(qf_bench)
target_link_libraries(qf_bench PRIVATE qf::queue_fabric)
