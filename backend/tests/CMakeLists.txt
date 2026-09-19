cmake_minimum_required(VERSION 3.16)

# 启用测试
enable_testing()

# 查找线程库
find_package(Threads REQUIRED)

# 测试源文件
set(TEST_SOURCES
    test_main.cpp
    test_pipe.cpp
    test_named_pipe.cpp
    test_shared_memory.cpp
    test_message_queue.cpp
    test_signal.cpp
    test_socket.cpp
    test_supervisor.cpp
)

# 测试可执行文件
add_executable(ipc_tests ${TEST_SOURCES})

# 监管器/受控子进程：端到端测试直接拉起这两个真实二进制
add_dependencies(ipc_tests ipc_supervisor ipc_worker)

# 包含目录
target_include_directories(ipc_tests PRIVATE
    ${CMAKE_SOURCE_DIR}/src/include
    ${CMAKE_CURRENT_SOURCE_DIR}
)

# 让测试在运行时能找到同目录构建出的 ipc_supervisor / ipc_worker
target_compile_definitions(ipc_tests PRIVATE
    IPC_BINARY_DIR="${CMAKE_BINARY_DIR}"
)

# 链接库
target_link_libraries(ipc_tests PRIVATE Threads::Threads rt)

# 添加测试
add_test(NAME ipc_tests COMMAND ipc_tests)
