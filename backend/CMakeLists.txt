cmake_minimum_required(VERSION 3.16)
project(ipc_demo VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# 查找线程库
find_package(Threads REQUIRED)

# 源文件
set(SOURCES
    src/main.cpp
    src/ipc/pipe_demo.cpp
    src/ipc/named_pipe_demo.cpp
    src/ipc/shared_memory_demo.cpp
    src/ipc/message_queue_demo.cpp
    src/ipc/signal_demo.cpp
    src/ipc/socket_demo.cpp
    src/ipc/supervisor_demo.cpp
    src/ipc/supervisor.cpp
)

# 可执行文件
add_executable(ipc_demo ${SOURCES})

# Supervisor 独立可执行文件（演示/测试中由配置直接拉起）
add_executable(ipc_supervisor
    src/supervisor_main.cpp
    src/ipc/supervisor.cpp
)

# 确定性行为受控的子进程辅助程序
add_executable(ipc_worker
    src/worker_main.cpp
    src/ipc/worker.cpp
)

# 包含目录
target_include_directories(ipc_demo PRIVATE ${CMAKE_SOURCE_DIR}/src/include)
target_include_directories(ipc_supervisor PRIVATE ${CMAKE_SOURCE_DIR}/src/include)
target_include_directories(ipc_worker PRIVATE ${CMAKE_SOURCE_DIR}/src/include)

# 链接库
target_link_libraries(ipc_demo PRIVATE Threads::Threads rt)
target_link_libraries(ipc_supervisor PRIVATE Threads::Threads rt)
target_link_libraries(ipc_worker PRIVATE Threads::Threads rt)

# 安装
install(TARGETS ipc_demo ipc_supervisor ipc_worker DESTINATION bin)

# 添加测试子目录
option(BUILD_TESTS "Build tests" ON)
if(BUILD_TESTS)
    add_subdirectory(tests)
endif()
