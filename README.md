# C++ 进程间通信 (IPC) 演示项目

## 1. 运行方式

### 方式一：Docker 运行（推荐）

```bash
# 构建并运行演示
docker-compose up --build

# 运行测试
docker-compose --profile test up --build ipc-test

# 或者单独构建后运行
docker-compose build
docker-compose run --rm ipc-demo demo    # 运行演示
docker-compose run --rm ipc-demo test    # 运行测试
```

### 方式二：本地编译运行

```bash
cd backend
mkdir -p build && cd build
cmake -DBUILD_TESTS=ON ..
make

# 运行演示
./ipc_demo           # 交互模式
./ipc_demo --all     # 运行所有演示

# Supervisor 进程监管模式（另见 backend/examples/supervisor.conf）
./ipc_supervisor --config <配置文件>
# 热加载: kill -HUP <pid>    整体关停: kill -TERM <pid> 或 Ctrl-C

# 受控子进程辅助程序（supervisor 演示/测试使用）
./ipc_worker --name w cycle 1000

# 运行测试
./tests/ipc_tests
```

## 2. 服务说明

| 服务名   | 命令 | 说明              |
| -------- | ---- | ----------------- |
| ipc-demo | demo | 运行所有 IPC 演示 |
| ipc-test | test | 运行所有测试用例  |

## 3. 测试账号

本项目为控制台演示程序，无需账号。

## 4. 项目介绍

本项目演示了 Linux/Unix 系统下 C++ 进程间通信的 **6 种主要 IPC 方式**，
并额外提供一个 **Supervisor 进程监管** 实训模式：

1. **管道 (Pipe)** - 匿名管道，用于父子进程通信
2. **命名管道 (Named Pipe / FIFO)** - 可用于无亲缘关系进程通信
3. **共享内存 (Shared Memory)** - 最快的 IPC 方式
4. **消息队列 (Message Queue)** - 结构化消息传递
5. **信号 (Signal)** - 异步通知机制
6. **Socket (Unix Domain Socket)** - 本地套接字通信
7. **Supervisor (进程监管)** - 从配置启动命名子进程并接管其退出/崩溃/卡死生命周期

---

## Supervisor 进程监管模式

运维培训场景：演示服务进程在**正常退出、异常崩溃、卡死（忽略优雅信号）**
三种情况下分别怎样被接管。`ipc_supervisor` 从简洁的配置文件启动若干命名
子进程，并持续输出 **JSON Lines 结构化事件**，便于按时间线复盘真实生命周期。

### 配置文件（INI 风格，每个服务一个段）

```ini
[service:web]
command        = ./ipc_worker --name web cycle 1000  # 命令及参数
stop_signal    = TERM          # 优雅退出信号 TERM/INT/HUP/QUIT/USR1/USR2/ALRM
stop_grace_ms  = 2000          # 宽限期，超时仍未退出则 SIGKILL
start_retries  = 5             # 生命周期内启动总次数上限（含首次）
backoff_ms     = 100,200,400   # 连续失败逐次退避，超出后取末值
stable_ms      = 1000          # 连续存活超过它即清零失败计数，恢复完整重试额度
```

### 信号与事件

| 信号 | 行为 |
| ---- | ---- |
| SIGCHLD | 交回主循环，`waitpid(WNOHANG)` 循环回收，多个子进程同时退出也全部回收、不留僵尸 |
| SIGHUP | 两阶段热加载：先完整解析，失败发 `config_invalid` 且旧服务不受影响；成功则新增即启动、删除按各自策略停止、修改有序替换 |
| SIGTERM / SIGINT | 整体关停：按配置顺序先发各自优雅信号，宽限期后 SIGKILL |

信号处理函数只做一次异步信号安全的 `write()`（self-pipe 机制），所有策略
都在 `poll()` 主循环中执行；退避/宽限期/稳定阈值均由 `timerfd` 截止时间驱动。

关键事件：`service_started`、`service_healthy`、`service_died`、
`service_backoff`、`service_stable_reset`、`service_failed`、
`service_stopping`、`service_force_kill`、`service_exited`、
`service_replaced`、`supervisor_stopping`、`supervisor_stopped`。

监管器在有服务耗尽重试（或启动即失败）时以**非零码**退出。

### 受控子进程 ipc_worker

`ipc_worker` 行为完全确定，自身也用 self-pipe 处理信号，供演示与测试使用：

```bash
ipc_worker [--name N] [--ignore-term] sleep <ms>          # 常驻，收到优雅信号退出
ipc_worker [--name N] exit <code> [after_ms]             # 延时后以指定码退出
ipc_worker [--name N] crash [after_ms]                   # 延时后 SIGABRT 崩溃
ipc_worker [--name N] cycle <interval_ms>                # 周期心跳
ipc_worker [--name N] fail_n <n> <statefile> [linger_ms] # 前 n 次启动失败，之后常驻
```



## Docker 详细使用指南

### 1. 构建镜像

```bash
# 构建所有服务
docker-compose build

# 仅构建 ipc-demo 服务
docker-compose build ipc-demo
```

### 2. 运行演示

```bash
# 方式1：使用 docker-compose up（前台运行，可看到输出）
docker-compose up ipc-demo

# 方式2：使用 docker-compose run（一次性运行）
docker-compose run --rm ipc-demo demo

# 方式3：直接使用 docker run
docker run --rm --privileged -v /tmp:/tmp --ipc=host ipc-demo demo
```

### 3. 运行测试

```bash
# 方式1：使用 profile 运行测试服务
docker-compose --profile test up ipc-test

# 方式2：使用 run 命令
docker-compose run --rm ipc-demo test

# 方式3：直接使用 docker run
docker run --rm --privileged -v /tmp:/tmp --ipc=host ipc-demo test
```

### 4. 交互模式

```bash
# 进入交互式菜单
docker-compose run --rm ipc-demo interactive

# 或者进入容器 shell
docker run -it --rm --privileged -v /tmp:/tmp --ipc=host ipc-demo /bin/bash
```

### 5. 查看日志

```bash
# 查看容器日志
docker-compose logs ipc-demo

# 实时查看日志
docker-compose logs -f ipc-demo
```

### 6. 清理资源

```bash
# 停止并删除容器
docker-compose down

# 删除构建的镜像
docker-compose down --rmi all

# 清理所有未使用的资源
docker system prune -f
```

---

## 测试用例说明

项目包含 **50 个测试用例**，覆盖所有 IPC 方式及 Supervisor 进程监管：

### Pipe (管道) - 5 个用例

| 测试名                  | 说明               |
| ----------------------- | ------------------ |
| pipe_create             | 测试管道创建       |
| pipe_read_write         | 测试管道读写       |
| pipe_fork_communication | 测试父子进程通信   |
| pipe_multiple_messages  | 测试多条消息传输   |
| pipe_close_write_end    | 测试关闭写端后读取 |

### Named Pipe (命名管道) - 6 个用例

| 测试名                 | 说明               |
| ---------------------- | ------------------ |
| fifo_create            | 测试 FIFO 创建     |
| fifo_create_duplicate  | 测试重复创建       |
| fifo_read_write        | 测试 FIFO 读写     |
| fifo_struct_transfer   | 测试结构化数据传输 |
| fifo_nonblocking       | 测试非阻塞模式     |
| fifo_multiple_messages | 测试多条消息       |

### Shared Memory (共享内存) - 7 个用例

| 测试名                 | 说明             |
| ---------------------- | ---------------- |
| shm_create             | 测试共享内存创建 |
| shm_attach_detach      | 测试附加和分离   |
| shm_read_write         | 测试读写操作     |
| shm_fork_communication | 测试父子进程通信 |
| shm_size               | 测试共享内存大小 |
| shm_atomic_operations  | 测试原子操作     |
| shm_array_data         | 测试数组数据共享 |

### Message Queue (消息队列) - 7 个用例

| 测试名                  | 说明             |
| ----------------------- | ---------------- |
| msgq_create             | 测试消息队列创建 |
| msgq_send_receive       | 测试发送和接收   |
| msgq_type_filter        | 测试消息类型过滤 |
| msgq_nonblocking        | 测试非阻塞接收   |
| msgq_fork_communication | 测试父子进程通信 |
| msgq_status             | 测试队列状态     |
| msgq_receive_any_type   | 测试接收任意类型 |

### Signal (信号) - 8 个用例

| 测试名                    | 说明                 |
| ------------------------- | -------------------- |
| signal_register_handler   | 测试注册信号处理函数 |
| signal_send_to_self       | 测试发送信号给自己   |
| signal_fork_communication | 测试父子进程信号通信 |
| signal_ignore             | 测试忽略信号         |
| signal_multiple_signals   | 测试多个信号         |
| signal_kill               | 测试 kill() 发送信号 |
| signal_mask               | 测试信号掩码         |
| signal_check_process      | 测试检查进程是否存在 |

### Supervisor (进程监管) - 9 个用例（事件驱动，不依赖固定 sleep）

| 测试名                  | 说明                                               |
| ----------------------- | -------------------------------------------------- |
| sup_signal_burst        | 信号突发：8 个子进程同时被 KILL，全部回收          |
| sup_zombie_reaping      | 僵尸回收：多子进程同时退出后无 Z 状态残留          |
| sup_backoff_restart     | 退避节奏重启 + 稳定运行清零失败计数                |
| sup_retry_exhausted     | 重试耗尽 -> service_failed，监管器非零退出         |
| sup_exec_failure        | 命令不存在计入重试并非零退出                       |
| sup_bad_reload          | 解析失败的 SIGHUP 不影响旧服务；合法加载增删换     |
| sup_shutdown_order      | 各自优雅信号 -> 宽限期 -> 卡死者 SIGKILL 的顺序    |
| sup_inline_comments     | 配置行内注释剥离                                   |
| sup_reload_during_shutdown | 关停中 SIGHUP 新增的服务也必须被关停，不得挂起   |

### Socket (Unix Domain Socket) - 8 个用例

| 测试名                   | 说明                  |
| ------------------------ | --------------------- |
| socket_create            | 测试 socket 创建      |
| socket_bind              | 测试绑定              |
| socket_listen            | 测试监听              |
| socket_client_server     | 测试客户端-服务器通信 |
| socket_multiple_messages | 测试多条消息          |
| socket_dgram             | 测试 DGRAM socket     |
| socket_nonblocking       | 测试非阻塞 socket     |
| socket_bidirectional     | 测试双向通信          |

---

## 项目结构

```
├── README.md               # 项目说明
├── docker-compose.yml      # Docker Compose 配置
├── .gitignore              # Git 忽略文件
└── backend/
    ├── CMakeLists.txt      # CMake 构建配置
    ├── Dockerfile          # Docker 构建文件
    ├── README.md           # 后端说明
    ├── examples/
    │   └── supervisor.conf # Supervisor 示例配置
    ├── src/
    │   ├── main.cpp        # 主程序入口（含 --supervisor 模式）
    │   ├── supervisor_main.cpp  # ipc_supervisor 入口
    │   ├── worker_main.cpp      # ipc_worker 入口
    │   ├── include/
    │   │   ├── ipc_demo.h  # 头文件
    │   │   ├── supervisor.h
    │   │   └── worker.h
    │   └── ipc/
    │       ├── pipe_demo.cpp           # 管道演示
    │       ├── named_pipe_demo.cpp     # 命名管道演示
    │       ├── shared_memory_demo.cpp  # 共享内存演示
    │       ├── message_queue_demo.cpp  # 消息队列演示
    │       ├── signal_demo.cpp         # 信号演示
    │       ├── socket_demo.cpp         # Socket 演示
    │       ├── supervisor.cpp          # Supervisor 监管器核心
    │       ├── supervisor_demo.cpp     # Supervisor 脚本式教学演示
    │       └── worker.cpp              # 确定性子进程 ipc_worker
    └── tests/
        ├── CMakeLists.txt          # 测试构建配置
        ├── test_framework.h        # 测试框架
        ├── test_main.cpp           # 测试主程序
        ├── test_pipe.cpp           # 管道测试
        ├── test_named_pipe.cpp     # 命名管道测试
        ├── test_shared_memory.cpp  # 共享内存测试
        ├── test_message_queue.cpp  # 消息队列测试
        ├── test_signal.cpp         # 信号测试
        ├── test_supervisor.cpp     # Supervisor 端到端测试
        └── test_socket.cpp         # Socket 测试
```

---

## 各 IPC 方式对比

| 方式            | 速度 | 复杂度 | 数据量 | 适用场景         |
| --------------- | ---- | ------ | ------ | ---------------- |
| 管道 (Pipe)     | 中   | 低     | 小     | 父子进程简单通信 |
| 命名管道 (FIFO) | 中   | 低     | 小     | 无亲缘进程通信   |
| 共享内存        | 高   | 高     | 大     | 大数据量高频通信 |
| 消息队列        | 中   | 中     | 中     | 结构化消息传递   |
| 信号            | 高   | 低     | 极小   | 异步事件通知     |
| Socket          | 中   | 中     | 中     | 灵活的双向通信   |

---

## 常见问题

### Q: Docker 运行时报权限错误？

A: IPC 功能需要特权模式，确保使用 `--privileged` 参数或在 docker-compose.yml 中设置 `privileged: true`。

### Q: 共享内存或消息队列创建失败？

A: 可能是之前的 IPC 资源未清理，运行以下命令清理：

```bash
# 查看 IPC 资源
ipcs

# 清理共享内存
ipcrm -a
```

### Q: 测试在 macOS 上失败？

A: 部分 System V IPC 功能在 macOS 上行为不同，建议使用 Docker 运行测试。
