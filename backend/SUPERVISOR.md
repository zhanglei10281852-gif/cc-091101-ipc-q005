# Supervisor 进程监管模式

面向运维培训：从一个简洁配置文件启动若干**命名子进程**，持续向 stdout
输出**一行一个 JSON 的结构化事件**，完整复盘服务进程的**退出、崩溃、卡死**
分别如何被接管。原有 6 个 IPC 信号/进程演示保持不变。

## 1. 启动

```bash
ipc_demo --supervisor <config-file>
# 或省略路径（读取 ./supervisor.conf，或环境变量 SUPERVISOR_CONFIG）
ipc_demo --supervisor
```

事件流可直接用 `jq`、`grep` 复盘，例如：

```bash
ipc_demo --supervisor examples/supervisor/supervisor.conf | jq -c 'select(.service=="worker")'
```

## 2. 配置文件格式

行式格式，`#` 为注释，等号可省略，字符串需用双引号。解析是**全有或全无**：
任何语法/语义错误都会带行号拒绝整个文件，因此 SIGHUP 热加载失败时
**运行中的旧服务完全不受影响**。

```ini
global {
    backoff_start_ms = 200      # 首次失败后的退避起始间隔
    backoff_cap_ms   = 5000     # 退避间隔上限（指数 200→400→800… 封顶）
    stable_uptime_ms = 2000     # 稳定运行达到该时长后死亡，失败计数清零
}

service "web" {
    command         = "/usr/bin/myapp" "--port" "8080"  # 命令及参数
    stop_signal     = TERM        # 优雅退出信号（TERM/INT/HUP/QUIT/USR1/USR2…）
    stop_timeout_ms = 3000        # 优雅退出宽限期，超时 SIGKILL
    max_restarts    = 3           # 首次启动之外允许的重启次数
    cwd             = "/srv/web"  # 可选：工作目录
    env LANG        = "C.UTF-8"   # 可选：附加环境变量（可多条）
}
```

## 3. 信号与接管语义

| 信号                 | 行为                                                                 |
| -------------------- | -------------------------------------------------------------------- |
| `SIGCHLD`（子进程）  | 主循环 `waitpid(-1, WNOHANG)` **循环取尽**，多进程同时退出也无僵尸  |
| `SIGHUP`             | 重新解析配置：新增服务启动、删除服务按其策略停止、修改服务有序替换   |
| `SIGTERM`/`SIGINT`   | 整体有序关停；再次收到则立即对所有残留服务 `SIGKILL`                 |

信号安全：四个信号的 handler **只向 self-pipe `write` 一个字节**
（异步信号安全），其余逻辑全部在主循环 `poll()` 唤醒后执行。

### 重启与退避

- 进程在 `stable_uptime_ms` 内死亡视为失败：按 `backoff_start_ms` 起的
  **指数退避**调度重启；稳定运行达到阈值后死亡则**清零失败计数**。
- 重启次数超过 `max_restarts` 进入 `permanent_failure`，supervisor 不再拉起。
- `exec`/`spawn` 失败（命令不存在、cwd 无效等）同样计入失败生命周期。

### 热加载（SIGHUP）

- **新增**服务：立即启动；
- **删除**服务：先发其 `stop_signal`，宽限期后 `SIGKILL`，回收后移除；
- **修改**服务（命令/信号/宽限/重启/cwd/env 变化）：先按**旧**策略停掉
  旧进程，回收后再用新配置启动（有序替换，期间不并发跑两份）；
- 配置非法：输出 `reload_rejected`，旧服务、旧进程原样保留。

### 整体关停

1. 向所有存活服务**并发**发送各自的 `stop_signal`；
2. 在各自 `stop_timeout_ms` 宽限期内等待退出；
3. 到期仍存活的服务（卡死）逐个 `SIGKILL`；
4. 全部回收后输出 `shutdown_complete` 并退出。

**退出码**：`0` 全部正常；`2` 启动时配置文件非法；`3` 关停时仍有服务
启动失败或已耗尽重试（`permanent_failure`）。

## 4. 事件一览

`starting`、`healthy`、`service_log`、`exited`（含 `code` 或
`signal`/`signal_name`）、`exec_failed`、`spawn_failed`、
`restart_scheduled`、`failure_count_reset`、`permanent_failure`、
`stopping`、`force_kill`、`stopped`、`replaced`、`reloading`、
`reload_rejected`、`service_added`、`service_removed`、
`service_replacing`、`reloaded`、`shutdown_begin`、
`shutdown_escalated`、`shutdown_complete`、`supervisor_starting`、
`supervisor_ready`、`fatal`。

每个事件都带 `ts`（steady-clock 毫秒）与 `event`，服务相关事件带
`service`、`pid`，可据此还原任意一次生命周期。

## 5. 示例

`examples/supervisor/` 提供三个脚本，分别演示优雅退出、崩溃退避、
卡死强杀：

```bash
ipc_demo --supervisor examples/supervisor/supervisor.conf
# 另一个终端：
kill -HUP  $(pidof ipc_demo)   # 改完配置后热加载
kill -TERM $(pidof ipc_demo)   # 观察有序关停
```
