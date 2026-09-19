# IPC Demo Backend

C++ 进程间通信演示程序。

## 本地编译

```bash
mkdir -p build && cd build
cmake ..
make
./ipc_demo
```

## 运行选项

- 交互模式：`./ipc_demo`
- 运行所有演示：`./ipc_demo --all`
- Supervisor 模式：`./ipc_demo --supervisor --config <file>`（等价于独立的 `./ipc_supervisor`）
- 受控子进程：`./ipc_worker --help`（见 `examples/supervisor.conf`）

构建会额外产出 `ipc_supervisor`、`ipc_worker` 两个二进制，供监管模式与端到端测试使用。

## 依赖

- GCC 9+ 或 Clang 10+
- CMake 3.16+
- POSIX 兼容系统 (Linux/macOS)
