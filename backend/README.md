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
- Supervisor 进程监管模式：`./ipc_demo --supervisor [config-file]`
  （详见 [SUPERVISOR.md](SUPERVISOR.md)，示例见 `examples/supervisor/`）

## 依赖

- GCC 9+ 或 Clang 10+
- CMake 3.16+
- POSIX 兼容系统 (Linux/macOS)
