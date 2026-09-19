#ifndef WORKER_H
#define WORKER_H

// 监管器演示/测试用的确定性子进程：
//   ipc_worker [--name <名称>] [--ignore-term] [--log <文件>] <mode> [参数...]
//     sleep  <ms>               存活指定毫秒；收到 TERM/INT/QUIT/USR1 优雅退出
//     exit   <code> [after_ms]  延时后以指定码退出（默认立即）
//     crash  [after_ms]         延时后自杀（SIGABRT），模拟崩溃
//     cycle  <interval_ms>      持续运行并周期打印心跳
//     fail_n <n> <statefile> [linger_ms]
//                               跨重启共享计数文件，前 n 次启动退出 1，之后常驻
// 自身也通过 self-pipe 异步信号安全地处理信号，并输出单行 JSON 事件。
int run_worker(int argc, char** argv);

#endif // WORKER_H
