/**
 * ipc_worker —— supervisor 的确定性子进程
 *
 * 用法:
 *   ipc_worker [--name <名称>] [--ignore-term] [--log <文件>] <mode> [参数...]
 *
 *   sleep  <ms>               存活指定毫秒；收到 TERM/INT/QUIT/USR1 立即优雅退出(0)
 *   exit   <code> [after_ms]  延时 after_ms 后以 code 退出（默认立即）
 *   crash  [after_ms]         延时 after_ms 后主动 SIGABRT，模拟崩溃
 *   cycle  <interval_ms>      持续运行，周期输出心跳，直到收到优雅退出信号
 *   fail_n <n> <statefile> [linger_ms]
 *                             跨重启共享计数文件：第 1..n 次启动立即 exit 1，
 *                             之后常驻（用于确定性演示退避→稳定清零）
 *
 * --ignore-term 时 TERM/INT 只记录不退出，用于演示“卡死”后被 SIGKILL。
 * 信号处理同样只用 self-pipe（异步信号安全），主循环 poll 处理。
 * 事件输出为单行 JSON（stdout，可另用 --log 落盘）。
 */

#include "worker.h"

#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <string>
#include <sys/timerfd.h>
#include <unistd.h>
#include <vector>

namespace {

int g_pipe_w = -1;
bool g_fail_n_active = false;
int g_fail_n_code = 1;

void handler(int sig) {
    int saved = errno;
    unsigned char b = (unsigned char)sig;
    ssize_t r = write(g_pipe_w, &b, 1);
    (void)r;
    errno = saved;
}

// 统一的事件构造：fields 为形如 ",\"k\":v" 的已格式化片段
void emit_json(const std::string& name, const char* type,
               const std::string& fields, int log_fd) {
    std::string s = "{\"event\":\"worker_";
    s += type;
    s += "\",\"name\":\"";
    s += name;
    s += "\",\"pid\":";
    s += std::to_string((long)getpid());
    s += fields;
    s += "}\n";
    ssize_t r = write(STDOUT_FILENO, s.data(), s.size());
    (void)r;
    if (log_fd >= 0) {
        r = write(log_fd, s.data(), s.size());
        (void)r;
    }
}

long arg_long(const char* s, long dflt) {
    if (!s) return dflt;
    char* end = nullptr;
    long v = std::strtol(s, &end, 10);
    return (end == s) ? dflt : v;
}

void arm_timer(int tfd, long ms, bool interval) {
    // timerfd 的 it_value 为 0 表示“解除定时器”而非立即到期，最少给 1ms
    if (ms < 1) ms = 1;
    struct itimerspec its;
    std::memset(&its, 0, sizeof(its));
    its.it_value.tv_sec = ms / 1000;
    its.it_value.tv_nsec = (long)(ms % 1000) * 1000000L;
    if (interval) its.it_interval = its.it_value;
    timerfd_settime(tfd, 0, &its, nullptr);
}

} // namespace

int run_worker(int argc, char** argv) {
    std::string name = "worker";
    bool ignore_term = false;
    std::string log_path;

    int i = 1;
    for (; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--name" && i + 1 < argc) name = argv[++i];
        else if (a == "--ignore-term") ignore_term = true;
        else if (a == "--log" && i + 1 < argc) log_path = argv[++i];
        else if (a.rfind("--", 0) == 0) { /* 忽略未知开关 */ }
        else break;
    }
    if (i >= argc) {
        std::fprintf(stderr, "用法: ipc_worker [--name N] [--ignore-term] [--log F] "
                             "<sleep|exit|crash|cycle> ...\n");
        return 2;
    }
    std::string mode = argv[i++];

    int log_fd = -1;
    if (!log_path.empty())
        log_fd = open(log_path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);

    // self-pipe：信号处理函数只做一次 write
    int sp[2];
    if (pipe2(sp, O_NONBLOCK | O_CLOEXEC) != 0) return 1;
    g_pipe_w = sp[1];

    struct sigaction sa;
    std::memset(&sa, 0, sizeof(sa));
    sa.sa_handler = handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;  // 不用 SA_RESTART
    sigaction(SIGTERM, &sa, nullptr);
    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGQUIT, &sa, nullptr);
    sigaction(SIGHUP, &sa, nullptr);
    sigaction(SIGUSR1, &sa, nullptr);
    sigaction(SIGUSR2, &sa, nullptr);
    struct sigaction ign;
    std::memset(&ign, 0, sizeof(ign));
    ign.sa_handler = SIG_IGN;
    sigemptyset(&ign.sa_mask);
    sigaction(SIGPIPE, &ign, nullptr);

    int tfd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);

    // 根据模式安排定时器
    long p1 = (i < argc) ? arg_long(argv[i], 0) : 0;
    if (mode == "sleep") {
        arm_timer(tfd, p1, false);
    } else if (mode == "exit") {
        long after = (i + 1 < argc) ? arg_long(argv[i + 1], 0) : 0;
        arm_timer(tfd, after, false);
    } else if (mode == "crash") {
        long after = (i < argc) ? arg_long(argv[i], 0) : 0;
        arm_timer(tfd, after, false);
    } else if (mode == "cycle") {
        arm_timer(tfd, p1 > 0 ? p1 : 100, true);
    } else if (mode == "fail_n") {
        // 参数: <总失败次数 n> <状态文件> [失败前停留 ms，默认 0]
        if (i + 1 >= argc) {
            std::fprintf(stderr, "ipc_worker fail_n: 需要 <n> <statefile>\n");
            return 2;
        }
        long total = arg_long(argv[i], 0);
        const char* statefile = argv[i + 1];
        long linger = (i + 2 < argc) ? arg_long(argv[i + 2], 0) : 0;

        long attempt = 0;
        if (FILE* f = std::fopen(statefile, "r")) {
            if (std::fscanf(f, "%ld", &attempt) != 1) attempt = 0;
            std::fclose(f);
        }
        ++attempt;
        if (FILE* f = std::fopen(statefile, "w")) {
            std::fprintf(f, "%ld\n", attempt);
            std::fclose(f);
        }
        emit_json(name, "fail_n_attempt",
                  std::string(",\"attempt\":") + std::to_string(attempt) +
                  ",\"fail_total\":" + std::to_string(total), log_fd);
        if (attempt <= total) {
            arm_timer(tfd, linger, false);  // 到点后由下方定时器分支 exit 1
            g_fail_n_code = 1;
            g_fail_n_active = true;
        }
        // attempt > total：不设定时器，常驻直到收到优雅退出信号
    } else {
        std::fprintf(stderr, "ipc_worker: 未知模式 %s\n", mode.c_str());
        return 2;
    }

    emit_json(name, "ready", "", log_fd);

    while (true) {
        pollfd fds[2];
        fds[0].fd = sp[0]; fds[0].events = POLLIN; fds[0].revents = 0;
        fds[1].fd = tfd;   fds[1].events = POLLIN; fds[1].revents = 0;
        int rc = poll(fds, 2, -1);
        if (rc < 0 && errno != EINTR) return 1;

        if (fds[0].revents & POLLIN) {
            char buf[16];
            ssize_t n = read(sp[0], buf, sizeof(buf));
            for (ssize_t k = 0; k < n; ++k) {
                int sig = buf[k];
                emit_json(name, "signal",
                          std::string(",\"signal\":") + std::to_string(sig), log_fd);
                if (sig == SIGTERM || sig == SIGINT ||
                    sig == SIGQUIT || sig == SIGUSR1) {
                    if (ignore_term && (sig == SIGTERM || sig == SIGINT)) {
                        emit_json(name, "ignore_term", "", log_fd);
                    } else {
                        emit_json(name, "graceful_exit", "", log_fd);
                        _exit(0);
                    }
                }
            }
        }

        if (fds[1].revents & POLLIN) {
            uint64_t exp = 0;
            ssize_t n = read(tfd, &exp, sizeof(exp));
            (void)n;
            if (mode == "sleep") {
                emit_json(name, "done", ",\"reason\":\"sleep_finished\"", log_fd);
                _exit(0);
            } else if (mode == "exit") {
                long code = (i < argc) ? arg_long(argv[i], 0) : 0;
                emit_json(name, "done",
                          std::string(",\"reason\":\"planned_exit\",\"code\":") +
                              std::to_string(code), log_fd);
                _exit((int)code);
            } else if (mode == "crash") {
                emit_json(name, "crashing", "", log_fd);
                // 恢复默认处理并自杀，真实产生“被信号终止”的崩溃形态
                signal(SIGABRT, SIG_DFL);
                raise(SIGABRT);
                _exit(128 + SIGABRT);
            } else if (mode == "cycle") {
                static int beat = 0;
                ++beat;
                emit_json(name, "heartbeat",
                          std::string(",\"beat\":") + std::to_string(beat), log_fd);
            } else if (mode == "fail_n") {
                emit_json(name, "done",
                          std::string(",\"reason\":\"planned_exit\",\"code\":") +
                              std::to_string(g_fail_n_code), log_fd);
                _exit(g_fail_n_code);
            }
        }
    }
}
