/**
 * supervisor 模式集成测试
 *
 * 全部用例均为“事件驱动”：fork 出 supervisor 子进程，读取其 stdout 上的
 * JSON 事件行，等待特定事件（带超时）来同步，绝不用固定 sleep 碰运气。
 *
 * 覆盖：
 *   1. 信号突发 + 僵尸回收：多个服务同一瞬间被 SIGKILL，全部 waitpid 回收
 *   2. 退避重启：崩溃服务按指数退避重启，重试耗尽后 permanent_failure
 *   3. 失败热加载：SIGHUP 配置非法被拒绝，旧服务丝毫不受影响
 *   4. 关停顺序：先各自优雅信号，宽限期后只对卡死服务 SIGKILL
 *   5. 失败计数清零：稳定运行超过阈值后死亡，计数清零、attempt 归 1
 */

#include "test_framework.h"
#include "supervisor.h"

#include <poll.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include <cstdlib>
#include <cstring>
#include <deque>
#include <fstream>
#include <map>
#include <string>
#include <vector>

using namespace test;

namespace {

// ---------------------------------------------------------------------------
// 极简 JSON 行事件解析（只按 key 提取，字段顺序任意）
// ---------------------------------------------------------------------------
struct Event {
    std::string raw;
    std::map<std::string, std::string> str;
    std::map<std::string, long long> num;
    std::map<std::string, bool> boolean;

    std::string event() const {
        auto it = str.find("event");
        return it == str.end() ? "" : it->second;
    }
};

long long parse_signed_int(const char* p, const char* end) {
    bool neg = false;
    if (*p == '-') { neg = true; ++p; }
    long long v = 0;
    while (p < end && *p >= '0' && *p <= '9') { v = v * 10 + (*p - '0'); ++p; }
    return neg ? -v : v;
}

Event parse_event(const std::string& line) {
    Event e;
    e.raw = line;
    for (size_t i = 0; i < line.size();) {
        if (line[i] != '"') { ++i; continue; }
        size_t ks = i + 1, ke = line.find('"', ks);
        if (ke == std::string::npos) break;
        size_t colon = ke + 1;
        while (colon < line.size() &&
               (line[colon] == ' ' || line[colon] == ':')) ++colon;
        if (colon >= line.size() || line[ke + 1] != ':') { i = ke + 1; continue; }
        std::string key = line.substr(ks, ke - ks);
        const char c = line[colon];
        if (c == '"') {
            std::string val;
            size_t j = colon + 1;
            while (j < line.size() && line[j] != '"') {
                if (line[j] == '\\' && j + 1 < line.size()) {
                    char n = line[j + 1];
                    val.push_back(n == 'n' ? '\n' : n == 't' ? '\t' : n);
                    j += 2;
                } else {
                    val.push_back(line[j++]);
                }
            }
            e.str[key] = val;
            i = j + 1;
        } else if (c == 't' || c == 'f') {
            e.boolean[key] = (c == 't');
            i = colon + (c == 't' ? 4 : 5);
        } else {
            const char* begin = line.c_str() + colon;
            const char* endp = begin;
            if (*endp == '-') ++endp;
            while (endp < line.c_str() + line.size() &&
                   *endp >= '0' && *endp <= '9') ++endp;
            e.num[key] = parse_signed_int(begin, endp);
            i = static_cast<size_t>(endp - line.c_str());
        }
    }
    return e;
}

// ---------------------------------------------------------------------------
// 临时配置目录
// ---------------------------------------------------------------------------
struct TempDir {
    std::string path;
    std::vector<std::string> files;

    TempDir() {
        char tmpl[] = "/tmp/sup_test_XXXXXX";
        char* p = mkdtemp(tmpl);
        if (!p) throw std::runtime_error("mkdtemp failed");
        path = p;
    }
    ~TempDir() {
        for (const auto& f : files) { ::unlink(f.c_str()); }
        ::rmdir(path.c_str());
    }
    std::string write(const std::string& name, const std::string& content) {
        std::string full = path + "/" + name;
        std::ofstream(full) << content;
        files.push_back(full);
        return full;
    }
};

// ---------------------------------------------------------------------------
// supervisor 子进程 + 事件读取
// ---------------------------------------------------------------------------
struct SupervisorProc {
    pid_t pid = -1;
    int fd = -1;
    std::string partial;
    std::deque<Event> queue;

    ~SupervisorProc() {
        if (pid > 0) {
            kill(-pid, SIGKILL);  // 兜底清理（含同组进程）
            int st;
            waitpid(pid, &st, 0);
        }
        if (fd >= 0) close(fd);
    }

    // 拉取更多字节并切分为事件入队（队列是否已有内容不影响读取）
    bool pump(int timeout_ms) {
        struct pollfd pfd{fd, POLLIN, 0};
        int pr = poll(&pfd, 1, timeout_ms);
        if (pr <= 0) return false;
        for (;;) {
            char buf[4096];
            ssize_t n = read(fd, buf, sizeof(buf));
            if (n > 0) {
                partial.append(buf, static_cast<size_t>(n));
                size_t pos;
                while ((pos = partial.find('\n')) != std::string::npos) {
                    std::string line = partial.substr(0, pos);
                    partial.erase(0, pos + 1);
                    if (!line.empty()) queue.push_back(parse_event(line));
                }
                continue;
            }
            if (n == -1 && errno == EINTR) continue;
            break;  // EAGAIN
        }
        return !queue.empty();
    }

    // 等待满足谓词的事件；不匹配的事件保留在队列中供后续等待使用
    bool wait_event(const std::function<bool(const Event&)>& pred,
                    Event& out, int timeout_ms = 5000) {
        long long deadline = now_mono() + timeout_ms;
        for (;;) {
            for (auto it = queue.begin(); it != queue.end(); ++it) {
                if (pred(*it)) { out = *it; queue.erase(it); return true; }
            }
            long long remain = deadline - now_mono();
            if (remain <= 0) return false;
            // 队列已全部扫描且无匹配：阻塞拉取更多事件后再扫
            if (!pump(static_cast<int>(remain))) return false;
        }
    }

    // 消费/跳过直到匹配（匹配事件同样被取出）
    bool wait_event_named(const char* name, const std::string& service,
                          Event& out, int timeout_ms = 5000) {
        return wait_event(
            [&](const Event& e) {
                auto it = e.str.find("service");
                return e.event() == name &&
                       (it == e.str.end() || it->second == service);
            },
            out, timeout_ms);
    }

    int wait_exit(int timeout_ms = 5000) {
        long long deadline = now_mono() + timeout_ms;
        for (;;) {
            int status;
            pid_t r = waitpid(pid, &status, WNOHANG);
            if (r == pid) {
                pid = -1;
                return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
            }
            if (now_mono() > deadline) return -999;
            usleep(2000);
        }
    }

    static long long now_mono() {
        struct timespec ts;
        clock_gettime(CLOCK_MONOTONIC, &ts);
        return ts.tv_sec * 1000LL + ts.tv_nsec / 1000000LL;
    }
};

SupervisorProc spawn_supervisor(const std::string& config_path) {
    SupervisorProc sp;
    int p[2];
    ASSERT_EQ(pipe2(p, O_NONBLOCK | O_CLOEXEC), 0);
    pid_t pid = fork();
    ASSERT_NE(pid, -1);
    if (pid == 0) {
        dup2(p[1], STDOUT_FILENO);
        dup2(p[1], STDERR_FILENO);
        if (p[1] > 2) close(p[1]);
        close(p[0]);
        setsid();  // 自成会话，便于父进程整组兜底清理
        int code = ipc::run_supervisor(config_path);
        _exit(static_cast<unsigned char>(code == -1 ? 1 : code));
    }
    close(p[1]);
    sp.pid = pid;
    sp.fd = p[0];
    return sp;
}

bool is_service_event(const Event& e, const char* type, const std::string& svc) {
    auto it = e.str.find("service");
    return e.event() == type && it != e.str.end() && it->second == svc;
}

// 轮询确认进程已被彻底回收（僵尸状态下 kill(pid,0) 仍返回 0）
bool wait_reaped(pid_t pid, int timeout_ms = 3000) {
    long long deadline = SupervisorProc::now_mono() + timeout_ms;
    for (;;) {
        if (kill(pid, 0) == -1 && errno == ESRCH) return true;
        if (SupervisorProc::now_mono() > deadline) return false;
        usleep(2000);
    }
}

// ---------------------------------------------------------------------------
// 受控服务脚本（/bin/sh）
// ---------------------------------------------------------------------------
const char* kGraceScript =
    "trap 'exit 0' TERM; while :; do sleep 1; done";          // 优雅响应 TERM
const char* kStuckScript =
    "trap '' TERM; while :; do sleep 1; done";                // 忽略 TERM，需 KILL
const char* kCrashScript =
    "kill -SEGV $$";                                           // 立即崩溃

std::string sh_command(const char* script) {
    return "command = \"/bin/sh\" \"-c\" \"" + std::string(script) + "\"";
}

}  // namespace

// ===========================================================================
// 1. 信号突发 + 僵尸回收：5 个服务同一瞬间被 SIGKILL，必须全部回收
// ===========================================================================
void test_supervisor_burst_reap() {
    TempDir dir;
    std::string conf =
        "global { backoff_start_ms = 50 stable_uptime_ms = 60000 }\n";
    for (int i = 0; i < 5; ++i)
        conf += "service \"s" + std::to_string(i) + "\" { " +
                sh_command(kGraceScript) +
                " stop_signal = TERM stop_timeout_ms = 500 max_restarts = 0 }\n";
    std::string path = dir.write("burst.conf", conf);

    SupervisorProc sp = spawn_supervisor(path);
    std::map<std::string, pid_t> pids;
    for (int i = 0; i < 5; ++i) {
        Event e;
        ASSERT_TRUE(sp.wait_event(
            [&](const Event& ev) {
                return ev.event() == "starting" &&
                       ev.str.count("service") && ev.num.count("pid");
            },
            e));
        pids[e.str.at("service")] = static_cast<pid_t>(e.num.at("pid"));
    }
    ASSERT_EQ(pids.size(), 5u);

    // 紧循环对 5 个进程组同一瞬间发 SIGKILL（SIGCHLD 突发且不排队）
    for (auto& [name, pid] : pids) ASSERT_EQ(kill(-pid, SIGKILL), 0);

    // 每个服务都必须出现 exited(KILL)，且之后被彻底回收（无僵尸）
    for (auto& [name, pid] : pids) {
        Event e;
        ASSERT_TRUE(sp.wait_event(
            [&](const Event& ev) { return is_service_event(ev, "exited", name); },
            e));
        auto sig = e.num.find("signal");
        ASSERT_TRUE(sig != e.num.end() && sig->second == SIGKILL);
        ASSERT_TRUE(wait_reaped(pid));
    }

    kill(sp.pid, SIGTERM);
    ASSERT_EQ(sp.wait_exit(), 3);  // 5 个服务均重试耗尽
}

// ===========================================================================
// 2. 退避重启：崩溃按指数退避，达到 max_restarts 后永久失败
// ===========================================================================
void test_supervisor_backoff() {
    TempDir dir;
    std::string conf =
        "global { backoff_start_ms = 100 backoff_cap_ms = 5000 "
        "stable_uptime_ms = 60000 }\n"
        "service \"cr\" { " + std::string(sh_command(kCrashScript)) +
        " stop_signal = TERM stop_timeout_ms = 500 max_restarts = 3 }\n";
    std::string path = dir.write("backoff.conf", conf);

    SupervisorProc sp = spawn_supervisor(path);

    std::vector<long long> delays;
    std::vector<long long> attempts;
    long long crashes = 0;
    bool permanent = false;
    long long deadline = SupervisorProc::now_mono() + 8000;

    while (!permanent) {
        Event e;
        ASSERT_TRUE(sp.wait_event(
            [&](const Event&) { return true; }, e,
            static_cast<int>(deadline - SupervisorProc::now_mono())));
        if (e.event() == "starting") attempts.push_back(e.num.at("attempt"));
        if (e.event() == "exited") {
            ASSERT_EQ(e.str.at("signal_name"), std::string("SEGV"));
            ++crashes;
        }
        if (e.event() == "restart_scheduled")
            delays.push_back(e.num.at("delay_ms"));
        if (e.event() == "permanent_failure") {
            permanent = true;
            ASSERT_EQ(e.num.at("attempt"), 4);
            ASSERT_EQ(e.num.at("max_restarts"), 3);
        }
    }

    // 4 次启动尝试（首次 + 3 次重启），3 次退避：100 -> 200 -> 400
    ASSERT_EQ(attempts.size(), 4u);
    ASSERT_EQ(attempts, (std::vector<long long>{1, 2, 3, 4}));
    ASSERT_EQ(delays.size(), 3u);
    ASSERT_EQ(delays, (std::vector<long long>{100, 200, 400}));
    ASSERT_EQ(crashes, 4);

    kill(sp.pid, SIGTERM);
    ASSERT_EQ(sp.wait_exit(), 3);
}

// ===========================================================================
// 3. 失败热加载：非法配置被拒绝，旧服务不受影响；随后合法配置仍可生效
// ===========================================================================
void test_supervisor_reload_invalid() {
    TempDir dir;
    std::string path = dir.write(
        "reload.conf",
        "global { stable_uptime_ms = 60000 }\n"
        "service \"svc\" { " + std::string(sh_command(kGraceScript)) +
            " stop_signal = TERM stop_timeout_ms = 500 max_restarts = 0 }\n");

    SupervisorProc sp = spawn_supervisor(path);
    Event start;
    ASSERT_TRUE(sp.wait_event_named("starting", "svc", start));
    pid_t old_pid = static_cast<pid_t>(start.num.at("pid"));

    // 3a. 非法配置：必须被拒绝
    std::ofstream(path) << "this is not valid {{{\n";
    ASSERT_EQ(kill(sp.pid, SIGHUP), 0);
    Event rejected;
    ASSERT_TRUE(sp.wait_event_named("reload_rejected", "", rejected));
    ASSERT_TRUE(rejected.str.count("error"));

    // 旧服务仍存活、未重启
    ASSERT_EQ(kill(old_pid, 0), 0);
    Event skipped;
    bool got_exit = sp.wait_event_named("exited", "svc", skipped, 300);
    ASSERT_FALSE(got_exit);

    // 3b. 合法配置（删除 svc、新增 svc2）必须正常生效，证明管道仍健康
    std::ofstream(path)
        << "global { stable_uptime_ms = 60000 }\n"
        << "service \"svc2\" { " << sh_command(kGraceScript)
        << " stop_signal = TERM stop_timeout_ms = 500 max_restarts = 0 }\n";
    ASSERT_EQ(kill(sp.pid, SIGHUP), 0);
    Event removed, added, started2, stopped;
    ASSERT_TRUE(sp.wait_event_named("service_removed", "svc", removed));
    ASSERT_TRUE(sp.wait_event_named("service_added", "svc2", added));
    ASSERT_TRUE(sp.wait_event_named("starting", "svc2", started2));
    ASSERT_TRUE(sp.wait_event_named("stopped", "svc", stopped));
    ASSERT_EQ(stopped.str.at("reason"), std::string("removed"));
    ASSERT_TRUE(wait_reaped(old_pid));

    kill(sp.pid, SIGTERM);
    ASSERT_EQ(sp.wait_exit(), 0);
}

// ===========================================================================
// 4. 关停顺序：先发各自优雅信号；宽限期后只对卡死服务强制 SIGKILL
// ===========================================================================
void test_supervisor_shutdown_order() {
    TempDir dir;
    std::string conf =
        "global { stable_uptime_ms = 100 }\n"
        "service \"grace\" { " + std::string(sh_command(kGraceScript)) +
        " stop_signal = TERM stop_timeout_ms = 600 max_restarts = 0 }\n"
        "service \"stuck\" { " + std::string(sh_command(kStuckScript)) +
        " stop_signal = TERM stop_timeout_ms = 250 max_restarts = 0 }\n";
    std::string path = dir.write("stop.conf", conf);

    SupervisorProc sp = spawn_supervisor(path);
    for (const char* n : {"grace", "stuck"}) {
        Event h;
        ASSERT_TRUE(sp.wait_event_named("healthy", n, h));
    }

    ASSERT_EQ(kill(sp.pid, SIGTERM), 0);

    Event begin, stop_g, stop_s, force, exit_g, exit_s, stopped_g,
        stopped_s, done;
    ASSERT_TRUE(sp.wait_event_named("shutdown_begin", "", begin));
    ASSERT_TRUE(sp.wait_event_named("stopping", "grace", stop_g));
    ASSERT_TRUE(sp.wait_event_named("stopping", "stuck", stop_s));
    ASSERT_EQ(stop_g.str.at("signal"), std::string("TERM"));

    // stuck 宽限期 250ms 后才被强杀
    ASSERT_TRUE(sp.wait_event_named("force_kill", "stuck", force));
    long long force_delay = force.num.at("ts") - stop_s.num.at("ts");
    ASSERT_GE(force_delay, 200);  // 允许少量调度误差，但必须等到宽限附近

    ASSERT_TRUE(sp.wait_event_named("exited", "grace", exit_g));
    ASSERT_TRUE(sp.wait_event_named("exited", "stuck", exit_s));
    ASSERT_EQ(exit_s.str.at("signal_name"), std::string("KILL"));
    ASSERT_TRUE(sp.wait_event_named("stopped", "grace", stopped_g));
    ASSERT_TRUE(sp.wait_event_named("stopped", "stuck", stopped_s));
    ASSERT_EQ(stopped_g.boolean.at("graceful"), true);
    ASSERT_EQ(stopped_s.boolean.at("graceful"), false);
    ASSERT_TRUE(sp.wait_event_named("shutdown_complete", "", done));
    ASSERT_EQ(done.boolean.at("had_failures"), false);

    ASSERT_EQ(sp.wait_exit(), 0);

    // grace 从未被强制
    Event dummy;
    ASSERT_FALSE(sp.wait_event_named("force_kill", "grace", dummy, 0));
}

// ===========================================================================
// 5. 稳定运行达到阈值后失败计数清零：重启后 attempt 重新从 1 开始
// ===========================================================================
void test_supervisor_stable_reset() {
    TempDir dir;
    std::string conf =
        "global { backoff_start_ms = 50 stable_uptime_ms = 300 }\n"
        "service \"st\" { " + std::string(sh_command(kGraceScript)) +
        " stop_signal = TERM stop_timeout_ms = 500 max_restarts = 5 }\n";
    std::string path = dir.write("reset.conf", conf);

    SupervisorProc sp = spawn_supervisor(path);
    Event first;
    ASSERT_TRUE(sp.wait_event_named("starting", "st", first));
    pid_t pid1 = static_cast<pid_t>(first.num.at("pid"));
    ASSERT_TRUE(sp.wait_event_named("healthy", "st", first));

    // 稳定运行后令其优雅死亡
    ASSERT_EQ(kill(-pid1, SIGTERM), 0);
    Event reset;
    ASSERT_TRUE(sp.wait_event_named("failure_count_reset", "st", reset));
    ASSERT_GE(reset.num.at("uptime_ms"), 300);

    // 清零后的第一次启动 attempt 必须为 1
    Event second;
    ASSERT_TRUE(sp.wait_event_named("starting", "st", second));
    ASSERT_EQ(second.num.at("attempt"), 1);

    kill(sp.pid, SIGTERM);
    ASSERT_EQ(sp.wait_exit(), 0);
}

// ===========================================================================
// 6. 配置解析：合法配置字段正确
// ===========================================================================
void test_config_parser_valid() {
    TempDir dir;
    std::string path = dir.write(
        "ok.conf",
        "# comment\n"
        "global { backoff_start_ms = 100 backoff_cap_ms = 9000 "
        "stable_uptime_ms = 1500 }\n"
        "service \"web\" {\n"
        "  command = \"/usr/bin/app\" \"--port\" \"8080\"\n"
        "  stop_signal = SIGINT\n"
        "  stop_timeout_ms = 2500\n"
        "  max_restarts = 7\n"
        "  cwd = \"/srv\"\n"
        "  env FOO = \"bar\"\n"
        "}\n");

    ipc::SupervisorConfig cfg;
    std::string err;
    ASSERT_TRUE(ipc::parse_supervisor_config(path, cfg, err));
    ASSERT_EQ(cfg.backoff_start_ms, 100);
    ASSERT_EQ(cfg.backoff_cap_ms, 9000);
    ASSERT_EQ(cfg.stable_uptime_ms, 1500);
    ASSERT_EQ(cfg.services.size(), 1u);

    const auto& svc = cfg.services.at("web");
    ASSERT_EQ(svc.argv.size(), 3u);
    ASSERT_EQ(svc.argv[0], std::string("/usr/bin/app"));
    ASSERT_EQ(svc.argv[1], std::string("--port"));
    ASSERT_EQ(svc.argv[2], std::string("8080"));
    ASSERT_EQ(svc.stop_signal, std::string("SIGINT"));
    ASSERT_EQ(svc.stop_timeout_ms, 2500);
    ASSERT_EQ(svc.max_restarts, 7);
    ASSERT_EQ(svc.cwd, std::string("/srv"));
    ASSERT_EQ(svc.env.size(), 1u);
    ASSERT_EQ(svc.env[0].first, std::string("FOO"));
    ASSERT_EQ(svc.env[0].second, std::string("bar"));
}

// ===========================================================================
// 7. 配置解析：各类非法输入必须带错误返回 false
// ===========================================================================
void test_config_parser_invalid() {
    TempDir dir;
    auto must_fail = [&](const std::string& name, const std::string& body) {
        std::string p = dir.write(name, body);
        ipc::SupervisorConfig cfg;
        std::string err;
        ASSERT_FALSE(ipc::parse_supervisor_config(p, cfg, err));
        ASSERT_FALSE(err.empty());
    };

    must_fail("a.conf", "garbage {\n");                        // 非法顶层块
    must_fail("b.conf", "service \"x\" { }\n");               // 缺 command
    must_fail("c.conf",
              "service \"x\" { command = \"/bin/true\" "
              "stop_signal = NOTASIG }\n");                    // 未知信号
    must_fail("d.conf",
              "global { backoff_start_ms = 500 "
              "backoff_cap_ms = 100 }\n");                    // cap < start
    must_fail("e.conf",
              "service \"x\" { command = \"/bin/true\" "
              "max_restarts = -1 }\n");                       // 负数
    must_fail("f.conf",
              "service \"x\" { command = \"/bin/true\" }\n"
              "service \"x\" { command = \"/bin/false\" }\n");// 重名
    must_fail("g.conf", "service \"x\" { command = \"/bin/true\"");  // 未闭合
    must_fail("h.conf", "global { unknown_key = 1 }\n"
                        "service \"x\" { command = \"/bin/true\" }\n");
}

// ===========================================================================
// 8. 空/仅注释配置合法，得到零个服务
// ===========================================================================
void test_config_parser_empty() {
    TempDir dir;
    std::string path = dir.write("empty.conf", "# 只有注释\n\n   \n");
    ipc::SupervisorConfig cfg;
    std::string err;
    ASSERT_TRUE(ipc::parse_supervisor_config(path, cfg, err));
    ASSERT_EQ(cfg.services.size(), 0u);

    std::string missing = dir.path + "/does_not_exist.conf";
    ASSERT_FALSE(ipc::parse_supervisor_config(missing, cfg, err));
    ASSERT_FALSE(err.empty());
}

// ===========================================================================
// 9. 启动时配置文件非法 -> 退出码 2
// ===========================================================================
void test_config_bad_startup_exit_code() {
    TempDir dir;
    std::string path = dir.write("bad.conf", "this is broken {{{\n");
    SupervisorProc sp = spawn_supervisor(path);
    int code = sp.wait_exit(3000);
    ASSERT_EQ(code, 2);
}

void test_supervisor_suite() {
    run_test("supervisor_burst_reap", test_supervisor_burst_reap);
    run_test("supervisor_backoff", test_supervisor_backoff);
    run_test("supervisor_reload_invalid", test_supervisor_reload_invalid);
    run_test("supervisor_shutdown_order", test_supervisor_shutdown_order);
    run_test("supervisor_stable_reset", test_supervisor_stable_reset);
    run_test("config_parser_valid", test_config_parser_valid);
    run_test("config_parser_invalid", test_config_parser_invalid);
    run_test("config_parser_empty", test_config_parser_empty);
    run_test("config_bad_startup_exit_code", test_config_bad_startup_exit_code);
}
