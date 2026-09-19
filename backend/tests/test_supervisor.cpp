/**
 * Supervisor 端到端测试
 *
 * 全部通过解析监管器输出的 JSON Lines 结构化事件进行同步与断言，
 * 以“等到某个事件出现”代替固定 sleep 碰运气；只有验证退避/宽限期
 * 这类时间语义时，才比较事件自带的 ts_ms 时间戳（宽松下界）。
 *
 * 覆盖：
 *   - sup_signal_burst      信号突发：8 个子进程同时被 KILL，全部回收
 *   - sup_zombie_reaping    僵尸回收：多子进程同时退出后不存在 Z 状态子进程
 *   - sup_backoff_restart   退避节奏重启 + 稳定运行清零失败计数
 *   - sup_retry_exhausted   重试耗尽 -> service_failed，监管器非零退出
 *   - sup_exec_failure      命令不存在 -> 启动失败计入重试，非零退出
 *   - sup_bad_reload        解析失败的 SIGHUP 不影响旧服务；合法加载再做增删换
 *   - sup_shutdown_order    关停顺序：各自优雅信号 -> 宽限期 -> 卡死者 SIGKILL
 */

#include "test_framework.h"

#include <algorithm>
#include <cctype>
#include <dirent.h>
#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/wait.h>

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

using namespace test;

namespace {

// ---------------------------------------------------------------------------
// 二进制定位：优先 /proc/self/exe 同目录（Docker 运行态），
// 其次 CMake 构建目录（ctest / 本地构建）
// ---------------------------------------------------------------------------

std::string self_dir() {
    char buf[4096];
    ssize_t n = ::readlink("/proc/self/exe", buf, sizeof(buf) - 1);
    if (n <= 0) return ".";
    buf[n] = '\0';
    std::string p(buf);
    size_t slash = p.rfind('/');
    return slash == std::string::npos ? "." : p.substr(0, slash);
}

bool exists_exec(const std::string& p) {
    return ::access(p.c_str(), X_OK) == 0;
}

std::string g_supervisor_bin;
std::string g_worker_bin;
std::string g_tmpdir;

std::string bin_path(const char* name) {
    std::string a = self_dir() + "/" + name;
    if (exists_exec(a)) return a;
#ifdef IPC_BINARY_DIR
    std::string b = std::string(IPC_BINARY_DIR) + "/" + name;
    if (exists_exec(b)) return b;
#endif
    return a;
}

// ---------------------------------------------------------------------------
// 极简 JSON 事件解析（监管器事件均为扁平 key:value，无嵌套对象）
// ---------------------------------------------------------------------------

struct Ev {
    std::unordered_map<std::string, std::string> s;
    std::unordered_map<std::string, long long> i;

    std::string type() const {
        auto it = s.find("event");
        return it == s.end() ? "" : it->second;
    }
    bool is(const char* t) const { return type() == t; }
    const std::string* str(const std::string& k) const {
        auto it = s.find(k);
        return it == s.end() ? nullptr : &it->second;
    }
    long long num_or(const std::string& k, long long dflt) const {
        auto it = i.find(k);
        return it == i.end() ? dflt : it->second;
    }
    bool has_num(const std::string& k) const { return i.count(k) > 0; }
    bool tf(const std::string& k) const {
        auto it = s.find(k);
        return it != s.end() && it->second == "true";
    }
    const std::string* svc() const { return str("service"); }
};

Ev parse_event(const std::string& line) {
    Ev e;
    size_t p = 0;
    const size_t n = line.size();
    while (p < n) {
        size_t q = line.find('"', p);
        if (q == std::string::npos) break;
        size_t qe = q + 1;
        while (qe < n && line[qe] != '"') {
            if (line[qe] == '\\') ++qe;
            ++qe;
        }
        if (qe >= n) break;
        std::string key = line.substr(q + 1, qe - q - 1);
        size_t colon = line.find(':', qe + 1);
        if (colon == std::string::npos) break;
        size_t v = colon + 1;
        while (v < n && (line[v] == ' ' || line[v] == '\t')) ++v;
        if (v < n && line[v] == '"') {
            std::string val;
            size_t e2 = v + 1;
            while (e2 < n && line[e2] != '"') {
                if (line[e2] == '\\' && e2 + 1 < n) {
                    char c = line[e2 + 1];
                    switch (c) {
                        case 'n': val.push_back('\n'); break;
                        case 't': val.push_back('\t'); break;
                        default:  val.push_back(c);
                    }
                    e2 += 2;
                } else {
                    val.push_back(line[e2++]);
                }
            }
            e.s[key] = val;
            p = e2 + 1;
        } else if (v < n && (line[v] == 't' || line[v] == 'f')) {
            size_t e2 = v;
            while (e2 < n && std::isalpha((unsigned char)line[e2])) ++e2;
            e.s[key] = line.substr(v, e2 - v);  // true / false
            p = e2;
        } else {
            size_t e2 = v;
            if (e2 < n && line[e2] == '-') ++e2;
            while (e2 < n && std::isdigit((unsigned char)line[e2])) ++e2;
            if (e2 > v) e.i[key] = std::atoll(line.substr(v, e2 - v).c_str());
            p = e2;
        }
    }
    return e;
}

// ---------------------------------------------------------------------------
// 监管器子进程封装
// ---------------------------------------------------------------------------

struct Runner {
    pid_t pid = -1;
    int fd = -1;
    std::string buf;
    std::vector<Ev> ev;
    std::set<pid_t> worker_pids;
};

bool start_runner(Runner& r, const std::string& cfg) {
    int fds[2];
    if (pipe(fds) != 0) return false;
    pid_t pid = fork();
    if (pid < 0) return false;
    if (pid == 0) {
        ::close(fds[0]);
        dup2(fds[1], STDOUT_FILENO);
        int devnull = open("/dev/null", O_WRONLY);
        if (devnull >= 0) {
            dup2(devnull, STDERR_FILENO);
            ::close(devnull);
        }
        if (fds[1] != STDOUT_FILENO) ::close(fds[1]);
        execl(g_supervisor_bin.c_str(), "ipc_supervisor",
              "--config", cfg.c_str(), (char*)nullptr);
        _exit(127);
    }
    ::close(fds[1]);
    fcntl(fds[0], F_SETFL, O_NONBLOCK);
    r.pid = pid;
    r.fd = fds[0];
    return true;
}

// 在 timeout_ms 内尽量读取并解析事件，返回新增事件数
int pump(Runner& r, int timeout_ms) {
    size_t before = r.ev.size();
    int waited = 0;
    while (waited < timeout_ms) {
        pollfd pfd;
        pfd.fd = r.fd;
        pfd.events = POLLIN;
        pfd.revents = 0;
        int pr = poll(&pfd, 1, 10);
        if (pr > 0 && (pfd.revents & (POLLIN | POLLHUP))) {
            char tmp[4096];
            ssize_t n = read(r.fd, tmp, sizeof(tmp));
            if (n > 0) {
                r.buf.append(tmp, (size_t)n);
                size_t pos;
                while ((pos = r.buf.find('\n')) != std::string::npos) {
                    std::string line = r.buf.substr(0, pos);
                    r.buf.erase(0, pos + 1);
                    if (line.empty()) continue;
                    Ev e = parse_event(line);
                    if (e.type().rfind("worker_", 0) == 0) continue;  // 忽略子进程自身事件
                    if (e.is("service_started") && e.has_num("pid"))
                        r.worker_pids.insert((pid_t)e.num_or("pid", -1));
                    r.ev.push_back(std::move(e));
                }
            }
        }
        waited += 10;
    }
    return (int)(r.ev.size() - before);
}

using Pred = std::function<bool(const std::vector<Ev>&)>;

// 事件驱动等待：持续读取直到谓词成立或整体超时；不依赖固定 sleep
bool wait_until(Runner& r, Pred pred, int timeout_ms = 5000) {
    if (pred(r.ev)) return true;
    int waited = 0;
    while (waited < timeout_ms) {
        pump(r, 100);
        if (pred(r.ev)) return true;
        waited += 100;
    }
    return pred(r.ev);
}

// 找最后一个满足条件的事件下标，找不到返回 -1
int find_idx(const std::vector<Ev>& es, const std::function<bool(const Ev&)>& pred) {
    for (size_t k = 0; k < es.size(); ++k)
        if (pred(es[k])) return (int)k;
    return -1;
}

int count_ev(const std::vector<Ev>& es, const std::function<bool(const Ev&)>& pred) {
    int c = 0;
    for (const auto& e : es) if (pred(e)) ++c;
    return c;
}

int wait_exit(Runner& r, int timeout_ms = 5000) {
    int waited = 0;
    while (waited < timeout_ms) {
        int status = 0;
        pid_t w = waitpid(r.pid, &status, WNOHANG);
        if (w == r.pid) {
            r.pid = -1;
            if (WIFEXITED(status)) return WEXITSTATUS(status);
            if (WIFSIGNALED(status)) return 128 + WTERMSIG(status);
            return -1;
        }
        usleep(10000);
        waited += 10;
    }
    return -999; // 超时
}

// RAII 清理，断言失败时也不泄漏进程
struct Cleanup {
    Runner& r;
    explicit Cleanup(Runner& x) : r(x) {}
    ~Cleanup() {
        for (pid_t p : r.worker_pids) kill(p, SIGKILL);
        if (r.pid > 0) {
            kill(r.pid, SIGKILL);
            int st;
            waitpid(r.pid, &st, 0);
        }
        if (r.fd >= 0) ::close(r.fd);
    }
};

// ---------------------------------------------------------------------------
// 配置/临时文件工具
// ---------------------------------------------------------------------------

void write_file(const std::string& path, const std::string& content) {
    FILE* f = std::fopen(path.c_str(), "w");
    ASSERT_TRUE(f != nullptr);
    std::fwrite(content.data(), 1, content.size(), f);
    std::fclose(f);
}

std::string cfg_path(const std::string& name) {
    return g_tmpdir + "/" + name;
}

// 扫描 /proc：统计 ppid 为 parent 且状态为 Z(僵尸) 的进程数
int count_zombies(pid_t parent) {
    int zombies = 0;
    DIR* d = opendir("/proc");
    if (!d) return -1;
    struct dirent* de;
    while ((de = readdir(d)) != nullptr) {
        if (de->d_name[0] < '0' || de->d_name[0] > '9') continue;
        std::string sp = std::string("/proc/") + de->d_name + "/stat";
        FILE* f = std::fopen(sp.c_str(), "r");
        if (!f) continue;
        char stat[1024];
        size_t nr = std::fread(stat, 1, sizeof(stat) - 1, f);
        stat[nr] = '\0';
        std::fclose(f);
        // pid (comm) state pparent ...  comm 可能含空格/括号，取最后一个 ')'
        char state = '?';
        int ppid = 0;
        char* rp = std::strrchr(stat, ')');
        if (rp && sscanf(rp + 1, " %c %d", &state, &ppid) == 2) {
            if (ppid == (int)parent && state == 'Z') ++zombies;
        }
    }
    closedir(d);
    return zombies;
}

std::string section_header(const std::string& svc) {
    return "[service:" + svc + "]\n";
}

// 取某服务最新一次 service_started 的 pid
pid_t latest_pid(const Runner& r, const std::string& svc) {
    pid_t pid = -1;
    for (const auto& e : r.ev) {
        if (e.is("service_started") && e.svc() && *e.svc() == svc && e.has_num("pid"))
            pid = (pid_t)e.num_or("pid", -1);
    }
    return pid;
}

bool alive(pid_t pid) {
    return pid > 0 && kill(pid, 0) == 0;
}

// ---------------------------------------------------------------------------
// 测试 1：信号突发 —— 8 个子进程同时被 SIGKILL，必须全部回收
// ---------------------------------------------------------------------------

void test_sup_signal_burst() {
    const std::string cfg = cfg_path("burst.conf");
    {
        std::string c;
        for (int k = 0; k < 8; ++k) {
            std::string s = "w" + std::to_string(k);
            c += section_header(s);
            c += "command = " + g_worker_bin + " --name " + s + " sleep 30000\n";
            c += "stable_ms = 60\n";
            c += "backoff_ms = 8000\n";
            c += "start_retries = 3\n\n";
        }
        write_file(cfg, c);
    }

    Runner r;
    Cleanup cu(r);
    ASSERT_TRUE(start_runner(r, cfg));

    ASSERT_TRUE(wait_until(r, [](const std::vector<Ev>& es) {
        return count_ev(es, [](const Ev& e) { return e.is("service_started"); }) >= 8;
    }));
    ASSERT_EQ((int)r.worker_pids.size(), 8);

    size_t marker = r.ev.size();

    // 信号突发：连续对 8 个子进程发 SIGKILL（SIGCHLD 可能被合并）
    std::vector<pid_t> pids(r.worker_pids.begin(), r.worker_pids.end());
    for (pid_t p : pids) kill(p, SIGKILL);

    ASSERT_TRUE(wait_until(r, [&](const std::vector<Ev>& es) {
        int n = 0;
        for (size_t k = marker; k < es.size(); ++k)
            if (es[k].is("service_exited")) ++n;
        return n >= 8;
    }));

    std::set<pid_t> exited;
    for (const auto& e : r.ev) {
        if (!e.is("service_exited")) continue;
        exited.insert((pid_t)e.num_or("pid", -1));
        // 每个退出都是“被信号 9 杀死、非预期”
        ASSERT_TRUE(e.str("reason") && *e.str("reason") == "unexpected");
        ASSERT_TRUE(e.str("how") && *e.str("how") == "signaled");
        ASSERT_EQ(e.num_or("code", -1), 9);
    }
    ASSERT_EQ((int)exited.size(), 8);

    int backoffs = 0;
    long long reaped = 0;
    for (size_t k = marker; k < r.ev.size(); ++k) {
        if (r.ev[k].is("service_backoff")) ++backoffs;
        if (r.ev[k].is("reap_batch")) reaped += r.ev[k].num_or("count", 0);
    }
    ASSERT_EQ(backoffs, 8);
    ASSERT_EQ(reaped, 8);  // 全部回收，无遗漏

    kill(r.pid, SIGTERM);
    ASSERT_EQ(wait_exit(r), 0);
}

// ---------------------------------------------------------------------------
// 测试 2：僵尸进程回收 —— 多子进程同时退出后无 Z 状态残留
// ---------------------------------------------------------------------------

void test_sup_zombie_reaping() {
    const std::string cfg = cfg_path("zombie.conf");
    {
        std::string c;
        for (int k = 0; k < 6; ++k) {
            std::string s = "z" + std::to_string(k);
            c += section_header(s);
            c += "command = " + g_worker_bin + " --name " + s + " sleep 30000\n";
            c += "stable_ms = 60\n";
            c += "backoff_ms = 8000\n";
            c += "start_retries = 2\n\n";
        }
        write_file(cfg, c);
    }

    Runner r;
    Cleanup cu(r);
    ASSERT_TRUE(start_runner(r, cfg));
    ASSERT_TRUE(wait_until(r, [](const std::vector<Ev>& es) {
        return count_ev(es, [](const Ev& e) {
            return (e.is("service_healthy") || e.is("service_stable_reset"));
        }) >= 6;
    }));

    size_t marker = r.ev.size();

    // 同时请求 6 个子进程优雅退出（worker 收到信号立即 _exit(0)）
    std::vector<pid_t> pids(r.worker_pids.begin(), r.worker_pids.end());
    ASSERT_EQ((int)pids.size(), 6);
    for (pid_t p : pids) kill(p, SIGTERM);

    ASSERT_TRUE(wait_until(r, [&](const std::vector<Ev>& es) {
        int n = 0;
        for (size_t k = marker; k < es.size(); ++k) {
            const Ev& e = es[k];
            if (e.is("service_exited") &&
                e.str("reason") && *e.str("reason") == "unexpected" &&
                e.str("how") && *e.str("how") == "exited" &&
                e.num_or("code", -1) == 0) {
                ++n;
            }
        }
        return n >= 6;
    }));

    // service_exited 在 waitpid 成功回收后才发出，此刻必须无僵尸
    for (int tries = 0; tries < 20; ++tries) {
        ASSERT_EQ(count_zombies(r.pid), 0);
        usleep(2000);
    }

    long long reaped = 0;
    for (size_t k = marker; k < r.ev.size(); ++k)
        if (r.ev[k].is("reap_batch")) reaped += r.ev[k].num_or("count", 0);
    ASSERT_EQ(reaped, 6);

    kill(r.pid, SIGTERM);
    ASSERT_EQ(wait_exit(r), 0);
}

// ---------------------------------------------------------------------------
// 测试 3：退避节奏重启 + 稳定运行后清零失败计数（清零后重新获得完整额度）
// ---------------------------------------------------------------------------

void test_sup_backoff_restart() {
    const std::string state = cfg_path("failn.state");
    ::unlink(state.c_str());
    const std::string cfg = cfg_path("backoff.conf");
    std::string c;
    c += section_header("flaky");
    c += "command = " + g_worker_bin + " --name flaky fail_n 2 " + state + "\n";
    c += "stable_ms = 120\n";
    c += "backoff_ms = 40,80\n";
    c += "start_retries = 5\n";
    write_file(cfg, c);

    Runner r;
    Cleanup cu(r);
    ASSERT_TRUE(start_runner(r, cfg));

    auto attempt_started = [&](int attempt) {
        return find_idx(r.ev, [&](const Ev& e) {
            return e.is("service_started") && e.num_or("attempt", 0) == attempt;
        });
    };
    auto backoff_with = [&](long long failures) {
        return find_idx(r.ev, [&](const Ev& e) {
            return e.is("service_backoff") && e.num_or("failure_count", 0) == failures;
        });
    };

    ASSERT_TRUE(wait_until(r, [&](const std::vector<Ev>&) { return attempt_started(1) >= 0; }));
    int st1 = attempt_started(1);

    ASSERT_TRUE(wait_until(r, [&](const std::vector<Ev>&) { return backoff_with(1) >= 0; }));
    int bo1 = backoff_with(1);
    ASSERT_EQ(r.ev[bo1].num_or("delay_ms", -1), 40);  // 第一次退避

    ASSERT_TRUE(wait_until(r, [&](const std::vector<Ev>&) {
        int idx = -1;
        for (size_t k = 0; k < r.ev.size(); ++k)
            if (r.ev[k].is("service_started") && r.ev[k].num_or("attempt", 0) == 2) idx = (int)k;
        return idx >= 0;
    }));
    int st2 = -1;
    for (size_t k = 0; k < r.ev.size(); ++k)
        if (r.ev[k].is("service_started") && r.ev[k].num_or("attempt", 0) == 2) st2 = (int)k;
    ASSERT_GE(st2, 0);

    ASSERT_TRUE(wait_until(r, [&](const std::vector<Ev>&) { return backoff_with(2) >= 0; }));
    int bo2 = backoff_with(2);
    ASSERT_EQ(r.ev[bo2].num_or("delay_ms", -1), 80);  // 退避节奏递增

    // 第三次启动存活并稳定：失败计数清零
    ASSERT_TRUE(wait_until(r, [&](const std::vector<Ev>&) {
        for (const auto& e : r.ev)
            if (e.is("service_started") && e.num_or("attempt", 0) == 3) return true;
        return false;
    }));
    int st3 = -1;
    for (size_t k = 0; k < r.ev.size(); ++k)
        if (r.ev[k].is("service_started") && r.ev[k].num_or("attempt", 0) == 3) st3 = (int)k;
    ASSERT_GE(st3, 0);

    ASSERT_TRUE(wait_until(r, [](const std::vector<Ev>& es) {
        return find_idx(es, [](const Ev& e) { return e.is("service_stable_reset"); }) >= 0;
    }));
    int reset_idx = find_idx(r.ev, [](const Ev& e) { return e.is("service_stable_reset"); });
    ASSERT_EQ(r.ev[reset_idx].num_or("cleared_failures", -1), 2);

    // 时间语义：退避确实被等待（宽松下界，避免 CI 抖动误报）
    ASSERT_GE(r.ev[st2].num_or("ts_ms", 0) - r.ev[st1].num_or("ts_ms", 0), 30);
    ASSERT_GE(r.ev[st3].num_or("ts_ms", 0) - r.ev[st2].num_or("ts_ms", 0), 60);

    // 清零后再杀一次：失败计数从 1 重新开始，证明计数确已清零
    pid_t good_pid = (pid_t)r.ev[st3].num_or("pid", -1);
    ASSERT_TRUE(alive(good_pid));
    size_t marker = r.ev.size();
    kill(good_pid, SIGKILL);
    ASSERT_TRUE(wait_until(r, [&](const std::vector<Ev>& es) {
        for (size_t k = marker; k < es.size(); ++k)
            if (es[k].is("service_backoff") && es[k].num_or("failure_count", 0) == 1)
                return true;
        return false;
    }));
    int bo_again = -1;
    for (size_t k = marker; k < r.ev.size(); ++k)
        if (r.ev[k].is("service_backoff") && r.ev[k].num_or("failure_count", 0) == 1)
            bo_again = (int)k;
    ASSERT_GE(bo_again, 0);
    ASSERT_EQ(r.ev[bo_again].num_or("delay_ms", -1), 40);

    kill(r.pid, SIGTERM);
    ASSERT_EQ(wait_exit(r), 0);
}

// ---------------------------------------------------------------------------
// 测试 4：重试耗尽 -> failed，监管器以非零码退出
// ---------------------------------------------------------------------------

void test_sup_retry_exhausted() {
    const std::string cfg = cfg_path("exhaust.conf");
    std::string c;
    c += section_header("doomed");
    c += "command = " + g_worker_bin + " --name doomed exit 1\n";
    c += "stable_ms = 100\n";
    c += "backoff_ms = 10\n";
    c += "start_retries = 3\n";
    write_file(cfg, c);

    Runner r;
    Cleanup cu(r);
    ASSERT_TRUE(start_runner(r, cfg));

    ASSERT_TRUE(wait_until(r, [](const std::vector<Ev>& es) {
        return find_idx(es, [](const Ev& e) { return e.is("service_failed"); }) >= 0;
    }));
    int failed = find_idx(r.ev, [](const Ev& e) { return e.is("service_failed"); });
    ASSERT_EQ(r.ev[failed].num_or("attempts", -1), 3);

    ASSERT_EQ(count_ev(r.ev, [](const Ev& e) { return e.is("service_started"); }), 3);
    ASSERT_EQ(count_ev(r.ev, [](const Ev& e) { return e.is("service_backoff"); }), 2);

    ASSERT_EQ(wait_exit(r, 3000), 1);  // 非零退出码反映失败服务

    int stopped = find_idx(r.ev, [](const Ev& e) {
        return e.is("supervisor_stopped") && e.num_or("failed_services", 0) >= 1;
    });
    ASSERT_GE(stopped, 0);
}

// ---------------------------------------------------------------------------
// 测试 5：命令不存在（启动失败）同样计入重试并导致非零退出
// ---------------------------------------------------------------------------

void test_sup_exec_failure() {
    const std::string cfg = cfg_path("badbin.conf");
    std::string c;
    c += section_header("ghost");
    c += "command = /nonexistent/ipc_worker_ghost --name ghost\n";
    c += "backoff_ms = 10\n";
    c += "start_retries = 2\n";
    write_file(cfg, c);

    Runner r;
    Cleanup cu(r);
    ASSERT_TRUE(start_runner(r, cfg));

    ASSERT_TRUE(wait_until(r, [](const std::vector<Ev>& es) {
        return find_idx(es, [](const Ev& e) { return e.is("service_failed"); }) >= 0;
    }));

    // execvp 失败的子进程以 127 退出
    ASSERT_EQ(count_ev(r.ev, [](const Ev& e) {
        return e.is("service_exited") &&
               e.str("how") && *e.str("how") == "exited" &&
               e.num_or("code", 0) == 127;
    }), 2);
    ASSERT_EQ(wait_exit(r, 3000), 1);
}

// ---------------------------------------------------------------------------
// 测试 6：失败热加载不影响旧服务；随后合法加载完成新增/删除/有序替换
// ---------------------------------------------------------------------------

void test_sup_bad_reload() {
    const std::string cfg = cfg_path("reload.conf");
    auto common = [&](const std::string& svc, const std::string& args) {
        std::string c = section_header(svc);
        c += "command = " + g_worker_bin + " " + args + "\n";
        c += "stop_signal = TERM\n";
        c += "stop_grace_ms = 400\n";
        c += "stable_ms = 50\n";
        c += "backoff_ms = 8000\n";
        c += "start_retries = 2\n\n";
        return c;
    };
    write_file(cfg, common("web", "--name web cycle 500") +
                     common("extra", "--name extra sleep 30000"));

    Runner r;
    Cleanup cu(r);
    ASSERT_TRUE(start_runner(r, cfg));
    ASSERT_TRUE(wait_until(r, [](const std::vector<Ev>& es) {
        int n = 0;
        for (const auto& e : es)
            if ((e.is("service_healthy") || e.is("service_stable_reset")) &&
                e.svc() && (*e.svc() == "web" || *e.svc() == "extra")) ++n;
        return n >= 2;
    }));

    pid_t web_old = latest_pid(r, "web");
    pid_t extra_old = latest_pid(r, "extra");
    ASSERT_TRUE(alive(web_old) && alive(extra_old));

    // 非法配置（键值行缺少 '='）：只产生 config_invalid，旧服务完全不受影响
    size_t marker = r.ev.size();
    write_file(cfg, common("web", "--name web cycle 500") +
                     common("extra", "--name extra sleep 30000") +
                     "this_line_has_no_equal_sign\n");
    kill(r.pid, SIGHUP);

    ASSERT_TRUE(wait_until(r, [&](const std::vector<Ev>& es) {
        for (size_t k = marker; k < es.size(); ++k)
            if (es[k].is("config_invalid")) return true;
        return false;
    }));
    // 排空管道中可能残留的事件，再严格断言没有任何状态变更
    pump(r, 100);
    for (size_t k = marker; k < r.ev.size(); ++k) {
        const std::string t = r.ev[k].type();
        ASSERT_TRUE(t != "service_stopping");
        ASSERT_TRUE(t != "service_changed");
        ASSERT_TRUE(t != "service_deleted");
        ASSERT_TRUE(t != "service_removed");
        ASSERT_TRUE(t != "service_added");
        ASSERT_TRUE(t != "service_replaced");
    }
    ASSERT_TRUE(alive(web_old));
    ASSERT_TRUE(alive(extra_old));

    // 合法重载：替换 web、删除 extra、新增 newcomer
    size_t marker2 = r.ev.size();
    write_file(cfg, common("web", "--name web cycle 120") +
                     common("newcomer", "--name newcomer sleep 30000"));
    kill(r.pid, SIGHUP);

    // 修改过的服务有序替换：先发旧实例优雅信号（replaced=true）
    ASSERT_TRUE(wait_until(r, [&](const std::vector<Ev>& es) {
        for (size_t k = marker2; k < es.size(); ++k) {
            const Ev& e = es[k];
            if (e.is("service_stopping") && e.svc() && *e.svc() == "web")
                return true;
        }
        return false;
    }));
    int stopping = -1;
    for (size_t k = marker2; k < r.ev.size(); ++k)
        if (r.ev[k].is("service_stopping") && r.ev[k].svc() && *r.ev[k].svc() == "web")
            stopping = (int)k;
    ASSERT_GE(stopping, 0);
    ASSERT_TRUE(r.ev[stopping].tf("replaced"));

    ASSERT_TRUE(wait_until(r, [&](const std::vector<Ev>& es) {
        bool add = false, del = false, rep = false, rem = false;
        for (size_t k = marker2; k < es.size(); ++k) {
            const Ev& e = es[k];
            if (e.is("service_added") && e.svc() && *e.svc() == "newcomer") add = true;
            if (e.is("service_deleted") && e.svc() && *e.svc() == "extra") del = true;
            if (e.is("service_replaced") && e.svc() && *e.svc() == "web") rep = true;
            if (e.is("service_removed") && e.svc() && *e.svc() == "extra") rem = true;
        }
        return add && del && rep && rem;
    }));

    // 旧 web 已退出，新 web 是另一个 pid；newcomer 已启动
    pid_t web_new = latest_pid(r, "web");
    pid_t newcomer = latest_pid(r, "newcomer");
    ASSERT_NE(web_new, web_old);
    ASSERT_TRUE(alive(web_new));
    ASSERT_TRUE(alive(newcomer));
    ASSERT_FALSE(alive(web_old));

    kill(r.pid, SIGTERM);
    ASSERT_EQ(wait_exit(r), 0);
}

// ---------------------------------------------------------------------------
// 测试 7：整体关停顺序 —— 各自优雅信号；卡死服务宽限期后被 SIGKILL
// ---------------------------------------------------------------------------

void test_sup_shutdown_order() {
    const std::string cfg = cfg_path("shutdown.conf");
    auto sec = [&](const std::string& name, const std::string& sig, int grace,
                   bool ignore) {
        std::string c = section_header(name);
        c += "command = " + g_worker_bin + " --name " + name +
             (ignore ? " --ignore-term" : "") + " sleep 30000\n";
        c += "stop_signal = " + sig + "\n";
        c += "stop_grace_ms = " + std::to_string(grace) + "\n";
        c += "stable_ms = 50\n";
        c += "backoff_ms = 8000\n";
        c += "start_retries = 2\n\n";
        return c;
    };
    write_file(cfg, sec("a", "USR1", 300, false) +
                     sec("b", "TERM", 300, false) +
                     sec("c", "TERM", 80, true));

    Runner r;
    Cleanup cu(r);
    ASSERT_TRUE(start_runner(r, cfg));
    ASSERT_TRUE(wait_until(r, [](const std::vector<Ev>& es) {
        int n = 0;
        for (const auto& e : es)
            if ((e.is("service_healthy") || e.is("service_stable_reset")) &&
                e.svc() && (*e.svc() == "a" || *e.svc() == "b" || *e.svc() == "c"))
                ++n;
        return n >= 3;
    }));

    kill(r.pid, SIGTERM);

    ASSERT_TRUE(wait_until(r, [](const std::vector<Ev>& es) {
        return find_idx(es, [](const Ev& e) { return e.is("supervisor_stopped"); }) >= 0;
    }, 3000));

    auto stopping_pos = [&](const char* svc) {
        return find_idx(r.ev, [&](const Ev& e) {
            return e.is("service_stopping") && e.svc() && *e.svc() == svc;
        });
    };
    int ia = stopping_pos("a");
    int ib = stopping_pos("b");
    int ic = stopping_pos("c");
    ASSERT_TRUE(ia >= 0 && ib >= 0 && ic >= 0);
    ASSERT_TRUE(ia < ib && ib < ic);  // 按配置顺序发优雅信号

    ASSERT_EQ(r.ev[ia].num_or("signal", -1), SIGUSR1);
    ASSERT_EQ(r.ev[ib].num_or("signal", -1), SIGTERM);
    ASSERT_EQ(r.ev[ic].num_or("signal", -1), SIGTERM);
    ASSERT_EQ(r.ev[ic].num_or("grace_ms", -1), 80);

    // a、b 优雅退出（expected），从未被强杀
    for (const char* s : {"a", "b"}) {
        int ex = find_idx(r.ev, [&](const Ev& e) {
            return e.is("service_exited") && e.svc() && *e.svc() == s &&
                   e.str("reason") && *e.str("reason") == "expected";
        });
        ASSERT_GE(ex, 0);
        ASSERT_EQ(count_ev(r.ev, [&](const Ev& e) {
            return e.is("service_force_kill") && e.svc() && *e.svc() == s;
        }), 0);
    }

    // c 卡死：宽限期到后 SIGKILL，退出为 signaled/9
    int fk = find_idx(r.ev, [](const Ev& e) {
        return e.is("service_force_kill") && e.svc() && *e.svc() == "c";
    });
    ASSERT_GE(fk, 0);
    ASSERT_GE(r.ev[fk].num_or("ts_ms", 0) - r.ev[ic].num_or("ts_ms", 0), 50);

    int cx = find_idx(r.ev, [](const Ev& e) {
        return e.is("service_exited") && e.svc() && *e.svc() == "c" &&
               e.str("how") && *e.str("how") == "signaled" &&
               e.num_or("code", 0) == 9 &&
               e.str("reason") && *e.str("reason") == "expected";
    });
    ASSERT_GE(cx, 0);
    ASSERT_TRUE(fk < cx);  // 先强杀，后回收

    // supervisor_stopping 在最前，supervisor_stopped 是最后一个事件
    int i_begin = find_idx(r.ev, [](const Ev& e) { return e.is("supervisor_stopping"); });
    int i_end = find_idx(r.ev, [](const Ev& e) { return e.is("supervisor_stopped"); });
    ASSERT_TRUE(i_begin >= 0 && i_begin < ia);
    ASSERT_EQ(i_end, (int)r.ev.size() - 1);

    ASSERT_EQ(wait_exit(r, 2000), 0);
}

// ---------------------------------------------------------------------------
// 测试 8：配置行内注释（空白后的 # / ;）必须被正确剥离
// ---------------------------------------------------------------------------

void test_sup_inline_comments() {
    const std::string cfg = cfg_path("comments.conf");
    std::string c;
    c += "# 整行注释\n";
    c += section_header("s");
    c += "command = " + g_worker_bin + " --name s sleep 30000   # 行尾注释\n";
    c += "stop_signal = TERM ; 分号注释\n";
    c += "stop_grace_ms = 200\n";
    c += "stable_ms = 50\n";
    c += "backoff_ms = 8000\n";
    c += "start_retries = 2\n";
    write_file(cfg, c);

    Runner r;
    Cleanup cu(r);
    ASSERT_TRUE(start_runner(r, cfg));
    ASSERT_TRUE(wait_until(r, [](const std::vector<Ev>& es) {
        return find_idx(es, [](const Ev& e) {
            return e.is("service_healthy") && e.svc() && *e.svc() == "s";
        }) >= 0;
    }));
    // 没有 config_invalid 即说明注释被正确剥离、命令完整
    ASSERT_EQ(count_ev(r.ev, [](const Ev& e) { return e.is("config_invalid"); }), 0);

    kill(r.pid, SIGTERM);
    ASSERT_EQ(wait_exit(r), 0);
}

// ---------------------------------------------------------------------------
// 测试 9：关停进行中又收到 SIGHUP，新增的服务也必须被关停（不得挂起）
// ---------------------------------------------------------------------------

void test_sup_reload_during_shutdown() {
    const std::string cfg = cfg_path("rsd.conf");
    auto one = [&](const std::string& name) {
        std::string c = section_header(name);
        c += "command = " + g_worker_bin + " --name " + name + " sleep 30000\n";
        c += "stop_signal = TERM\n";
        c += "stop_grace_ms = 300\n";
        c += "stable_ms = 40\n";
        c += "backoff_ms = 8000\n";
        c += "start_retries = 2\n\n";
        return c;
    };
    write_file(cfg, one("a"));

    Runner r;
    Cleanup cu(r);
    ASSERT_TRUE(start_runner(r, cfg));
    ASSERT_TRUE(wait_until(r, [](const std::vector<Ev>& es) {
        return find_idx(es, [](const Ev& e) {
            return e.is("service_healthy") && e.svc() && *e.svc() == "a";
        }) >= 0;
    }));

    // 先发 SIGTERM，紧接写入“新增 b”的配置并发 SIGHUP
    kill(r.pid, SIGTERM);
    write_file(cfg, one("a") + one("b"));
    kill(r.pid, SIGHUP);

    ASSERT_TRUE(wait_until(r, [](const std::vector<Ev>& es) {
        return find_idx(es, [](const Ev& e) { return e.is("supervisor_stopped"); }) >= 0;
    }, 3000));

    // a 与 b 都走到了停止/回收流程，监管器干净退出（非挂起）
    int a_stop = find_idx(r.ev, [](const Ev& e) {
        return (e.is("service_stopping") || e.is("service_stopped")) &&
               e.svc() && *e.svc() == "a";
    });
    int b_stop = find_idx(r.ev, [](const Ev& e) {
        return (e.is("service_stopping") || e.is("service_stopped")) &&
               e.svc() && *e.svc() == "b";
    });
    ASSERT_GE(a_stop, 0);
    ASSERT_GE(b_stop, 0);  // 新增的 b 也必须被关停
    ASSERT_EQ(wait_exit(r, 2000), 0);
}

} // namespace

void test_supervisor_suite() {
    g_supervisor_bin = bin_path("ipc_supervisor");
    g_worker_bin = bin_path("ipc_worker");

    if (!exists_exec(g_supervisor_bin) || !exists_exec(g_worker_bin)) {
        std::cout << "\n  [SKIP] 未找到 ipc_supervisor/ipc_worker 二进制: "
                  << g_supervisor_bin << ", " << g_worker_bin << std::endl;
        return;
    }

    char tmpl[] = "/tmp/ipc_sup_test_XXXXXX";
    char* d = mkdtemp(tmpl);
    if (!d) {
        std::cout << "\n  [SKIP] mkdtemp 失败: " << std::strerror(errno) << std::endl;
        return;
    }
    g_tmpdir = d;

    run_test("sup_signal_burst", test_sup_signal_burst);
    run_test("sup_zombie_reaping", test_sup_zombie_reaping);
    run_test("sup_backoff_restart", test_sup_backoff_restart);
    run_test("sup_retry_exhausted", test_sup_retry_exhausted);
    run_test("sup_exec_failure", test_sup_exec_failure);
    run_test("sup_bad_reload", test_sup_bad_reload);
    run_test("sup_shutdown_order", test_sup_shutdown_order);
    run_test("sup_inline_comments", test_sup_inline_comments);
    run_test("sup_reload_during_shutdown", test_sup_reload_during_shutdown);
}
