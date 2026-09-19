/**
 * Supervisor 模式
 *
 * 一个最小的进程监管器，用于运维培训中演示子进程在
 *   - 正常退出 (exit)
 *   - 异常崩溃 (crash / 被信号杀死)
 *   - 卡死 (收到优雅退出信号后不退出)
 * 三种情况下分别如何被接管。
 *
 * 设计要点：
 * 1. 所有信号处理路径都是异步信号安全的：处理函数只做一次 write()
 *    （self-pipe），把 SIGCHLD/SIGHUP/SIGTERM/SIGINT 交回主循环处理；
 *    SIGCHLD 不带 SA_RESTART，避免 poll 被自动重启而丢失唤醒。
 * 2. 主循环以 poll() 同时监听 self-pipe 与 per-service timerfd，
 *    所有节奏（退避重启、终止宽限期、稳定运行阈值）都是截止时间驱动，
 *    代码内部没有任何固定 sleep。
 * 3. SIGCHLD 在主循环中以 waitpid(WNOHANG) 循环回收，多个子进程同时
 *    退出（信号可能被合并）也能一次全部回收，不留僵尸。
 * 4. SIGHUP 热加载采用“先完整解析、再应用”的两阶段方式：
 *    解析失败则旧服务完全不受影响；应用时新增即启动、删除按各自退出
 *    策略停止、修改有序替换（先停旧实例，回收后再启新实例）。
 * 5. 持续输出 JSON Lines 结构化事件，便于复盘真实生命周期与自动化测试。
 *
 * 配置文件为简洁的逐行 key=value 格式，每个服务由 [service:<名称>] 段定义：
 *
 *   [service:worker]
 *   command        = /path/to/prog arg1 arg2
 *   stop_signal    = TERM            # 可选，默认 TERM
 *   stop_grace_ms  = 500             # 可选，默认 2000
 *   start_retries  = 3               # 可选，默认 3（生命周期内启动总次数上限）
 *   backoff_ms     = 50,100,200,400  # 可选，逐次退避（超出后循环使用末值）
 *   stable_ms      = 1000            # 可选，默认 1000，连续存活超过它即清零失败计数
 */

#include "supervisor.h"

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <chrono>
#include <cstdarg>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <string>
#include <sys/timerfd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <unordered_map>
#include <vector>

namespace sv {

namespace {

// ---------------------------------------------------------------------------
// 小工具
// ---------------------------------------------------------------------------

using Clock = std::chrono::steady_clock;

static std::string trim(const std::string& s) {
    size_t b = s.find_first_not_of(" \t\r\n");
    if (b == std::string::npos) return "";
    size_t e = s.find_last_not_of(" \t\r\n");
    return s.substr(b, e - b + 1);
}

// 按空白切分，支持单/双引号包裹的参数
static std::vector<std::string> split_ws(const std::string& s) {
    std::vector<std::string> out;
    size_t i = 0;
    while (i < s.size()) {
        while (i < s.size() && (s[i] == ' ' || s[i] == '\t')) ++i;
        if (i >= s.size()) break;
        char q = 0;
        if (s[i] == '"' || s[i] == '\'') q = s[i++];
        std::string tok;
        while (i < s.size()) {
            char c = s[i];
            if (q) {
                if (c == q) { ++i; break; }
                if (c == '\\' && i + 1 < s.size()) { tok.push_back(s[++i]); ++i; continue; }
                tok.push_back(c); ++i;
            } else {
                if (c == ' ' || c == '\t') break;
                tok.push_back(c); ++i;
            }
        }
        out.push_back(tok);
    }
    return out;
}

static std::vector<std::string> split_commas(const std::string& s) {
    std::vector<std::string> out;
    size_t start = 0;
    while (true) {
        size_t p = s.find(',', start);
        std::string item = trim(s.substr(start, p == std::string::npos ? std::string::npos : p - start));
        if (!item.empty()) out.push_back(item);
        if (p == std::string::npos) break;
        start = p + 1;
    }
    return out;
}

static long parse_long(const std::string& s) {
    errno = 0;
    char* end = nullptr;
    long v = std::strtol(s.c_str(), &end, 10);
    if (errno != 0 || end == s.c_str() || trim(end) != "") {
        errno = EINVAL;
        return -1;
    }
    return v;
}

// 信号名 -> 编号，只允许监管过程实际会使用的停止信号
static int parse_signal(const std::string& name) {
    std::string n = name;
    for (auto& c : n) c = (char)std::toupper((unsigned char)c);
    if (n == "TERM") return SIGTERM;
    if (n == "INT")  return SIGINT;
    if (n == "HUP")  return SIGHUP;
    if (n == "QUIT") return SIGQUIT;
    if (n == "USR1") return SIGUSR1;
    if (n == "USR2") return SIGUSR2;
    if (n == "ALRM") return SIGALRM;
    errno = EINVAL;
    return -1;
}

static std::string json_escape(const std::string& s) {
    std::string out;
    out.reserve(s.size() + 2);
    for (char c : s) {
        switch (c) {
            case '"':  out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case '\t': out += "\\t"; break;
            default:
                if ((unsigned char)c < 0x20) {
                    char buf[8];
                    std::snprintf(buf, sizeof(buf), "\\u%04x", c);
                    out += buf;
                } else {
                    out.push_back(c);
                }
        }
    }
    return out;
}

static int64_t now_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               Clock::now().time_since_epoch()).count();
}

// ---------------------------------------------------------------------------
// 配置
// ---------------------------------------------------------------------------

struct ServiceConfig {
    std::string name;
    std::vector<std::string> argv;   // argv[0] 为可执行文件
    int stop_signal = SIGTERM;
    int stop_grace_ms = 2000;
    int start_retries = 3;           // 生命周期内启动总次数上限（含首次）
    std::vector<int> backoff_ms = {50, 100, 200, 400, 800};
    int stable_ms = 1000;
};

struct Config {
    std::unordered_map<std::string, ServiceConfig> services;
    std::vector<std::string> order;  // 保留配置中的出现顺序
};

struct ParseError {
    int line = 0;
    std::string message;
};

// 成功返回 0 并填充 cfg；失败返回 -1 并填充 perr
static int parse_config(const std::string& path, Config& cfg, ParseError& perr) {
    FILE* fp = std::fopen(path.c_str(), "r");
    if (!fp) {
        perr.message = std::string("无法打开配置文件: ") + std::strerror(errno);
        return -1;
    }

    Config out;
    ServiceConfig* cur = nullptr;
    std::string cur_section;
    char* line = nullptr;
    size_t cap = 0;
    ssize_t n;
    int lineno = 0;

    auto fail = [&](const char* fmt, ...) {
        char buf[512];
        va_list ap;
        va_start(ap, fmt);
        std::vsnprintf(buf, sizeof(buf), fmt, ap);
        va_end(ap);
        perr.line = lineno;
        perr.message = buf;
        std::free(line);
        std::fclose(fp);
        return -1;
    };

    while ((n = getline(&line, &cap, fp)) != -1) {
        ++lineno;
        // 剥离行内注释：# 或 ; 前必须有空白（且不在引号内），避免误伤命令参数
        {
            char q = 0;
            for (size_t k = 0; k < (size_t)n; ++k) {
                char c = line[k];
                if (q) { if (c == q) q = 0; continue; }
                if (c == '"' || c == '\'') { q = c; continue; }
                if ((c == '#' || c == ';') && k > 0 &&
                    (line[k - 1] == ' ' || line[k - 1] == '\t')) {
                    line[k] = '\0';  // 截断 C 字符串（trim 以 '\0' 为界）
                    n = (ssize_t)k;
                    break;
                }
            }
        }
        std::string s = trim(line);
        if (s.empty() || s[0] == '#' || s[0] == ';') continue;

        if (s.front() == '[' && s.back() == ']') {
            std::string section = trim(s.substr(1, s.size() - 2));
            const std::string prefix = "service:";
            if (section.rfind(prefix, 0) != 0)
                return fail("未知配置段 [%s]，只允许 [service:<名称>]", section.c_str());
            std::string name = trim(section.substr(prefix.size()));
            if (name.empty())
                return fail("服务名不能为空");
            for (char c : name)
                if (c == ' ' || c == '\t' || c == '=')
                    return fail("服务名含非法字符: %s", name.c_str());
            if (out.services.count(name))
                return fail("服务重复定义: %s", name.c_str());
            cur_section = name;
            cur = &out.services.emplace(name, ServiceConfig{}).first->second;
            cur->name = name;
            out.order.push_back(name);
            continue;
        }

        if (!cur)
            return fail("键值对必须位于 [service:*] 段内: %s", s.c_str());

        size_t eq = s.find('=');
        if (eq == std::string::npos)
            return fail("无法解析(应为 key = value): %s", s.c_str());
        std::string key = trim(s.substr(0, eq));
        std::string val = trim(s.substr(eq + 1));

        if (key == "command") {
            cur->argv = split_ws(val);
            if (cur->argv.empty())
                return fail("[%s] command 不能为空", cur_section.c_str());
        } else if (key == "stop_signal") {
            int sig = parse_signal(val);
            if (sig < 0) return fail("[%s] 不支持的停止信号: %s", cur_section.c_str(), val.c_str());
            cur->stop_signal = sig;
        } else if (key == "stop_grace_ms") {
            long v = parse_long(val);
            if (v < 0) return fail("[%s] stop_grace_ms 非法: %s", cur_section.c_str(), val.c_str());
            cur->stop_grace_ms = (int)v;
        } else if (key == "start_retries") {
            long v = parse_long(val);
            if (v < 1) return fail("[%s] start_retries 必须 >= 1: %s", cur_section.c_str(), val.c_str());
            cur->start_retries = (int)v;
        } else if (key == "backoff_ms") {
            std::vector<int> bo;
            for (const auto& item : split_commas(val)) {
                long v = parse_long(item);
                if (v < 0) return fail("[%s] backoff_ms 非法: %s", cur_section.c_str(), item.c_str());
                bo.push_back((int)v);
            }
            if (bo.empty()) return fail("[%s] backoff_ms 至少需要一个值", cur_section.c_str());
            cur->backoff_ms = bo;
        } else if (key == "stable_ms") {
            long v = parse_long(val);
            if (v < 0) return fail("[%s] stable_ms 非法: %s", cur_section.c_str(), val.c_str());
            cur->stable_ms = (int)v;
        } else {
            return fail("[%s] 未知配置项: %s", cur_section.c_str(), key.c_str());
        }
    }

    std::free(line);
    std::fclose(fp);

    for (const auto& name : out.order) {
        if (out.services[name].argv.empty()) {
            perr.line = 0;
            perr.message = "服务 " + name + " 缺少 command";
            return -1;
        }
    }

    cfg = std::move(out);
    return 0;
}

// ---------------------------------------------------------------------------
// 结构化事件（单行 JSON）。主循环单线程产生，单条消息一次 write 写完。
// ---------------------------------------------------------------------------

static int write_all(int fd, const char* p, size_t len) {
    while (len > 0) {
        ssize_t w = ::write(fd, p, len);
        if (w < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        p += w;
        len -= (size_t)w;
    }
    return 0;
}

static int g_event_fd = STDOUT_FILENO;

// 每个事件持有独立缓冲：即使外层 Event 仍存活时又产生了内层事件
// （例如 begin_shutdown 中调用 request_stop），两行 JSON 也不会相互拼接。
struct Event {
    std::string buf;

    explicit Event(const char* type) {
        buf += "{\"event\":\"";
        buf += type;
        buf += "\",\"ts_ms\":";
        buf += std::to_string(now_ms());
    }
    Event(const Event&) = delete;
    Event& operator=(const Event&) = delete;

    void str_field(const char* k, const std::string& v) {
        buf += ",\""; buf += k; buf += "\":\"";
        buf += json_escape(v); buf += "\"";
    }
    void int_field(const char* k, long long v) {
        buf += ",\""; buf += k; buf += "\":";
        buf += std::to_string(v);
    }
    void bool_field(const char* k, bool v) {
        buf += ",\""; buf += k; buf += "\":";
        buf += v ? "true" : "false";
    }
    ~Event() {
        buf += "}\n";
        write_all(g_event_fd, buf.data(), buf.size());
    }
};

// ---------------------------------------------------------------------------
// 异步信号安全：self-pipe。处理函数只使用 volatile sig_atomic 标志 + write()
// ---------------------------------------------------------------------------

static int g_sigpipe_r = -1;
static int g_sigpipe_w = -1;
static volatile sig_atomic_t g_got_sigchld = 0;
static volatile sig_atomic_t g_got_hup = 0;
static volatile sig_atomic_t g_got_term = 0;

static void sig_handler(int signo) {
    int saved_errno = errno;
    unsigned char byte = (unsigned char)signo;
    if (signo == SIGCHLD) g_got_sigchld = 1;
    else if (signo == SIGHUP) g_got_hup = 1;
    else if (signo == SIGTERM || signo == SIGINT) g_got_term = 1;
    // 全部工作就是一次异步信号安全的 write()。管道满时丢弃也无妨：
    // 三类信号都是电平事件，sig_atomic 标志已经置位，不会丢事件。
    ssize_t r = write(g_sigpipe_w, &byte, 1);
    (void)r;
    errno = saved_errno;
}

static int install_signals() {
    int fds[2];
    if (pipe2(fds, O_NONBLOCK | O_CLOEXEC) != 0) return -1;
    g_sigpipe_r = fds[0];
    g_sigpipe_w = fds[1];

    struct sigaction sa;
    std::memset(&sa, 0, sizeof(sa));
    sa.sa_handler = sig_handler;
    sigemptyset(&sa.sa_mask);
    // 故意不使用 SA_RESTART：让 poll() 被信号打断后立即回到循环重新评估状态
    sa.sa_flags = 0;
    if (sigaction(SIGCHLD, &sa, nullptr) < 0) return -1;
    if (sigaction(SIGHUP, &sa, nullptr) < 0) return -1;
    if (sigaction(SIGTERM, &sa, nullptr) < 0) return -1;
    if (sigaction(SIGINT, &sa, nullptr) < 0) return -1;

    // 监管器自身忽略 SIGPIPE
    struct sigaction ign;
    std::memset(&ign, 0, sizeof(ign));
    ign.sa_handler = SIG_IGN;
    sigemptyset(&ign.sa_mask);
    sigaction(SIGPIPE, &ign, nullptr);
    return 0;
}

// ---------------------------------------------------------------------------
// 监管状态机
// ---------------------------------------------------------------------------

enum class SvcState {
    Starting,    // 已 fork，尚未达到稳定运行阈值
    Running,     // 稳定存活
    Backoff,     // 失败后等待退避再重启
    Stopping,    // 已发优雅信号，等待宽限期
    Killing,     // 宽限期到，已发 SIGKILL，等待回收
    Failed,      // 重启次数耗尽
    Stopped,     // 已停止并回收（关停 / 退避中被关停）
};

struct Service {
    explicit Service(ServiceConfig c) : cfg(std::move(c)) {}

    ServiceConfig cfg;
    SvcState state = SvcState::Starting;
    pid_t pid = -1;
    int attempts = 0;                    // 本生命周期已发起的启动次数
    int failures = 0;                    // 未被稳定运行清零的连续失败次数
    int timerfd = -1;                    // 复用：退避 / 稳定阈值 / 宽限期
    bool marked_delete = false;          // 热加载：待删除（旧实例退出后摘除）
    bool replace_pending = false;        // 热加载：旧实例回收后以新配置启动
    ServiceConfig pending_cfg;
    bool reported_failure = false;       // 是否已计入整体失败数
};

static void timer_arm(int fd, int ms) {
    // timerfd 的 it_value 为 0 表示“解除定时器”而非立即到期，最少给 1ms
    if (ms < 1) ms = 1;
    struct itimerspec its;
    std::memset(&its, 0, sizeof(its));
    its.it_value.tv_sec = ms / 1000;
    its.it_value.tv_nsec = (long)(ms % 1000) * 1000000L;
    timerfd_settime(fd, 0, &its, nullptr);
}

static void timer_disarm(int fd) {
    struct itimerspec its;
    std::memset(&its, 0, sizeof(its));
    timerfd_settime(fd, 0, &its, nullptr);
}

// 向服务进程组发信号；若子进程尚未完成 setpgid，则回退为直接发给该 pid
static void signal_service(pid_t pid, int sig) {
    if (pid <= 0) return;
    if (kill(-pid, sig) < 0 && errno == ESRCH)
        kill(pid, sig);
}

class Supervisor {
public:
    int init(const std::string& config_path) {
        config_path_ = config_path;
        if (install_signals() < 0) {
            std::fprintf(stderr, "supervisor: 安装信号处理失败: %s\n", std::strerror(errno));
            return 1;
        }
        if (reload_config(/*initial=*/true) < 0) return 1;
        return 0;
    }

    int run() {
        // 按配置顺序启动初始服务
        for (const auto& name : order_)
            start_service(services_.find(name)->second);

        while (true) {
            // 1. self-pipe 中只是“有信号”的通知，状态以 sig_atomic 标志为准
            drain_self_pipe();
            if (g_got_hup) {
                g_got_hup = 0;
                reload_config(/*initial=*/false);
            }
            shutting_ = (g_got_term != 0);

            // 2. 回收退出子进程 / 推进关停 / 处理到期定时器
            reap_children();
            if (shutting_) begin_shutdown();
            service_timers();

            // 3. 终止条件
            if (shutting_ && all_reaped()) {
                Event e("supervisor_stopped");
                e.int_field("failed_services", failed_count_);
                for (auto& kv : services_)
                    if (kv.second.timerfd >= 0) ::close(kv.second.timerfd);
                return failed_count_ > 0 ? 1 : 0;
            }
            if (!shutting_ && all_services_failed()) {
                // 非关停状态下所有服务均耗尽重试（空配置时继续驻留，等待 SIGHUP/SIGTERM）
                Event e("supervisor_stopped");
                e.int_field("failed_services", failed_count_);
                for (auto& kv : services_)
                    if (kv.second.timerfd >= 0) ::close(kv.second.timerfd);
                return failed_count_ > 0 ? 1 : 0;
            }

            // 4. poll 等待 self-pipe 与各服务定时器
            poll_fds_.clear();
            pollfd pr;
            pr.fd = g_sigpipe_r;
            pr.events = POLLIN;
            pr.revents = 0;
            poll_fds_.push_back(pr);
            for (auto& kv : services_) {
                Service& s = kv.second;
                if (s.timerfd >= 0 &&
                    s.state != SvcState::Failed &&
                    s.state != SvcState::Stopped) {
                    pollfd pe;
                    pe.fd = s.timerfd;
                    pe.events = POLLIN;
                    pe.revents = 0;
                    poll_fds_.push_back(pe);
                }
            }
            int rc = poll(poll_fds_.data(), poll_fds_.size(), -1);
            if (rc < 0 && errno != EINTR) {
                std::fprintf(stderr, "supervisor: poll 失败: %s\n", std::strerror(errno));
                return 1;
            }
        }
    }

private:
    std::string config_path_;
    std::unordered_map<std::string, Service> services_;
    std::vector<std::string> order_;
    std::vector<pollfd> poll_fds_;
    int failed_count_ = 0;        // 耗尽重试（含初始启动即失败）的服务数
    bool shutdown_started_ = false;
    bool shutting_ = false;

    static bool config_equal(const ServiceConfig& a, const ServiceConfig& b) {
        return a.argv == b.argv &&
               a.stop_signal == b.stop_signal &&
               a.stop_grace_ms == b.stop_grace_ms &&
               a.start_retries == b.start_retries &&
               a.backoff_ms == b.backoff_ms &&
               a.stable_ms == b.stable_ms;
    }

    // -- 配置热加载 ---------------------------------------------------------

    int reload_config(bool initial) {
        Config cfg;
        ParseError perr;
        if (parse_config(config_path_, cfg, perr) < 0) {
            Event e("config_invalid");
            e.str_field("path", config_path_);
            e.int_field("line", perr.line);
            e.str_field("error", perr.message);
            if (initial) {
                std::fprintf(stderr, "supervisor: 初始配置解析失败: %s (line %d)\n",
                             perr.message.c_str(), perr.line);
            }
            return -1;
        }

        {
            Event e("config_loaded");
            e.str_field("path", config_path_);
            e.int_field("services", (long long)cfg.order.size());
        }

        if (initial) {
            for (const auto& name : cfg.order) {
                Service s(cfg.services.find(name)->second);
                s.timerfd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
                services_.emplace(name, std::move(s));
            }
            order_ = cfg.order;
            return 0;
        }

        // a) 新增 / 修改
        for (const auto& name : cfg.order) {
            const ServiceConfig& nc = cfg.services.find(name)->second;
            auto it = services_.find(name);

            if (it == services_.end()) {
                // 全新服务：立即启动
                Service s(nc);
                s.timerfd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
                auto pos = services_.emplace(name, std::move(s)).first;
                {
                    Event add("service_added");
                    add.str_field("service", name);
                }
                start_service(pos->second);
                continue;
            }

            Service& cur = it->second;
            // 同名服务在旧实例退出排空期间被“重新加入”：撤销删除标记
            if (cur.marked_delete) {
                cur.marked_delete = false;
                cur.replace_pending = true;
                cur.pending_cfg = nc;
                Event add("service_added");
                add.str_field("service", name);
                // 旧实例仍在退出流程中，handle_exit 回收后按新配置启动
                continue;
            }
            if (config_equal(cur.cfg, nc)) continue;

            {
                Event ch("service_changed");
                ch.str_field("service", name);
            }
            cur.pending_cfg = nc;
            cur.replace_pending = true;

            switch (cur.state) {
                case SvcState::Starting:
                case SvcState::Running:
                    // 有序替换：先发旧实例的优雅信号，回收后再启新实例
                    request_stop(cur);
                    break;
                case SvcState::Stopping:
                case SvcState::Killing:
                    // 上一次替换尚未排空，更新为最新配置即可
                    break;
                case SvcState::Backoff:
                    // 没有存活进程：直接换配置，重新计数
                    timer_disarm(cur.timerfd);
                    apply_new_lease(cur, nc);
                    enter_backoff(cur);
                    break;
                case SvcState::Failed:
                    // 配置变更视为新的生命周期，重新给予完整重试额度
                    apply_new_lease(cur, nc);
                    start_service(cur);
                    break;
                case SvcState::Stopped:
                    if (shutting_) break;  // 关停中不再重启
                    apply_new_lease(cur, nc);
                    start_service(cur);
                    break;
            }
        }

        // b) 删除：按各自退出策略停止；无存活进程的直接摘除
        for (const auto& name : order_) {
            if (cfg.services.count(name)) continue;
            Service& s = services_.find(name)->second;
            if (s.marked_delete) continue;  // 已在排空
            s.marked_delete = true;
            {
                Event del("service_deleted");
                del.str_field("service", name);
            }
            switch (s.state) {
                case SvcState::Starting:
                case SvcState::Running:
                    request_stop(s);
                    break;
                case SvcState::Stopping:
                case SvcState::Killing:
                    break;  // 等回收后摘除
                case SvcState::Backoff:
                case SvcState::Failed:
                case SvcState::Stopped:
                    unregister_service(s);
                    break;
            }
        }

        // c) 期望集合即新配置的顺序（排空中的删除服务不在其中）
        order_ = cfg.order;

        // 关停过程中收到的合法加载：新增/复活的服务也要立即进入停止流程，
        // 否则它们会在监管器准备退出时才刚启动，造成永久等待
        if (shutdown_started_) {
            for (const auto& name : order_) {
                auto it = services_.find(name);
                if (it == services_.end()) continue;
                Service& s = it->second;
                if (s.state == SvcState::Starting || s.state == SvcState::Running) {
                    request_stop(s);
                } else if (s.state == SvcState::Backoff) {
                    timer_disarm(s.timerfd);
                    s.state = SvcState::Stopped;
                    Event st("service_stopped");
                    st.str_field("service", s.cfg.name);
                    st.str_field("how", "skipped_backoff");
                }
            }
        }
        return 0;
    }

    // 给服务一个新的生命周期（配置变更 / 稳定运行后调用）
    void apply_new_lease(Service& s, const ServiceConfig& nc) {
        if (s.reported_failure) {
            s.reported_failure = false;
            --failed_count_;
        }
        s.cfg = nc;
        s.attempts = 0;
        s.failures = 0;
        s.replace_pending = false;
    }

    void unregister_service(Service& s) {
        Event e("service_removed");
        e.str_field("service", s.cfg.name);
        if (s.reported_failure) {
            // 失败服务被人工摘除，其失败不再计入整体退出码
            s.reported_failure = false;
            --failed_count_;
        }
        if (s.timerfd >= 0) {
            ::close(s.timerfd);
            s.timerfd = -1;
        }
        services_.erase(s.cfg.name);
    }

    // -- 启动 / 停止 --------------------------------------------------------

    void start_service(Service& s) {
        if (s.attempts >= s.cfg.start_retries) {
            enter_failed(s);
            return;
        }
        std::vector<char*> argv;
        argv.reserve(s.cfg.argv.size() + 1);
        for (auto& a : s.cfg.argv) argv.push_back(a.data());
        argv.push_back(nullptr);

        pid_t pid = fork();
        if (pid < 0) {
            {
                Event e("spawn_error");
                e.str_field("service", s.cfg.name);
                e.str_field("error", std::strerror(errno));
            }
            ++s.attempts;
            ++s.failures;
            if (s.attempts >= s.cfg.start_retries) enter_failed(s);
            else enter_backoff(s);
            return;
        }
        if (pid == 0) {
            // 子进程：恢复默认信号处理、解除继承的信号阻塞、独立进程组
            struct sigaction dfl;
            dfl.sa_handler = SIG_DFL;
            sigemptyset(&dfl.sa_mask);
            dfl.sa_flags = 0;
            sigaction(SIGCHLD, &dfl, nullptr);
            sigaction(SIGHUP, &dfl, nullptr);
            sigaction(SIGTERM, &dfl, nullptr);
            sigaction(SIGINT, &dfl, nullptr);
            sigaction(SIGPIPE, &dfl, nullptr);
            sigset_t empty;
            sigemptyset(&empty);
            sigprocmask(SIG_SETMASK, &empty, nullptr);
            setpgid(0, 0);
            execvp(argv[0], argv.data());
            // 仅当 exec 失败才会到达这里
            std::fprintf(stderr, "supervisor: execvp(%s) 失败: %s\n",
                         argv[0], std::strerror(errno));
            _exit(127);
        }

        // 父进程同样设置进程组（与子进程竞争，二者必有一个生效）
        setpgid(pid, pid);

        s.pid = pid;
        s.state = SvcState::Starting;
        ++s.attempts;
        // 稳定运行阈值定时器：存活到点即转为 Running 并清零失败计数
        timer_arm(s.timerfd, s.cfg.stable_ms);
        Event e("service_started");
        e.str_field("service", s.cfg.name);
        e.int_field("pid", (long long)pid);
        e.int_field("attempt", s.attempts);
    }

    // 发出优雅停止信号。宽限期由 per-service timerfd 计时。
    void request_stop(Service& s) {
        if (s.state != SvcState::Running && s.state != SvcState::Starting) return;
        signal_service(s.pid, s.cfg.stop_signal);
        s.state = SvcState::Stopping;
        timer_arm(s.timerfd, s.cfg.stop_grace_ms);
        Event e("service_stopping");
        e.str_field("service", s.cfg.name);
        e.int_field("pid", (long long)s.pid);
        e.int_field("signal", s.cfg.stop_signal);
        e.int_field("grace_ms", s.cfg.stop_grace_ms);
        e.bool_field("replaced", s.replace_pending);
    }

    void force_kill(Service& s) {
        signal_service(s.pid, SIGKILL);
        s.state = SvcState::Killing;
        timer_disarm(s.timerfd);
        Event e("service_force_kill");
        e.str_field("service", s.cfg.name);
        e.int_field("pid", (long long)s.pid);
    }

    void enter_backoff(Service& s) {
        s.state = SvcState::Backoff;
        int idx = std::min(std::max(s.failures - 1, 0),
                           (int)s.cfg.backoff_ms.size() - 1);
        int wait_ms = s.cfg.backoff_ms[idx];
        timer_arm(s.timerfd, wait_ms);
        Event e("service_backoff");
        e.str_field("service", s.cfg.name);
        e.int_field("delay_ms", wait_ms);
        e.int_field("failure_count", s.failures);
        e.int_field("attempts", s.attempts);
    }

    void enter_failed(Service& s) {
        s.state = SvcState::Failed;
        timer_disarm(s.timerfd);
        if (!s.reported_failure) {
            s.reported_failure = true;
            ++failed_count_;
        }
        Event e("service_failed");
        e.str_field("service", s.cfg.name);
        e.int_field("attempts", s.attempts);
        e.int_field("failure_count", s.failures);
    }

    // -- SIGCHLD 回收：waitpid(WNOHANG) 循环，同时退出的多个子进程全部回收 --

    void reap_children() {
        if (!g_got_sigchld) return;
        g_got_sigchld = 0;

        int reaped = 0;
        while (true) {
            int status = 0;
            pid_t pid = waitpid(-1, &status, WNOHANG);
            if (pid == 0) break;        // 暂无可回收子进程
            if (pid < 0) {
                if (errno == EINTR) continue;
                break;                  // ECHILD：已全部回收
            }
            ++reaped;
            Service* s = find_by_pid(pid);
            if (s) handle_exit(*s, status);
        }
        if (reaped > 0) {
            Event e("reap_batch");
            e.int_field("count", reaped);
        }
    }

    Service* find_by_pid(pid_t pid) {
        for (auto& kv : services_) {
            if (kv.second.pid == pid &&
                (kv.second.state == SvcState::Starting ||
                 kv.second.state == SvcState::Running ||
                 kv.second.state == SvcState::Stopping ||
                 kv.second.state == SvcState::Killing)) {
                return &kv.second;
            }
        }
        return nullptr;
    }

    void handle_exit(Service& s, int status) {
        bool expected = (s.state == SvcState::Stopping || s.state == SvcState::Killing);
        int code = 0;
        const char* how = "exited";
        if (WIFEXITED(status)) {
            code = WEXITSTATUS(status);
            how = "exited";
        } else if (WIFSIGNALED(status)) {
            code = WTERMSIG(status);
            how = "signaled";
        } else {
            return; // WIFSTOPPED：作业控制停止，不处理
        }

        {
            Event e("service_exited");
            e.str_field("service", s.cfg.name);
            e.int_field("pid", (long long)s.pid);
            e.str_field("reason", expected ? "expected" : "unexpected");
            e.str_field("how", how);
            e.int_field("code", code);
        }
        s.pid = -1;
        timer_disarm(s.timerfd);

        // 热加载删除：旧实例回收后摘除服务
        if (s.marked_delete) {
            unregister_service(s);
            return;
        }

        // 整体关停中：任何退出都只进入 Stopped，绝不重启
        if (shutting_) {
            s.state = SvcState::Stopped;
            Event st("service_stopped");
            st.str_field("service", s.cfg.name);
            st.str_field("how", how);
            return;
        }

        // 有序替换：旧实例回收完毕，立即以新配置启动
        if (s.replace_pending) {
            ServiceConfig nc = s.pending_cfg;
            {
                Event r("service_replaced");
                r.str_field("service", s.cfg.name);
            }
            apply_new_lease(s, nc);
            start_service(s);
            return;
        }

        // 优雅停止的正常收尾
        if (expected) {
            s.state = SvcState::Stopped;
            Event st("service_stopped");
            st.str_field("service", s.cfg.name);
            st.str_field("how", how);
            return;
        }

        // 意外退出 / 崩溃：累计失败，按退避节奏安排重启
        ++s.failures;
        bool clean = (std::strcmp(how, "exited") == 0 && code == 0);
        {
            Event d("service_died");
            d.str_field("service", s.cfg.name);
            d.str_field("how", how);
            d.int_field("code", code);
            d.bool_field("clean", clean);
        }

        if (s.attempts >= s.cfg.start_retries) {
            enter_failed(s);
        } else {
            enter_backoff(s);
        }
    }

    // -- 定时器：退避重启 / 稳定阈值 / 终止宽限期 ----------------------------

    void service_timers() {
        for (auto& kv : services_) {
            Service& s = kv.second;
            if (s.timerfd < 0) continue;
            uint64_t expirations = 0;
            ssize_t r = read(s.timerfd, &expirations, sizeof(expirations));
            if (r != sizeof(expirations)) continue;  // 未到期
            (void)expirations;

            switch (s.state) {
                case SvcState::Backoff:
                    start_service(s);
                    break;
                case SvcState::Starting: {
                    // 存活达到稳定阈值：转为 Running，失败计数与重启额度清零
                    s.state = SvcState::Running;
                    int cleared = s.failures;
                    s.failures = 0;
                    s.attempts = 0;
                    if (cleared > 0) {
                        Event r("service_stable_reset");
                        r.str_field("service", s.cfg.name);
                        r.int_field("cleared_failures", cleared);
                    } else {
                        Event r("service_healthy");
                        r.str_field("service", s.cfg.name);
                    }
                    timer_disarm(s.timerfd);
                    break;
                }
                case SvcState::Stopping:
                    // 宽限期满仍未退出：判定卡死，强制终止
                    force_kill(s);
                    break;
                case SvcState::Running:
                case SvcState::Killing:
                case SvcState::Failed:
                case SvcState::Stopped:
                    break;
            }
        }
    }

    // -- 整体关停：先发各自优雅信号，宽限期后 SIGKILL ------------------------

    void begin_shutdown() {
        if (shutdown_started_) return;
        shutdown_started_ = true;
        {
            Event e("supervisor_stopping");
            e.int_field("services", (long long)order_.size());
        }

        // 按配置顺序逐个发送优雅信号，事件顺序即可复盘关停顺序
        for (const auto& name : order_) {
            auto it = services_.find(name);
            if (it == services_.end()) continue;
            Service& s = it->second;
            if (s.state == SvcState::Starting || s.state == SvcState::Running) {
                request_stop(s);
            } else if (s.state == SvcState::Backoff) {
                timer_disarm(s.timerfd);
                s.state = SvcState::Stopped;
                Event st("service_stopped");
                st.str_field("service", s.cfg.name);
                st.str_field("how", "skipped_backoff");
            }
        }
    }

    bool all_reaped() const {
        for (const auto& kv : services_) {
            const Service& s = kv.second;
            if (s.pid > 0) return false;
            if (s.state == SvcState::Starting ||
                s.state == SvcState::Running ||
                s.state == SvcState::Stopping ||
                s.state == SvcState::Killing) {
                return false;
            }
        }
        return true;
    }

    bool all_services_failed() const {
        bool saw_failed = false;
        for (const auto& kv : services_) {
            const Service& s = kv.second;
            if (s.state == SvcState::Failed) saw_failed = true;
            else if (s.state != SvcState::Stopped) return false;
        }
        return saw_failed;  // 空表返回 false：监管器继续驻留
    }

    void drain_self_pipe() {
        char buf[256];
        while (true) {
            ssize_t r = read(g_sigpipe_r, buf, sizeof(buf));
            if (r > 0) continue;
            break; // EAGAIN / EINTR 时下次再来；标志位保证不丢事件
        }
    }
};

} // anonymous namespace

int run_supervisor(int argc, char** argv) {
    std::string config_path;
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if ((a == "--config" || a == "-c") && i + 1 < argc) {
            config_path = argv[++i];
        } else if (a.rfind("--config=", 0) == 0) {
            config_path = a.substr(9);
        }
    }
    if (config_path.empty()) {
        std::fprintf(stderr,
            "用法: %s --supervisor --config <配置文件>\n"
            "配置为 INI 风格，每个服务一个 [service:<名称>] 段。\n",
            argv[0]);
        return 2;
    }

    Supervisor sup;
    int rc = sup.init(config_path);
    if (rc != 0) return rc;
    return sup.run();
}

} // namespace sv
