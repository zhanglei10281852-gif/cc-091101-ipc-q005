/**
 * supervisor 模式
 *
 * 从简洁配置文件拉起若干命名子进程，持续向 stdout 输出一行一个 JSON 事件，
 * 完整复盘服务进程的启动、崩溃、退避重启、热加载与有序关停生命周期。
 *
 * 信号安全：
 *   SIGCHLD / SIGHUP / SIGTERM / SIGINT 的 handler 只做一件事 —— 向
 *   self-pipe 写入一个字节（write() 是异步信号安全函数）。所有实际逻辑
 *   都在主循环 poll() 返回后执行。标准信号不排队，因此每次被 SIGCHLD
 *   唤醒后都以 waitpid(-1, WNOHANG) 循环取尽所有已退出子进程，保证多个
 *   子进程同时退出时全部被回收、不残留僵尸。
 *
 * 子进程管理：
 *   每个服务子进程 setsid() 自成会话/进程组，终端的 Ctrl+C 只会送达
 *   supervisor；子进程合并后的 stdout/stderr 通过管道回流入主循环，
 *   以 service_log 事件输出，不污染事件流；需要时可整组发送信号。
 */

#include "supervisor.h"
#include "signal_util.h"

#include <poll.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

namespace ipc {

namespace {

using std::int64_t;

using svp::signal_from_name;
using svp::signal_name;

std::string g_config_path;

int64_t now_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

// ---------------------------------------------------------------------------
// JSON 行事件输出
// ---------------------------------------------------------------------------

struct EventBuilder {
    std::ostringstream os;
    bool first = true;

    explicit EventBuilder(const char* type) {
        os << "{\"ts\":" << now_ms() << ",\"event\":\"" << type << "\"";
    }

    void raw_comma() {
        if (first) first = false;
        os << ",";
    }

    static std::string escape(const std::string& s) {
        std::string out;
        out.reserve(s.size() + 2);
        for (unsigned char c : s) {
            switch (c) {
                case '"':  out += "\\\""; break;
                case '\\': out += "\\\\"; break;
                case '\n': out += "\\n"; break;
                case '\r': out += "\\r"; break;
                case '\t': out += "\\t"; break;
                default:
                    if (c < 0x20) {
                        char buf[8];
                        std::snprintf(buf, sizeof(buf), "\\u%04x", c);
                        out += buf;
                    } else {
                        out.push_back(static_cast<char>(c));
                    }
            }
        }
        return out;
    }

    EventBuilder& put(const char* key, const std::string& value) {
        raw_comma();
        os << "\"" << key << "\":\"" << escape(value) << "\"";
        return *this;
    }

    EventBuilder& put(const char* key, const char* value) {
        return put(key, std::string(value));
    }

    EventBuilder& put(const char* key, int64_t value) {
        raw_comma();
        os << "\"" << key << "\":" << value;
        return *this;
    }

    EventBuilder& put_bool(const char* key, bool value) {
        raw_comma();
        os << "\"" << key << "\":" << (value ? "true" : "false");
        return *this;
    }

    void emit() {
        os << "}" << std::endl;
        std::cout << os.str();
        std::cout.flush();
    }
};

void emit_event(EventBuilder&& b) { b.emit(); }

// ---------------------------------------------------------------------------
// self-pipe 信号中继（handler 内只用异步信号安全函数）
// ---------------------------------------------------------------------------

int g_pipe_rd = -1;
int g_pipe_wr = -1;

void signal_to_pipe(int signum) {
    const unsigned char byte = static_cast<unsigned char>(signum);
    ssize_t n;
    do {
        n = ::write(g_pipe_wr, &byte, 1);
    } while (n == -1 && errno == EINTR);
    // 管道满在持续排空的主循环下不可能发生；其余错误无法在 handler 中处理
}

bool install_signal_handlers() {
    int pipefd[2];
    if (pipe2(pipefd, O_NONBLOCK | O_CLOEXEC) != 0) return false;
    g_pipe_rd = pipefd[0];
    g_pipe_wr = pipefd[1];

    struct sigaction sa;
    std::memset(&sa, 0, sizeof(sa));
    sa.sa_handler = signal_to_pipe;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;  // 无 SA_RESTART：主循环以 poll 为中心，EINTR 直接重循
    // supervisor 自身不向已关闭管道写入，忽略 SIGPIPE 防止误杀
    struct sigaction ign;
    std::memset(&ign, 0, sizeof(ign));
    ign.sa_handler = SIG_IGN;
    sigemptyset(&ign.sa_mask);
    sigaction(SIGPIPE, &ign, nullptr);
    return sigaction(SIGCHLD, &sa, nullptr) == 0 &&
           sigaction(SIGHUP, &sa, nullptr) == 0 &&
           sigaction(SIGTERM, &sa, nullptr) == 0 &&
           sigaction(SIGINT, &sa, nullptr) == 0;
}

// ---------------------------------------------------------------------------
// 服务运行时状态
// ---------------------------------------------------------------------------

enum class SvcState { DEAD, STARTING, HEALTHY, STOPPING };

struct Service {
    explicit Service(ServiceConfig c) : cfg(std::move(c)) {}

    ServiceConfig cfg;
    pid_t pid = -1;
    int exec_fd = -1;   // 子进程 spawn/exec 失败时回传 errno 的管道读端
    int out_fd = -1;    // 子进程合并 stdout/stderr 的管道读端
    std::string out_buf;

    SvcState state = SvcState::DEAD;
    int attempt = 0;          // 已启动次数（首次为 1）
    bool permanent_failure = false;
    int64_t started_at = 0;
    int64_t stop_signal_at = 0;
    bool force_sent = false;

    bool want = true;         // 是否应当处于运行状态（删除后置 false）
    int64_t next_start_at = 0;

    bool replacing = false;   // 配置变更：旧进程退出后换上 pending_cfg
    ServiceConfig pending_cfg;

    int last_code = 0;
    bool last_signaled = false;
    int last_signal = 0;
    int last_exec_errno = 0;
};

class Supervisor {
public:
    explicit Supervisor(SupervisorConfig cfg) : cfg_(std::move(cfg)) {}

    int run() {
        if (!install_signal_handlers()) {
            std::fprintf(stderr,
                         "{\"event\":\"fatal\",\"error\":\"signal setup failed: %s\"}\n",
                         std::strerror(errno));
            return 2;
        }

        for (const auto& [name, sc] : cfg_.services)
            services_.emplace(name, std::make_unique<Service>(sc));

        {
            EventBuilder b("supervisor_starting");
            b.put("pid", static_cast<int64_t>(getpid()));
            b.os << ",\"services\":[";
            bool first = true;
            for (const auto& [name, svc] : services_) {
                if (!first) b.os << ",";
                first = false;
                b.os << "\"" << EventBuilder::escape(name) << "\"";
            }
            b.os << "]";
            emit_event(std::move(b));
        }

        for (auto& [name, svc] : services_) launch(*svc);

        EventBuilder ready("supervisor_ready");
        ready.put("services", static_cast<int64_t>(services_.size()));
        emit_event(std::move(ready));

        main_loop();

        bool any_failed = false;
        for (const auto& [name, svc] : services_) {
            if (svc->permanent_failure) {
                any_failed = true;
                EventBuilder b("service_failed_at_shutdown");
                b.put("service", name);
                b.put("attempt", static_cast<int64_t>(svc->attempt));
                emit_event(std::move(b));
            }
        }
        EventBuilder done("shutdown_complete");
        done.put_bool("had_failures", any_failed);
        emit_event(std::move(done));
        return any_failed ? 3 : 0;
    }

private:
    SupervisorConfig cfg_;
    std::map<std::string, std::unique_ptr<Service>> services_;
    std::map<pid_t, std::string> pid_to_name_;
    bool shutting_down_ = false;
    int term_signals_ = 0;
    int last_term_signum_ = SIGTERM;

    // ------------------------------------------------------------------
    // 信号分发
    // ------------------------------------------------------------------
    void drain_signal_pipe(bool& got_chld, bool& got_hup, bool& got_term) {
        for (;;) {
            unsigned char buf[256];
            ssize_t n = ::read(g_pipe_rd, buf, sizeof(buf));
            if (n > 0) {
                for (ssize_t i = 0; i < n; ++i) {
                    switch (buf[i]) {
                        case SIGCHLD: got_chld = true; break;
                        case SIGHUP:  got_hup = true; break;
                        case SIGTERM:
                        case SIGINT:
                            got_term = true;
                            ++term_signals_;
                            last_term_signum_ = buf[i];
                            break;
                        default: break;
                    }
                }
                continue;
            }
            if (n == -1 && errno == EINTR) continue;
            break;  // EAGAIN：已排空
        }
    }

    void main_loop() {
        for (;;) {
            struct pollfd pfd{g_pipe_rd, POLLIN, 0};
            int pr = ::poll(&pfd, 1, next_timeout_ms());
            (void)pr;  // EINTR/错误无妨：信号字节仍在管道中，排空即可推进

            bool got_chld = false, got_hup = false, got_term = false;
            drain_signal_pipe(got_chld, got_hup, got_term);

            if (got_term) {
                if (!shutting_down_) begin_shutdown(last_term_signum_);
                if (term_signals_ >= 2) force_kill_all("second termination signal");
            }
            if (got_hup && !shutting_down_) reload_config();
            drain_outputs();  // 先排空输出，使 service_log 先于 exited
            if (got_chld) reap_all();

            drain_outputs();
            tick(now_ms());

            if (shutting_down_ && live_count() == 0) break;
        }
    }

    int next_timeout_ms() {
        int64_t now = now_ms();
        int64_t deadline = now + 1000;  // 兜底唤醒
        for (const auto& [name, s] : services_) {
            if (s->pid != -1 && s->state == SvcState::STOPPING && !s->force_sent)
                deadline = std::min(deadline, s->stop_signal_at + s->cfg.stop_timeout_ms);
            if (!shutting_down_ && s->pid == -1 && s->want &&
                s->state == SvcState::DEAD && !s->permanent_failure)
                deadline = std::min(deadline, s->next_start_at);
            if (s->pid != -1 && s->state == SvcState::STARTING &&
                cfg_.stable_uptime_ms > 0)
                deadline = std::min(deadline, s->started_at + cfg_.stable_uptime_ms);
        }
        int64_t t = std::max<int64_t>(deadline - now, 1);
        return static_cast<int>(std::min<int64_t>(t, 60000));
    }

    int live_count() const {
        int n = 0;
        for (const auto& [name, s] : services_)
            if (s->pid != -1) ++n;
        return n;
    }

    // ------------------------------------------------------------------
    // 子进程 stdout/stderr 回收为 service_log 事件
    // ------------------------------------------------------------------
    void drain_outputs() {
        for (auto& [name, s] : services_) {
            if (s->out_fd == -1) continue;
            for (;;) {
                char buf[4096];
                ssize_t n = ::read(s->out_fd, buf, sizeof(buf));
                if (n > 0) {
                    s->out_buf.append(buf, static_cast<size_t>(n));
                    flush_log_lines(*s, false);
                    continue;
                }
                if (n == -1 && errno == EINTR) continue;
                if (n == 0) {
                    flush_log_lines(*s, true);
                    ::close(s->out_fd);
                    s->out_fd = -1;
                }
                break;  // EAGAIN
            }
        }
    }

    void flush_log_lines(Service& s, bool eof) {
        size_t pos;
        while ((pos = s.out_buf.find('\n')) != std::string::npos) {
            std::string line = s.out_buf.substr(0, pos);
            s.out_buf.erase(0, pos + 1);
            if (!line.empty() && line.back() == '\r') line.pop_back();
            if (!line.empty()) {
                EventBuilder b("service_log");
                b.put("service", s.cfg.name);
                b.put("line", line);
                emit_event(std::move(b));
            }
        }
        if (eof && !s.out_buf.empty()) {
            EventBuilder b("service_log");
            b.put("service", s.cfg.name);
            b.put("line", s.out_buf);
            emit_event(std::move(b));
            s.out_buf.clear();
        }
    }

    // ------------------------------------------------------------------
    // 启动
    // ------------------------------------------------------------------
    void launch(Service& s) {
        // 防御：替换/异常路径上若残留旧管道，先收尾避免 fd 泄漏
        if (s.exec_fd != -1) { ::close(s.exec_fd); s.exec_fd = -1; }
        if (s.out_fd != -1) { ::close(s.out_fd); s.out_fd = -1; }
        s.out_buf.clear();

        int err_pipe[2] = {-1, -1};
        int out_pipe[2] = {-1, -1};
        if (pipe2(err_pipe, O_NONBLOCK | O_CLOEXEC) != 0 ||
            pipe2(out_pipe, O_NONBLOCK | O_CLOEXEC) != 0) {
            int e = errno;
            if (err_pipe[0] != -1) { ::close(err_pipe[0]); ::close(err_pipe[1]); }
            if (out_pipe[0] != -1) { ::close(out_pipe[0]); ::close(out_pipe[1]); }
            errno = e;
            EventBuilder b("spawn_failed");
            b.put("service", s.cfg.name);
            b.put("error", std::string("pipe2: ") + std::strerror(e));
            emit_event(std::move(b));
            note_start_impossible(s);
            return;
        }

        pid_t pid = fork();
        if (pid < 0) {
            int e = errno;
            ::close(err_pipe[0]); ::close(err_pipe[1]);
            ::close(out_pipe[0]); ::close(out_pipe[1]);
            EventBuilder b("spawn_failed");
            b.put("service", s.cfg.name);
            b.put("error", std::string("fork: ") + std::strerror(e));
            emit_event(std::move(b));
            note_start_impossible(s);
            return;
        }

        if (pid == 0) {
            // ---- 子进程：恢复默认信号环境，避免继承 supervisor 处置 ----
            ::close(err_pipe[0]);
            ::close(out_pipe[0]);
            struct sigaction dfl;
            std::memset(&dfl, 0, sizeof(dfl));
            dfl.sa_handler = SIG_DFL;
            sigemptyset(&dfl.sa_mask);
            sigaction(SIGCHLD, &dfl, nullptr);
            sigaction(SIGHUP, &dfl, nullptr);
            sigaction(SIGTERM, &dfl, nullptr);
            sigaction(SIGINT, &dfl, nullptr);
            sigaction(SIGPIPE, &dfl, nullptr);
            sigset_t all;
            sigemptyset(&all);
            sigprocmask(SIG_SETMASK, &all, nullptr);
            ::close(g_pipe_rd);
            ::close(g_pipe_wr);

            // stdout/stderr 合流入管道；stdin 接 /dev/null
            int devnull = ::open("/dev/null", O_RDONLY);
            if (devnull >= 0) { dup2(devnull, STDIN_FILENO); ::close(devnull); }
            dup2(out_pipe[1], STDOUT_FILENO);
            dup2(out_pipe[1], STDERR_FILENO);
            if (out_pipe[1] > STDERR_FILENO) ::close(out_pipe[1]);

            // 自成会话：脱离终端进程组，只接受 supervisor 显式发出的信号；
            // pid 即进程组 id，强杀可覆盖整组子孙。
            setsid();

            if (!s.cfg.cwd.empty() && chdir(s.cfg.cwd.c_str()) != 0) {
                int e = errno;  // 约定 126 = spawn 阶段失败
                ssize_t w;
                do { w = ::write(err_pipe[1], &e, sizeof(e)); }
                while (w == -1 && errno == EINTR);
                _exit(126);
            }
            for (const auto& [k, v] : s.cfg.env) setenv(k.c_str(), v.c_str(), 1);

            std::vector<char*> argv;
            argv.reserve(s.cfg.argv.size() + 1);
            for (auto& a : s.cfg.argv) argv.push_back(a.data());
            argv.push_back(nullptr);
            execvp(argv[0], argv.data());
            int e = errno;  // execvp 仅失败时返回，约定 127 = exec 失败
            ssize_t w;
            do { w = ::write(err_pipe[1], &e, sizeof(e)); }
            while (w == -1 && errno == EINTR);
            _exit(127);
        }

        // ---- 父进程 ----
        ::close(err_pipe[1]);
        ::close(out_pipe[1]);
        s.pid = pid;
        s.exec_fd = err_pipe[0];
        s.out_fd = out_pipe[0];
        s.out_buf.clear();
        s.state = SvcState::STARTING;
        s.started_at = now_ms();
        s.force_sent = false;
        s.stop_signal_at = 0;
        ++s.attempt;
        pid_to_name_[pid] = s.cfg.name;

        EventBuilder b("starting");
        b.put("service", s.cfg.name);
        b.put("pid", static_cast<int64_t>(pid));
        b.put("attempt", static_cast<int64_t>(s.attempt));
        b.put("command", s.cfg.argv[0]);
        emit_event(std::move(b));
    }

    void note_start_impossible(Service& s) {
        s.pid = -1;
        s.state = SvcState::DEAD;
        ++s.attempt;
        handle_wanted_death(s, now_ms(), /*short_lifespan=*/true);
    }

    // ------------------------------------------------------------------
    // 回收：waitpid 循环取尽，防止僵尸与漏收
    // ------------------------------------------------------------------
    void reap_all() {
        for (;;) {
            int status;
            pid_t pid = waitpid(-1, &status, WNOHANG);
            if (pid == 0) return;       // 暂无更多僵尸
            if (pid == -1) {
                if (errno == EINTR) continue;
                return;                 // ECHILD：无子进程
            }
            reap_one(pid, status);
        }
    }

    void reap_one(pid_t pid, int status) {
        auto it = pid_to_name_.find(pid);
        Service* s = nullptr;
        if (it != pid_to_name_.end()) {
            auto sit = services_.find(it->second);
            if (sit != services_.end()) s = sit->second.get();
            pid_to_name_.erase(it);
        }

        int exec_err = 0;
        if (s && s->exec_fd != -1) {
            int value = 0;
            ssize_t n = ::read(s->exec_fd, &value, sizeof(value));
            if (n == static_cast<ssize_t>(sizeof(value))) exec_err = value;
            ::close(s->exec_fd);
            s->exec_fd = -1;
        }

        int64_t ts = now_ms();
        if (!s) {
            EventBuilder b("unknown_child_exited");
            b.put("pid", static_cast<int64_t>(pid));
            emit_event(std::move(b));
            return;
        }

        s->pid = -1;
        s->last_signaled = WIFSIGNALED(status);
        s->last_exec_errno = exec_err;
        s->last_signal = s->last_signaled ? WTERMSIG(status) : 0;
        if (WIFEXITED(status)) s->last_code = WEXITSTATUS(status);

        bool was_stopping = (s->state == SvcState::STOPPING);
        s->state = SvcState::DEAD;

        {
            const char* evt = "exited";
            if (exec_err) evt = (WIFEXITED(status) && WEXITSTATUS(status) == 126)
                                   ? "spawn_failed" : "exec_failed";
            EventBuilder b(evt);
            b.put("service", s->cfg.name);
            b.put("pid", static_cast<int64_t>(pid));
            if (exec_err) {
                b.put("error", std::strerror(exec_err));
                b.put("errno", static_cast<int64_t>(exec_err));
            } else if (s->last_signaled) {
                b.put("signal", static_cast<int64_t>(s->last_signal));
                b.put("signal_name", signal_name(s->last_signal));
                b.put_bool("core_dumped", WCOREDUMP(status) != 0);
            } else {
                b.put("code", static_cast<int64_t>(s->last_code));
            }
            b.put("attempt", static_cast<int64_t>(s->attempt));
            b.put("uptime_ms", ts - s->started_at);
            emit_event(std::move(b));
        }

        if (s->replacing) {
            EventBuilder rb("replaced");
            rb.put("service", s->cfg.name);
            rb.put("old_pid", static_cast<int64_t>(pid));
            emit_event(std::move(rb));
            apply_pending_and_launch(*s);
            return;
        }
        if (!s->want || shutting_down_) {
            EventBuilder sb("stopped");
            sb.put("service", s->cfg.name);
            sb.put("pid", static_cast<int64_t>(pid));
            sb.put("reason", shutting_down_ ? "shutdown" : "removed");
            sb.put_bool("graceful", !s->force_sent);
            emit_event(std::move(sb));
            return;
        }
        if (was_stopping) {  // 防御：STOPPING 仅用于关停/移除/替换
            EventBuilder sb("stopped");
            sb.put("service", s->cfg.name);
            sb.put("pid", static_cast<int64_t>(pid));
            sb.put("reason", "unknown");
            emit_event(std::move(sb));
            return;
        }

        handle_wanted_death(*s, ts, false);
    }

    // 期望运行的服务死亡：稳定运行清零 / 退避重启 / 重试耗尽
    void handle_wanted_death(Service& s, int64_t ts, bool short_lifespan) {
        int64_t lifespan = ts - s.started_at;
        bool stable_reset = cfg_.stable_uptime_ms == 0 ||
                            (!short_lifespan && lifespan >= cfg_.stable_uptime_ms);
        if (stable_reset) {
            EventBuilder b("failure_count_reset");
            b.put("service", s.cfg.name);
            b.put("uptime_ms", lifespan);
            emit_event(std::move(b));
            s.attempt = 0;
            s.permanent_failure = false;
            s.next_start_at = ts;
            return;
        }

        // attempt = 含本次在内的启动次数；attempt > max_restarts 说明重启次数已用尽
        if (s.attempt > s.cfg.max_restarts) {
            s.permanent_failure = true;
            EventBuilder b("permanent_failure");
            b.put("service", s.cfg.name);
            b.put("attempt", static_cast<int64_t>(s.attempt));
            b.put("max_restarts", static_cast<int64_t>(s.cfg.max_restarts));
            if (s.last_exec_errno) b.put("reason", "exec_failed");
            else if (s.last_signaled) b.put("reason", "crashed");
            else b.put("reason", "exited_abnormally");
            emit_event(std::move(b));
            return;
        }

        int64_t delay = backoff_delay(s.attempt);
        s.next_start_at = ts + delay;
        EventBuilder b("restart_scheduled");
        b.put("service", s.cfg.name);
        b.put("next_attempt", static_cast<int64_t>(s.attempt + 1));
        b.put("delay_ms", delay);
        emit_event(std::move(b));
    }

    int64_t backoff_delay(int attempt) const {
        // 指数退避：start, 2*start, 4*start ... 封顶 cap
        int64_t delay = cfg_.backoff_start_ms;
        for (int i = 1; i < attempt; ++i) {
            if (delay > cfg_.backoff_cap_ms / 2) { delay = cfg_.backoff_cap_ms; break; }
            delay *= 2;
        }
        return std::min<int64_t>(delay, cfg_.backoff_cap_ms);
    }

    // ------------------------------------------------------------------
    // 周期推进
    // ------------------------------------------------------------------
    void tick(int64_t now) {
        std::vector<std::string> erase_names;

        for (auto& [name, s] : services_) {
            if (!shutting_down_ && s->pid == -1 && s->want &&
                s->state == SvcState::DEAD && !s->permanent_failure &&
                now >= s->next_start_at) {
                launch(*s);  // 只改本对象与 pid 表，不动 map 结构
                continue;
            }

            if (s->pid != -1 && s->state == SvcState::STARTING &&
                cfg_.stable_uptime_ms > 0 &&
                now - s->started_at >= cfg_.stable_uptime_ms) {
                s->state = SvcState::HEALTHY;
                EventBuilder b("healthy");
                b.put("service", name);
                b.put("pid", static_cast<int64_t>(s->pid));
                b.put("attempt", static_cast<int64_t>(s->attempt));
                emit_event(std::move(b));
            }

            if (s->pid != -1 && s->state == SvcState::STOPPING &&
                !s->force_sent &&
                now - s->stop_signal_at >= s->cfg.stop_timeout_ms) {
                force_kill(*s, "grace period expired");
            }

            if (!s->want && !s->replacing && s->pid == -1 && s->out_fd == -1)
                erase_names.push_back(name);
        }

        for (const auto& n : erase_names) services_.erase(n);
    }

    int kill_service(Service& s, int sig) {
        if (s.pid == -1) { errno = ESRCH; return -1; }
        // 子进程已 setsid：负 pid 表示整组；组不存在时退回单进程
        int rc = kill(-s.pid, sig);
        if (rc == -1 && errno == ESRCH) rc = kill(s.pid, sig);
        return rc;
    }

    void force_kill(Service& s, const std::string& reason) {
        s.force_sent = true;
        int rc = kill_service(s, SIGKILL);
        EventBuilder b("force_kill");
        b.put("service", s.cfg.name);
        b.put("pid", static_cast<int64_t>(s.pid));
        b.put("reason", reason);
        b.put("grace_ms", static_cast<int64_t>(s.cfg.stop_timeout_ms));
        if (rc != 0) b.put("error", std::strerror(errno));
        emit_event(std::move(b));
    }

    // ------------------------------------------------------------------
    // 有序关停
    // ------------------------------------------------------------------
    void begin_shutdown(int signum) {
        shutting_down_ = true;
        EventBuilder b("shutdown_begin");
        b.put("signal", signal_name(signum));
        b.put("live", static_cast<int64_t>(live_count()));
        emit_event(std::move(b));

        // 阶段一：先向所有存活服务并发发出各自的优雅退出信号
        for (auto& [name, s] : services_) {
            if (s->pid == -1 || !s->want) continue;
            int sig = signal_from_name(s->cfg.stop_signal);
            if (sig < 0) sig = SIGTERM;
            s->state = SvcState::STOPPING;
            s->stop_signal_at = now_ms();
            s->force_sent = false;
            int rc = kill_service(*s, sig);

            EventBuilder e("stopping");
            e.put("service", name);
            e.put("pid", static_cast<int64_t>(s->pid));
            e.put("signal", signal_name(sig));
            e.put("grace_ms", static_cast<int64_t>(s->cfg.stop_timeout_ms));
            if (rc != 0) e.put("error", std::strerror(errno));
            emit_event(std::move(e));
        }
        // 阶段二：tick() 中宽限期到仍存活的服务被 SIGKILL
    }

    void force_kill_all(const std::string& reason) {
        EventBuilder b("shutdown_escalated");
        b.put("reason", reason);
        emit_event(std::move(b));
        for (auto& [name, s] : services_) {
            if (s->pid != -1 && s->want) {
                s->state = SvcState::STOPPING;
                if (!s->force_sent) force_kill(*s, reason);
            }
        }
    }

    // ------------------------------------------------------------------
    // SIGHUP 热加载：解析失败则旧服务完全不受影响
    // ------------------------------------------------------------------
    void reload_config() {
        SupervisorConfig parsed;
        std::string err;
        emit_event(EventBuilder("reloading"));
        if (!parse_supervisor_config(g_config_path, parsed, err)) {
            EventBuilder b("reload_rejected");
            b.put("error", err);
            emit_event(std::move(b));
            return;  // 旧配置、旧进程均保持原样
        }

        cfg_.backoff_start_ms = parsed.backoff_start_ms;
        cfg_.backoff_cap_ms = parsed.backoff_cap_ms;
        cfg_.stable_uptime_ms = parsed.stable_uptime_ms;

        int added = 0, removed = 0, changed = 0, unchanged = 0;
        std::vector<std::string> dead_removed;

        // 删除的服务：按其旧退出策略停止
        for (auto& [name, s] : services_) {
            if (parsed.services.count(name)) continue;
            ++removed;
            EventBuilder e("service_removed");
            e.put("service", name);
            emit_event(std::move(e));
            s->want = false;
            s->replacing = false;
            if (s->pid != -1) {
                begin_service_stop(*s, "removed");
            } else {
                if (s->out_fd != -1) { ::close(s->out_fd); s->out_fd = -1; }
                dead_removed.push_back(name);
            }
        }
        for (const auto& n : dead_removed) services_.erase(n);

        // 新增 / 修改 / 不变
        for (const auto& [name, nc] : parsed.services) {
            auto it = services_.find(name);
            if (it == services_.end()) {
                ++added;
                EventBuilder e("service_added");
                e.put("service", name);
                emit_event(std::move(e));
                auto svc = std::make_unique<Service>(nc);
                Service* raw = svc.get();
                services_.emplace(name, std::move(svc));
                launch(*raw);
                continue;
            }
            Service& s = *it->second;
            if (s.replacing) {
                // 上一轮替换尚未完成：以最新配置为准
                s.pending_cfg = nc;
                ++changed;
                continue;
            }
            if (s.cfg.runtime_signature() == nc.runtime_signature()) {
                ++unchanged;
                continue;
            }
            ++changed;
            EventBuilder e("service_replacing");
            e.put("service", name);
            e.put("old_pid", static_cast<int64_t>(s.pid));
            emit_event(std::move(e));
            if (s.pid != -1) {
                // 有序替换：旧进程按旧策略退出后，再启动新进程
                s.replacing = true;
                s.pending_cfg = nc;
                begin_service_stop(s, "replaced");
            } else {
                s.cfg = nc;
                s.attempt = 0;
                s.permanent_failure = false;
                s.next_start_at = now_ms();
                launch(s);
            }
        }

        EventBuilder done("reloaded");
        done.put("added", static_cast<int64_t>(added));
        done.put("removed", static_cast<int64_t>(removed));
        done.put("changed", static_cast<int64_t>(changed));
        done.put("unchanged", static_cast<int64_t>(unchanged));
        emit_event(std::move(done));
    }

    void begin_service_stop(Service& s, const char* reason) {
        int sig = signal_from_name(s.cfg.stop_signal);
        if (sig < 0) sig = SIGTERM;
        s.state = SvcState::STOPPING;
        s.stop_signal_at = now_ms();
        s.force_sent = false;
        int rc = kill_service(s, sig);
        EventBuilder b("stopping");
        b.put("service", s.cfg.name);
        b.put("pid", static_cast<int64_t>(s.pid));
        b.put("signal", signal_name(sig));
        b.put("grace_ms", static_cast<int64_t>(s.cfg.stop_timeout_ms));
        b.put("reason", reason);
        if (rc != 0) b.put("error", std::strerror(errno));
        emit_event(std::move(b));
    }

    void apply_pending_and_launch(Service& s) {
        s.cfg = std::move(s.pending_cfg);
        s.pending_cfg = ServiceConfig{};
        s.replacing = false;
        s.want = true;
        s.state = SvcState::DEAD;
        s.attempt = 0;
        s.permanent_failure = false;
        s.next_start_at = now_ms();
        launch(s);
    }
};

}  // namespace

int run_supervisor(const std::string& config_path) {
    SupervisorConfig cfg;
    std::string err;
    if (!parse_supervisor_config(config_path, cfg, err)) {
        std::fprintf(stderr,
                     "{\"ts\":%lld,\"event\":\"fatal\",\"stage\":\"config\",\"error\":\"%s\"}\n",
                     static_cast<long long>(now_ms()),
                     EventBuilder::escape(err).c_str());
        return 2;
    }
    g_config_path = config_path;
    Supervisor sup(std::move(cfg));
    return sup.run();
}

}  // namespace ipc
