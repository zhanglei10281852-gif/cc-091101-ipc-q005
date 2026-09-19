/**
 * Supervisor 演示（脚本式教学场景）
 *
 * 生成一份临时配置后在子进程中运行监管器，按时间轴演练：
 *   1. flaky 连续启动失败，按退避节奏重启，最终耗尽重试进入 failed；
 *   2. SIGHUP 热加载：修改 web（有序替换）、摘除 flaky、新增 extra；
 *   3. SIGTERM 整体关停：web/extra 优雅退出，stuck 忽略优雅信号，
 *      宽限期后被 SIGKILL 强制接管。
 * 全程输出 JSON Lines 事件，可直接对照复盘。
 */

#include "ipc_demo.h"
#include "supervisor.h"

#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <signal.h>
#include <string>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

namespace ipc {
namespace {

std::string exe_dir() {
    char buf[4096];
    ssize_t n = ::readlink("/proc/self/exe", buf, sizeof(buf) - 1);
    if (n <= 0) return ".";
    buf[n] = '\0';
    std::string p(buf);
    size_t slash = p.rfind('/');
    return slash == std::string::npos ? "." : p.substr(0, slash);
}

bool write_file(const std::string& path, const std::string& content) {
    FILE* fp = std::fopen(path.c_str(), "w");
    if (!fp) return false;
    std::fwrite(content.data(), 1, content.size(), fp);
    std::fclose(fp);
    return true;
}

std::string common_section(const std::string& worker) {
    return "stop_grace_ms = 400\n"
           "backoff_ms    = 60,120,240\n"
           "stable_ms     = 400\n"
           "start_retries = 3\n"
           "command       = " + worker + " ";
}

} // namespace

void demo_supervisor() {
    Logger::demo("8. Supervisor 进程监管演示 (生命周期接管)");

    const std::string worker = exe_dir() + "/ipc_worker";
    if (::access(worker.c_str(), X_OK) != 0) {
        Logger::error("SUPERVISOR", "找不到 ipc_worker（应与本程序同目录），跳过演示");
        return;
    }

    char tmpl[] = "/tmp/ipc_sup_demo_XXXXXX";
    if (!mkdtemp(tmpl)) {
        Logger::error("SUPERVISOR", "创建临时目录失败");
        return;
    }
    std::string dir(tmpl);
    std::string cfg1 = dir + "/sup.conf";

    const std::string base = common_section(worker);
    std::string v1 =
        "[service:web]\n" + base + "--name web cycle 300\n\n"
        "[service:flaky]\n" + base + "--name flaky exit 1\n\n"
        "[service:stuck]\n" + base + "--name stuck --ignore-term sleep 60000\n";
    write_file(cfg1, v1);

    Logger::info("SUPERVISOR", "配置文件: " + cfg1);
    Logger::info("SUPERVISOR", "场景: web=稳定服务, flaky=启动即失败, stuck=收到优雅信号后卡死");
    Logger::info("SUPERVISOR", "以下为监管器输出的结构化事件 (JSON Lines):\n");

    pid_t pid = fork();
    if (pid < 0) {
        Logger::error("SUPERVISOR", "fork 失败");
        return;
    }

    if (pid == 0) {
        char* argv[] = {
            const_cast<char*>("ipc_demo"),
            const_cast<char*>("--supervisor"),
            const_cast<char*>("--config"),
            const_cast<char*>(cfg1.c_str()),
            nullptr
        };
        _exit(sv::run_supervisor(4, argv));
    }

    // t≈1.2s：flaky 已耗尽 3 次重试；热加载——替换 web、摘除 flaky、新增 extra
    usleep(1200000);
    Logger::info("SUPERVISOR", ">>> 发送 SIGHUP：修改 web / 删除 flaky / 新增 extra");
    std::string v2 =
        "[service:web]\n" + base + "--name web cycle 120\n\n"
        "[service:stuck]\n" + base + "--name stuck --ignore-term sleep 60000\n\n"
        "[service:extra]\n" + base + "--name extra sleep 60000\n";
    write_file(cfg1, v2);
    kill(pid, SIGHUP);

    // t≈2.4s：整体关停
    usleep(1200000);
    Logger::info("SUPERVISOR", ">>> 发送 SIGTERM：整体关停（stuck 将在 400ms 宽限期后被 SIGKILL）");
    kill(pid, SIGTERM);

    int status = 0;
    waitpid(pid, &status, 0);
    int code = WIFEXITED(status) ? WEXITSTATUS(status) : -1;

    Logger::info("SUPERVISOR", "");
    Logger::info("SUPERVISOR", "复盘要点:");
    Logger::info("SUPERVISOR", "  - flaky 每次意外退出都产生 service_backoff，延迟 60→120 逐次退避");
    Logger::info("SUPERVISOR", "  - 3 次启动耗尽后 service_failed；SIGHUP 删除该服务即视为人工确认");
    Logger::info("SUPERVISOR", "  - web 配置变化: service_stopping(replaced) → service_exited → service_replaced");
    Logger::info("SUPERVISOR", "  - stuck 忽略 SIGTERM，宽限期到后 service_force_kill (SIGKILL) 接管");
    Logger::info("SUPERVISOR", "  - 监管器退出码 = " + std::to_string(code) +
                 "（0 表示最终无失败服务；有服务耗尽重试时为 1）");
    Logger::info("SUPERVISOR", "Supervisor 演示完成\n");
}

} // namespace ipc
