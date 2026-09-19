#ifndef SUPERVISOR_H
#define SUPERVISOR_H

#include <map>
#include <string>
#include <utility>
#include <vector>

namespace ipc {

// 单个受管服务的配置
struct ServiceConfig {
    std::string name;
    std::vector<std::string> argv;        // argv[0] 为可执行文件
    std::string stop_signal = "TERM";     // 优雅退出信号
    int stop_timeout_ms = 5000;           // 终止宽限期（毫秒）
    int max_restarts = 3;                 // 首次启动之外允许的重启次数
    std::string cwd;                      // 可选：工作目录
    std::vector<std::pair<std::string, std::string>> env;  // 可选：附加环境变量

    // 用于热加载比较：只有这些字段变化才需要真正替换进程
    std::string runtime_signature() const;
};

// supervisor 全局配置
struct SupervisorConfig {
    std::map<std::string, ServiceConfig> services;
    int backoff_start_ms = 200;           // 退避起始间隔
    int backoff_cap_ms = 5000;            // 退避间隔上限
    int stable_uptime_ms = 2000;          // 稳定运行阈值，达到后清零失败计数
};

// 解析配置文件。失败时返回 false 并在 err 中给出带行号的原因，
// 调用方据此保证“解析失败则旧服务不受影响”。
bool parse_supervisor_config(const std::string& path,
                             SupervisorConfig& out,
                             std::string& err);

// 以前台 supervisor 模式运行：拉起配置中的全部服务、输出 JSON 事件流，
// 直到收到 SIGTERM/SIGINT 完成有序关停。返回值即进程退出码：
//   0 - 全部服务正常关停
//   2 - 启动时配置文件非法
//   3 - 仍有服务启动失败或已耗尽重试次数
int run_supervisor(const std::string& config_path);

} // namespace ipc

#endif // SUPERVISOR_H
