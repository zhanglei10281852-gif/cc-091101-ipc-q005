#ifndef SUPERVISOR_SIGNAL_UTIL_H
#define SUPERVISOR_SIGNAL_UTIL_H

#include <signal.h>

#include <cctype>
#include <string>
#include <unordered_map>

namespace ipc {
namespace svp {

// 信号名 -> 编号（允许 SIG 前缀，大小写不敏感），未知返回 -1
inline int signal_from_name(const std::string& raw) {
    static const std::unordered_map<std::string, int> names = {
        {"HUP", SIGHUP},       {"INT", SIGINT},       {"QUIT", SIGQUIT},
        {"ILL", SIGILL},       {"TRAP", SIGTRAP},     {"ABRT", SIGABRT},
        {"BUS", SIGBUS},       {"FPE", SIGFPE},       {"KILL", SIGKILL},
        {"USR1", SIGUSR1},     {"SEGV", SIGSEGV},     {"USR2", SIGUSR2},
        {"PIPE", SIGPIPE},     {"ALRM", SIGALRM},     {"TERM", SIGTERM},
        {"CHLD", SIGCHLD},     {"CONT", SIGCONT},     {"STOP", SIGSTOP},
        {"TSTP", SIGTSTP},     {"TTIN", SIGTTIN},     {"TTOU", SIGTTOU},
        {"URG", SIGURG},       {"XCPU", SIGXCPU},     {"XFSZ", SIGXFSZ},
        {"VTALRM", SIGVTALRM}, {"PROF", SIGPROF},     {"WINCH", SIGWINCH},
        {"IO", SIGIO},         {"PWR", SIGPWR},       {"SYS", SIGSYS},
    };
    std::string name = raw;
    for (char& c : name)
        c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    if (name.rfind("SIG", 0) == 0 && name.size() > 3) name = name.substr(3);
    auto it = names.find(name);
    return it == names.end() ? -1 : it->second;
}

// 编号 -> 短名（不带 SIG 前缀）
inline const char* signal_name(int sig) {
    switch (sig) {
        case SIGHUP: return "HUP";   case SIGINT: return "INT";
        case SIGQUIT: return "QUIT"; case SIGILL: return "ILL";
        case SIGTRAP: return "TRAP"; case SIGABRT: return "ABRT";
        case SIGBUS: return "BUS";   case SIGFPE: return "FPE";
        case SIGKILL: return "KILL"; case SIGUSR1: return "USR1";
        case SIGSEGV: return "SEGV"; case SIGUSR2: return "USR2";
        case SIGPIPE: return "PIPE"; case SIGALRM: return "ALRM";
        case SIGTERM: return "TERM"; case SIGCHLD: return "CHLD";
        case SIGCONT: return "CONT"; case SIGSTOP: return "STOP";
        case SIGTSTP: return "TSTP"; case SIGTTIN: return "TTIN";
        case SIGTTOU: return "TTOU"; case SIGURG: return "URG";
        case SIGXCPU: return "XCPU"; case SIGXFSZ: return "XFSZ";
        case SIGVTALRM: return "VTALRM";
        case SIGPROF: return "PROF"; case SIGWINCH: return "WINCH";
        case SIGIO: return "IO";     case SIGPWR: return "PWR";
        case SIGSYS: return "SYS";
        default: return "?";
    }
}

}  // namespace svp
}  // namespace ipc

#endif  // SUPERVISOR_SIGNAL_UTIL_H
