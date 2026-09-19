#ifndef SUPERVISOR_H
#define SUPERVISOR_H

namespace sv {

// 以 supervisor 模式运行：解析配置、启动并监管命名子进程。
// argv 形式: --config <path>
// 返回值即进程退出码。
int run_supervisor(int argc, char** argv);

} // namespace sv

#endif // SUPERVISOR_H
