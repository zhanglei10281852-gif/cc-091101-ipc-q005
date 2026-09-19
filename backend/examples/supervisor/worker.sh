#!/bin/sh
# 崩溃示例：运行约 1.5 秒后 SEGV 自杀，用于演示退避重启。
# 热加载时也能优雅停止（trap TERM 优先于 sleep）。
trap 'exit 0' TERM
echo "[worker] 启动，pid=$$"
sleep 1.5
echo "[worker] 发生故障，崩溃!"
kill -SEGV $$
