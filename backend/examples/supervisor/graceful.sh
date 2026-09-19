#!/bin/sh
# 优雅退出示例：收到 stop_signal（TERM）后完成收尾再退出。
trap 'echo "[graceful] 收到 TERM，正在保存状态并退出..."; sleep 0.2; exit 0' TERM

echo "[graceful] 启动，pid=$$"
while :; do
    sleep 1
done
