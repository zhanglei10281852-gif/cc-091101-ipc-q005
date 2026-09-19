#!/bin/sh
# 卡死示例：显式忽略 TERM，宽限期过后被 supervisor 以 SIGKILL 强制接管。
trap '' TERM
echo "[hang] 启动，pid=$$，将忽略 TERM"
while :; do
    sleep 1
done
