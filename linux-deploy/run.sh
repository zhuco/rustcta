#!/bin/bash

# Rust CTA 交易系统启动脚本
# 使用方法: ./run.sh [start|stop|restart|status]

APP_NAME="rustcta"
APP_PATH="./rustcta-linux"
PID_FILE="/tmp/${APP_NAME}.pid"
LOG_FILE="./logs/${APP_NAME}.log"

# 创建日志目录
mkdir -p logs

start() {
    if [ -f "$PID_FILE" ] && kill -0 $(cat "$PID_FILE") 2>/dev/null; then
        echo "$APP_NAME 已经在运行中 (PID: $(cat $PID_FILE))"
        return 1
    fi
    
    echo "启动 $APP_NAME..."
    chmod +x "$APP_PATH"
    nohup "$APP_PATH" > "$LOG_FILE" 2>&1 &
    echo $! > "$PID_FILE"
    echo "$APP_NAME 已启动 (PID: $!)"
}

stop() {
    if [ ! -f "$PID_FILE" ]; then
        echo "$APP_NAME 未运行"
        return 1
    fi
    
    PID=$(cat "$PID_FILE")
    if kill -0 "$PID" 2>/dev/null; then
        echo "停止 $APP_NAME (PID: $PID)..."
        kill "$PID"
        rm -f "$PID_FILE"
        echo "$APP_NAME 已停止"
    else
        echo "$APP_NAME 进程不存在，清理PID文件"
        rm -f "$PID_FILE"
    fi
}

status() {
    if [ -f "$PID_FILE" ] && kill -0 $(cat "$PID_FILE") 2>/dev/null; then
        echo "$APP_NAME 正在运行 (PID: $(cat $PID_FILE))"
    else
        echo "$APP_NAME 未运行"
    fi
}

restart() {
    stop
    sleep 2
    start
}

case "$1" in
    start)
        start
        ;;
    stop)
        stop
        ;;
    restart)
        restart
        ;;
    status)
        status
        ;;
    *)
        echo "使用方法: $0 {start|stop|restart|status}"
        echo "  start   - 启动应用"
        echo "  stop    - 停止应用"
        echo "  restart - 重启应用"
        echo "  status  - 查看状态"
        exit 1
        ;;
esac

exit 0