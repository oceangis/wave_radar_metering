#!/bin/bash
# =============================================================
# 雷达模拟器启动脚本
# 创建3对虚拟串口 (socat)，然后启动 wave_simulator.py
#
# 使用方式:
#   sudo ./start_sim.sh                     # 默认场景 (moderate_sea)
#   sudo ./start_sim.sh --scenario storm    # 指定场景
# =============================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# 检查socat是否安装
if ! command -v socat &>/dev/null; then
    echo "ERROR: socat is not installed. Install with: sudo apt install socat"
    exit 1
fi

# 检查是否以root运行（创建 /dev/ 符号链接需要）
if [ "$(id -u)" -ne 0 ]; then
    echo "ERROR: This script must be run as root (sudo) to create /dev/ symlinks"
    exit 1
fi

# 获取运行此脚本的真实用户（sudo时获取原始用户）
REAL_USER="${SUDO_USER:-$(whoami)}"

echo "=== Wave Simulator Startup ==="
echo "Script dir: $SCRIPT_DIR"
echo ""

# 清理函数
cleanup() {
    echo ""
    echo "=== Cleaning up ==="
    # 停止socat进程
    for pid in "${SOCAT_PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
        fi
    done
    # 移除符号链接
    for link in /dev/radar1 /dev/radar2 /dev/radar3; do
        if [ -L "$link" ]; then
            rm -f "$link"
            echo "Removed $link"
        fi
    done
    # 停止模拟器
    if [ -n "$SIM_PID" ] && kill -0 "$SIM_PID" 2>/dev/null; then
        kill "$SIM_PID" 2>/dev/null || true
    fi
    echo "Cleanup done."
}

trap cleanup EXIT

# 创建临时目录存放pty路径
PTY_DIR=$(mktemp -d)

SOCAT_PIDS=()
SIM_PORTS=()
DEV_LINKS=()

echo "Creating virtual serial port pairs..."

for i in 1 2 3; do
    COLLECTOR_PTY="$PTY_DIR/collector_$i"
    SIMULATOR_PTY="$PTY_DIR/simulator_$i"

    # 创建pty pair，将路径写入文件
    socat -d -d \
        "PTY,raw,echo=0,link=$PTY_DIR/pty_a_$i" \
        "PTY,raw,echo=0,link=$PTY_DIR/pty_b_$i" &
    SOCAT_PIDS+=($!)

    # 等待socat创建pty
    sleep 0.5

    # pty_a = collector端 (符号链接到 /dev/radarN)
    # pty_b = simulator端 (传给 wave_simulator.py)
    PTY_A=$(readlink -f "$PTY_DIR/pty_a_$i")
    PTY_B=$(readlink -f "$PTY_DIR/pty_b_$i")

    # 创建 /dev/radarN 符号链接
    ln -sf "$PTY_A" "/dev/radar$i"
    chmod 666 "$PTY_A" "$PTY_B"

    echo "  Radar $i: /dev/radar$i -> $PTY_A  |  Simulator -> $PTY_B"

    SIM_PORTS+=("$PTY_B")
    DEV_LINKS+=("/dev/radar$i")
done

echo ""
echo "Virtual serial ports created:"
echo "  Collector reads:  /dev/radar1, /dev/radar2, /dev/radar3"
echo "  Simulator writes: ${SIM_PORTS[*]}"
echo ""

# 启动模拟器（以原始用户身份运行）
echo "Starting wave simulator..."
sudo -u "$REAL_USER" python3 "$SCRIPT_DIR/wave_simulator.py" \
    --ports "${SIM_PORTS[@]}" \
    "$@" &
SIM_PID=$!

echo "Simulator PID: $SIM_PID"
echo ""
echo "=== Simulator is running ==="
echo "Press Ctrl+C to stop."
echo ""

# 等待模拟器退出
wait $SIM_PID 2>/dev/null || true
