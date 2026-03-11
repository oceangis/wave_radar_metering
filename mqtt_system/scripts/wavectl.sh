#!/bin/bash
# =====================================================
# 波浪监测系统 - 服务管理工具
# 用法: wavectl.sh {start|stop|restart|status|enable|disable}
# =====================================================

# 核心服务列表（按启动顺序）
CORE_SERVICES=(
    "wave-collector"
    "wave-storage"
    "wave-analyzer"
    "wave-tide-analyzer"
    "wave-web"
)

# 可选服务
OPTIONAL_SERVICES=(
    "wave-thingsboard"
    "wave-ec800-thingsboard"
)

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

print_status() {
    local service=$1
    local status=$(systemctl is-active "$service" 2>/dev/null)
    case $status in
        active)
            echo -e "  ${GREEN}●${NC} $service: ${GREEN}running${NC}"
            ;;
        inactive)
            echo -e "  ${RED}○${NC} $service: ${RED}stopped${NC}"
            ;;
        *)
            echo -e "  ${YELLOW}?${NC} $service: $status"
            ;;
    esac
}

do_start() {
    echo "启动波浪监测系统..."
    echo ""

    # 确保依赖服务运行
    echo "检查依赖服务..."
    sudo systemctl start mosquitto 2>/dev/null
    sudo systemctl start postgresql 2>/dev/null
    sleep 1

    # 按顺序启动核心服务
    for service in "${CORE_SERVICES[@]}"; do
        echo "  启动 $service..."
        sudo systemctl start "$service"
        sleep 1
    done

    echo ""
    echo "核心服务已启动"
    do_status
}

do_stop() {
    echo "停止波浪监测系统..."
    echo ""

    # 逆序停止服务
    for ((i=${#CORE_SERVICES[@]}-1; i>=0; i--)); do
        service="${CORE_SERVICES[$i]}"
        echo "  停止 $service..."
        sudo systemctl stop "$service"
    done

    # 停止可选服务
    for service in "${OPTIONAL_SERVICES[@]}"; do
        sudo systemctl stop "$service" 2>/dev/null
    done

    echo ""
    echo "所有服务已停止"
}

do_restart() {
    echo "重启波浪监测系统..."
    do_stop
    sleep 2
    do_start
}

do_status() {
    echo ""
    echo "========== 核心服务状态 =========="
    for service in "${CORE_SERVICES[@]}"; do
        print_status "$service"
    done

    echo ""
    echo "========== 可选服务状态 =========="
    for service in "${OPTIONAL_SERVICES[@]}"; do
        print_status "$service"
    done

    echo ""
    echo "========== 依赖服务状态 =========="
    print_status "mosquitto"
    print_status "postgresql"
    echo ""
}

do_enable() {
    echo "设置开机自启..."
    for service in "${CORE_SERVICES[@]}"; do
        sudo systemctl enable "$service"
    done
    echo "核心服务已设置开机自启"
}

do_disable() {
    echo "取消开机自启..."
    for service in "${CORE_SERVICES[@]}"; do
        sudo systemctl disable "$service"
    done
    echo "核心服务已取消开机自启"
}

do_logs() {
    local service=${2:-wave-collector}
    echo "查看 $service 日志 (Ctrl+C 退出)..."
    sudo journalctl -u "$service" -f
}

# 主逻辑
case "$1" in
    start)
        do_start
        ;;
    stop)
        do_stop
        ;;
    restart)
        do_restart
        ;;
    status)
        do_status
        ;;
    enable)
        do_enable
        ;;
    disable)
        do_disable
        ;;
    logs)
        do_logs "$@"
        ;;
    *)
        echo "波浪监测系统 - 服务管理工具"
        echo ""
        echo "用法: $0 {start|stop|restart|status|enable|disable|logs [service]}"
        echo ""
        echo "命令说明:"
        echo "  start    - 启动所有核心服务"
        echo "  stop     - 停止所有服务"
        echo "  restart  - 重启所有服务"
        echo "  status   - 查看服务状态"
        echo "  enable   - 设置开机自启"
        echo "  disable  - 取消开机自启"
        echo "  logs     - 查看服务日志 (默认: wave-collector)"
        echo ""
        echo "示例:"
        echo "  $0 start"
        echo "  $0 logs wave-analyzer"
        exit 1
        ;;
esac
