#!/bin/bash
# =====================================================
# 波浪监测系统 - 卸载脚本
# =====================================================
# 用法: sudo bash uninstall.sh
# =====================================================

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

if [ "$EUID" -ne 0 ]; then
    echo -e "${RED}请使用 sudo 运行此脚本${NC}"
    exit 1
fi

echo "=================================================="
echo "  波浪监测系统 - 卸载"
echo "=================================================="
echo ""
echo -e "${YELLOW}警告: 此操作将停止并移除所有波浪监测服务${NC}"
echo ""
read -p "确认卸载? (y/N): " confirm
if [ "$confirm" != "y" ] && [ "$confirm" != "Y" ]; then
    echo "已取消"
    exit 0
fi

echo ""

# 停止服务
echo "停止所有服务..."
SERVICES=(
    "wave-ec800-thingsboard"
    "wave-thingsboard"
    "wave-web"
    "wave-tide-analyzer"
    "wave-analyzer"
    "wave-storage"
    "wave-collector"
)

for svc in "${SERVICES[@]}"; do
    systemctl stop "$svc" 2>/dev/null || true
    systemctl disable "$svc" 2>/dev/null || true
done

# 移除 systemd 文件
echo "移除 systemd 服务文件..."
rm -f /etc/systemd/system/wave-collector.service
rm -f /etc/systemd/system/wave-storage.service
rm -f /etc/systemd/system/wave-analyzer.service
rm -f /etc/systemd/system/wave-tide-analyzer.service
rm -f /etc/systemd/system/wave-web.service
rm -f /etc/systemd/system/wave-thingsboard.service
rm -f /etc/systemd/system/wave-ec800-thingsboard.service
rm -f /etc/systemd/system/wave-monitor.target
systemctl daemon-reload

echo -e "${GREEN}服务已移除${NC}"
echo ""

# 可选: 删除数据库
read -p "是否删除数据库 wave_monitoring? (y/N): " del_db
if [ "$del_db" = "y" ] || [ "$del_db" = "Y" ]; then
    sudo -u postgres psql -c "DROP DATABASE IF EXISTS wave_monitoring;" 2>/dev/null || true
    sudo -u postgres psql -c "DROP USER IF EXISTS wave_user;" 2>/dev/null || true
    echo -e "${GREEN}数据库已删除${NC}"
fi

# 可选: 删除 Mosquitto 配置
read -p "是否删除 Mosquitto 配置? (y/N): " del_mqtt
if [ "$del_mqtt" = "y" ] || [ "$del_mqtt" = "Y" ]; then
    rm -f /etc/mosquitto/conf.d/wave_monitoring.conf
    rm -f /etc/mosquitto/passwd
    systemctl restart mosquitto 2>/dev/null || true
    echo -e "${GREEN}Mosquitto 配置已删除${NC}"
fi

# 可选: 删除 udev 规则
if [ -f /etc/udev/rules.d/99-radar-ports.rules ]; then
    read -p "是否删除 udev 串口规则? (y/N): " del_udev
    if [ "$del_udev" = "y" ] || [ "$del_udev" = "Y" ]; then
        rm -f /etc/udev/rules.d/99-radar-ports.rules
        udevadm control --reload-rules 2>/dev/null || true
        echo -e "${GREEN}udev 规则已删除${NC}"
    fi
fi

echo ""
echo "=================================================="
echo -e "${GREEN}卸载完成${NC}"
echo "=================================================="
echo ""
echo "注意: 项目文件未删除，如需完全清除请手动执行:"
echo "  rm -rf ~/radar/mqtt_system"
echo ""
