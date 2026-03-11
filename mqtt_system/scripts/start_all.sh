#!/bin/bash
# =====================================================
# 启动所有波浪监测服务
# =====================================================

echo "启动波浪监测系统服务..."
echo ""

# 启动Mosquitto
echo "启动 Mosquitto MQTT Broker..."
sudo systemctl start mosquitto
sleep 2

# 启动PostgreSQL
echo "启动 PostgreSQL..."
sudo systemctl start postgresql
sleep 2

# 启动数据采集服务
echo "启动数据采集服务..."
sudo systemctl start wave-collector
sleep 2

# 启动数据存储服务
echo "启动数据存储服务..."
sudo systemctl start wave-storage
sleep 2

# 启动波浪分析服务
echo "启动波浪分析服务..."
sudo systemctl start wave-analyzer
sleep 2

# 启动Web服务
echo "启动Web服务..."
sudo systemctl start wave-web
sleep 2

echo ""
echo "=================================================="
echo "所有服务已启动"
echo "=================================================="
echo ""

# 检查服务状态
echo "服务状态:"
echo ""
sudo systemctl status wave-collector --no-pager -l | grep "Active:"
sudo systemctl status wave-storage --no-pager -l | grep "Active:"
sudo systemctl status wave-analyzer --no-pager -l | grep "Active:"
sudo systemctl status wave-web --no-pager -l | grep "Active:"

echo ""
echo "访问Web界面: http://localhost:8080"
echo "查看日志: sudo journalctl -u wave-collector -f"
echo ""
