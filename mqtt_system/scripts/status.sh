#!/bin/bash
# =====================================================
# 查看所有服务状态
# =====================================================

echo "=================================================="
echo "波浪监测系统 - 服务状态"
echo "=================================================="
echo ""

# Mosquitto
echo "Mosquitto MQTT Broker:"
sudo systemctl status mosquitto --no-pager -l | grep "Active:"
echo ""

# PostgreSQL
echo "PostgreSQL:"
sudo systemctl status postgresql --no-pager -l | grep "Active:"
echo ""

# 数据采集
echo "数据采集服务:"
sudo systemctl status wave-collector --no-pager -l | grep "Active:"
echo ""

# 数据存储
echo "数据存储服务:"
sudo systemctl status wave-storage --no-pager -l | grep "Active:"
echo ""

# 波浪分析
echo "波浪分析服务:"
sudo systemctl status wave-analyzer --no-pager -l | grep "Active:"
echo ""

# Web服务
echo "Web服务:"
sudo systemctl status wave-web --no-pager -l | grep "Active:"
echo ""

echo "=================================================="
echo "详细状态查看:"
echo "  sudo systemctl status wave-collector"
echo "  sudo systemctl status wave-storage"
echo "  sudo systemctl status wave-analyzer"
echo "  sudo systemctl status wave-web"
echo ""
echo "查看日志:"
echo "  sudo journalctl -u wave-collector -f"
echo "  sudo journalctl -u wave-storage -f"
echo "  sudo journalctl -u wave-analyzer -f"
echo "  sudo journalctl -u wave-web -f"
echo "=================================================="
