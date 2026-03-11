#!/bin/bash
# =====================================================
# 停止所有波浪监测服务
# =====================================================

echo "停止波浪监测系统服务..."
echo ""

# 停止所有服务
echo "停止 Web 服务..."
sudo systemctl stop wave-web

echo "停止波浪分析服务..."
sudo systemctl stop wave-analyzer

echo "停止数据存储服务..."
sudo systemctl stop wave-storage

echo "停止数据采集服务..."
sudo systemctl stop wave-collector

echo ""
echo "所有服务已停止"
echo ""

# 检查状态
sudo systemctl status wave-collector --no-pager -l | grep "Active:"
sudo systemctl status wave-storage --no-pager -l | grep "Active:"
sudo systemctl status wave-analyzer --no-pager -l | grep "Active:"
sudo systemctl status wave-web --no-pager -l | grep "Active:"
